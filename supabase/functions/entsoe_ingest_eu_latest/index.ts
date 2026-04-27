import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// Bidding zone EICs for EU + EEA + UK (practical set).
// Note: Some countries are split into multiple bidding zones (DK, SE, NO, IT, etc.).
const DOMAINS: Record<string, string> = {
  AT: "10YAT-APG------L",
  BE: "10YBE----------2",
  BG: "10YCA-BULGARIA-R",
  HR: "10YHR-HEP------M",
  CY: "10YCY-1001A0003J",
  CZ: "10YCZ-CEPS-----N",
  DK1: "10YDK-1--------W",
  DK2: "10YDK-2--------M",
  EE: "10Y1001A1001A39I",
  FI: "10YFI-1--------U",
  FR: "10YFR-RTE------C",
  DE: "10Y1001A1001A83F",
  GR: "10YGR-HTSO-----Y",
  HU: "10YHU-MAVIR----U",
  IE: "10YIE-1001A00010",
  IT: "10YIT-GRTN-----B",
  LV: "10YLV-1001A00074",
  LT: "10YLT-1001A0008Q",
  MT: "10YMT-1001A0003F",
  NL: "10YNL----------L",
  NO1: "10YNO-1--------2",
  NO2: "10YNO-2--------T",
  NO3: "10YNO-3--------J",
  NO4: "10YNO-4--------9",
  NO5: "10Y1001A1001A48H",
  PL: "10YPL-AREA-----S",
  PT: "10YPT-REN------W",
  RO: "10YRO-TEL------P",
  SK: "10YSK-SEPS-----K",
  SI: "10YSI-ELES-----O",
  ES: "10YES-REE------0",
  SE1: "10Y1001A1001A44P",
  SE2: "10Y1001A1001A45N",
  SE3: "10Y1001A1001A46L",
  SE4: "10Y1001A1001A47J",
  CH: "10YCH-SWISSGRIDZ",
  GB: "10YGB----------A",
};

function entsoeFormatYmdHm(date: Date) {
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    String(date.getUTCFullYear()) +
    pad(date.getUTCMonth() + 1) +
    pad(date.getUTCDate()) +
    pad(date.getUTCHours()) +
    pad(date.getUTCMinutes())
  );
}

async function entsoeFetchText(url: string) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25_000);
  const res = await fetch(url, { signal: controller.signal }).finally(() => clearTimeout(timeout));
  const text = await res.text();
  if (!res.ok) throw new Error(`ENTSOE HTTP ${res.status}: ${text.slice(0, 300)}`);
  return text;
}

function pickAll(text: string, regex: RegExp) {
  const out: RegExpExecArray[] = [];
  let m: RegExpExecArray | null;
  while ((m = regex.exec(text))) out.push(m);
  return out;
}

function parseGenerationPerTypeLatest(xml: string) {
  const timeSeriesBlocks = pickAll(xml, /<TimeSeries[\s\S]*?<\/TimeSeries>/g);
  // ENTSO-E can emit multiple TimeSeries with the same psrType (e.g. different
  // businessType / production units). Summing their "latest" quantities can
  // double-count the same physical generation and inflate totals (spikes).
  // We take the max per psrType across blocks (conservative vs double-count).
  const latestQtyByType: Record<string, number> = {};
  let bestTs: string | null = null;

  for (const m of timeSeriesBlocks) {
    const block = m[0];
    const psrType = (block.match(/<psrType>([^<]+)<\/psrType>/) || [])[1];
    if (!psrType) continue;

    const periodStart = (block.match(/<timeInterval>\s*<start>([^<]+)<\/start>/) || [])[1];
    const resolution = (block.match(/<resolution>([^<]+)<\/resolution>/) || [])[1];
    const startMs = periodStart ? Date.parse(periodStart) : NaN;

    const points = pickAll(block, /<Point>[\s\S]*?<\/Point>/g).map((pm) => pm[0]);
    let latestPos = -1;
    let latestQty: number | null = null;
    for (const p of points) {
      const pos = Number((p.match(/<position>([^<]+)<\/position>/) || [])[1]);
      const qty = Number((p.match(/<quantity>([^<]+)<\/quantity>/) || [])[1]);
      if (!Number.isFinite(pos) || !Number.isFinite(qty)) continue;
      if (pos > latestPos) {
        latestPos = pos;
        latestQty = qty;
      }
    }
    if (latestPos < 0 || latestQty == null) continue;

    const prev = latestQtyByType[psrType];
    latestQtyByType[psrType] =
      prev == null ? latestQty : Math.max(prev, latestQty);

    if (Number.isFinite(startMs)) {
      const stepMinutes = resolution === "PT15M" ? 15 : resolution === "PT30M" ? 30 : 60;
      const ts = new Date(startMs + (latestPos - 1) * stepMinutes * 60 * 1000).toISOString();
      if (!bestTs || Date.parse(ts) > Date.parse(bestTs)) bestTs = ts;
    }
  }

  const byType: Record<string, number> = latestQtyByType;

  const renewablePsr = new Set(["B01", "B09", "B11", "B12", "B13", "B15", "B16", "B17", "B18", "B19"]);
  const total = Object.values(byType).reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0);
  const renewable = Object.entries(byType).reduce((a, [k, v]) => a + (renewablePsr.has(k) ? v : 0), 0);
  const renewablePercent = total > 0 ? (renewable / total) * 100 : null;

  return { ts: bestTs, renewablePercent, totalMw: total, renewableMw: renewable, byType };
}

async function fetchZoneLatest(token: string, zone: string, domain: string) {
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), 0, 0));
  const start = new Date(end.getTime() - 3 * 60 * 60 * 1000);

  const params = new URLSearchParams({
    securityToken: token,
    documentType: "A75",
    processType: "A16",
    in_Domain: domain,
    periodStart: entsoeFormatYmdHm(start),
    periodEnd: entsoeFormatYmdHm(end),
  });
  const url = `https://web-api.tp.entsoe.eu/api?${params.toString()}`;
  const xml = await entsoeFetchText(url);
  const parsed = parseGenerationPerTypeLatest(xml);
  return { zone, domain, ...parsed };
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    const entsoeToken = Deno.env.get("ENTSOE_API_TOKEN");
    if (!entsoeToken) return json({ error: "missing_config", message: "Missing ENTSOE_API_TOKEN" }, 500);

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRole) {
      return json({ error: "missing_config", message: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
    }

    const body = await req.json().catch(() => ({}));
    const zones = (Array.isArray(body?.zones) && body.zones.length ? body.zones : Object.keys(DOMAINS)).map(String);

    // Throttle: keep average ~5 req/sec (well under 400 req/min even with bursts).
    const perRequestDelayMs = Number(body?.delay_ms ?? 200);

    const perZone: any[] = [];
    let euRenewableMw = 0;
    let euTotalMw = 0;
    let bestTs: string | null = null;
    const errors: Record<string, string> = {};
    const skipped: Record<string, string> = {};

    // Concurrency pool so this finishes under the 150s idle timeout.
    const concurrency = Math.max(1, Math.min(8, Number(body?.concurrency ?? 6)));
    const queue = [...zones];
    const workers = Array.from({ length: concurrency }, async () => {
      while (queue.length) {
        const z = String(queue.shift());
        const domain = DOMAINS[z];
        if (!domain) continue;
        try {
          const r = await fetchZoneLatest(entsoeToken, z, domain);
          perZone.push({
            zone: z,
            ts: r.ts,
            renewablePercent: r.renewablePercent,
            totalMw: r.totalMw,
            renewableMw: r.renewableMw,
            byType: r.byType,
          });

          // Skip zones with no usable data in the requested window so the aggregate isn't biased toward 0.
          if (!r.ts || !r.totalMw || r.totalMw <= 0) {
            skipped[z] = "no_usable_data";
          } else {
            euRenewableMw += r.renewableMw || 0;
            euTotalMw += r.totalMw || 0;
          }
          if (r.ts && (!bestTs || Date.parse(r.ts) > Date.parse(bestTs))) bestTs = r.ts;
        } catch (e) {
          errors[z] = e?.message ?? String(e);
        }
        if (perRequestDelayMs > 0) await sleep(perRequestDelayMs);
      }
    });

    await Promise.all(workers);

    // Ensure we never store null if we had at least some usable zones.
    const euRenewablePercent = euTotalMw > 0 ? (euRenewableMw / euTotalMw) * 100 : 0;
    const ts = bestTs ?? new Date().toISOString();

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    // Upsert per-zone snapshots (so you can click a country/zone and chart it)
    // Note: timestamps may differ per zone; we store each zone with its own ts.
    const zoneRows = perZone
      .filter((z) => z.zone && z.ts && typeof z.renewablePercent === "number")
      .map((z) => ({
        zone_id: String(z.zone),
        country_code: String(z.zone),
        ts: z.ts,
        renewable_percent: z.renewablePercent,
        carbon_intensity_g_per_kwh: null,
        source: "entsoe",
        raw: { totalMw: z.totalMw, renewableMw: z.renewableMw, byType: z.byType ?? {} },
      }));

    if (zoneRows.length) {
      // Insert in chunks to avoid request size limits
      const chunkSize = 200;
      for (let i = 0; i < zoneRows.length; i += chunkSize) {
        const chunk = zoneRows.slice(i, i + chunkSize);
        const { error: zoneErr } = await supabase
          .from("energy_mix_snapshots")
          .upsert(chunk, { onConflict: "source,zone_id,ts" });
        if (zoneErr) return json({ error: "db_error", message: zoneErr.message }, 500);
      }
    }

    // Upsert per-zone, per-psrType generation points into generation table.
    const genRows: any[] = [];
    for (const z of perZone) {
      if (!z.zone || !z.ts || !z.byType) continue;
      for (const [psrType, mw] of Object.entries(z.byType as Record<string, number>)) {
        if (typeof mw !== "number" || mw < 0) continue;
        genRows.push({ zone_id: String(z.zone), ts: z.ts, psr_type: psrType, mw, source: "entsoe" });
      }
    }
    if (genRows.length) {
      const chunkSize = 500;
      for (let i = 0; i < genRows.length; i += chunkSize) {
        const { error: genErr } = await supabase
          .from("electricity_generation_snapshots")
          .upsert(genRows.slice(i, i + chunkSize), { onConflict: "source,zone_id,ts,psr_type" });
        if (genErr) console.error("Generation by type upsert failed:", genErr.message);
      }
    }

    // Store EU aggregate as zone_id='EU'
    const payload = {
      zone_id: "EU",
      country_code: null,
      ts,
      renewable_percent: euRenewablePercent,
      carbon_intensity_g_per_kwh: null,
      source: "entsoe",
      raw: { scope: "EU+EEA+UK", euRenewableMw, euTotalMw, zones: perZone, skipped, errors },
    };

    const { error } = await supabase
      .from("energy_mix_snapshots")
      .upsert(payload, { onConflict: "source,zone_id,ts" });

    if (error) return json({ error: "db_error", message: error.message }, 500);

    return json({
      ok: true,
      inserted: payload,
      zones_total: perZone.length,
      zones_used_in_aggregate: perZone.length - Object.keys(skipped).length,
      zones_skipped: Object.keys(skipped).length,
      errors: Object.keys(errors).length,
      zone_rows_upserted: zoneRows.length,
      concurrency,
    });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});

