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
// This mirrors `entsoe_ingest_eu_latest` so the UI can join zones consistently.
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
  const res = await fetch(url);
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

// Parse the latest load value for the time window.
// ENTSO-E load comes as one or more TimeSeries; we take the maximum "latest"
// quantity across series to avoid double-counting duplicates.
function parseLoadLatest(xml: string) {
  const timeSeriesBlocks = pickAll(xml, /<TimeSeries[\s\S]*?<\/TimeSeries>/g);
  let bestTs: string | null = null;
  let bestQty: number | null = null;

  for (const m of timeSeriesBlocks) {
    const block = m[0];
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

    // Pick the max across series (dedupe-ish).
    if (bestQty == null || latestQty > bestQty) bestQty = latestQty;

    if (Number.isFinite(startMs)) {
      const stepMinutes = resolution === "PT15M" ? 15 : resolution === "PT30M" ? 30 : 60;
      const ts = new Date(startMs + (latestPos - 1) * stepMinutes * 60 * 1000).toISOString();
      if (!bestTs || Date.parse(ts) > Date.parse(bestTs)) bestTs = ts;
    }
  }

  return { ts: bestTs, loadMw: bestQty };
}

async function fetchZoneLoadLatest(token: string, zone: string, domain: string) {
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), 0, 0));
  const start = new Date(end.getTime() - 3 * 60 * 60 * 1000);

  // Load: Total load value (Actual) is typically DocumentType=A65, ProcessType=A16.
  // (ENTSO-E naming: Total Load, but still returned as timeseries of quantities.)
  const params = new URLSearchParams({
    securityToken: token,
    documentType: "A65",
    processType: "A16",
    outBiddingZone_Domain: domain,
    periodStart: entsoeFormatYmdHm(start),
    periodEnd: entsoeFormatYmdHm(end),
  });
  const url = `https://web-api.tp.entsoe.eu/api?${params.toString()}`;
  const xml = await entsoeFetchText(url);
  const parsed = parseLoadLatest(xml);
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
    const perRequestDelayMs = Number(body?.delay_ms ?? 200);

    const perZone: any[] = [];
    let bestTs: string | null = null;
    const errors: Record<string, string> = {};
    const skipped: Record<string, string> = {};

    for (const z of zones) {
      const domain = DOMAINS[z];
      if (!domain) continue;
      try {
        const r = await fetchZoneLoadLatest(entsoeToken, z, domain);
        perZone.push({ zone: z, ts: r.ts, loadMw: r.loadMw });
        if (!r.ts || r.loadMw == null || r.loadMw <= 0) skipped[z] = "no_usable_data";
        if (r.ts && (!bestTs || Date.parse(r.ts) > Date.parse(bestTs))) bestTs = r.ts;
      } catch (e) {
        errors[z] = e?.message ?? String(e);
      }
      if (perRequestDelayMs > 0) await sleep(perRequestDelayMs);
    }

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const rows = perZone
      .filter((z) => z.zone && z.ts && typeof z.loadMw === "number")
      .map((z) => ({
        zone_id: String(z.zone),
        country_code: String(z.zone),
        ts: z.ts,
        load_mw: z.loadMw,
        source: "entsoe",
        raw: { loadMw: z.loadMw },
      }));

    if (rows.length) {
      const chunkSize = 200;
      for (let i = 0; i < rows.length; i += chunkSize) {
        const chunk = rows.slice(i, i + chunkSize);
        const { error: zoneErr } = await supabase
          .from("electricity_load_snapshots")
          .upsert(chunk, { onConflict: "source,zone_id,ts" });
        if (zoneErr) return json({ error: "db_error", message: zoneErr.message }, 500);
      }

      // Refresh MWh materialized views so daily/weekly charts stay current.
      const { error: refreshErr } = await supabase.rpc("refresh_electricity_load_mvs");
      if (refreshErr) console.error("MV refresh failed:", refreshErr.message);
    }

    return json({
      ok: true,
      ts: bestTs ?? null,
      zones_total: zones.length,
      zone_rows_upserted: rows.length,
      zones_skipped: Object.keys(skipped).length,
      errors: Object.keys(errors).length,
    });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});

