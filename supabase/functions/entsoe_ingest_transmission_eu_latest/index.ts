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

function parseA11Latest(xml: string) {
  const tsBlocks = pickAll(xml, /<TimeSeries[\s\S]*?<\/TimeSeries>/g).map((m) => m[0]);
  let bestTs: string | null = null;
  let bestMw: number | null = null;

  for (const block of tsBlocks) {
    const periodStart = (block.match(/<timeInterval>\s*<start>([^<]+)<\/start>/) || [])[1];
    const resolution = (block.match(/<resolution>([^<]+)<\/resolution>/) || [])[1];
    const startMs = periodStart ? Date.parse(periodStart) : NaN;
    if (!Number.isFinite(startMs)) continue;
    const stepMinutes = resolution === "PT15M" ? 15 : resolution === "PT30M" ? 30 : 60;

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

    const ts = new Date(startMs + (latestPos - 1) * stepMinutes * 60 * 1000).toISOString();
    if (!bestTs || Date.parse(ts) > Date.parse(bestTs)) {
      bestTs = ts;
      bestMw = latestQty;
    }
  }

  return { ts: bestTs, mw: bestMw };
}

async function fetchPairLatest(token: string, fromZone: string, toZone: string, fromDomain: string, toDomain: string) {
  const now = new Date();
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), 0, 0));
  const start = new Date(end.getTime() - 6 * 60 * 60 * 1000);

  const params = new URLSearchParams({
    securityToken: token,
    documentType: "A11",
    processType: "A16",
    in_Domain: fromDomain,
    out_Domain: toDomain,
    periodStart: entsoeFormatYmdHm(start),
    periodEnd: entsoeFormatYmdHm(end),
  });

  const url = `https://web-api.tp.entsoe.eu/api?${params.toString()}`;
  const xml = await entsoeFetchText(url);
  const parsed = parseA11Latest(xml);
  return { fromZone, toZone, ...parsed };
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
    const perRequestDelayMs = Number(body?.delay_ms ?? 100);
    const concurrency = Math.max(1, Math.min(8, Number(body?.concurrency ?? 6)));

    // For "latest" we don't need all-to-all pairs (explodes). Start with a curated neighbor list
    // and allow override later. For now: compute pairs from provided `pairs` if present.
    const pairs: Array<{ from: string; to: string }> = Array.isArray(body?.pairs) ? body.pairs : [];
    const work = pairs.length
      ? pairs.map((p) => ({ from: String(p.from).toUpperCase(), to: String(p.to).toUpperCase() }))
      : [];

    if (!work.length) {
      return json({
        ok: false,
        message: "No pairs provided. Pass {pairs:[{from:'FR',to:'DE'},...]}",
      }, 400);
    }

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const errors: Record<string, string> = {};
    let rowsUpserted = 0;

    const queue = [...work];
    const workers = Array.from({ length: concurrency }, async () => {
      while (queue.length) {
        const p = queue.shift();
        if (!p) return;
        const fromDomain = DOMAINS[p.from];
        const toDomain = DOMAINS[p.to];
        if (!fromDomain || !toDomain) continue;
        try {
          const r = await fetchPairLatest(entsoeToken, p.from, p.to, fromDomain, toDomain);
          if (!r.ts || r.mw == null) continue;
          const payload = {
            ts: r.ts,
            from_zone: p.from,
            to_zone: p.to,
            mw: r.mw,
            source: "entsoe",
            raw: { in_domain: fromDomain, out_domain: toDomain },
          };
          const { error } = await supabase
            .from("electricity_crossborder_flows")
            .upsert(payload, { onConflict: "source,from_zone,to_zone,ts" });
          if (error) throw new Error(error.message);
          rowsUpserted += 1;
        } catch (e) {
          errors[`${p.from}->${p.to}`] = e?.message ?? String(e);
        }
        if (perRequestDelayMs > 0) await sleep(perRequestDelayMs);
      }
    });

    await Promise.all(workers);

    return json({
      ok: true,
      pairs: work.length,
      concurrency,
      rows_upserted: rowsUpserted,
      errors: Object.keys(errors).length,
      errors_by_pair: errors,
    });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});

