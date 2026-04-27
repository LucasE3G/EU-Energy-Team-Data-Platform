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
// Mirrors the electricity generation/load functions for consistency.
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
  // Note: prices use different EICs than generation/load for some zones.
  // DE day-ahead prices are available under 10Y1001A1001A82H.
  DE: "10Y1001A1001A82H",
  GR: "10YGR-HTSO-----Y",
  HU: "10YHU-MAVIR----U",
  IE: "10YIE-1001A00010",
  // Italy is split into multiple price areas; use IT-NORD as a practical proxy for now.
  IT: "10Y1001A1001A73I",
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

// For some countries, ENTSO-E day-ahead prices are published per price area.
// We can combine multiple areas into a single country-level proxy by averaging
// prices by timestamp across the available areas.
const PRICE_DOMAIN_OVERRIDES: Record<string, string[]> = {
  // IT-NORD + IT-CNOR are commonly available. If one is missing for a window,
  // the other will still contribute.
  IT: ["10Y1001A1001A73I", "10Y1001A1001A74G"],
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

function parseDayAheadPrices(xml: string) {
  const timeSeriesBlocks = pickAll(xml, /<TimeSeries[\s\S]*?<\/TimeSeries>/g).map((m) => m[0]);
  const pointsOut: Array<{ ts: string; price: number }> = [];

  for (const block of timeSeriesBlocks) {
    const periodStart = (block.match(/<timeInterval>\s*<start>([^<]+)<\/start>/) || [])[1];
    const resolution = (block.match(/<resolution>([^<]+)<\/resolution>/) || [])[1];
    const startMs = periodStart ? Date.parse(periodStart) : NaN;
    if (!Number.isFinite(startMs)) continue;

    const stepMinutes = resolution === "PT15M" ? 15 : resolution === "PT30M" ? 30 : 60;

    const points = pickAll(block, /<Point>[\s\S]*?<\/Point>/g).map((pm) => pm[0]);
    for (const p of points) {
      const pos = Number((p.match(/<position>([^<]+)<\/position>/) || [])[1]);
      const priceStr =
        (p.match(/<price\.amount>([^<]+)<\/price\.amount>/) || [])[1] ||
        (p.match(/<quantity>([^<]+)<\/quantity>/) || [])[1];
      const price = Number(priceStr);
      if (!Number.isFinite(pos) || !Number.isFinite(price)) continue;
      const ts = new Date(startMs + (pos - 1) * stepMinutes * 60 * 1000).toISOString();
      pointsOut.push({ ts, price });
    }
  }

  return pointsOut;
}

async function fetchZoneDayAheadPrices(token: string, zone: string, domain: string, start: Date, end: Date) {
  const params = new URLSearchParams({
    securityToken: token,
    documentType: "A44",
    processType: "A01", // Day-ahead
    in_Domain: domain,
    out_Domain: domain, // prices are per area; set same domain
    periodStart: entsoeFormatYmdHm(start),
    periodEnd: entsoeFormatYmdHm(end),
  });
  const url = `https://web-api.tp.entsoe.eu/api?${params.toString()}`;
  const xml = await entsoeFetchText(url);
  const points = parseDayAheadPrices(xml);
  return { zone, domain, points };
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
    const perRequestDelayMs = Number(body?.delay_ms ?? 250);

    // Fetch a tight window that always includes "today" and "tomorrow" day-ahead results.
    const now = new Date();
    const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1, 0, 0, 0));
    const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 2, 0, 0, 0));

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    let totalPoints = 0;
    const errors: Record<string, string> = {};

    // Concurrency pool so the function finishes within typical edge timeouts.
    const concurrency = Math.max(1, Math.min(8, Number(body?.concurrency ?? 6)));
    const queue = [...zones];
    const workers = Array.from({ length: concurrency }, async () => {
      while (queue.length) {
        const z = String(queue.shift());
        const domain = DOMAINS[z];
        if (!domain) continue;
        try {
        const domains = PRICE_DOMAIN_OVERRIDES[z] ?? [domain];
        const sumByTs: Record<string, number> = {};
        const nByTs: Record<string, number> = {};

        for (const dom of domains) {
          const { points } = await fetchZoneDayAheadPrices(entsoeToken, z, dom, start, end);
          if (!points.length) continue;

          // Dedupe timestamps within a response (ENTSO-E can return multiple
          // TimeSeries that overlap). Postgres rejects upsert batches that
          // contain duplicate conflict keys.
          const byTs: Record<string, number> = {};
          for (const p of points) {
            if (!p.ts || !Number.isFinite(p.price)) continue;
            byTs[p.ts] = p.price;
          }

          for (const [ts, price] of Object.entries(byTs)) {
            sumByTs[ts] = (sumByTs[ts] ?? 0) + price;
            nByTs[ts] = (nByTs[ts] ?? 0) + 1;
          }
        }

        const rows = Object.keys(sumByTs).sort().map((ts) => ({
              zone_id: String(z),
            ts,
            price_eur_per_mwh: sumByTs[ts] / nByTs[ts],
              currency: "EUR",
              source: "entsoe",
            raw: { domains },
          }));

          const chunkSize = 500;
          for (let i = 0; i < rows.length; i += chunkSize) {
            const { error } = await supabase
              .from("electricity_day_ahead_prices")
              .upsert(rows.slice(i, i + chunkSize), { onConflict: "source,zone_id,ts" });
            if (error) throw new Error(error.message);
          }
          totalPoints += rows.length;
        } catch (e) {
          errors[z] = e?.message ?? String(e);
        }
        if (perRequestDelayMs > 0) await sleep(perRequestDelayMs);
      }
    });

    await Promise.all(workers);

    // Refresh EU aggregates after upsert.
    const { error: refreshErr } = await supabase.rpc("refresh_electricity_price_mvs");
    if (refreshErr) console.error("Price MV refresh failed:", refreshErr.message);

    return json({
      ok: true,
      window: { start: start.toISOString(), end: end.toISOString() },
      zones_total: zones.length,
      concurrency,
      points_upserted: totalPoints,
      errors: Object.keys(errors).length,
      errors_by_zone: errors,
    });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});

