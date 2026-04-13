import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

type RteTokenResponse = {
  access_token?: string;
};

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store",
    },
  });
}

async function fetchRteAccessToken(clientId: string, clientSecret: string) {
  const basic = btoa(`${clientId}:${clientSecret}`);
  const res = await fetch("https://digital.iservices.rte-france.com/token/oauth/", {
    method: "POST",
    headers: {
      authorization: `Basic ${basic}`,
      "content-type": "application/x-www-form-urlencoded",
    },
    body: "",
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`RTE token HTTP ${res.status}: ${text.slice(0, 300)}`);
  const payload = JSON.parse(text) as RteTokenResponse;
  if (!payload.access_token) throw new Error("RTE token response missing access_token");
  return payload.access_token;
}

function isRenewableKey(key: string) {
  if (key.startsWith("HYDRO:HYDRO_PUMPED_STORAGE")) return false;
  return key === "SOLAR" || key.startsWith("WIND") || key.startsWith("HYDRO") || key.startsWith("BIOENERGY");
}

function isNonRenewableKey(key: string) {
  return key === "NUCLEAR" || key.startsWith("FOSSIL_");
}

function parseRteMixTimeseries(mixJson: any) {
  const rows = Array.isArray(mixJson?.generation_mix_15min_time_scale)
    ? mixJson.generation_mix_15min_time_scale
    : [];

  const excludedTypes = new Set(["EXCHANGE", "PUMPING"]);
  const buckets = new Map<string, { renewable: number; nonRenewable: number }>();

  for (const series of rows) {
    const pType = series?.production_type;
    const pSubtype = series?.production_subtype;
    if (!pType || excludedTypes.has(pType)) continue;
    if (!Array.isArray(series?.values) || !series.values.length) continue;

    const key = pSubtype && pSubtype !== "TOTAL" ? `${pType}:${pSubtype}` : pType;

    for (const v of series.values) {
      if (!v?.start_date || typeof v?.value !== "number") continue;
      const ts = new Date(v.start_date).toISOString();
      const b = buckets.get(ts) ?? { renewable: 0, nonRenewable: 0 };

      if (isRenewableKey(key)) b.renewable += v.value;
      else if (isNonRenewableKey(key)) b.nonRenewable += v.value;
      else b.nonRenewable += v.value; // conservative default

      buckets.set(ts, b);
    }
  }

  const points = Array.from(buckets.entries())
    .map(([ts, b]) => {
      const total = b.renewable + b.nonRenewable;
      const renewablePercent = total > 0 ? (b.renewable / total) * 100 : null;
      return { ts, renewablePercent };
    })
    .filter((p) => typeof p.renewablePercent === "number" && !Number.isNaN(p.renewablePercent))
    .sort((a, b) => Date.parse(a.ts) - Date.parse(b.ts));

  return points;
}

function toIso(d: Date) {
  return d.toISOString();
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    // Optional shared secret
    const expected = Deno.env.get("INGEST_TOKEN");
    if (expected) {
      const auth = req.headers.get("authorization") || "";
      if (auth !== `Bearer ${expected}`) return json({ error: "unauthorized" }, 401);
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRole) {
      return json({ error: "missing_config", message: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
    }

    const clientId = Deno.env.get("RTE_CLIENT_ID");
    const clientSecret = Deno.env.get("RTE_CLIENT_SECRET");
    if (!clientId || !clientSecret) {
      return json({ error: "missing_config", message: "Missing RTE_CLIENT_ID or RTE_CLIENT_SECRET" }, 500);
    }

    const body = await req.json().catch(() => ({}));

    // How far back we want to fill (years from "now")
    const years = Math.min(Math.max(Number(body?.years ?? 5), 1), 20);
    // Chunk size per run (RTE recommends <= 14 days per call)
    const chunkDays = Math.min(Math.max(Number(body?.days ?? 14), 1), 14);

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    // Find the oldest timestamp we already have for France (so we backfill older chunks each run)
    const { data: minRows, error: minErr } = await supabase
      .from("energy_mix_snapshots")
      .select("ts")
      .eq("zone_id", "FR")
      .eq("source", "rte")
      .order("ts", { ascending: true })
      .limit(1);

    if (minErr) return json({ error: "db_error", message: minErr.message }, 500);

    const now = new Date();
    const targetStart = new Date(now);
    targetStart.setFullYear(targetStart.getFullYear() - years);

    // If table is empty, start with "now" as the current end boundary.
    const currentMinTs = minRows?.[0]?.ts ? new Date(minRows[0].ts) : now;

    if (currentMinTs.getTime() <= targetStart.getTime()) {
      return json({
        ok: true,
        done: true,
        oldest_ts: currentMinTs.toISOString(),
        target_start: targetStart.toISOString(),
      });
    }

    // Next chunk range: [start, end] where end is the current oldest ts we have.
    const end = currentMinTs;
    const startCandidate = new Date(end.getTime() - chunkDays * 24 * 60 * 60 * 1000);
    const start = startCandidate.getTime() < targetStart.getTime() ? targetStart : startCandidate;

    const accessToken = await fetchRteAccessToken(clientId, clientSecret);

    const params = new URLSearchParams({
      start_date: toIso(start),
      end_date: toIso(end),
    });

    const url =
      `https://digital.iservices.rte-france.com/open_api/actual_generation/v1/generation_mix_15min_time_scale?${params.toString()}`;

    const mixRes = await fetch(url, { headers: { authorization: `Bearer ${accessToken}` } });
    const mixText = await mixRes.text();
    if (!mixRes.ok) return json({ error: "rte_error", message: `HTTP ${mixRes.status}: ${mixText.slice(0, 300)}` }, 502);

    const mixJson = JSON.parse(mixText);
    const points = parseRteMixTimeseries(mixJson);

    // Insert in chunks to avoid payload limits
    let inserted = 0;
    const chunkSize = 500;
    for (let i = 0; i < points.length; i += chunkSize) {
      const chunk = points.slice(i, i + chunkSize).map((p) => ({
        zone_id: "FR",
        country_code: "FR",
        ts: p.ts,
        renewable_percent: p.renewablePercent,
        carbon_intensity_g_per_kwh: null,
        source: "rte",
        raw: null,
      }));

      const { error } = await supabase
        .from("energy_mix_snapshots")
        .upsert(chunk, { onConflict: "source,zone_id,ts" });

      if (error) return json({ error: "db_error", message: error.message }, 500);
      inserted += chunk.length;
    }

    return json({
      ok: true,
      done: false,
      years,
      chunk_days: chunkDays,
      range_start: start.toISOString(),
      range_end: end.toISOString(),
      points: points.length,
      inserted,
    });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});

