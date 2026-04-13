import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

type RteTokenResponse = {
  access_token?: string;
  token_type?: string;
  expires_in?: number;
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

  let payload: RteTokenResponse;
  try {
    payload = JSON.parse(text);
  } catch {
    throw new Error(`RTE token returned non-JSON: ${text.slice(0, 300)}`);
  }

  if (!payload.access_token) throw new Error("RTE token response missing access_token");
  return payload.access_token;
}

function parseRteMixLatest(mixJson: any) {
  const rows = Array.isArray(mixJson?.generation_mix_15min_time_scale)
    ? mixJson.generation_mix_15min_time_scale
    : [];

  const excludedTypes = new Set(["EXCHANGE", "PUMPING"]);
  const byType: Record<string, number> = {};
  let bestTs: string | null = null;

  for (const series of rows) {
    const pType = series?.production_type;
    const pSubtype = series?.production_subtype;
    if (!pType || excludedTypes.has(pType)) continue;
    if (!Array.isArray(series?.values) || !series.values.length) continue;

    let latest: any = null;
    for (const v of series.values) {
      if (!v?.start_date || typeof v?.value !== "number") continue;
      if (!latest || Date.parse(v.start_date) > Date.parse(latest.start_date)) latest = v;
    }
    if (!latest) continue;

    const key = pSubtype && pSubtype !== "TOTAL" ? `${pType}:${pSubtype}` : pType;
    byType[key] = (byType[key] || 0) + latest.value;

    const t = Date.parse(latest.start_date);
    if (!Number.isNaN(t) && (!bestTs || t > Date.parse(bestTs))) bestTs = new Date(t).toISOString();
  }

  let renewableMw = 0;
  let nonRenewableMw = 0;

  for (const [k, v] of Object.entries(byType)) {
    if (k.startsWith("HYDRO:HYDRO_PUMPED_STORAGE")) continue; // storage

    if (k === "SOLAR" || k.startsWith("WIND") || k.startsWith("HYDRO") || k.startsWith("BIOENERGY")) {
      renewableMw += v;
      continue;
    }

    if (k === "NUCLEAR" || k.startsWith("FOSSIL_")) {
      nonRenewableMw += v;
      continue;
    }

    nonRenewableMw += v; // conservative default
  }

  const totalMw = renewableMw + nonRenewableMw;
  const renewablePercent = totalMw > 0 ? (renewableMw / totalMw) * 100 : null;

  return {
    ts: bestTs,
    renewablePercent,
    totals: { renewableMw, nonRenewableMw, totalMw },
    raw: { byType },
  };
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    // Optional shared secret to prevent public triggering
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

    const accessToken = await fetchRteAccessToken(clientId, clientSecret);

    const mixRes = await fetch(
      "https://digital.iservices.rte-france.com/open_api/actual_generation/v1/generation_mix_15min_time_scale",
      { headers: { authorization: `Bearer ${accessToken}` } },
    );
    const mixText = await mixRes.text();
    if (!mixRes.ok) return json({ error: "rte_error", message: `HTTP ${mixRes.status}: ${mixText.slice(0, 300)}` }, 502);

    const mixJson = JSON.parse(mixText);
    const parsed = parseRteMixLatest(mixJson);

    const ts = parsed.ts ?? new Date().toISOString();
    const payload = {
      zone_id: "FR",
      country_code: "FR",
      ts,
      renewable_percent: parsed.renewablePercent,
      carbon_intensity_g_per_kwh: null,
      source: "rte",
      raw: { ...parsed.raw, totals: parsed.totals },
    };

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const { error } = await supabase
      .from("energy_mix_snapshots")
      .upsert(payload, { onConflict: "source,zone_id,ts" });

    if (error) return json({ error: "db_error", message: error.message }, 500);

    return json({ ok: true, inserted: payload });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});

