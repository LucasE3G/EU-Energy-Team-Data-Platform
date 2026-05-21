import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

interface AgsiRecord {
  gasDayStart: string;
  gasInStorage: number | null;
  full: number | null;
  trend: number | null;
  injection: number | null;
  withdrawal: number | null;
  workingGasVolume: number | null;
  status: string | null;
}

// Countries with GIE AGSI+ storage data (ISO 2-letter, lowercase for API param).
const GIE_COUNTRIES = [
  "at", "be", "bg", "cz", "de", "dk", "es", "fr", "hr", "hu",
  "it", "lt", "lv", "nl", "pl", "pt", "ro", "se", "si", "sk",
];

// GIE AGSI+ API — requires a free API key from https://agsi.gie.eu/account
async function fetchAgsiPage(
  apiKey: string,
  from: string,
  to: string,
  page: number,
  size: number,
  country?: string,
): Promise<{ data: AgsiRecord[]; lastPage: number; rawBody?: unknown }> {
  const typeParam = country ? `type=country&country=${country}` : "type=eu";
  const url =
    `https://agsi.gie.eu/api?${typeParam}&from=${from}&to=${to}&page=${page}&size=${size}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);

  let res: Response;
  try {
    res = await fetch(url, {
      signal: controller.signal,
      headers: { "x-key": apiKey },
    });
  } finally {
    clearTimeout(timeout);
  }

  const rawText = await res.text();
  if (!res.ok) throw new Error(`GIE AGSI HTTP ${res.status}: ${rawText.slice(0, 300)}`);

  let body: Record<string, unknown> | null;
  try {
    const parsed = JSON.parse(rawText);
    body = (parsed !== null && typeof parsed === "object" && !Array.isArray(parsed))
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    throw new Error(`GIE AGSI returned non-JSON response: ${rawText.slice(0, 200)}`);
  }

  if (!body) throw new Error(`GIE AGSI returned unexpected body: ${rawText.slice(0, 200)}`);
  if (body["error"]) throw new Error(`GIE AGSI error: ${body["error"]}`);

  const data = Array.isArray(body["data"]) ? (body["data"] as AgsiRecord[]) : [];
  const lastPage = typeof body["last_page"] === "number" ? body["last_page"] : 1;

  return { data, lastPage, rawBody: body };
}

async function fetchAllAgsi(
  apiKey: string,
  from: string,
  to: string,
  country?: string,
): Promise<AgsiRecord[]> {
  const size = 3000;
  const first = await fetchAgsiPage(apiKey, from, to, 1, size, country);
  const records: AgsiRecord[] = [...first.data];
  for (let p = 2; p <= first.lastPage; p++) {
    const { data } = await fetchAgsiPage(apiKey, from, to, p, size, country);
    records.push(...data);
  }
  return records;
}

function safeNum(v: unknown): number | null {
  if (v == null || v === "" || v === "-") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function recordsToRows(
  records: AgsiRecord[],
  source: string,
  country?: string,
): Record<string, unknown>[] {
  return records
    .filter((r) => r.gasDayStart)
    .map((r) => ({
      gas_day: r.gasDayStart,
      gas_in_storage_twh: safeNum(r.gasInStorage),
      full_pct: safeNum(r.full),
      trend_pct: safeNum(r.trend),
      injection_twh: safeNum(r.injection),
      withdrawal_twh: safeNum(r.withdrawal),
      working_gas_volume_twh: safeNum(r.workingGasVolume),
      status: r.status ?? null,
      source,
      ...(country ? { country: country.toUpperCase() } : {}),
    }));
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    const agsiKey = Deno.env.get("GIE_API_KEY") ?? Deno.env.get("GIE_AGSI_API_KEY");

    if (!supabaseUrl || !serviceRole) {
      return json(
        { error: "missing_config", message: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" },
        500,
      );
    }
    if (!agsiKey) {
      return json(
        {
          error: "missing_key",
          message:
            "GIE_API_KEY not set in Supabase secrets. Register at https://agsi.gie.eu/account",
        },
        500,
      );
    }

    const reqBody = await req.json().catch(() => ({}));
    const debug = reqBody.debug === true;
    const skipCountries = reqBody.skip_countries === true;

    const toDate: string = (typeof reqBody.to === "string" && reqBody.to)
      ? reqBody.to
      : new Date().toISOString().slice(0, 10);
    const fromDate: string = (typeof reqBody.from === "string" && reqBody.from)
      ? reqBody.from
      : (() => {
        const d = new Date();
        d.setDate(d.getDate() - 10);
        return d.toISOString().slice(0, 10);
      })();

    if (debug) {
      const raw = await fetchAgsiPage(agsiKey, fromDate, toDate, 1, 5);
      return json({
        debug: true,
        fromDate,
        toDate,
        rawBody: raw.rawBody,
        sampleRecords: raw.data.slice(0, 3),
      });
    }

    const db = createClient(supabaseUrl, serviceRole);

    // ── EU aggregate ──────────────────────────────────────────────────────────
    const euRecords = await fetchAllAgsi(agsiKey, fromDate, toDate);
    let euInserted = 0;
    if (euRecords.length > 0) {
      const euRows = recordsToRows(euRecords, "gie_agsi");
      const { error: euErr } = await db
        .from("gas_storage_eu_daily")
        .upsert(euRows, { onConflict: "source,gas_day" });
      if (euErr) throw new Error(`EU upsert failed: ${euErr.message}`);
      euInserted = euRows.length;
    }

    if (skipCountries) {
      const latest = euRecords.length > 0
        ? recordsToRows(euRecords, "gie_agsi").sort((a, b) =>
          String(b.gas_day).localeCompare(String(a.gas_day))
        )[0]
        : null;
      return json({ ok: true, eu_inserted: euInserted, range: `${fromDate}/${toDate}`, latest });
    }

    // ── Per-country ───────────────────────────────────────────────────────────
    const countryResults: { country: string; inserted: number; error?: string }[] = [];
    for (const c of GIE_COUNTRIES) {
      try {
        const records = await fetchAllAgsi(agsiKey, fromDate, toDate, c);
        if (records.length === 0) {
          countryResults.push({ country: c.toUpperCase(), inserted: 0 });
          continue;
        }
        const rows = recordsToRows(records, "gie_agsi", c);
        const { error: cErr } = await db
          .from("gas_storage_country_daily")
          .upsert(rows, { onConflict: "source,country,gas_day" });
        if (cErr) throw new Error(cErr.message);
        countryResults.push({ country: c.toUpperCase(), inserted: rows.length });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        countryResults.push({ country: c.toUpperCase(), inserted: 0, error: msg });
      }
    }

    const totalCountryInserted = countryResults.reduce((s, r) => s + r.inserted, 0);
    return json({
      ok: true,
      eu_inserted: euInserted,
      country_inserted: totalCountryInserted,
      country_results: countryResults,
      range: `${fromDate}/${toDate}`,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("gas_ingest_storage_eu error:", msg);
    return json({ error: msg }, 500);
  }
});
