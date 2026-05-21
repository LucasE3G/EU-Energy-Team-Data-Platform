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

// GIE AGSI+ API — requires a free API key from https://agsi.gie.eu/account
// Returns EU aggregate gas storage data (daily).
async function fetchAgsiPage(
  apiKey: string,
  from: string,
  to: string,
  page: number,
  size: number,
): Promise<{ data: AgsiRecord[]; lastPage: number; rawBody?: unknown }> {
  const url =
    `https://agsi.gie.eu/api?type=eu&from=${from}&to=${to}&page=${page}&size=${size}`;
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

  // Parse JSON defensively — API occasionally returns null or non-standard bodies
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

  // body.error is a string field in the GIE response ("" when OK, message when error)
  if (body["error"]) throw new Error(`GIE AGSI error: ${body["error"]}`);

  const data = Array.isArray(body["data"]) ? (body["data"] as AgsiRecord[]) : [];
  // GIE returns last_page (int) and total (record count). Use last_page for pagination.
  const lastPage = typeof body["last_page"] === "number" ? body["last_page"] : 1;

  return { data, lastPage, rawBody: body };
}

async function fetchAllAgsi(apiKey: string, from: string, to: string): Promise<AgsiRecord[]> {
  // Use a large page size (3000) to cover up to ~8 years in one request and avoid
  // sequential round-trips that can push the function past the wall-clock timeout.
  const size = 3000;
  const first = await fetchAgsiPage(apiKey, from, to, 1, size);
  const records: AgsiRecord[] = [...first.data];
  // Fetch additional pages only if needed (very large date ranges)
  for (let p = 2; p <= first.lastPage; p++) {
    const { data } = await fetchAgsiPage(apiKey, from, to, p, size);
    records.push(...data);
  }
  return records;
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    // Accepts both GIE_API_KEY (matches .env) and GIE_AGSI_API_KEY for backwards compat
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
          message: "GIE_API_KEY not set in Supabase secrets. Register at https://agsi.gie.eu/account",
        },
        500,
      );
    }

    const reqBody = await req.json().catch(() => ({}));
    const debug = reqBody.debug === true;

    // Default: last 10 days. For backfill pass from/to as YYYY-MM-DD strings.
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

    // Debug mode: return raw API response without writing to DB
    if (debug) {
      const raw = await fetchAgsiPage(agsiKey, fromDate, toDate, 1, 5);
      return json({ debug: true, fromDate, toDate, rawBody: raw.rawBody, sampleRecords: raw.data.slice(0, 3) });
    }

    const records = await fetchAllAgsi(agsiKey, fromDate, toDate);
    if (records.length === 0) {
      return json({ ok: true, inserted: 0, range: `${fromDate}/${toDate}`, message: "No records returned by GIE API" });
    }

    const db = createClient(supabaseUrl, serviceRole);

    const rows = records
      .filter((r) => r.gasDayStart)
      .map((r) => ({
        gas_day: r.gasDayStart,
        gas_in_storage_twh: r.gasInStorage != null ? Number(r.gasInStorage) : null,
        full_pct: r.full != null ? Number(r.full) : null,
        trend_pct: r.trend != null ? Number(r.trend) : null,
        injection_twh: r.injection != null ? Number(r.injection) : null,
        withdrawal_twh: r.withdrawal != null ? Number(r.withdrawal) : null,
        working_gas_volume_twh: r.workingGasVolume != null ? Number(r.workingGasVolume) : null,
        status: r.status ?? null,
        source: "gie_agsi",
      }));

    const { error: upsertError } = await db
      .from("gas_storage_eu_daily")
      .upsert(rows, { onConflict: "source,gas_day" });

    if (upsertError) throw new Error(`DB upsert failed: ${upsertError.message}`);

    const latest = [...rows].sort((a, b) => b.gas_day.localeCompare(a.gas_day))[0];
    return json({ ok: true, inserted: rows.length, range: `${fromDate}/${toDate}`, latest });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error("gas_ingest_storage_eu error:", msg);
    return json({ error: msg }, 500);
  }
});
