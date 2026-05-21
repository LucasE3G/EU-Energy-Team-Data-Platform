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
async function fetchAgsiPage(apiKey: string, from: string, to: string, page: number, size: number): Promise<{ data: AgsiRecord[]; total: number }> {
  const url = `https://agsi.gie.eu/api?type=eu&from=${from}&to=${to}&page=${page}&size=${size}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30_000);
  const res = await fetch(url, {
    signal: controller.signal,
    headers: { "x-key": apiKey },
  }).finally(() => clearTimeout(timeout));

  if (!res.ok) throw new Error(`GIE AGSI HTTP ${res.status}: ${await res.text()}`);
  const body = await res.json();
  if (body.error) throw new Error(`GIE AGSI error: ${body.error}`);
  return { data: body.data ?? [], total: body.total ?? 0 };
}

async function fetchAllAgsi(apiKey: string, from: string, to: string): Promise<AgsiRecord[]> {
  const size = 300;
  const first = await fetchAgsiPage(apiKey, from, to, 1, size);
  const records: AgsiRecord[] = [...first.data];
  const pages = Math.ceil(first.total / size);
  for (let p = 2; p <= pages; p++) {
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
    const agsiKey = Deno.env.get("GIE_AGSI_API_KEY");

    if (!supabaseUrl || !serviceRole) {
      return json({ error: "missing_config", message: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
    }
    if (!agsiKey) {
      return json({ error: "missing_key", message: "GIE_AGSI_API_KEY not set. Register at https://agsi.gie.eu/account" }, 500);
    }

    const body = await req.json().catch(() => ({}));
    // Default: last 10 days. For backfill pass from/to as YYYY-MM-DD strings.
    const toDate = body.to ?? new Date().toISOString().slice(0, 10);
    const fromDate = body.from ?? (() => {
      const d = new Date();
      d.setDate(d.getDate() - 10);
      return d.toISOString().slice(0, 10);
    })();

    const records = await fetchAllAgsi(agsiKey, fromDate, toDate);
    if (records.length === 0) return json({ ok: true, inserted: 0, range: `${fromDate}/${toDate}` });

    const db = createClient(supabaseUrl, serviceRole);

    const rows = records
      .filter(r => r.gasDayStart)
      .map(r => ({
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

    const { error } = await db.from("gas_storage_eu_daily")
      .upsert(rows, { onConflict: "source,gas_day" });

    if (error) throw new Error(error.message);

    const latest = rows.sort((a, b) => b.gas_day.localeCompare(a.gas_day))[0];
    return json({ ok: true, inserted: rows.length, range: `${fromDate}/${toDate}`, latest });
  } catch (err) {
    console.error("gas_ingest_storage_eu error:", err);
    return json({ error: String(err) }, 500);
  }
});
