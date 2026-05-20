import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

// Yahoo Finance chart API — no key required.
// TTF=F is the front-month TTF Natural Gas futures contract, priced in EUR/MWh.
// We fetch daily OHLCV and upsert each trading day as a price record.
async function fetchTtfOhlcv(range: string): Promise<{ ts: string; open: number | null; high: number | null; low: number | null; close: number | null }[]> {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/TTF%3DF?interval=1d&range=${encodeURIComponent(range)}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20_000);
  const res = await fetch(url, {
    signal: controller.signal,
    headers: { "user-agent": "Mozilla/5.0" },
  }).finally(() => clearTimeout(timeout));

  if (!res.ok) throw new Error(`Yahoo Finance HTTP ${res.status}`);
  const body = await res.json();

  const result = body?.chart?.result?.[0];
  if (!result) throw new Error("No chart result in Yahoo Finance response");

  const timestamps: number[] = result.timestamp ?? [];
  const quote = result.indicators?.quote?.[0] ?? {};
  const opens: (number | null)[] = quote.open ?? [];
  const highs: (number | null)[] = quote.high ?? [];
  const lows: (number | null)[] = quote.low ?? [];
  const closes: (number | null)[] = quote.close ?? [];

  return timestamps.map((unixSec, i) => {
    const d = new Date(unixSec * 1000);
    const ts = d.toISOString().slice(0, 10); // YYYY-MM-DD
    return {
      ts,
      open: opens[i] ?? null,
      high: highs[i] ?? null,
      low: lows[i] ?? null,
      close: closes[i] ?? null,
    };
  }).filter(r => r.close !== null);
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRole) {
      return json({ error: "missing_config", message: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" }, 500);
    }

    const body = await req.json().catch(() => ({}));
    // Default: fetch last 5 days (daily refresh). Pass range="2y" for backfill.
    const range: string = body?.range ?? "5d";

    const rows = await fetchTtfOhlcv(range);
    if (!rows.length) return json({ ok: true, inserted: 0, message: "No data returned" });

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const dbRows = rows.map(r => ({
      ts: r.ts,
      close_eur_per_mwh: r.close,
      open_eur_per_mwh: r.open,
      high_eur_per_mwh: r.high,
      low_eur_per_mwh: r.low,
      source: "yahoo_finance",
    }));

    const { error } = await supabase
      .from("gas_price_ttf_daily")
      .upsert(dbRows, { onConflict: "source,ts" });

    if (error) return json({ error: "db_error", message: error.message }, 500);

    return json({ ok: true, inserted: dbRows.length, range, latest: dbRows.at(-1) });
  } catch (e) {
    return json({ error: "internal_error", message: e?.message ?? String(e) }, 500);
  }
});
