import { serve } from "https://deno.land/std@0.224.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2.45.4";

function json(data: unknown, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

const BROWSER_UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// Yahoo Finance requires a session cookie + crumb for authenticated API calls.
// Without this, requests from server IPs get 401 or are rate-limited.
async function getYahooCrumb(): Promise<{ cookie: string; crumb: string }> {
  const cookieRes = await fetch("https://fc.yahoo.com", {
    headers: { "user-agent": BROWSER_UA, "accept": "text/html" },
    redirect: "follow",
  });
  const rawCookie = cookieRes.headers.get("set-cookie") ?? "";
  const cookie = rawCookie.split(";")[0];
  if (!cookie) throw new Error("Failed to obtain Yahoo Finance session cookie from fc.yahoo.com");

  const crumbRes = await fetch("https://query1.finance.yahoo.com/v1/test/getcrumb", {
    headers: {
      "user-agent": BROWSER_UA,
      "cookie": cookie,
      "accept": "text/plain,*/*",
    },
  });
  if (!crumbRes.ok) {
    throw new Error(`Yahoo crumb endpoint returned HTTP ${crumbRes.status}`);
  }
  const crumb = (await crumbRes.text()).trim();
  if (!crumb) throw new Error("Yahoo Finance returned an empty crumb");
  return { cookie, crumb };
}

async function fetchTtfOhlcv(
  range: string,
): Promise<{ ts: string; open: number | null; high: number | null; low: number | null; close: number | null }[]> {
  const { cookie, crumb } = await getYahooCrumb();

  const url =
    `https://query1.finance.yahoo.com/v8/finance/chart/TTF%3DF` +
    `?interval=1d&range=${encodeURIComponent(range)}&crumb=${encodeURIComponent(crumb)}`;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 25_000);
  const res = await fetch(url, {
    signal: controller.signal,
    headers: {
      "user-agent": BROWSER_UA,
      "cookie": cookie,
      "accept": "application/json,text/plain,*/*",
      "referer": "https://finance.yahoo.com/",
    },
  }).finally(() => clearTimeout(timeout));

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Yahoo Finance chart API returned HTTP ${res.status}: ${body.slice(0, 200)}`);
  }

  const body = await res.json();
  const result = body?.chart?.result?.[0];
  if (!result) throw new Error("No chart result in Yahoo Finance response");

  const timestamps: number[] = result.timestamp ?? [];
  const quote = result.indicators?.quote?.[0] ?? {};
  const opens: (number | null)[] = quote.open ?? [];
  const highs: (number | null)[] = quote.high ?? [];
  const lows: (number | null)[] = quote.low ?? [];
  const closes: (number | null)[] = quote.close ?? [];

  return timestamps
    .map((unixSec, i) => ({
      ts: new Date(unixSec * 1000).toISOString().slice(0, 10),
      open: opens[i] ?? null,
      high: highs[i] ?? null,
      low: lows[i] ?? null,
      close: closes[i] ?? null,
    }))
    .filter((r) => r.close !== null);
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
    const range: string = body?.range ?? "5d";

    const rows = await fetchTtfOhlcv(range);
    if (!rows.length) return json({ ok: true, inserted: 0, message: "No data returned from Yahoo Finance" });

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const dbRows = rows.map((r) => ({
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
