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

const ENTSOG_BASE = "https://transparency.entsog.eu/api/v1";
const GIE_BASE = "https://agsi.gie.eu/api";
const EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data";
const ENTSOE_API = "https://web-api.tp.entsoe.eu/api";

const EU27 = [
  "AT","BE","BG","HR","CY","CZ","DK","EE","FI","FR","DE","GR","HU","IE","IT",
  "LV","LT","LU","MT","NL","PL","PT","RO","SK","SI","ES","SE",
];

const ENTSOE_DOMAIN: Record<string, string> = {
  AT: "10YAT-APG------L",
  BE: "10YBE----------2",
  BG: "10YCA-BULGARIA-R",
  HR: "10YHR-HEP------M",
  CY: "10YCY-1001A0003J",
  CZ: "10YCZ-CEPS-----N",
  DK: "10YDK-1--------W",
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
  LU: "10YLU-CEGEDEL-NQ",
  MT: "10YMT-1001A0003F",
  NL: "10YNL----------L",
  PL: "10YPL-AREA-----S",
  PT: "10YPT-REN------W",
  RO: "10YRO-TEL------P",
  SK: "10YSK-SEPS-----K",
  SI: "10YSI-ELES-----O",
  ES: "10YES-REE------0",
  SE: "10YSE-1--------K",
};

function isoDate(d: Date) {
  return d.toISOString().slice(0, 10);
}

function ymdhm(d: Date): string {
  const u = new Date(d.toISOString());
  const y = u.getUTCFullYear().toString().padStart(4, "0");
  const m = (u.getUTCMonth() + 1).toString().padStart(2, "0");
  const dd = u.getUTCDate().toString().padStart(2, "0");
  const hh = u.getUTCHours().toString().padStart(2, "0");
  const mm = u.getUTCMinutes().toString().padStart(2, "0");
  return `${y}${m}${dd}${hh}${mm}`;
}

function toMwh(value: number, unit: string) {
  const u = String(unit || "").trim();
  if (u === "kWh/d" || u === "kWh") return value / 1000.0;
  if (u === "MWh/d" || u === "MWh") return value;
  if (u === "GWh/d" || u === "GWh") return value * 1000.0;
  if (u === "TWh/d" || u === "TWh") return value * 1_000_000.0;
  return value;
}

function tjToMwh(tj: number) {
  return tj * (1_000_000.0 / 3600.0);
}

function daysInMonth(yyyyMmDd: string) {
  const y = Number(yyyyMmDd.slice(0, 4));
  const m = Number(yyyyMmDd.slice(5, 7));
  return new Date(Date.UTC(y, m, 0)).getUTCDate();
}

type DirectionId = { tsoItemIdentifier: string; direction: "entry" | "exit" };

async function fetchInterconnectionDirections(country: string): Promise<DirectionId[]> {
  const out: DirectionId[] = [];
  for (const params of [
    { toCountryKey: country, limit: "-1" },
    { fromCountryKey: country, limit: "-1" },
  ]) {
    const url = new URL(`${ENTSOG_BASE}/interconnections`);
    for (const [k, v] of Object.entries(params)) url.searchParams.set(k, String(v));
    const res = await fetch(url);
    const text = await res.text();
    if (res.status === 404) continue;
    if (!res.ok) throw new Error(`ENTSOG interconnections HTTP ${res.status}: ${text.slice(0, 200)}`);
    const j = JSON.parse(text);
    const rows = Array.isArray(j?.interconnections) ? j.interconnections : [];
    for (const it of rows) {
      const fromC = it?.fromCountryKey;
      const toC = it?.toCountryKey;
      if (!fromC || !toC || fromC === toC) continue;
      if (toC === country && String(it?.toHasData) === "1") {
        const tid = it?.toTsoItemIdentifier;
        const dir = it?.toDirectionKey;
        if (tid && (dir === "entry" || dir === "exit")) out.push({ tsoItemIdentifier: tid, direction: dir });
      }
      if (fromC === country && String(it?.fromHasData) === "1") {
        const tid = it?.fromTsoItemIdentifier;
        const dir = it?.fromDirectionKey;
        if (tid && (dir === "entry" || dir === "exit")) out.push({ tsoItemIdentifier: tid, direction: dir });
      }
    }
  }
  const seen = new Set<string>();
  return out.filter((d) => {
    const k = `${d.tsoItemIdentifier}:${d.direction}`;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

async function fetchPhysicalFlowsDaily(tids: string[], from: string, to: string) {
  const rows: any[] = [];
  const batchSize = 40;
  for (let i = 0; i < tids.length; i += batchSize) {
    const batch = tids.slice(i, i + batchSize);
    const url = new URL(`${ENTSOG_BASE}/operationaldatas`);
    url.searchParams.set("indicator", "Physical Flow");
    url.searchParams.set("periodType", "day");
    url.searchParams.set("from", from);
    url.searchParams.set("to", to);
    url.searchParams.set("tsoItemIdentifier", batch.join(","));
    url.searchParams.set("limit", "-1");
    url.searchParams.set("includeExemptions", "0");
    const res = await fetch(url);
    const text = await res.text();
    if (res.status === 404) continue;
    if (!res.ok) throw new Error(`ENTSOG operationaldatas HTTP ${res.status}: ${text.slice(0, 200)}`);
    const j = JSON.parse(text);
    const data = Array.isArray(j?.operationaldatas) ? j.operationaldatas : [];
    rows.push(...data);
    await sleep(50);
  }
  return rows;
}

function computeNetImportsByDay(directions: DirectionId[], flowRows: any[]) {
  const tidSet = new Set(directions.map((d) => d.tsoItemIdentifier));
  const imports = new Map<string, number>();
  const exports = new Map<string, number>();
  for (const r of flowRows) {
    const tid = r?.tsoItemIdentifier;
    if (!tid || !tidSet.has(tid)) continue;
    const pf = r?.periodFrom;
    if (!pf) continue;
    const day = String(pf).slice(0, 10);
    const v = Number(r?.value);
    if (!Number.isFinite(v)) continue;
    const mwh = toMwh(v, r?.unit || "kWh/d");
    const rowDir = r?.directionKey;
    if (rowDir === "entry") imports.set(day, (imports.get(day) || 0) + mwh);
    else if (rowDir === "exit") exports.set(day, (exports.get(day) || 0) + mwh);
  }
  const out = new Map<string, number>();
  for (const d of new Set<string>([...imports.keys(), ...exports.keys()])) {
    out.set(d, (imports.get(d) || 0) - (exports.get(d) || 0));
  }
  return out;
}

async function fetchGieStorage(country: string, from: string, to: string, apiKey: string) {
  const rows: any[] = [];
  let page = 1;
  const size = 300;
  while (true) {
    const url = new URL(GIE_BASE);
    url.searchParams.set("page", String(page));
    url.searchParams.set("size", String(size));
    url.searchParams.set("country", country);
    url.searchParams.set("from", from);
    url.searchParams.set("to", to);
    const res = await fetch(url, { headers: { "x-key": apiKey } });
    const text = await res.text();
    if (!res.ok) throw new Error(`GIE HTTP ${res.status}: ${text.slice(0, 200)}`);
    const j = JSON.parse(text);
    const data = Array.isArray(j?.data) ? j.data : [];
    rows.push(...data);
    const lastPage = Number(j?.last_page || 0);
    if (!lastPage || page >= lastPage) break;
    page += 1;
    await sleep(50);
  }
  return rows;
}

function computeNetWithdrawalByDay(gieRows: any[]) {
  const out = new Map<string, number>();
  for (const r of gieRows) {
    const gasDay = r?.gasDayStart || r?.gasDay;
    if (!gasDay) continue;
    const day = String(gasDay).slice(0, 10);
    const unit = r?.unit || r?.gasInStorageUnit || "GWh";
    let net = r?.netWithdrawal;
    if (net == null) {
      const w = Number(r?.withdrawal || 0);
      const inj = Number(r?.injection || 0);
      net = w - inj;
    }
    const v = Number(net);
    if (!Number.isFinite(v)) continue;
    let mwh = toMwh(v, unit);
    if (mwh < 0) mwh = 0;
    out.set(day, (out.get(day) || 0) + mwh);
  }
  return out;
}

async function fetchEurostatIcObsMonthlyTj(country: string): Promise<Record<string, number>> {
  const url = new URL(`${EUROSTAT_BASE}/nrg_cb_gasm`);
  url.searchParams.set("freq", "M");
  url.searchParams.set("nrg_bal", "IC_OBS");
  url.searchParams.set("siec", "G3000");
  url.searchParams.set("unit", "TJ_GCV");
  url.searchParams.set("geo", country);
  const res = await fetch(url);
  if (!res.ok) return {};
  const j: any = await res.json();
  const values = j?.value || {};
  const idx = j?.dimension?.time?.category?.index || {};
  const byPos: Record<number, string> = {};
  for (const [k, pos] of Object.entries(idx)) byPos[Number(pos)] = String(k);
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(values)) {
    const ym = byPos[Number(k)];
    if (!ym) continue;
    const n = Number(v);
    if (Number.isFinite(n)) out[ym] = n;
  }
  return out;
}

async function fetchEurostatNonPowerShares(country: string, year: number): Promise<{ household: number; industry: number } | null> {
  async function fetchOne(nrgBal: string): Promise<number | null> {
    const url = new URL(`${EUROSTAT_BASE}/nrg_bal_c`);
    url.searchParams.set("freq", "A");
    url.searchParams.set("nrg_bal", nrgBal);
    url.searchParams.set("siec", "G3000");
    url.searchParams.set("unit", "TJ");
    url.searchParams.set("geo", country);
    url.searchParams.set("time", String(year));
    const res = await fetch(url);
    if (!res.ok) return null;
    const j: any = await res.json();
    const values = j?.value;
    if (!values) return null;
    const v = values["0"] ?? values[0];
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  const ind = await fetchOne("FC_IND_E");
  const hh = await fetchOne("FC_OTH_HH_E");
  if (ind == null || hh == null) return null;
  const denom = ind + hh;
  if (denom <= 0) return null;
  return { household: hh / denom, industry: ind / denom };
}

async function fetchBestAvailableEurostatShares(
  country: string,
  targetYear: number,
  maxLookbackYears: number,
): Promise<{ shares: { household: number; industry: number }; yearUsed: number; mode: "exact" | "previous" } | null> {
  for (let y = targetYear; y >= targetYear - maxLookbackYears; y--) {
    const sh = await fetchEurostatNonPowerShares(country, y);
    if (sh) return { shares: sh, yearUsed: y, mode: y === targetYear ? "exact" : "previous" };
    await sleep(100);
  }
  return null;
}

async function fetchEntsoeA75Xml(token: string, domain: string, start: Date, end: Date): Promise<string> {
  const url = new URL(ENTSOE_API);
  url.searchParams.set("securityToken", token);
  url.searchParams.set("documentType", "A75");
  url.searchParams.set("processType", "A16");
  url.searchParams.set("in_Domain", domain);
  url.searchParams.set("periodStart", ymdhm(start));
  url.searchParams.set("periodEnd", ymdhm(end));
  const res = await fetch(url);
  const text = await res.text();
  if (!res.ok) {
    if (res.status === 400) return ""; // no data in range
    throw new Error(`ENTSOE HTTP ${res.status}: ${text.slice(0, 200)}`);
  }
  return text;
}

function parseEntsoeGasGenerationMwhByDay(xml: string): Map<string, number> {
  const out = new Map<string, number>();
  if (!xml) return out;

  const tsRegex = /<TimeSeries\b[\s\S]*?<\/TimeSeries>/g;
  const psrRegex = /<psrType>\s*([A-Z0-9]+)\s*<\/psrType>/;
  const startRegex = /<timeInterval>[\s\S]*?<start>([^<]+)<\/start>[\s\S]*?<\/timeInterval>/;
  const resolutionRegex = /<resolution>\s*([A-Z0-9]+)\s*<\/resolution>/;
  const pointRegex = /<Point>\s*<position>(\d+)<\/position>\s*<quantity>([\-0-9.eE+]+)<\/quantity>\s*<\/Point>/g;

  for (const tsMatch of xml.matchAll(tsRegex)) {
    const ts = tsMatch[0];
    const psr = ts.match(psrRegex)?.[1];
    if (psr !== "B04") continue;

    const startIso = ts.match(startRegex)?.[1];
    const resolution = ts.match(resolutionRegex)?.[1] || "PT60M";
    if (!startIso) continue;

    const start = new Date(startIso);
    if (Number.isNaN(start.getTime())) continue;

    let stepMinutes = 60;
    if (resolution === "PT15M") stepMinutes = 15;
    else if (resolution === "PT30M") stepMinutes = 30;
    else if (resolution === "PT60M" || resolution === "PT1H") stepMinutes = 60;

    const stepHours = stepMinutes / 60.0;

    for (const pm of ts.matchAll(pointRegex)) {
      const pos = Number(pm[1]);
      const mw = Number(pm[2]);
      if (!Number.isFinite(pos) || !Number.isFinite(mw)) continue;
      const ts2 = new Date(start.getTime() + (pos - 1) * stepMinutes * 60 * 1000);
      const day = isoDate(ts2);
      out.set(day, (out.get(day) || 0) + mw * stepHours);
    }
  }
  return out;
}

type DayInput = {
  day: string;
  netImp: number;
  netWd: number;
  totalUnclamped: number;
  impliedTotal: number;
  impliedOk: boolean;
  rawPower: number;
  euroTj: number | null;
  euroMonthMwh: number | null;
  euroDayMwh: number | null;
};

function classifyImpliedOk(impliedTotal: number, rawPower: number) {
  return impliedTotal > 0 && (rawPower <= 0 || impliedTotal >= rawPower);
}

function makeRow(
  country: string,
  methodVersion: string,
  efficiency: number,
  domain: string | undefined,
  tidsCount: number,
  di: DayInput,
  total: number,
  selector: string,
  sourceTotal: string,
  sourceSplit: string,
  budgetMode: string,
  monthTargetMwh: number | null,
  monthImpliedSum: number | null,
  monthRemainder: number | null,
  monthScale: number | null,
  monthIsComplete: boolean | null,
  hhShare: number,
  indShare: number,
  shareYearTarget: number,
  shareYearUsed: number | null,
  shareSource: string,
  monthUnallocatedMwh: number | null = null,
) {
  const power = Math.max(0, Math.min(di.rawPower, total));
  let qualityFlag: string;
  if (total <= 0 && !di.impliedOk) {
    qualityFlag = "fallback_zero_month_incomplete_or_no_budget";
  } else if (di.rawPower > total) {
    qualityFlag = "power_capped_to_total";
  } else {
    qualityFlag = di.impliedOk ? "observed_total_entsoe_power" : "eurostat_fallback_allocated";
  }
  const nonpower = Math.max(0, total - power);
  const hh = nonpower * hhShare;
  let ind = nonpower * indShare;
  ind += nonpower - (hh + ind);
  return {
    country_code: country,
    gas_day: di.day,
    total_mwh: total,
    power_mwh: power,
    household_mwh: hh,
    industry_mwh: ind,
    source_total: sourceTotal,
    source_power: "entsoe_a75_b04",
    source_split: sourceSplit,
    method_version: methodVersion,
    quality_flag: qualityFlag,
    raw: {
      total_selector: selector,
      total_budget_mode: budgetMode,
      month_target_mwh: monthTargetMwh,
      month_implied_sum_mwh: monthImpliedSum,
      month_remainder_mwh: monthRemainder,
      month_unallocated_mwh: monthUnallocatedMwh,
      month_scale: monthScale,
      month_is_complete: monthIsComplete,
      net_imports_mwh: di.netImp,
      net_withdrawal_mwh: di.netWd,
      total_unclamped_mwh: di.totalUnclamped,
      implied_total_mwh: di.impliedTotal,
      eurostat_ic_obs_month: di.day.slice(0, 7),
      eurostat_ic_obs_tj_gcv: di.euroTj,
      eurostat_ic_obs_month_mwh: di.euroMonthMwh,
      eurostat_ic_obs_day_mwh: di.euroDayMwh,
      entsoe_domain: domain ?? null,
      efficiency,
      raw_power_mwh: di.rawPower,
      hh_share_nonpower: hhShare,
      ind_share_nonpower: indShare,
      tso_item_identifiers: tidsCount,
      shares_year_target: shareYearTarget,
      shares_year_used: shareYearUsed,
      shares_source: shareSource,
    },
  };
}

function allocateRemainderCapped(
  remainder: number,
  fallbackDays: DayInput[],
  impliedDayTotals: number[],
  daysInMonth: number,
): { alloc: Map<string, number>; unallocated: number } {
  const F = fallbackDays.length;
  const alloc = new Map<string, number>();
  if (F === 0) return { alloc, unallocated: remainder };
  if (remainder <= 0) {
    for (const d of fallbackDays) alloc.set(d.day, 0);
    return { alloc, unallocated: 0 };
  }
  const monthlyTotal = remainder + impliedDayTotals.reduce((s, v) => s + v, 0);
  const monthlyAvg = monthlyTotal / Math.max(daysInMonth, 1);
  const positive = impliedDayTotals.filter((v) => v > 0).sort((a, b) => a - b);
  let cap: number;
  if (positive.length > 0) {
    const mid = Math.floor(positive.length / 2);
    const median = positive.length % 2 === 0
      ? (positive[mid - 1] + positive[mid]) / 2
      : positive[mid];
    cap = Math.max(2.5 * median, 1.2 * monthlyAvg);
  } else {
    cap = Math.max(2.0 * monthlyAvg, remainder / F);
  }
  for (const d of fallbackDays) alloc.set(d.day, remainder / F);
  for (let iter = 0; iter < 10; iter++) {
    let overflow = 0;
    for (const d of fallbackDays) {
      const v = alloc.get(d.day) || 0;
      if (v > cap) {
        overflow += v - cap;
        alloc.set(d.day, cap);
      }
    }
    if (overflow <= 1e-6) return { alloc, unallocated: 0 };
    const uncapped = fallbackDays.filter((d) => (alloc.get(d.day) || 0) < cap - 1e-9);
    if (uncapped.length === 0) return { alloc, unallocated: overflow };
    const share = overflow / uncapped.length;
    for (const d of uncapped) alloc.set(d.day, (alloc.get(d.day) || 0) + share);
  }
  let finalUnalloc = 0;
  for (const d of fallbackDays) {
    const v = alloc.get(d.day) || 0;
    if (v > cap) {
      finalUnalloc += v - cap;
      alloc.set(d.day, cap);
    }
  }
  return { alloc, unallocated: finalUnalloc };
}

function budgetMonth(
  country: string,
  methodVersion: string,
  efficiency: number,
  domain: string | undefined,
  tidsCount: number,
  ym: string,
  inputs: DayInput[],
  hhShare: number,
  indShare: number,
  sourceSplit: string,
  shareYearTarget: number,
  shareYearUsed: number | null,
  shareSource: string,
) {
  // Rule: implied daily values are NEVER rewritten. For each month with a
  // Eurostat monthly target, fallback days share max(0, eurostat_month - implied_sum)
  // with a per-day cap to avoid spikes. Months that are not calendar-complete in
  // our window never get Eurostat allocation (fallback days stay at 0).
  inputs.sort((a, b) => (a.day < b.day ? -1 : a.day > b.day ? 1 : 0));

  const y = Number(ym.slice(0, 4));
  const m = Number(ym.slice(5, 7));
  const expectedDays = new Date(Date.UTC(y, m, 0)).getUTCDate();
  const monthIsComplete = new Set(inputs.map((d) => d.day)).size === expectedDays;

  const monthTarget = inputs.find((d) => d.euroMonthMwh != null)?.euroMonthMwh ?? null;
  const impliedDays = inputs.filter((d) => d.impliedOk);
  const fallbackDays = inputs.filter((d) => !d.impliedOk);
  const impliedSum = impliedDays.reduce((s, d) => s + d.impliedTotal, 0);

  const rows: any[] = [];

  const pushImplied = (di: DayInput, budgetMode: string, remainder: number | null, unallocated: number | null) => {
    rows.push(
      makeRow(
        country, methodVersion, efficiency, domain, tidsCount, di,
        di.impliedTotal, "implied_observed", "entsog_gie_implied_daily",
        sourceSplit, budgetMode, monthTarget, impliedSum,
        remainder, null, monthIsComplete, hhShare, indShare,
        shareYearTarget, shareYearUsed, shareSource, unallocated,
      ),
    );
  };

  // Case 1: no Eurostat target, or no fallback days, or month is incomplete.
  // Implied days stay observed; fallback days stay at 0 (no budget allocation).
  if (monthTarget == null || fallbackDays.length === 0 || !monthIsComplete) {
    let budgetMode: string;
    if (monthTarget == null) budgetMode = "no_eurostat_month_implied_only";
    else if (!monthIsComplete) budgetMode = "month_incomplete_eurostat_budget_skipped";
    else budgetMode = "implied_untouched_no_fallback_allocation";

    for (const di of inputs) {
      if (di.impliedOk) {
        pushImplied(di, budgetMode, null, null);
        continue;
      }
      let selector: string;
      let sourceTotal: string;
      if (monthTarget == null) {
        selector = "no_fallback_filler_no_eurostat";
        sourceTotal = "none_no_eurostat_month";
      } else if (!monthIsComplete) {
        selector = "fallback_no_data_month_incomplete";
        sourceTotal = "none_month_incomplete";
      } else {
        selector = "fallback_no_data";
        sourceTotal = "none_no_fallback_days";
      }
      rows.push(
        makeRow(
          country, methodVersion, efficiency, domain, tidsCount, di,
          0, selector, sourceTotal, sourceSplit, budgetMode,
          monthTarget, monthTarget == null ? null : impliedSum,
          null, null, monthIsComplete, hhShare, indShare,
          shareYearTarget, shareYearUsed, shareSource, null,
        ),
      );
    }
    return rows;
  }

  // Case 2: complete month with Eurostat target and fallback days: capped allocation.
  const remainder = Math.max(0, monthTarget - impliedSum);
  const { alloc, unallocated } = allocateRemainderCapped(
    remainder,
    fallbackDays,
    impliedDays.map((d) => d.impliedTotal),
    expectedDays,
  );

  for (const di of inputs) {
    if (di.impliedOk) {
      pushImplied(di, "implied_untouched_eurostat_fills_remainder_capped", remainder, unallocated);
    } else {
      rows.push(
        makeRow(
          country, methodVersion, efficiency, domain, tidsCount, di,
          alloc.get(di.day) || 0,
          "budget_eurostat_allocated_remainder_capped",
          "eurostat_nrg_cb_gasm_ic_obs_monthly_budgeted",
          sourceSplit, "implied_untouched_eurostat_fills_remainder_capped",
          monthTarget, impliedSum, remainder, null, true,
          hhShare, indShare, shareYearTarget, shareYearUsed, shareSource, unallocated,
        ),
      );
    }
  }
  return rows;
}

serve(async (req) => {
  try {
    if (req.method !== "POST") return json({ error: "method_not_allowed" }, 405);

    const expected = Deno.env.get("INGEST_TOKEN");
    if (expected) {
      const auth = req.headers.get("authorization") || "";
      if (auth !== `Bearer ${expected}`) return json({ error: "unauthorized" }, 401);
    }

    const supabaseUrl = Deno.env.get("SUPABASE_URL");
    const serviceRole = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY");
    if (!supabaseUrl || !serviceRole) return json({ error: "missing_config" }, 500);
    const gieKey = Deno.env.get("GIE_API_KEY");
    if (!gieKey) return json({ error: "missing_config", message: "Missing GIE_API_KEY" }, 500);
    const entsoeToken = Deno.env.get("ENTSOE_API_TOKEN");
    if (!entsoeToken) return json({ error: "missing_config", message: "Missing ENTSOE_API_TOKEN" }, 500);

    const body = await req.json().catch(() => ({}));
    const days = Math.min(Math.max(Number(body?.days ?? 10), 1), 45);
    const countries = (Array.isArray(body?.countries) && body.countries.length ? body.countries : EU27).map(String);

    const methodVersion = Deno.env.get("GAS_METHOD_VERSION") || "v2_bruegel_power_entsoe";
    const efficiency = Number(Deno.env.get("GAS_POWER_EFFICIENCY") ?? "0.5");
    const fallbackHouseholdShare = Number(Deno.env.get("GAS_HOUSEHOLD_SHARE") ?? "0.5");
    const fallbackIndustryShare = Number(Deno.env.get("GAS_INDUSTRY_SHARE") ?? "0.5");

    const end = new Date();
    const start = new Date(end.getTime() - days * 24 * 60 * 60 * 1000);
    const from = isoDate(start);
    const to = isoDate(end);
    const shareYear = Number(Deno.env.get("GAS_SHARE_YEAR") ?? String(new Date().getUTCFullYear() - 2));
    const shareLookbackYears = Number(Deno.env.get("GAS_SHARE_LOOKBACK_YEARS") ?? "6");

    const supabase = createClient(supabaseUrl, serviceRole, {
      auth: { persistSession: false, autoRefreshToken: false },
    });

    const results: any[] = [];
    const errors: Record<string, string> = {};

    for (const c of countries) {
      try {
        const best = await fetchBestAvailableEurostatShares(c, shareYear, shareLookbackYears);
        const euroShares = best?.shares ?? null;
        const hhShareNonpower = euroShares?.household ?? fallbackHouseholdShare;
        const indShareNonpower = euroShares?.industry ?? fallbackIndustryShare;
        const sourceSplit = euroShares
          ? best?.mode === "exact"
            ? "eurostat_exact_year_nrg_bal_c"
            : `eurostat_previous_year_nrg_bal_c:${best?.yearUsed}`
          : "fallback_env_shares_nonpower";

        const directions = await fetchInterconnectionDirections(c);
        const tids = directions.map((d) => d.tsoItemIdentifier);
        const flowRows = tids.length ? await fetchPhysicalFlowsDaily(tids, from, to) : [];
        const netImports = computeNetImportsByDay(directions, flowRows);

        const gieRows = await fetchGieStorage(c, from, to, gieKey);
        const netWithdrawals = computeNetWithdrawalByDay(gieRows);

        const euroIcObsByMonth = await fetchEurostatIcObsMonthlyTj(c);

        // ENTSO-E gas generation (B04) for the window
        const domain = ENTSOE_DOMAIN[c];
        const gasPowerByDay = new Map<string, number>();
        if (domain) {
          const xml = await fetchEntsoeA75Xml(entsoeToken, domain, start, end);
          const elecByDay = parseEntsoeGasGenerationMwhByDay(xml);
          for (const [day, mwh] of elecByDay) {
            gasPowerByDay.set(day, (gasPowerByDay.get(day) || 0) + mwh / Math.max(efficiency, 1e-6));
          }
        }

        // Assemble day inputs
        const byMonth = new Map<string, DayInput[]>();
        for (let i = 0; i <= days; i++) {
          const d = new Date(start.getTime() + i * 24 * 60 * 60 * 1000);
          const day = isoDate(d);
          const ym = day.slice(0, 7);
          const netImp = netImports.get(day) || 0;
          const netWd = netWithdrawals.get(day) || 0;
          const totalUnclamped = netImp + netWd;
          const impliedTotal = Math.max(0, totalUnclamped);
          const rawPower = gasPowerByDay.get(day) || 0;
          const euroTj = euroIcObsByMonth[ym] ?? null;
          const euroMonthMwh = euroTj != null && Number.isFinite(euroTj) ? tjToMwh(euroTj) : null;
          const euroDayMwh = euroMonthMwh != null ? euroMonthMwh / daysInMonth(day) : null;
          const impliedOk = classifyImpliedOk(impliedTotal, rawPower);
          const di: DayInput = {
            day,
            netImp,
            netWd,
            totalUnclamped,
            impliedTotal,
            impliedOk,
            rawPower,
            euroTj: euroTj != null && Number.isFinite(euroTj) ? euroTj : null,
            euroMonthMwh,
            euroDayMwh,
          };
          if (!byMonth.has(ym)) byMonth.set(ym, []);
          byMonth.get(ym)!.push(di);
        }

        const rows: any[] = [];
        for (const [ym, inputs] of byMonth) {
          rows.push(
            ...budgetMonth(
              c,
              methodVersion,
              efficiency,
              domain,
              tids.length,
              ym,
              inputs,
              hhShareNonpower,
              indShareNonpower,
              sourceSplit,
              shareYear,
              best?.yearUsed ?? null,
              euroShares ? "eurostat" : "fallback_env",
            ),
          );
        }

        const chunkSize = 200;
        for (let i = 0; i < rows.length; i += chunkSize) {
          const chunk = rows.slice(i, i + chunkSize);
          const { error } = await supabase.from("gas_demand_daily").upsert(chunk, {
            onConflict: "method_version,country_code,gas_day",
          });
          if (error) throw new Error(error.message);
        }

        results.push({ country: c, ok: true, days: rows.length, interconnections: tids.length, entsoe: Boolean(domain) });
      } catch (e) {
        const msg = (e as Error)?.message ?? String(e);
        errors[c] = msg;
        results.push({ country: c, ok: false, error: msg });
      }
      await sleep(100);
    }

    return json({ ok: true, from, to, days, method_version: methodVersion, results, errors: Object.keys(errors).length });
  } catch (e) {
    const msg = (e as Error)?.message ?? String(e);
    return json({ error: "internal_error", message: msg }, 500);
  }
});
