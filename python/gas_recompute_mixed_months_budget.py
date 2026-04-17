"""
Re-apply month-level budgeting to `gas_demand_daily` rows so that monthly totals
never exceed the Eurostat monthly gas demand (IC_OBS) target, and so that Eurostat
monthly allocation only ever fills the *remainder* not already covered by implied
daily values.

Rule: *never rewrite observed (implied) daily values*. Only touch days that were
on the Eurostat monthly fallback, and allocate only the remainder:

  For each (country, month) with Eurostat monthly target T and observed implied sum S:
    - Implied-ok days keep their implied_total_mwh exactly.
    - Fallback days get a share of max(0, T - S), weighted by raw_power_mwh
      (uniform if all weights are 0). This guarantees month sum <= T (plus any
      excess S beyond T, which we accept as the observed signal).
    - If the month has no Eurostat target, nothing is changed.
    - If the month has a target but no fallback days at all: nothing is changed.
      Implied stays as observed even if it doesn't equal T.

Sector split on every updated row: power = min(raw_power_mwh, total); non-power is
split by hh_share_nonpower / ind_share_nonpower from raw.

Run modes (env vars):
  GAS_REBUDGET_METHOD_VERSION    target method_version (default v2_bruegel_power_entsoe)
  GAS_REBUDGET_COUNTRIES         comma-separated country list; empty => all
  GAS_REBUDGET_DRY_RUN           1/true => don't upsert, only write report
  GAS_REBUDGET_PAGE_SIZE         Supabase page size (default 5000)
  GAS_REBUDGET_REPORT_PATH       report output path (default python/mixed_months_rebudget_report.json)
  GAS_REBUDGET_MAX_MONTHS        safety cap on months to rebudget (default 1_000_000)
"""

from __future__ import annotations

import os
import json
import statistics
import time
from calendar import monthrange
from collections import defaultdict
from datetime import date
from typing import DefaultDict, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv


def _allocate_remainder_capped(
    remainder: float,
    fallback_day_keys: List[str],
    implied_day_totals: List[float],
    days_in_month: int,
) -> Tuple[Dict[str, float], float]:
    """Distribute remainder uniformly across fallback days with a per-day cap.

    Cap = max(2.5 * median(implied_day_totals>0), 1.2 * monthly_average);
    fallback to max(2.0 * monthly_average, remainder / F) when no implied days.
    Returns (alloc_by_day, unallocated_mwh) so spikes cannot exceed the cap.
    """
    F = len(fallback_day_keys)
    if F == 0:
        return {}, remainder
    if remainder <= 0:
        return {k: 0.0 for k in fallback_day_keys}, 0.0

    monthly_total_est = remainder + sum(implied_day_totals)
    monthly_avg = monthly_total_est / max(days_in_month, 1)
    positive = [t for t in implied_day_totals if t > 0]
    if positive:
        med = statistics.median(positive)
        cap = max(2.5 * med, 1.2 * monthly_avg)
    else:
        cap = max(2.0 * monthly_avg, remainder / F)

    alloc: Dict[str, float] = {k: remainder / F for k in fallback_day_keys}
    for _ in range(10):
        overflow = 0.0
        for k in fallback_day_keys:
            if alloc[k] > cap:
                overflow += alloc[k] - cap
                alloc[k] = cap
        if overflow <= 1e-6:
            return alloc, 0.0
        uncapped = [k for k in fallback_day_keys if alloc[k] < cap - 1e-9]
        if not uncapped:
            return alloc, overflow
        share = overflow / len(uncapped)
        for k in uncapped:
            alloc[k] += share
    final_unalloc = 0.0
    for k in fallback_day_keys:
        if alloc[k] > cap:
            final_unalloc += alloc[k] - cap
            alloc[k] = cap
    return alloc, final_unalloc

try:
    from python.gas_entsog_gie_implied import make_retrying_session  # type: ignore
except ModuleNotFoundError:
    from gas_entsog_gie_implied import make_retrying_session  # type: ignore


def supabase_headers(service_role: str) -> dict:
    return {
        "apikey": service_role,
        "Authorization": f"Bearer {service_role}",
        "Content-Type": "application/json",
    }


def supabase_upsert_headers(service_role: str) -> dict:
    h = supabase_headers(service_role)
    h["Prefer"] = "resolution=merge-duplicates,return=minimal"
    return h


def fetch_rows_page(
    s: requests.Session,
    supabase_url: str,
    service_role: str,
    method_version: str,
    limit: int,
    offset: int,
    country: Optional[str] = None,
) -> List[dict]:
    base = f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily"
    params: Dict[str, str] = {
        "select": "country_code,gas_day,total_mwh,power_mwh,household_mwh,industry_mwh,source_total,source_split,source_power,quality_flag,raw,method_version",
        "method_version": f"eq.{method_version}",
        "order": "country_code.asc,gas_day.asc",
        "limit": str(limit),
        "offset": str(offset),
    }
    if country:
        params["country_code"] = f"eq.{country}"
    # Use Range header so Supabase returns more than its default cap when applicable.
    headers = supabase_headers(service_role)
    headers["Range-Unit"] = "items"
    headers["Range"] = f"{offset}-{offset + limit - 1}"
    r = s.get(base, headers=headers, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def _dedupe_rows(rows: List[dict]) -> List[dict]:
    """Keep the last occurrence for each (method_version, country_code, gas_day) key.
    PostgREST 409s if a batch contains duplicate conflict keys.
    """
    by_key: Dict[Tuple[str, str, str], dict] = {}
    for r in rows:
        k = (str(r.get("method_version")), str(r.get("country_code")), str(r.get("gas_day"))[:10])
        by_key[k] = r
    return list(by_key.values())


def upsert_rows(s: requests.Session, supabase_url: str, service_role: str, rows: List[dict]) -> None:
    if not rows:
        return
    rows = _dedupe_rows(rows)
    url = f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily?on_conflict=method_version,country_code,gas_day"
    headers = supabase_upsert_headers(service_role)
    chunk = 200
    base_sleep = 1.0
    max_retries = 6
    for i in range(0, len(rows), chunk):
        payload = rows[i : i + chunk]
        attempt = 0
        while True:
            attempt += 1
            r = s.post(url, headers=headers, json=payload, timeout=60)
            if r.ok:
                break
            if r.status_code in (408, 425, 429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep(base_sleep * (2 ** (attempt - 1)))
                continue
            raise RuntimeError(f"Supabase upsert failed HTTP {r.status_code}: {r.text[:500]}")
        time.sleep(0.05)


def classify_day(raw: dict) -> str:
    """Return 'implied_ok' or 'fallback' using raw fields produced by v2 backfill."""
    implied = float(raw.get("implied_total_mwh") or 0.0)
    raw_power = float(raw.get("raw_power_mwh") or 0.0)
    if implied > 0 and (raw_power <= 0 or implied >= raw_power):
        return "implied_ok"
    return "fallback"


def month_is_complete(rows: List[dict], ym: str) -> bool:
    """Check whether our rows cover every calendar day of `ym`."""
    y = int(ym[:4])
    m = int(ym[5:7])
    expected = monthrange(y, m)[1]
    seen_days = {str(r["gas_day"])[:10] for r in rows}
    return len(seen_days) == expected


def rebudget_month(
    country: str,
    ym: str,
    rows: List[dict],
    method_version: str,
) -> Tuple[List[dict], dict]:
    """Return (updated_rows, report_entry). updated_rows is empty if no change needed."""
    rows = sorted(rows, key=lambda x: str(x["gas_day"]))

    month_target = None
    for r in rows:
        raw = r.get("raw") or {}
        mt = raw.get("eurostat_ic_obs_month_mwh")
        if mt is not None:
            try:
                month_target = float(mt)
                break
            except Exception:
                continue

    if month_target is None:
        return [], {
            "country_code": country,
            "month": ym,
            "days": len(rows),
            "status": "skipped_no_eurostat_target",
        }

    implied_days: List[dict] = []
    fallback_days: List[dict] = []
    for r in rows:
        raw = r.get("raw") or {}
        if classify_day(raw) == "implied_ok":
            implied_days.append(r)
        else:
            fallback_days.append(r)

    implied_sum = sum(float((r.get("raw") or {}).get("implied_total_mwh") or 0.0) for r in implied_days)
    complete = month_is_complete(rows, ym)
    y_i, m_i = int(ym[:4]), int(ym[5:7])
    expected_days = monthrange(y_i, m_i)[1]

    def _build_fallback_row(r: dict, total: float, selector: str, source_total: str,
                            budget_mode: str, remainder_val: Optional[float],
                            unallocated: Optional[float]) -> dict:
        raw = dict(r.get("raw") or {})
        raw_power = float(raw.get("raw_power_mwh") or 0.0)
        power = min(raw_power, total)
        if total <= 0:
            quality_flag = "fallback_zero_month_incomplete_or_no_budget"
        elif raw_power > total:
            quality_flag = "power_capped_to_total"
        else:
            quality_flag = "eurostat_fallback_allocated"
        nonpower = max(0.0, total - power)
        hh_share = float(raw.get("hh_share_nonpower") or 0.5)
        ind_share = float(raw.get("ind_share_nonpower") or 0.5)
        hh = nonpower * hh_share
        ind = nonpower * ind_share
        ind += nonpower - (hh + ind)
        raw.update(
            {
                "total_selector": selector,
                "total_budget_mode": budget_mode,
                "month_target_mwh": month_target,
                "month_implied_sum_mwh": implied_sum,
                "month_remainder_mwh": remainder_val,
                "month_unallocated_mwh": unallocated,
                "month_scale": None,
                "month_is_complete": complete,
                "day_is_fallback": True,
            }
        )
        return {
            "country_code": country,
            "gas_day": r["gas_day"],
            "method_version": method_version,
            "total_mwh": total,
            "power_mwh": power if total > 0 else 0.0,
            "household_mwh": hh if total > 0 else 0.0,
            "industry_mwh": ind if total > 0 else 0.0,
            "source_total": source_total,
            "source_power": "entsoe_a75_b04",
            "source_split": r.get("source_split") or (r.get("raw") or {}).get("source_split") or "eurostat_annual_nrg_bal_c",
            "quality_flag": quality_flag,
            "raw": raw,
        }

    if not fallback_days:
        return [], {
            "country_code": country,
            "month": ym,
            "days": len(rows),
            "complete_month": complete,
            "target_mwh": month_target,
            "implied_sum_mwh": implied_sum,
            "status": "no_fallback_days_left_untouched",
        }

    # Month incomplete: never apply Eurostat monthly budget. Reset fallback days
    # to 0 (and store provenance), to avoid dumping a full-month total on the
    # few fallback days present so far.
    if not complete:
        updated = [
            _build_fallback_row(
                r,
                total=0.0,
                selector="fallback_no_data_month_incomplete",
                source_total="none_month_incomplete",
                budget_mode="month_incomplete_eurostat_budget_skipped",
                remainder_val=None,
                unallocated=None,
            )
            for r in fallback_days
        ]
        return updated, {
            "country_code": country,
            "month": ym,
            "days": len(rows),
            "complete_month": False,
            "target_mwh": month_target,
            "implied_sum_mwh": implied_sum,
            "implied_days": len(implied_days),
            "fallback_days": len(fallback_days),
            "status": "month_incomplete_fallbacks_zeroed",
        }

    remainder = max(0.0, month_target - implied_sum)
    fallback_keys = [r["gas_day"] for r in fallback_days]
    implied_totals = [float((r.get("raw") or {}).get("implied_total_mwh") or 0.0) for r in implied_days]
    alloc_by_day, unallocated = _allocate_remainder_capped(
        remainder=remainder,
        fallback_day_keys=fallback_keys,
        implied_day_totals=implied_totals,
        days_in_month=expected_days,
    )

    updated = [
        _build_fallback_row(
            r,
            total=float(alloc_by_day.get(r["gas_day"], 0.0)),
            selector="budget_eurostat_allocated_remainder_capped",
            source_total="eurostat_nrg_cb_gasm_ic_obs_monthly_budgeted",
            budget_mode="implied_untouched_eurostat_fills_remainder_capped",
            remainder_val=remainder,
            unallocated=unallocated,
        )
        for r in fallback_days
    ]

    return updated, {
        "country_code": country,
        "month": ym,
        "days": len(rows),
        "complete_month": True,
        "target_mwh": month_target,
        "implied_sum_mwh": implied_sum,
        "remainder_mwh": remainder,
        "month_unallocated_mwh": unallocated,
        "implied_days": len(implied_days),
        "fallback_days": len(fallback_days),
        "status": "fallback_allocated_remainder_capped",
    }


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_REBUDGET_METHOD_VERSION", "v2_bruegel_power_entsoe")
    # Supabase defaults max-rows=1000. Keep page size at/under the server cap and paginate via offset.
    page_size = int(os.getenv("GAS_REBUDGET_PAGE_SIZE", "1000"))
    dry_run = os.getenv("GAS_REBUDGET_DRY_RUN", "0").strip() in ("1", "true", "TRUE", "yes", "YES")
    max_months = int(os.getenv("GAS_REBUDGET_MAX_MONTHS", "1000000"))
    countries_env = (os.getenv("GAS_REBUDGET_COUNTRIES") or "").strip()
    countries = [c.strip().upper() for c in countries_env.split(",") if c.strip()] if countries_env else []

    s = make_retrying_session()

    def fetch_all_for_country(country: Optional[str]) -> List[dict]:
        acc: List[dict] = []
        offset = 0
        while True:
            page = fetch_rows_page(
                s, supabase_url, service_role, method_version, page_size, offset, country=country
            )
            if not page:
                break
            acc.extend(page)
            offset += len(page)
            if len(page) < page_size:
                break
            if offset % 10000 == 0:
                print(f"  fetched {offset} rows{' for ' + country if country else ''} so far...")
        return acc

    if countries:
        all_rows: List[dict] = []
        for c in countries:
            print(f"fetching {c} ...")
            all_rows.extend(fetch_all_for_country(c))
    else:
        print("fetching all countries ...")
        all_rows = fetch_all_for_country(None)

    by_cm: DefaultDict[Tuple[str, str], List[dict]] = defaultdict(list)
    for r in all_rows:
        day = str(r["gas_day"])[:10]
        ym = day[:7]
        by_cm[(r["country_code"], ym)].append(r)

    print(f"Scanned rows={len(all_rows)} country-months={len(by_cm)}")

    report: List[dict] = []
    to_upsert: List[dict] = []
    processed = 0
    for (c, ym), rows in by_cm.items():
        if processed >= max_months:
            break
        updated, entry = rebudget_month(c, ym, rows, method_version)
        report.append(entry)
        if updated:
            to_upsert.extend(updated)
        processed += 1

    if not dry_run and to_upsert:
        print(f"Upserting {len(to_upsert)} rows ...")
        upsert_rows(s, supabase_url, service_role, to_upsert)

    out_path = os.getenv("GAS_REBUDGET_REPORT_PATH", "python/mixed_months_rebudget_report.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(f"Done. Report written: {out_path} months={len(report)} upserted={0 if dry_run else len(to_upsert)} dry_run={dry_run}")


if __name__ == "__main__":
    main()
