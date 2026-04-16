import os
import json
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import DefaultDict, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from python.gas_backfill_5y_bruegel import compute_country_backfill  # type: ignore
    from python.gas_entsog_gie_implied import make_retrying_session, supabase_upsert_gas_rows  # type: ignore
except ModuleNotFoundError:
    from gas_backfill_5y_bruegel import compute_country_backfill  # type: ignore
    from gas_entsog_gie_implied import make_retrying_session, supabase_upsert_gas_rows  # type: ignore


def supabase_select_rows(
    session: requests.Session,
    supabase_url: str,
    service_role_key: str,
    method_version: str,
    start: Optional[str],
    end: Optional[str],
    limit: int = 5000,
    offset: int = 0,
) -> List[dict]:
    # Pull rows where total_mwh==0 or household/industry==0; PostgREST filter is numeric eq.0
    # NOTE: we can't easily filter on raw_power_mwh, so we start broad and then narrow in Python.
    base = (
        f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily"
        f"?select=country_code,gas_day,total_mwh,power_mwh,household_mwh,industry_mwh,quality_flag,raw,method_version"
        f"&method_version=eq.{method_version}"
        f"&or=(total_mwh.eq.0,power_mwh.eq.0,household_mwh.eq.0,industry_mwh.eq.0)"
    )
    if start:
        base += f"&gas_day=gte.{start}"
    if end:
        base += f"&gas_day=lte.{end}"
    url = f"{base}&order=country_code.asc,gas_day.asc&limit={limit}&offset={offset}"
    headers = {"apikey": service_role_key, "Authorization": f"Bearer {service_role_key}"}
    r = session.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


def group_into_ranges(rows: List[dict]) -> Dict[str, List[Tuple[date, date]]]:
    by_country: DefaultDict[str, List[date]] = defaultdict(list)
    for r in rows:
        by_country[str(r["country_code"])].append(date.fromisoformat(str(r["gas_day"])[:10]))

    ranges: Dict[str, List[Tuple[date, date]]] = {}
    for c, days in by_country.items():
        days = sorted(set(days))
        if not days:
            continue
        out: List[Tuple[date, date]] = []
        a = b = days[0]
        for d in days[1:]:
            if d == b + timedelta(days=1):
                b = d
            else:
                out.append((a, b))
                a = b = d
        out.append((a, b))
        ranges[c] = out
    return ranges


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    gie_key = os.getenv("GIE_API_KEY")
    entsoe_token = os.getenv("ENTSOE_API_TOKEN")
    if not supabase_url or not service_role or not gie_key or not entsoe_token:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY / GIE_API_KEY / ENTSOE_API_TOKEN")

    method_version = os.getenv("GAS_FIX_METHOD_VERSION", "v2_bruegel_power_entsoe")
    # If GAS_FIX_START/END are not set, we scan the entire table for method_version.
    fix_start = (os.getenv("GAS_FIX_START") or "").strip() or None
    fix_end = (os.getenv("GAS_FIX_END") or "").strip() or None
    # Still used for range expansion if start/end are provided.
    end_d = date.fromisoformat(fix_end) if fix_end else date.today()
    start_d = date.fromisoformat(fix_start) if fix_start else (end_d - timedelta(days=int(os.getenv("GAS_FIX_DAYS", "3650"))))
    mode = (os.getenv("GAS_FIX_MODE", "broad") or "broad").strip().lower()
    page_size = int(os.getenv("GAS_FIX_PAGE_SIZE", "5000"))

    start = fix_start
    end = fix_end

    s = make_retrying_session()
    # Paginate (PostgREST default limits can hide results if we don't page)
    candidates: List[dict] = []
    offset = 0
    while True:
        page = supabase_select_rows(
            s, supabase_url, service_role, method_version, start, end, limit=page_size, offset=offset
        )
        if not page:
            break
        candidates.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    # Decide which zeros we want to treat as "suspicious" for recomputation.
    # broad: recompute ANY row where any of the 4 columns is 0.
    # strict: only recompute if we have evidence the 0 is likely due to missing/inconsistent components.
    suspicious: List[dict] = []
    for r in candidates:
        total = float(r.get("total_mwh") or 0.0)
        power = float(r.get("power_mwh") or 0.0)
        hh = float(r.get("household_mwh") or 0.0)
        ind = float(r.get("industry_mwh") or 0.0)
        has_any_zero = (total == 0.0) or (power == 0.0) or (hh == 0.0) or (ind == 0.0)
        if not has_any_zero:
            continue
        raw = r.get("raw") or {}
        raw_power = float(raw.get("raw_power_mwh") or 0.0)
        wd = float(raw.get("net_withdrawal_mwh") or 0.0)
        unclamped = float(raw.get("total_unclamped_mwh") or 0.0)
        qf = str(r.get("quality_flag") or "")
        tso_ids = int(raw.get("tso_item_identifiers") or 0)

        if mode == "broad":
            suspicious.append(r)
            continue

        # strict mode
        if qf == "power_capped_to_total":
            suspicious.append(r)
        elif raw_power > 0.0:
            suspicious.append(r)
        elif wd > 0.0:
            suspicious.append(r)
        elif unclamped != 0.0:
            suspicious.append(r)
        elif tso_ids > 0:
            # net imports may have been negative and clamped (still worth recompute)
            suspicious.append(r)

    if not suspicious:
        print(f"No suspicious zeros found in the selected window (mode={mode}, rows_scanned={len(candidates)}).")
        return

    ranges = group_into_ranges(suspicious)
    print(f"Found suspicious zero ranges for {len(ranges)} countries.")

    persisted_report: List[dict] = []

    for country, rr in ranges.items():
        for a, b in rr:
            # Expand 1 day on both sides to handle boundary effects
            aa = max(start_d, a - timedelta(days=1))
            bb = min(end_d, b + timedelta(days=1))
            print(f"{country}: recomputing {aa} -> {bb}")
            rows = compute_country_backfill(country, aa, bb, gie_apikey=gie_key, entsoe_token=entsoe_token, session=s)
            for x in rows:
                x["method_version"] = method_version
            supabase_upsert_gas_rows(supabase_url, service_role, rows)
            # After recompute: explain any 0s that persist in this window.
            for x in rows:
                total = float(x.get("total_mwh") or 0.0)
                power = float(x.get("power_mwh") or 0.0)
                hh = float(x.get("household_mwh") or 0.0)
                ind = float(x.get("industry_mwh") or 0.0)
                if not (total == 0.0 or power == 0.0 or hh == 0.0 or ind == 0.0):
                    continue
                raw = x.get("raw") or {}
                net_imp = float(raw.get("net_imports_mwh") or 0.0)
                net_wd = float(raw.get("net_withdrawal_mwh") or 0.0)
                total_unclamped = float(raw.get("total_unclamped_mwh") or (net_imp + net_wd))
                raw_power = float(raw.get("raw_power_mwh") or 0.0)
                tso_ids = int(raw.get("tso_item_identifiers") or 0)
                hh_share = raw.get("hh_share_nonpower")
                ind_share = raw.get("ind_share_nonpower")

                nonpower = max(0.0, total - power)

                reasons: Dict[str, str] = {}
                if total == 0.0:
                    if tso_ids == 0:
                        reasons["total_mwh"] = "no_entsog_interconnections"
                    elif total_unclamped < 0:
                        reasons["total_mwh"] = "implied_total_negative_clamped"
                    elif total_unclamped == 0 and net_wd == 0 and abs(net_imp) < 1e-6:
                        reasons["total_mwh"] = "all_components_zero"
                    elif total_unclamped == 0 and net_wd == 0:
                        reasons["total_mwh"] = "no_gie_withdrawal_and_net_imports_zero"
                    elif total_unclamped == 0 and abs(net_imp) > 0:
                        reasons["total_mwh"] = "net_imports_cancelled_or_missing_components"
                    else:
                        reasons["total_mwh"] = "unknown_zero"

                if power == 0.0:
                    if total == 0.0 and raw_power > 0.0:
                        reasons["power_mwh"] = "power_capped_because_total_zero"
                    elif raw_power == 0.0:
                        reasons["power_mwh"] = "entsoe_gas_generation_zero_or_missing"
                    else:
                        reasons["power_mwh"] = "unknown_zero"

                if hh == 0.0:
                    if nonpower == 0.0:
                        reasons["household_mwh"] = "nonpower_zero"
                    elif hh_share == 0 or hh_share == 0.0:
                        reasons["household_mwh"] = "household_share_zero"
                    else:
                        reasons["household_mwh"] = "rounding_or_underflow"

                if ind == 0.0:
                    if nonpower == 0.0:
                        reasons["industry_mwh"] = "nonpower_zero"
                    elif ind_share == 0 or ind_share == 0.0:
                        reasons["industry_mwh"] = "industry_share_zero"
                    else:
                        reasons["industry_mwh"] = "rounding_or_underflow"

                persisted_report.append(
                    {
                        "country_code": x.get("country_code"),
                        "gas_day": x.get("gas_day"),
                        "method_version": method_version,
                        "quality_flag": x.get("quality_flag"),
                        "reasons": reasons,
                        "net_imports_mwh": net_imp,
                        "net_withdrawal_mwh": net_wd,
                        "total_unclamped_mwh": total_unclamped,
                        "raw_power_mwh": raw_power,
                        "total_mwh": total,
                        "power_mwh": power,
                        "household_mwh": hh,
                        "industry_mwh": ind,
                        "nonpower_mwh": nonpower,
                        "tso_item_identifiers": tso_ids,
                        "entsoe_domain": raw.get("entsoe_domain"),
                    }
                )
            time.sleep(0.2)

    report_path = os.getenv("GAS_ZERO_REPORT_PATH", "python/zero_persistence_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(persisted_report, f, indent=2)

    print(f"Done. Wrote report: {report_path} (rows: {len(persisted_report)})")


if __name__ == "__main__":
    main()

