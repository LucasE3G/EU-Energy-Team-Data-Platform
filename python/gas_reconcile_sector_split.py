import os
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

try:
    from python.gas_backfill_5y_bruegel import fetch_best_available_shares  # type: ignore
except ModuleNotFoundError:
    from gas_backfill_5y_bruegel import fetch_best_available_shares  # type: ignore


EU27 = [
    "AT",
    "BE",
    "BG",
    "HR",
    "CY",
    "CZ",
    "DK",
    "EE",
    "FI",
    "FR",
    "DE",
    "GR",
    "HU",
    "IE",
    "IT",
    "LV",
    "LT",
    "LU",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SK",
    "SI",
    "ES",
    "SE",
]


def supabase_admin_headers(service_role_key: str) -> dict:
    return {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def fetch_rows(
    supabase_url: str,
    service_role_key: str,
    country: str,
    start: str,
    end: str,
    method_version: str,
) -> List[dict]:
    url = (
        f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily"
        f"?select=country_code,gas_day,total_mwh,power_mwh,method_version"
        f"&country_code=eq.{country}"
        f"&method_version=eq.{method_version}"
        f"&gas_day=gte.{start}&gas_day=lte.{end}"
        f"&order=gas_day.asc"
        f"&limit=5000"
    )
    r = requests.get(url, headers=supabase_admin_headers(service_role_key), timeout=60)
    r.raise_for_status()
    return r.json()


def upsert_rows(supabase_url: str, service_role_key: str, rows: List[dict]) -> None:
    if not rows:
        return
    url = f"{supabase_url.rstrip('/')}/rest/v1/gas_demand_daily?on_conflict=method_version,country_code,gas_day"
    r = requests.post(url, headers=supabase_admin_headers(service_role_key), json=rows, timeout=60)
    r.raise_for_status()


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_RECONCILE_METHOD_VERSION", "v1")
    days = int(os.getenv("GAS_RECONCILE_DAYS", "60"))
    max_lookback = int(os.getenv("GAS_SHARE_LOOKBACK_YEARS", "6"))

    end_d = date.today()
    start_d = end_d - timedelta(days=days)
    start = start_d.isoformat()
    end = end_d.isoformat()

    countries_env = os.getenv("GAS_COUNTRIES")
    countries = [c.strip().upper() for c in countries_env.split(",")] if countries_env else list(EU27)

    s = requests.Session()
    for c in countries:
        rows = fetch_rows(supabase_url, service_role, c, start, end, method_version=method_version)
        if not rows:
            continue

        patched: List[dict] = []
        for r in rows:
            gas_day = r["gas_day"]
            y = int(str(gas_day)[:4])

            sh, year_used = fetch_best_available_shares(c, y, session=s, max_lookback_years=max_lookback)
            if not sh or year_used is None:
                continue  # can't improve

            total = float(r.get("total_mwh") or 0.0)
            power = float(r.get("power_mwh") or 0.0)
            nonpower = max(0.0, total - min(power, total))

            hh_share = float(sh["household"])
            ind_share = float(sh["industry"])
            hh = nonpower * hh_share
            ind = nonpower * ind_share
            ind += nonpower - (hh + ind)

            patched.append(
                {
                    "country_code": c,
                    "gas_day": gas_day,
                    "method_version": method_version,
                    "household_mwh": hh,
                    "industry_mwh": ind,
                    "source_split": "eurostat_exact_year_nrg_bal_c" if year_used == y else f"eurostat_previous_year_nrg_bal_c:{year_used}",
                    "raw": {"shares_reconciled": {"year_used": year_used, "year_target": y}},
                }
            )

        if patched:
            upsert_rows(supabase_url, service_role, patched)
            print(f"{c}: updated {len(patched)} rows ({start}..{end})")
        time.sleep(0.2)


if __name__ == "__main__":
    main()

