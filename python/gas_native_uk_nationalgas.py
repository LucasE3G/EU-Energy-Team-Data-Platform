"""
Native UK gas-demand backfill from National Gas (Bruegel methodology).

Uses the National Gas data portal CSV API which returns three daily publication
objects (NTS offtakes, all in kWh):

  PUBOBJ1026  NTS Energy Offtaken, Industrial Offtake Total   -> industry
  PUBOBJ1025  NTS Energy Offtaken, LDZ Offtake Total          -> household (LDZ proxy)
  PUBOBJ1023  NTS Energy Offtaken, Powerstations Total        -> power

Endpoint provides ~5 years of rolling history. Querying older dates returns an
empty CSV, so we iterate year-by-year from ``GAS_NATIVE_UK_START_YEAR``
(default today.year - 5) and accept whatever the API returns.

Values are converted from kWh -> MWh (divide by 1_000).

Source reference: Bruegel uk_scraper.py / uk_demand.py.
"""
from __future__ import annotations

import io
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows

API_URL = "https://data.nationalgas.com/api/find-gas-data-download"
IDS = {
    "PUBOBJ1026": "industry",
    "PUBOBJ1025": "household",  # LDZ offtake (proxy; includes small commercial)
    "PUBOBJ1023": "power",
}


def fetch_year_csv(
    session: requests.Session, year: int, end: date | None = None
) -> pd.DataFrame:
    dfrom = date(year, 1, 1)
    dto = min(end or date(year, 12, 31), date(year, 12, 31))
    if dfrom > dto:
        return pd.DataFrame()
    params = {
        "applicableFor": "Y",
        "dateFrom": f"{dfrom.isoformat()}T00:00:00",
        "dateTo": f"{dto.isoformat()}T23:59:59",
        "dateType": "GASDAY",
        "latestFlag": "Y",
        "ids": ",".join(IDS.keys()),
        "type": "CSV",
    }
    r = session.get(API_URL, params=params, timeout=120)
    r.raise_for_status()
    text = r.text
    if not text or text.strip() == "":
        return pd.DataFrame()
    try:
        return pd.read_csv(io.StringIO(text))
    except Exception as e:
        print(f"  year {year}: failed to parse CSV ({e})", flush=True)
        return pd.DataFrame()


def parse_rows(df: pd.DataFrame) -> Dict[date, Dict[str, float]]:
    """Return {gas_day: {industry_kwh, household_kwh, power_kwh}}."""
    if df.empty:
        return {}
    needed = {"Applicable For", "Data Item", "Value"}
    if not needed.issubset(df.columns):
        return {}

    by_day: Dict[date, Dict[str, float]] = defaultdict(dict)
    name_to_sector = {
        "NTS Energy Offtaken, Industrial Offtake Total": "industry",
        "NTS Energy Offtaken, LDZ Offtake Total": "household",
        "NTS Energy Offtaken, Powerstations Total": "power",
    }

    for _, row in df.iterrows():
        try:
            d = datetime.strptime(str(row["Applicable For"]).strip(), "%d/%m/%Y").date()
        except Exception:
            continue
        item = str(row["Data Item"]).strip()
        sector = name_to_sector.get(item)
        if sector is None:
            continue
        try:
            v = float(row["Value"])
        except Exception:
            continue
        by_day[d][sector] = v  # kWh
    return by_day


def build_rows(
    by_day: Dict[date, Dict[str, float]],
    method_version: str,
) -> List[dict]:
    rows: List[dict] = []
    for d in sorted(by_day):
        vals = by_day[d]
        # Require at least one sector; missing sectors treated as 0
        industry_kwh = float(vals.get("industry") or 0.0)
        household_kwh = float(vals.get("household") or 0.0)
        power_kwh = float(vals.get("power") or 0.0)
        total_kwh = industry_kwh + household_kwh + power_kwh
        if total_kwh <= 0:
            continue
        rows.append({
            "country_code": "UK",
            "gas_day": d.isoformat(),
            "method_version": method_version,
            "total_mwh": total_kwh / 1000.0,
            "power_mwh": power_kwh / 1000.0 if "power" in vals else None,
            "household_mwh": household_kwh / 1000.0 if "household" in vals else None,
            "industry_mwh": industry_kwh / 1000.0 if "industry" in vals else None,
            "source_total": "nationalgas_daily",
            "source_split": "nationalgas_daily",
            "source_power": "nationalgas_daily",
            "quality_flag": "native_nationalgas_daily",
            "raw": {
                "source_origin": "native",
                "native_source": "nationalgas",
                "total_selector": "native_nationalgas_daily",
                "total_budget_mode": "native_source_no_budgeting",
                "nts_industry_kwh": industry_kwh,
                "nts_ldz_kwh": household_kwh,
                "nts_powerstations_kwh": power_kwh,
            },
        })
    return rows


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    today = date.today()
    start_year = int(os.getenv("GAS_NATIVE_UK_START_YEAR") or today.year - 5)
    end = today
    dry_run = (os.getenv("GAS_NATIVE_UK_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    s = make_retrying_session()
    all_days: Dict[date, Dict[str, float]] = {}
    for year in range(start_year, today.year + 1):
        print(f"Fetching NationalGas {year} ...", flush=True)
        df = fetch_year_csv(s, year, end)
        if df.empty:
            print(f"  year {year}: empty (likely outside rolling window)", flush=True)
            continue
        day_map = parse_rows(df)
        all_days.update(day_map)
        print(f"  year {year}: {len(day_map)} days", flush=True)

    rows = build_rows(all_days, method_version)
    print(f"Built {len(rows)} UK native rows.")
    if rows:
        first, last = rows[0], rows[-1]
        print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh")
        print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh")

    if dry_run:
        print("Dry-run: not upserting.")
        return
    if not rows:
        return
    print(f"Upserting {len(rows)} UK native rows ...")
    upsert_rows(s, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
