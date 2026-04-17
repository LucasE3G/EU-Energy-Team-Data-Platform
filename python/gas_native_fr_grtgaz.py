"""
Native France gas-demand backfill from GRTGaz (Bruegel methodology).

GRTGaz publishes the official French transmission system daily demand per sector
(total, industry, power, household, pirr=network balancing) in kWh as XLS files
downloadable per year at smart.grtgaz.com. This script:

  1. Downloads each year's XLS (2021..current year).
  2. Parses it with pandas (Bruegel's signature: skiprows=2, 6 columns).
  3. Converts kWh -> MWh.
  4. Upserts rows into public.gas_demand_daily with method_version unchanged
     (v2_bruegel_power_entsoe) so the UI picks them up immediately, but with
     a native source tag so the Eurostat rebudget leaves them untouched.

Run:  python python/gas_native_fr_grtgaz.py

Env:
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY                 (required)
  GAS_NATIVE_FR_START_YEAR                                (default: current_year - 4)
  GAS_NATIVE_FR_END_YEAR                                  (default: current_year)
  GAS_METHOD_VERSION                                      (default: v2_bruegel_power_entsoe)
  GAS_NATIVE_FR_DRY_RUN                                   ("1" to skip upsert)
"""
from __future__ import annotations
import io
import os
import sys
import time
from datetime import date
from typing import List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# Reuse robust upsert from the rebudget module.
from gas_recompute_mixed_months_budget import (
    make_retrying_session,
    upsert_rows,
)


GRTGAZ_BASE = "https://smart.grtgaz.com"
GRTGAZ_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/vnd.ms-excel",
    "Referer": "https://smart.grtgaz.com/en/consommation",
}


def download_year(session: requests.Session, year: int) -> Optional[bytes]:
    url = (
        f"{GRTGAZ_BASE}/api/v1/en/consommation/export/Zone.xls"
        f"?startDate={year}-01-01&endDate={year}-12-31&range=daily"
    )
    for attempt in range(1, 4):
        try:
            r = session.get(url, headers=GRTGAZ_HEADERS, timeout=90)
            if r.status_code == 200 and r.content and len(r.content) > 1000:
                return r.content
            print(f"  year={year} attempt {attempt}: HTTP {r.status_code} size={len(r.content)}")
        except Exception as e:
            print(f"  year={year} attempt {attempt} error: {e}")
        time.sleep(2 * attempt)
    return None


def parse_year_xls(blob: bytes) -> pd.DataFrame:
    """Return dataframe with columns: dates (datetime), total/industry/power/pirr/household (kWh)."""
    df = pd.read_excel(
        io.BytesIO(blob),
        engine="openpyxl",
        skiprows=2,
        usecols=[0, 1, 2, 3, 4, 5],
        names=["dates", "total", "industry", "power", "pirr", "household"],
    )
    df["dates"] = pd.to_datetime(df["dates"], errors="coerce")
    df = df.dropna(subset=["dates"])
    return df


def make_row(method_version: str, day: date, total_kwh: float, industry_kwh: float,
             power_kwh: float, pirr_kwh: float, household_kwh: float) -> dict:
    # Bruegel-style sector allocation: industry absorbs pirr (network balancing residual)
    # so total = household + industry(+pirr) + power.
    total_mwh = float(total_kwh) / 1000.0
    power_mwh = float(power_kwh) / 1000.0
    household_mwh = float(household_kwh) / 1000.0
    industry_mwh = (float(industry_kwh) + float(pirr_kwh)) / 1000.0

    return {
        "country_code": "FR",
        "gas_day": day.isoformat(),
        "method_version": method_version,
        "total_mwh": total_mwh,
        "power_mwh": power_mwh,
        "household_mwh": household_mwh,
        "industry_mwh": industry_mwh,
        "source_total": "grtgaz_daily",
        "source_split": "grtgaz_daily",
        "source_power": "grtgaz_daily",
        "quality_flag": "native_grtgaz_daily",
        "raw": {
            "source_origin": "native",
            "native_source": "grtgaz",
            "total_selector": "native_grtgaz_daily",
            "total_budget_mode": "native_source_no_budgeting",
            "grtgaz_total_kwh": float(total_kwh),
            "grtgaz_industry_kwh": float(industry_kwh),
            "grtgaz_power_kwh": float(power_kwh),
            "grtgaz_pirr_kwh": float(pirr_kwh),
            "grtgaz_household_kwh": float(household_kwh),
        },
    }


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    today = date.today()
    start_year = int(os.getenv("GAS_NATIVE_FR_START_YEAR") or today.year - 5)
    end_year = int(os.getenv("GAS_NATIVE_FR_END_YEAR") or today.year)
    dry_run = (os.getenv("GAS_NATIVE_FR_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    s = make_retrying_session()

    all_rows: List[dict] = []
    for year in range(start_year, end_year + 1):
        print(f"Downloading GRTGaz {year} ...")
        blob = download_year(s, year)
        if blob is None:
            print(f"  skipped {year} (download failed)")
            continue
        df = parse_year_xls(blob)
        print(f"  parsed {len(df)} rows "
              f"({df['dates'].min().date()} -> {df['dates'].max().date()})")

        for _, r in df.iterrows():
            d = r["dates"].date()
            if d > today:
                continue
            try:
                total_kwh = float(r["total"])
                if pd.isna(total_kwh) or total_kwh <= 0:
                    continue
                all_rows.append(
                    make_row(
                        method_version,
                        d,
                        total_kwh,
                        float(r["industry"] or 0),
                        float(r["power"] or 0),
                        float(r["pirr"] or 0),
                        float(r["household"] or 0),
                    )
                )
            except Exception as e:
                print(f"  row error {d}: {e}")

    print(f"Built {len(all_rows)} native FR rows.")
    if not all_rows:
        print("Nothing to upsert.")
        return

    # Quick sanity print
    first = all_rows[0]
    last = all_rows[-1]
    print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh "
          f"(HH={first['household_mwh']/1000:.1f}, Ind={first['industry_mwh']/1000:.1f}, "
          f"Pow={first['power_mwh']/1000:.1f})")
    print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh "
          f"(HH={last['household_mwh']/1000:.1f}, Ind={last['industry_mwh']/1000:.1f}, "
          f"Pow={last['power_mwh']/1000:.1f})")

    if dry_run:
        print("Dry-run: not upserting.")
        return

    print(f"Upserting {len(all_rows)} rows to Supabase ...")
    upsert_rows(s, supabase_url, service_role, all_rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
