"""
Native Ireland gas-demand backfill from CSO NGSD02 (Bruegel methodology).

The Central Statistics Office publishes "Networked Gas Daily Demand" as a
JSON-stat 2.0 cube at:

  https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/
  NGSD02/JSON-stat/2.0/en

Dimensions (in order):
  STATISTIC                 -> single value (NGSD02C01, Daily Demand in GWh)
  TLIST(D1)                 -> gas day (e.g. 2018M01D01)
  C04132V04898              -> Networked Gas Customer Type
    20 = Non-Daily Metered (NDM)                                 -> household
    30 = Daily Metered (annual 5.55 - 57.5 GWh)                  -> industry
    40 = Large Daily Metered (annual >= 57.5 GWh)                -> industry
    10 = Power Plants                                            -> power
    -  = All Networked Gas Customers                             -> total

Values are in GWh; we multiply by 1_000 to store MWh.

The dataset is updated regularly (last-update timestamp in the response). Data
lags by ~1-6 months vs today; we upsert whatever is available and let the
ENTSOG-based edge function handle the recent fringe for days without native
data.

Source reference: CSO Ireland (Berna Lawlor, CSO Climate & Energy Division).
Bruegel scraper (ireland_scraper.py) uses Selenium against gasnetworks.ie; the
CSO dataset is the equivalent cleaner direct API.
"""
from __future__ import annotations

import os
import sys
from datetime import date, datetime
from typing import Dict, List

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows

CSO_URL = (
    "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/"
    "NGSD02/JSON-stat/2.0/en"
)


def parse_tlist_day(key: str) -> date | None:
    """Parse CSO time keys like '2018M01D01' -> date(2018, 1, 1)."""
    try:
        y = int(key[0:4])
        m = int(key[5:7])
        d = int(key[8:10])
        return date(y, m, d)
    except Exception:
        return None


def fetch_cso(session: requests.Session) -> dict:
    r = session.get(CSO_URL, timeout=120)
    r.raise_for_status()
    return r.json()


def extract_rows(
    cube: dict,
    start: date,
    end: date,
    method_version: str,
) -> List[dict]:
    size = cube.get("size") or []
    if len(size) != 3:
        raise RuntimeError(f"Unexpected NGSD02 size: {size}")
    n_stat, n_time, n_cust = size

    tlist = cube["dimension"]["TLIST(D1)"]["category"]
    tlist_index = tlist["index"]
    if isinstance(tlist_index, dict):
        time_keys = sorted(tlist_index, key=lambda k: tlist_index[k])
    else:
        time_keys = list(tlist_index)

    cust = cube["dimension"]["C04132V04898"]["category"]
    cust_index = cust["index"]
    if isinstance(cust_index, dict):
        customer_keys = sorted(cust_index, key=lambda k: cust_index[k])
    else:
        customer_keys = list(cust_index)

    sector_map = {"20": "household", "30": "industry", "40": "industry",
                  "10": "power", "-": "total"}

    vals = cube["value"]
    updated = cube.get("updated")

    rows: List[dict] = []
    for ti, tkey in enumerate(time_keys):
        d = parse_tlist_day(tkey)
        if d is None or d < start or d > end:
            continue
        household = industry = power = total = None
        for ci, ckey in enumerate(customer_keys):
            offset = ti * n_cust + ci
            if offset >= len(vals):
                continue
            v = vals[offset]
            if v is None:
                continue
            try:
                gwh = float(v)
            except Exception:
                continue
            sector = sector_map.get(ckey)
            if sector == "household":
                household = gwh
            elif sector == "industry":
                industry = (industry or 0.0) + gwh
            elif sector == "power":
                power = gwh
            elif sector == "total":
                total = gwh

        if total is None:
            pieces = [x for x in (household, industry, power) if x is not None]
            if not pieces:
                continue
            total = sum(pieces)
        if total <= 0:
            continue
        rows.append({
            "country_code": "IE",
            "gas_day": d.isoformat(),
            "method_version": method_version,
            "total_mwh": total * 1000.0,
            "power_mwh": (power * 1000.0) if power is not None else None,
            "household_mwh": (household * 1000.0) if household is not None else None,
            "industry_mwh": (industry * 1000.0) if industry is not None else None,
            "source_total": "cso_ngsd02",
            "source_split": "cso_ngsd02",
            "source_power": "cso_ngsd02",
            "quality_flag": "native_cso_daily",
            "raw": {
                "source_origin": "native",
                "native_source": "cso_ngsd02",
                "total_selector": "native_cso_daily",
                "total_budget_mode": "native_source_no_budgeting",
                "cso_last_updated": updated,
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
    start_year = int(os.getenv("GAS_NATIVE_IE_START_YEAR") or today.year - 5)
    start = date(start_year, 1, 1)
    end = today
    dry_run = (os.getenv("GAS_NATIVE_IE_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    s = make_retrying_session()
    print(f"Fetching CSO NGSD02 (Ireland daily gas demand) ...", flush=True)
    cube = fetch_cso(s)
    print(f"  dataset updated: {cube.get('updated')}", flush=True)

    rows = extract_rows(cube, start, end, method_version)
    print(f"Built {len(rows)} IE native rows.")
    if rows:
        first, last = rows[0], rows[-1]
        print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh")
        print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh")

    if dry_run:
        print("Dry-run: not upserting.")
        return
    if not rows:
        return
    print(f"Upserting {len(rows)} IE native rows ...")
    upsert_rows(s, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
