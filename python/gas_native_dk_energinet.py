"""
Native Denmark gas-demand backfill from Energinet (Bruegel methodology).

Energinet publishes the Gasflow dataset via the Energi Data Service API:
  https://api.energidataservice.dk/dataset/Gasflow

Field used:
  KWhToDenmark   net inflow into Denmark grid (kWh). Demand = -KWhToDenmark.
  GasDay         gas-day date string.

Source: Bruegel denmark_demand.py / denmark_scraper.py
"""
from __future__ import annotations
import os
import sys
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows
from gas_native_splits_helper import enrich_rows_with_split

DATASET_URL = "https://api.energidataservice.dk/dataset/Gasflow"


def fetch_records(session: requests.Session, start: date, end: date) -> List[dict]:
    # API supports pagination; we'll page through in chunks of 10k.
    all_rows: List[dict] = []
    offset = 0
    page = 10_000
    while True:
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "timezone": "utc",
            "limit": str(page),
            "offset": str(offset),
        }
        r = session.get(DATASET_URL, params=params, timeout=120)
        r.raise_for_status()
        j = r.json()
        rows = j.get("records") or []
        all_rows.extend(rows)
        if len(rows) < page:
            break
        offset += page
    return all_rows


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    today = date.today()
    start_year = int(os.getenv("GAS_NATIVE_DK_START_YEAR") or today.year - 5)
    start = date(start_year, 1, 1)
    end = today
    dry_run = (os.getenv("GAS_NATIVE_DK_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    s = make_retrying_session()
    print(f"Fetching Energinet Gasflow {start} -> {end} ...")
    recs = fetch_records(s, start, end)
    print(f"  {len(recs)} raw records")

    # Energinet publishes multiple rows per day (by flow exit); aggregate by GasDay.
    by_day: Dict[date, float] = defaultdict(float)
    for rec in recs:
        gd = rec.get("GasDay") or rec.get("gasDay")
        if not gd:
            continue
        try:
            d = datetime.fromisoformat(gd.replace("Z", "+00:00")).date()
        except Exception:
            try:
                d = datetime.strptime(gd[:10], "%Y-%m-%d").date()
            except Exception:
                continue
        v = rec.get("KWhToDenmark")
        if v is None:
            continue
        try:
            by_day[d] += -float(v)  # inflow positive -> demand = negative of flow
        except Exception:
            continue

    rows = []
    for d in sorted(by_day):
        if d < start or d > end:
            continue
        kwh = by_day[d]
        if kwh <= 0:
            # Net export day: treat as NULL; we don't have a demand figure we trust.
            continue
        mwh = kwh / 1000.0
        rows.append({
            "country_code": "DK",
            "gas_day": d.isoformat(),
            "method_version": method_version,
            "total_mwh": mwh,
            "power_mwh": None,
            "household_mwh": None,
            "industry_mwh": None,
            "source_total": "energinet_daily",
            "source_split": "energinet_daily",
            "source_power": "energinet_daily",
            "quality_flag": "native_energinet_daily",
            "raw": {
                "source_origin": "native",
                "native_source": "energinet",
                "total_selector": "native_energinet_daily",
                "total_budget_mode": "native_source_no_budgeting",
                "energinet_kwh_to_denmark_inverted": kwh,
            },
        })

    print(f"Built {len(rows)} DK native rows.")

    # Enrich splits: ENTSO-E gas-fired power + Eurostat HH/industry shares.
    efficiency = float(os.getenv("GAS_POWER_EFFICIENCY", "0.5"))
    entsoe_token = os.getenv("ENTSOE_API_TOKEN")
    print("Enriching DK rows with ENTSO-E power + Eurostat shares ...", flush=True)
    rows = enrich_rows_with_split(
        rows, country="DK", entsoe_token=entsoe_token,
        efficiency=efficiency, session=s, log_prefix="  DK ",
    )

    if rows:
        first, last = rows[0], rows[-1]
        print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh")
        print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh")

    if dry_run:
        print("Dry-run: not upserting.")
        return
    print(f"Upserting {len(rows)} DK native rows ...")
    upsert_rows(s, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
