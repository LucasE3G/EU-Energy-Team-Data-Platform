"""
Native Austria gas-demand backfill from AGGM (via WIFO mirror, Bruegel methodology).

AGGM (Austrian Gas Grid Management) publishes daily total gas consumption.
WIFO mirrors this data as a daily-updated CSV at
https://energie.wifo.ac.at/data/gas/consumption-aggm.csv.

The CSV has columns: date, variable, value (in TWh/day).
"""
from __future__ import annotations
import csv
import io
import os
import sys
from datetime import date, datetime
from typing import List, Optional

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows

SRC_URL = "https://energie.wifo.ac.at/data/gas/consumption-aggm.csv"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/csv,application/json,*/*",
}


def fetch_csv(session: requests.Session) -> str:
    r = session.get(SRC_URL, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.text


def parse_csv(text: str, start: date, end: date) -> List[tuple]:
    """Return list of (date, kwh) tuples. AGGM values are in TWh -> convert to MWh (x1e6)."""
    reader = csv.DictReader(io.StringIO(text))
    out = []
    for row in reader:
        variable = (row.get("variable") or "").strip()
        if variable != "value":
            continue
        try:
            d = datetime.strptime(row["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if d < start or d > end:
            continue
        try:
            v = float(row["value"])
        except Exception:
            continue
        # Bruegel multiplies by 1e9 to get KWh -> so raw value is in TWh.
        # Convert TWh -> MWh: multiply by 1e6.
        mwh = v * 1_000_000.0
        if mwh <= 0:
            continue
        out.append((d, mwh))
    return out


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    today = date.today()
    # AGGM via WIFO publishes back to ~2018; pull 5 full calendar years so we
    # replace the inflated ENTSOG-derived rows that Eurostat calibration couldn't
    # fix (where the overshoot was below the 0.2× calibration floor).
    start_year = int(os.getenv("GAS_NATIVE_AT_START_YEAR") or today.year - 5)
    start = date(start_year, 1, 1)
    end = today
    dry_run = (os.getenv("GAS_NATIVE_AT_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    s = make_retrying_session()
    print(f"Downloading AGGM (WIFO) {start} -> {end} ...")
    text = fetch_csv(s)
    print(f"  size={len(text)} bytes")

    days = parse_csv(text, start, end)
    print(f"  parsed {len(days)} daily rows")
    if not days:
        print("Nothing to upsert.")
        return

    rows = []
    for d, mwh in days:
        rows.append({
            "country_code": "AT",
            "gas_day": d.isoformat(),
            "method_version": method_version,
            "total_mwh": mwh,
            # AGGM publishes only totals; keep sector splits NULL so UI can fall back.
            "power_mwh": None,
            "household_mwh": None,
            "industry_mwh": None,
            "source_total": "aggm_daily",
            "source_split": "aggm_daily",
            "source_power": "aggm_daily",
            "quality_flag": "native_aggm_daily",
            "raw": {
                "source_origin": "native",
                "native_source": "aggm",
                "total_selector": "native_aggm_daily",
                "total_budget_mode": "native_source_no_budgeting",
                "aggm_value_twh": mwh / 1_000_000.0,
            },
        })

    first, last = rows[0], rows[-1]
    print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh")
    print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh")

    if dry_run:
        print("Dry-run: not upserting.")
        return
    print(f"Upserting {len(rows)} AT native rows ...")
    upsert_rows(s, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
