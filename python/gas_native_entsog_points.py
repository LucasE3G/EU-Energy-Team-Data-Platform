"""
Native multi-country gas-demand backfill via ENTSOG off-take points.

This replicates Bruegel's (https://github.com/benmcwilliams/gas-demand) primary
methodology for 13 countries that do not publish a daily TSO feed but DO publish
sector-level off-take flows through ENTSOG's transparency platform:

    BE, BG, EE, HR, HU, IT, LU, LV, NL, PL, RO, SI

For each country, `python/data/entsog_points_mapping.json` lists one or more
ENTSOG point identifiers (e.g. ``nl-tso-0001dis-00187exit``) together with the
demand `type` they represent (`household`, `industry`, `power`,
`industry-power`, or `total`). We query ENTSOG

    GET /api/v1/operationalData
        ?pointDirection=<idt>
        &indicator=Physical Flow
        &periodType=day
        &from=YYYY-MM-DD
        &to=YYYY-MM-DD
        &timezone=CET
        &limit=-1

once per point, parse ``periodFrom`` / ``value`` / ``unit``, convert to MWh
and group by ``(country, date, type)`` summing values.

Reconstruction (Bruegel parity):

 * For ``HU, LU, RO`` ENTSOG reports ``industry-power`` as one aggregated
   final-consumer exit. We fetch ENTSO-E gas-fired generation (PSR=B04) /
   efficiency to get power and compute ``industry = max(0, industry-power - power)``.
 * For ``BE, HU, IT, LU, NL, RO`` the national total is the sum of the three
   sector flows (household + industry + power).
 * For ``BG, EE, HR, LV, PL, SI`` the ENTSOG feed only gives a ``total`` (one
   to five points). We enrich the sector split through the shared helper
   ``gas_native_splits_helper`` (ENTSO-E power + Eurostat HH/Ind shares).

Rows are flagged with ``raw.source_origin="native"`` so the Supabase edge
function (``gas_ingest_eu_latest``) will skip them on its next run.

Latency: typical ENTSOG publication lag is T+2 to T+3 for western TSOs (NL, BE,
IT, HU, LU, PT), T+3 to T+5 for PL/RO, and up to T+10 for BG/HR/LV/SI/EE.

Environment:
  GAS_NATIVE_ENTSOG_START_YEAR  (default: today.year - 5)  -- first year to backfill.
  GAS_NATIVE_ENTSOG_COUNTRIES   (default: all countries in the mapping) -- comma list.
  GAS_NATIVE_ENTSOG_DRY_RUN     (``1`` / ``true``)           -- log only, no upsert.
  GAS_NATIVE_ENTSOG_SLEEP_MS    (default: 300)               -- sleep between point calls.
  ENTSOE_API_TOKEN              -- required for power reconstruction / enrichment.
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY -- required for upserts.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows
from gas_native_splits_helper import (
    fetch_gas_for_power_by_day,
    resolve_shares_for_years,
    enrich_row_with_split,
)

ENTSOG_URL = "https://transparency.entsog.eu/api/v1/operationalData"

HERE = Path(__file__).parent
MAPPING_PATH = HERE / "data" / "entsog_points_mapping.json"

# Bruegel's two reconstruction rules (see module docstring).
INDUSTRY_FROM_INDPOWER_MINUS_POWER = {"HU", "LU", "RO"}
TOTAL_FROM_SECTOR_SUM = {"BE", "HU", "IT", "LU", "NL", "RO"}
TOTAL_ONLY_NEEDS_ENRICHMENT = {"BG", "EE", "HR", "LV", "PL", "SI"}


def _to_mwh(value: float, unit: Optional[str]) -> float:
    u = (unit or "kWh/d").strip()
    if u in ("kWh/d", "kWh"):
        return value / 1000.0
    if u in ("MWh/d", "MWh"):
        return value
    if u in ("GWh/d", "GWh"):
        return value * 1000.0
    if u in ("TWh/d", "TWh"):
        return value * 1_000_000.0
    return value / 1000.0


def fetch_point_daily(
    session: requests.Session,
    idt: str,
    start: date,
    end: date,
    timeout: int = 60,
) -> Dict[date, float]:
    """Return {day: mwh} summed across all entries for the single point id."""
    params = {
        "forceDownload": "true",
        "pointDirection": idt,
        "from": start.isoformat(),
        "to": end.isoformat(),
        "indicator": "Physical Flow",
        "periodType": "day",
        "timezone": "CET",
        "limit": "-1",
        "dataset": "1",
        "directDownload": "true",
    }
    r = session.get(ENTSOG_URL, params=params, timeout=timeout,
                    headers={"Accept": "application/json",
                             "User-Agent": "Mozilla/5.0 (EU-Energy-Team gas ingest)"})
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    try:
        js = r.json()
    except ValueError:
        return {}
    data = js.get("operationalData") or js.get("operationaldata") or []
    out: Dict[date, float] = {}
    for item in data:
        pf = item.get("periodFrom")
        v = item.get("value")
        if pf is None or v is None:
            continue
        try:
            d = datetime.fromisoformat(str(pf)[:19]).date()
        except ValueError:
            try:
                d = datetime.strptime(str(pf)[:10], "%Y-%m-%d").date()
            except ValueError:
                continue
        try:
            mwh = _to_mwh(float(v), item.get("unit"))
        except (TypeError, ValueError):
            continue
        out[d] = out.get(d, 0.0) + mwh
    return out


def load_mapping() -> Dict[str, List[dict]]:
    if not MAPPING_PATH.exists():
        raise RuntimeError(f"Missing mapping file: {MAPPING_PATH}")
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        js = json.load(f)
    return {k: v for k, v in (js.get("countries") or {}).items()}


def collect_by_type(
    session: requests.Session,
    country: str,
    points: List[dict],
    start: date,
    end: date,
    sleep_ms: int,
    log_prefix: str,
) -> Dict[date, Dict[str, float]]:
    """Return {day: {type: mwh}} aggregated across all points for one country."""
    out: Dict[date, Dict[str, float]] = {}
    for p in points:
        idt = p["idt"]
        tp = p["type"]
        try:
            flows = fetch_point_daily(session, idt, start, end)
        except requests.RequestException as e:
            print(f"{log_prefix}{country} | point {idt[:40]}... fetch error: {e}", flush=True)
            flows = {}
        print(f"{log_prefix}{country} | point [{tp:<14}] {idt}  -> {len(flows)} days", flush=True)
        for d, mwh in flows.items():
            bucket = out.setdefault(d, {})
            bucket[tp] = bucket.get(tp, 0.0) + mwh
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)
    return out


def reconstruct_rows(
    country: str,
    by_day_by_type: Dict[date, Dict[str, float]],
    gas_for_power: Dict[date, float],
    method_version: str,
) -> List[dict]:
    """Apply Bruegel's reconstruction rules and produce row dicts."""
    rows: List[dict] = []
    reconstruct_industry = country in INDUSTRY_FROM_INDPOWER_MINUS_POWER
    sum_sectors = country in TOTAL_FROM_SECTOR_SUM

    for d in sorted(by_day_by_type.keys()):
        t = by_day_by_type[d]
        household = t.get("household")
        industry = t.get("industry")
        power = t.get("power")
        indpow = t.get("industry-power")
        total_from_feed = t.get("total")

        # Step 1: derive power column.
        if power is None and indpow is not None:
            power = gas_for_power.get(d)

        # Step 2: derive industry from industry-power if needed.
        if reconstruct_industry and industry is None and indpow is not None:
            p_for_sub = power if power is not None else 0.0
            industry = max(0.0, indpow - p_for_sub)

        # Step 3: derive total.
        total: Optional[float] = None
        total_source: str
        if sum_sectors:
            parts = [x for x in (household, industry, power) if x is not None and x > 0]
            if len(parts) >= 2:
                total = sum(parts)
                total_source = "entsog_offtake_points_summed"
            else:
                # Fall back to industry-power + household if available
                if household is not None and indpow is not None:
                    total = household + indpow
                    total_source = "entsog_offtake_points_indpower_plus_household"
                else:
                    continue
        elif total_from_feed is not None:
            total = total_from_feed
            total_source = "entsog_offtake_points_total"
        else:
            continue

        if total is None or total <= 0.0:
            continue

        raw = {
            "source_origin": "native",
            "native_source": "entsog_offtake_points",
            "total_selector": "entsog_offtake_points_reconstructed",
            "total_budget_mode": "native_source_no_budgeting",
            "entsog_point_components": {k: v for k, v in t.items() if v is not None},
            "power_from_entsoe_a75": power if (indpow is not None or (sum_sectors and power is None)) else None,
        }

        row = {
            "country_code": country,
            "gas_day": d.isoformat(),
            "method_version": method_version,
            "total_mwh": float(total),
            "power_mwh": float(power) if power is not None else None,
            "household_mwh": float(household) if household is not None else None,
            "industry_mwh": float(industry) if industry is not None else None,
            "source_total": total_source,
            "source_split": (
                "entsog_offtake_points"
                if all(x is not None for x in (power, household, industry))
                else "entsog_offtake_points_partial"
            ),
            "source_power": (
                "entsog_offtake_points" if t.get("power") is not None
                else ("entsoe_a75_b04" if power is not None else None)
            ),
            "quality_flag": "native_entsog_offtake_points",
            "raw": raw,
        }
        rows.append(row)
    return rows


def process_country(
    session: requests.Session,
    country: str,
    points: List[dict],
    start: date,
    end: date,
    method_version: str,
    entsoe_token: Optional[str],
    efficiency: float,
    sleep_ms: int,
    log_prefix: str = "  ",
) -> List[dict]:
    print(f"\n{log_prefix}=== {country}: {len(points)} points, {start} -> {end} ===", flush=True)
    by_day_by_type = collect_by_type(session, country, points, start, end, sleep_ms, log_prefix)
    if not by_day_by_type:
        print(f"{log_prefix}{country}: no data from any point; skipping.", flush=True)
        return []

    # Fetch ENTSO-E gas-for-power if any point is 'industry-power' OR the
    # country summing-sectors is missing a native power feed.
    needs_entsoe_power = (
        any("industry-power" in t for t in by_day_by_type.values())
        or (country in TOTAL_FROM_SECTOR_SUM
            and any("power" not in t for t in by_day_by_type.values()))
    )
    gas_for_power: Dict[date, float] = {}
    if needs_entsoe_power and entsoe_token:
        print(f"{log_prefix}{country}: fetching ENTSO-E gas-for-power for reconstruction ...", flush=True)
        gas_for_power = fetch_gas_for_power_by_day(
            country, start, end, entsoe_token, efficiency, session, log_prefix=log_prefix
        )
    elif needs_entsoe_power and not entsoe_token:
        print(f"{log_prefix}{country}: ENTSOE_API_TOKEN missing, power will stay NULL", flush=True)

    rows = reconstruct_rows(country, by_day_by_type, gas_for_power, method_version)
    if not rows:
        print(f"{log_prefix}{country}: reconstruction produced 0 rows", flush=True)
        return []

    # Enrich totals-only countries with Eurostat HH/Ind shares.
    if country in TOTAL_ONLY_NEEDS_ENRICHMENT:
        years = sorted({date.fromisoformat(r["gas_day"]).year for r in rows})
        shares = resolve_shares_for_years(country, years, session, log_prefix=log_prefix)
        # Pull ENTSO-E power for enrichment (if not already fetched).
        if not gas_for_power and entsoe_token:
            gas_for_power = fetch_gas_for_power_by_day(
                country, start, end, entsoe_token, efficiency, session, log_prefix=log_prefix
            )
        rows = [enrich_row_with_split(r, gas_for_power, shares) for r in rows]

    first, last = rows[0], rows[-1]
    def gwh(v):
        return "n/a" if v is None else f"{v/1000:.1f}"
    print(f"{log_prefix}{country} built {len(rows)} rows.", flush=True)
    print(f"{log_prefix}  first: {first['gas_day']} total={gwh(first.get('total_mwh'))} GWh"
          f"  pw={gwh(first.get('power_mwh'))} hh={gwh(first.get('household_mwh'))}"
          f" ind={gwh(first.get('industry_mwh'))}", flush=True)
    print(f"{log_prefix}  last : {last['gas_day']} total={gwh(last.get('total_mwh'))} GWh"
          f"  pw={gwh(last.get('power_mwh'))} hh={gwh(last.get('household_mwh'))}"
          f" ind={gwh(last.get('industry_mwh'))}", flush=True)
    return rows


def main() -> int:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    entsoe_token = os.getenv("ENTSOE_API_TOKEN")
    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    efficiency = float(os.getenv("GAS_POWER_EFFICIENCY", "0.5"))

    today = date.today()
    start_year_env = os.getenv("GAS_NATIVE_ENTSOG_START_YEAR", "").strip()
    days_back_env  = os.getenv("GAS_NATIVE_ENTSOG_DAYS_BACK", "").strip()
    if start_year_env:
        # Explicit year override — used for full backfills via workflow_dispatch.
        start = date(int(start_year_env), 1, 1)
    elif days_back_env:
        # Incremental daily mode: only look back N days so all 13 countries finish
        # within the 45-min GitHub Actions limit.
        start = today - timedelta(days=int(days_back_env))
    else:
        # Local / manual fallback: full 5-year history.
        start = date(today.year - 5, 1, 1)
    end = today - timedelta(days=1)

    sleep_ms = int(os.getenv("GAS_NATIVE_ENTSOG_SLEEP_MS") or "300")
    dry_run = (os.getenv("GAS_NATIVE_ENTSOG_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    all_mapping = load_mapping()
    countries_env = (os.getenv("GAS_NATIVE_ENTSOG_COUNTRIES") or "").strip()
    if countries_env:
        wanted = {c.strip().upper() for c in countries_env.split(",") if c.strip()}
        mapping = {c: pts for c, pts in all_mapping.items() if c in wanted}
    else:
        mapping = all_mapping

    if not mapping:
        print("No countries selected; aborting.")
        return 0

    session = make_retrying_session()

    total_rows: List[dict] = []
    failed: List[str] = []
    for country in sorted(mapping.keys()):
        points = mapping[country]
        try:
            rows = process_country(
                session, country, points, start, end,
                method_version, entsoe_token, efficiency, sleep_ms,
            )
        except Exception as e:
            print(f"  {country}: ERROR: {e}", flush=True)
            failed.append(country)
            continue
        total_rows.extend(rows)

    print(f"\nTotal rows across all countries: {len(total_rows)}  failed={failed}", flush=True)

    if dry_run:
        print("Dry-run: not upserting.")
        return 1 if failed else 0
    if not total_rows:
        print("Nothing to upsert.")
        return 1 if failed else 0

    print(f"Upserting {len(total_rows)} ENTSOG-points native rows ...", flush=True)
    upsert_rows(session, supabase_url, service_role, total_rows)
    print("Done.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
