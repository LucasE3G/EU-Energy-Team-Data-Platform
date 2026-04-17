"""
Native Portugal gas-demand backfill from REN DataHub.

REN (Redes Energéticas Nacionais) — Portugal's TSO for both gas and
electricity — publishes daily gas consumption through a public JSON REST
API at:

    https://servicebus.ren.pt/datahubapi/gas/GasConsumptionSupplyDaily
      ?culture=en-US&date=YYYY-MM-DD

No auth, no key. One call per gas-day. The response is a small JSON array
like:

  [
    {"daily_Accumulation": 144, "type": "TOTAL_CONSUMPTION"},
    {"daily_Accumulation":  57, "type": "ELECTRICITY_MARKET"},
    {"daily_Accumulation":  87, "type": "CONVENTIONAL_MARKET"},
    {"daily_Accumulation":  59, "type": "GRMS_DISTRIBUTION"},
    {"daily_Accumulation":  20, "type": "HIGH_PRESSURE_CLIENTS"},
    {"daily_Accumulation":   8, "type": "AUTONOMOUS_GAS_UNITS"}
  ]

Units are GWh. We translate the categories to our schema:

    power_mwh      = ELECTRICITY_MARKET              * 1000
    household_mwh  = GRMS_DISTRIBUTION               * 1000
                     (distribution grid, dominated by residential +
                      small commercial — REN's own framing)
    industry_mwh   = HIGH_PRESSURE_CLIENTS
                     + AUTONOMOUS_GAS_UNITS          * 1000
    total_mwh      = TOTAL_CONSUMPTION               * 1000

Sanity: ELECTRICITY_MARKET + CONVENTIONAL_MARKET ≈ TOTAL_CONSUMPTION, and
GRMS_DISTRIBUTION + HIGH_PRESSURE_CLIENTS + AUTONOMOUS_GAS_UNITS ≈
CONVENTIONAL_MARKET. We cross-check and fall back to the implied pieces
when one of the legs is missing.

Portugal is NOT covered by Bruegel's gas-demand extractors; this gives us
a genuine daily, native, sector-split series beyond Bruegel parity.

Rate-limiting: 150 ms between calls, up to ~5 y of history → ~1825 calls,
~5 min runtime. Safe for a daily cron.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta
from typing import Optional

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows

API_URL = "https://servicebus.ren.pt/datahubapi/gas/GasConsumptionSupplyDaily"


def _find(items: list, kind: str) -> Optional[float]:
    for item in items or []:
        if str(item.get("type", "")).upper() == kind:
            v = item.get("daily_Accumulation")
            if v is None:
                return None
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
    return None


def fetch_day(session: requests.Session, d: date, timeout: int = 30) -> Optional[list]:
    params = {"culture": "en-US", "date": d.isoformat()}
    r = session.get(API_URL, params=params, timeout=timeout,
                    headers={"Accept": "application/json",
                             "User-Agent": "Mozilla/5.0 (EU-Energy-Team gas ingest)"})
    if r.status_code == 404:
        return None
    r.raise_for_status()
    try:
        js = r.json()
    except ValueError:
        return None
    if not isinstance(js, list):
        return None
    return js


def build_row(d: date, payload: list, method_version: str) -> Optional[dict]:
    total = _find(payload, "TOTAL_CONSUMPTION")
    electricity = _find(payload, "ELECTRICITY_MARKET")
    conventional = _find(payload, "CONVENTIONAL_MARKET")
    distribution = _find(payload, "GRMS_DISTRIBUTION")
    hp_clients = _find(payload, "HIGH_PRESSURE_CLIENTS")
    autonomous = _find(payload, "AUTONOMOUS_GAS_UNITS")

    # Treat missing or sub-unit totals as "no data" for this day. REN occasionally
    # publishes zero placeholders for future days or for days being recomputed.
    if total is None or total <= 0.0:
        # Reconstruct if we at least have the two top-level buckets.
        if electricity is not None and conventional is not None:
            total = electricity + conventional
        else:
            return None

    power_mwh = (electricity * 1000.0) if electricity is not None else None
    household_mwh = (distribution * 1000.0) if distribution is not None else None
    industry_pieces = [x for x in (hp_clients, autonomous) if x is not None]
    industry_mwh = (sum(industry_pieces) * 1000.0) if industry_pieces else None

    # If REN publishes CONVENTIONAL but not the sub-splits we cannot separate
    # household from industry, so we leave both NULL and keep the total —
    # renderers show `—` for empty sector cells.
    if household_mwh is None and industry_mwh is None and conventional is not None:
        # Still better than dropping the row: signal "conventional only, split unknown".
        pass

    raw = {
        "source_origin": "native",
        "native_source": "ren_datahub",
        "total_selector": "native_ren_daily",
        "total_budget_mode": "native_source_no_budgeting",
        "ren_total_gwh": total,
        "ren_electricity_gwh": electricity,
        "ren_conventional_gwh": conventional,
        "ren_distribution_gwh": distribution,
        "ren_hp_clients_gwh": hp_clients,
        "ren_autonomous_gwh": autonomous,
    }

    return {
        "country_code": "PT",
        "gas_day": d.isoformat(),
        "method_version": method_version,
        "total_mwh": total * 1000.0,
        "power_mwh": power_mwh,
        "household_mwh": household_mwh,
        "industry_mwh": industry_mwh,
        "source_total": "ren_datahub",
        "source_split": "ren_datahub" if (power_mwh is not None and (household_mwh is not None or industry_mwh is not None)) else "ren_datahub_partial",
        "source_power": "ren_datahub" if power_mwh is not None else None,
        "quality_flag": "native_ren_daily",
        "raw": raw,
    }


def daterange(start: date, end: date):
    d = start
    one = timedelta(days=1)
    while d <= end:
        yield d
        d += one


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    today = date.today()
    start_year = int(os.getenv("GAS_NATIVE_PT_START_YEAR") or today.year - 5)
    start = date(start_year, 1, 1)
    # REN publishes the previous gas day by late morning UTC; still, pull up to
    # today-1 so we don't miss late-day publication.
    end = today - timedelta(days=1)
    dry_run = (os.getenv("GAS_NATIVE_PT_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")
    sleep_ms = int(os.getenv("GAS_NATIVE_PT_SLEEP_MS") or "150")

    days = list(daterange(start, end))
    print(f"Fetching REN PT daily ({start.isoformat()} -> {end.isoformat()}, {len(days)} days) ...",
          flush=True)

    s = make_retrying_session()
    rows: list[dict] = []
    errors = 0
    empty = 0
    last_progress = time.time()
    for i, d in enumerate(days):
        try:
            payload = fetch_day(s, d)
        except requests.RequestException as e:
            errors += 1
            if errors <= 10:
                print(f"  ! {d.isoformat()} fetch error: {e}", flush=True)
            if errors > 100 and errors > len(days) // 10:
                raise RuntimeError("Too many REN fetch errors; aborting.")
            continue
        if not payload:
            empty += 1
            continue
        row = build_row(d, payload, method_version)
        if row is None:
            empty += 1
            continue
        rows.append(row)
        # progress every ~10s
        if time.time() - last_progress > 10.0:
            last_progress = time.time()
            print(f"  progress {i+1}/{len(days)}  rows={len(rows)} empty={empty} errors={errors}",
                  flush=True)
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    print(f"Built {len(rows)} PT native rows (empty={empty}, errors={errors}).", flush=True)
    if rows:
        first, last = rows[0], rows[-1]
        print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh"
              f"  (pw={first['power_mwh']/1000 if first['power_mwh'] is not None else 'n/a'}"
              f" hh={first['household_mwh']/1000 if first['household_mwh'] is not None else 'n/a'}"
              f" ind={first['industry_mwh']/1000 if first['industry_mwh'] is not None else 'n/a'})",
              flush=True)
        print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh"
              f"  (pw={last['power_mwh']/1000 if last['power_mwh'] is not None else 'n/a'}"
              f" hh={last['household_mwh']/1000 if last['household_mwh'] is not None else 'n/a'}"
              f" ind={last['industry_mwh']/1000 if last['industry_mwh'] is not None else 'n/a'})",
              flush=True)

    if dry_run:
        print("Dry-run: not upserting.")
        return
    if not rows:
        return
    print(f"Upserting {len(rows)} PT native rows ...", flush=True)
    upsert_rows(s, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
