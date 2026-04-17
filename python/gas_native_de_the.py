"""
Native Germany gas-demand backfill from THE (Trading Hub Europe, Bruegel methodology).

THE publishes daily aggregated gas consumption per category via a public JSON API:
  https://datenservice-api.tradinghub.eu/api/evoq/GetAggregierteVerbrauchsdatenTabelle

Fields (MWh):
  slPsyn_*, slPana_*  : standard load profile (households / small commercial)
  rlMmT_*, rlMoT_*    : measured load profile (industry + gas-fired power)

Bruegel aggregates these as:
  distribution    = slPsyn_H + slPana_H + slPsyn_L + slPana_L  (household + small commercial)
  industry-power  = rlMmT_H + rlMmT_L + rlMoT_H + rlMoT_L      (large industry + power)
  total           = distribution + industry-power

The power portion is estimated from ENTSO-E gas-fired electricity generation divided
by a thermal efficiency (default 0.5), then subtracted from industry-power to yield a
pure industry figure. This matches Bruegel's sector allocation exactly.
"""
from __future__ import annotations
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows
from gas_backfill_5y_bruegel import (
    ENTSOE_API,
    ENTSOE_DOMAIN,
    fetch_entsoe_a75_xml,
    parse_entsoe_gas_generation_mwh_by_day,
    ymdhm,
)

API_URL = "https://datenservice-api.tradinghub.eu/api/evoq/GetAggregierteVerbrauchsdatenTabelle"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Origin": "https://www.tradinghub.eu",
    "Referer": "https://www.tradinghub.eu/",
}

# Columns that exist in the THE response.
SLP_KEYS = ("slPsyn_H_Gas", "slPana_H_Gas", "slPsyn_L_Gas", "slPana_L_Gas")
RLM_KEYS = ("rlMmT_H_Gas", "rlMmT_L_Gas", "rlMoT_H_Gas", "rlMoT_L_Gas")


def fetch_the(session: requests.Session, start: date, end: date) -> List[dict]:
    params = {
        "DatumStart": start.strftime("%m-%d-%Y"),
        "DatumEnde": end.strftime("%m-%d-%Y"),
        "GasXType_Id": "all",
    }
    r = session.get(API_URL, params=params, headers=HEADERS, timeout=120)
    r.raise_for_status()
    return r.json() or []


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    today = date.today()
    start_year = int(os.getenv("GAS_NATIVE_DE_START_YEAR") or today.year - 5)
    start = date(start_year, 1, 1)
    end = today
    dry_run = (os.getenv("GAS_NATIVE_DE_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")

    efficiency = float(os.getenv("GAS_POWER_EFFICIENCY", "0.5"))
    entsoe_token = os.getenv("ENTSOE_API_TOKEN")

    s = make_retrying_session()
    print(f"Fetching THE (Trading Hub Europe) {start} -> {end} ...")
    records = fetch_the(s, start, end)
    print(f"  received {len(records)} rows")
    if not records:
        print("Nothing to upsert.")
        return

    # Fetch ENTSO-E gas-fired electricity generation for DE, chunked per month because
    # a full year of 15-minute PSR=B04 data exceeds the API's 90-second read budget.
    power_gas_mwh_by_day: Dict[date, float] = {}
    if entsoe_token:
        print("Fetching ENTSO-E gas generation for DE (chunked by month) ...")
        domain = ENTSOE_DOMAIN["DE"]
        cursor = date(start.year, start.month, 1)
        end_anchor = date(end.year, end.month, 1)
        while cursor <= end_anchor:
            year = cursor.year
            month = cursor.month
            next_month = date(year + (1 if month == 12 else 0),
                              1 if month == 12 else month + 1, 1)
            st = datetime(year, month, 1, tzinfo=timezone.utc)
            en = datetime(next_month.year, next_month.month, next_month.day, tzinfo=timezone.utc)
            for attempt in range(1, 4):
                try:
                    xml = fetch_entsoe_a75_xml(entsoe_token, domain, st, en, s)
                    daily = parse_entsoe_gas_generation_mwh_by_day(xml)
                    power_gas_mwh_by_day.update({
                        d: mwh_elec / max(efficiency, 1e-6)
                        for d, mwh_elec in daily.items()
                    })
                    print(f"  {year}-{month:02d}: {len(daily)} daily points")
                    break
                except Exception as e:
                    if attempt == 3:
                        print(f"  {year}-{month:02d}: ENTSO-E failed after 3 tries: {e}")
            cursor = next_month
    else:
        print("ENTSOE_API_TOKEN missing; power split will be NULL.")

    rows = []
    for rec in records:
        gastag = rec.get("gastag")
        if not gastag:
            continue
        try:
            d = datetime.fromisoformat(gastag.replace("Z", "+00:00")).date()
        except Exception:
            try:
                d = datetime.strptime(gastag[:10], "%Y-%m-%d").date()
            except Exception:
                continue
        if d < start or d > end:
            continue

        def _sum(keys):
            out = 0.0
            any_valid = False
            for k in keys:
                v = rec.get(k)
                if v is None:
                    continue
                try:
                    out += float(v)
                    any_valid = True
                except Exception:
                    pass
            return out if any_valid else None

        # THE API returns values in kWh (to match the GASPOOL/NCG-converted reference
        # Bruegel uses downstream). Convert kWh -> MWh so we match our schema (MWh everywhere).
        raw_dist = _sum(SLP_KEYS)
        raw_ip = _sum(RLM_KEYS)
        dist_mwh = raw_dist / 1000.0 if raw_dist is not None else None
        ip_mwh = raw_ip / 1000.0 if raw_ip is not None else None
        # THE publishes SLP (distribution) faster than RLM (industry+power):
        # SLP on T+1, RLM typically on T+2..T+3. If RLM is still missing we skip
        # the day entirely rather than emit an "industry=0, power=0" row that
        # would look like a real zero in the UI. The day reappears on the next
        # run once THE publishes the RLM series.
        if dist_mwh is None or ip_mwh is None:
            continue
        total_mwh = dist_mwh + ip_mwh
        if total_mwh <= 0:
            continue

        # distribution ~ household+small commercial (map to household)
        # industry-power ~ large industry + power; subtract power (from ENTSO-E) -> industry
        power_mwh = power_gas_mwh_by_day.get(d)
        if power_mwh is not None:
            power_mwh = max(0.0, min(float(power_mwh), ip_mwh))
            industry_mwh = max(0.0, ip_mwh - power_mwh)
            power_src = "entsoe_a75_b04"
        else:
            power_mwh = None
            industry_mwh = ip_mwh
            power_src = None

        rows.append({
            "country_code": "DE",
            "gas_day": d.isoformat(),
            "method_version": method_version,
            "total_mwh": total_mwh,
            "power_mwh": power_mwh,
            "household_mwh": dist_mwh,
            "industry_mwh": industry_mwh,
            "source_total": "the_daily",
            "source_split": "the_daily",
            "source_power": power_src or "the_daily",
            "quality_flag": "native_the_daily",
            "raw": {
                "source_origin": "native",
                "native_source": "the",
                "total_selector": "native_the_daily",
                "total_budget_mode": "native_source_no_budgeting",
                "the_distribution_mwh": dist_mwh,
                "the_industry_power_mwh": ip_mwh,
                "the_units_note": "THE API returns kWh; values divided by 1000 to store MWh.",
                "power_efficiency_assumed": efficiency if power_mwh is not None else None,
            },
        })

    rows.sort(key=lambda r: r["gas_day"])
    print(f"Built {len(rows)} DE native rows.")
    if rows:
        first, last = rows[0], rows[-1]
        print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh")
        print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh")

    if dry_run:
        print("Dry-run: not upserting.")
        return
    print(f"Upserting {len(rows)} DE native rows ...")
    upsert_rows(s, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
