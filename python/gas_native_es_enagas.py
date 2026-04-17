"""
Native Spain gas-demand backfill from Enagás (Bruegel methodology).

Enagás does not expose a public JSON API; the daily figures are rendered into
an Angular widget on this page:

  https://www.enagas.es/en/technical-management-system/energy-data/demand/forecast/

We reproduce Bruegel's approach (src/scrapers/spain_scraper.py +
src/utils/spain.py) with headless Chromium and a datepicker loop. The widget
shows one gas day at a time; we extract "Total demand" and "Natural gas for
power generation" (both GWh).

Rate-limited: ~2 s per day (+ random jitter), so a full 5-year backfill takes
several hours. The default run window is the last N days (``GAS_NATIVE_ES_DAYS``,
default 7) so daily GitHub Actions jobs stay cheap. For an initial backfill use:

  GAS_NATIVE_ES_START_DATE=2021-01-01 python python/gas_native_es_enagas.py

The script also skips days where a native ES row already exists in
``gas_demand_daily``, so resuming a partial backfill is safe.

Field mapping:
  total_mwh      = total_demand  * 1000   (GWh -> MWh)
  power_mwh     = power_generation * 1000 (GWh -> MWh)
  household_mwh = NULL   (Enagás does not publish an HH split)
  industry_mwh  = NULL   (we avoid a fake split; consumers of the table can
                          derive industry = total - power if they prefer)
"""
from __future__ import annotations

import os
import random
import sys
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set

import requests
from dotenv import load_dotenv

from gas_recompute_mixed_months_budget import make_retrying_session, upsert_rows
from gas_native_splits_helper import enrich_rows_with_split

ENAGAS_URL = (
    "https://www.enagas.es/en/technical-management-system/energy-data/demand/forecast/"
)
SHORT_PAUSE = float(os.getenv("GAS_NATIVE_ES_SHORT_PAUSE", "2.0"))
LONG_PAUSE = float(os.getenv("GAS_NATIVE_ES_LONG_PAUSE", "60.0"))
PAUSE_INTERVAL = int(os.getenv("GAS_NATIVE_ES_PAUSE_EVERY", "100"))


def _already_loaded_days(
    session: requests.Session,
    supabase_url: str,
    service_role: str,
    start: date,
    end: date,
) -> Set[date]:
    """Return the set of ES gas-days already stored with native Enagás rows."""
    params = {
        "select": "gas_day",
        "country_code": "eq.ES",
        "source_total": "eq.enagas_daily",
        "gas_day": f"gte.{start.isoformat()}",
        "and": f"(gas_day.lte.{end.isoformat()})",
    }
    r = session.get(
        f"{supabase_url}/rest/v1/gas_demand_daily",
        headers={"apikey": service_role, "Authorization": f"Bearer {service_role}"},
        params=params,
        timeout=60,
    )
    r.raise_for_status()
    out: Set[date] = set()
    for row in r.json() or []:
        try:
            out.add(datetime.fromisoformat(row["gas_day"]).date())
        except Exception:
            continue
    return out


def _make_driver():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1400,900")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    chrome_binary = os.getenv("CHROME_BINARY")
    if chrome_binary:
        options.binary_location = chrome_binary
    return webdriver.Chrome(options=options)


def _accept_cookies(driver, logger=print) -> None:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "onetrust-banner-sdk"))
        )
        btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", btn)
        time.sleep(0.5)
        btn.click()
        WebDriverWait(driver, 10).until(
            EC.invisibility_of_element_located((By.ID, "onetrust-banner-sdk"))
        )
    except Exception as e:
        logger(f"  cookie-banner: {e}")


def _change_date(driver, gas_day: date) -> bool:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    date_str = gas_day.strftime("%d/%m/%Y")
    try:
        inp = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.ID, "datepicker-intraday"))
        )
        inp.clear()
        inp.send_keys(date_str)
        submit = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "arrow-submit"))
        )
        submit.click()
        time.sleep(4)
        return True
    except Exception as e:
        print(f"  change_date {gas_day}: {e}", flush=True)
        return False


def _extract(driver, gas_day: date) -> Optional[Dict[str, float]]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    try:
        table = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table-row"))
        )
        out: Dict[str, float] = {}
        for row in table.find_elements(By.TAG_NAME, "tr"):
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) != 2:
                continue
            label = cells[0].text.strip()
            raw = cells[1].text.strip().replace("GWh", "").strip().replace(",", "")
            try:
                v = float(raw)
            except Exception:
                continue
            if "Total demand" in label:
                out["total_gwh"] = v
            elif "Natural gas for power generation" in label:
                out["power_gwh"] = v
            elif "Final Demand" in label:
                out["final_demand_gwh"] = v
        if "total_gwh" not in out:
            return None
        return out
    except Exception as e:
        print(f"  extract {gas_day}: {e}", flush=True)
        return None


def _build_row(gas_day: date, sample: Dict[str, float], method_version: str) -> dict:
    total_mwh = sample["total_gwh"] * 1000.0
    power_mwh = sample.get("power_gwh")
    power_mwh_raw = power_mwh
    power_mwh = power_mwh * 1000.0 if power_mwh is not None else None
    return {
        "country_code": "ES",
        "gas_day": gas_day.isoformat(),
        "method_version": method_version,
        "total_mwh": total_mwh,
        "power_mwh": power_mwh,
        "household_mwh": None,
        "industry_mwh": None,
        "source_total": "enagas_daily",
        "source_split": "enagas_daily",
        "source_power": "enagas_daily",
        "quality_flag": "native_enagas_daily",
        "raw": {
            "source_origin": "native",
            "native_source": "enagas",
            "total_selector": "native_enagas_daily",
            "total_budget_mode": "native_source_no_budgeting",
            "enagas_total_gwh": sample.get("total_gwh"),
            "enagas_power_gwh": power_mwh_raw,
            "enagas_final_demand_gwh": sample.get("final_demand_gwh"),
        },
    }


def _resolve_window() -> tuple[date, date]:
    today = date.today()
    start_env = (os.getenv("GAS_NATIVE_ES_START_DATE") or "").strip()
    end_env = (os.getenv("GAS_NATIVE_ES_END_DATE") or "").strip()
    days = int(os.getenv("GAS_NATIVE_ES_DAYS") or 7)
    if start_env:
        start = datetime.strptime(start_env, "%Y-%m-%d").date()
    else:
        start = today - timedelta(days=days)
    if end_env:
        end = datetime.strptime(end_env, "%Y-%m-%d").date()
    else:
        end = today - timedelta(days=1)
    return start, end


def main() -> None:
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY")

    method_version = os.getenv("GAS_METHOD_VERSION", "v2_bruegel_power_entsoe")
    dry_run = (os.getenv("GAS_NATIVE_ES_DRY_RUN") or "").strip() in ("1", "true", "TRUE", "yes")
    skip_existing = (os.getenv("GAS_NATIVE_ES_SKIP_EXISTING") or "1").strip() in (
        "1", "true", "TRUE", "yes"
    )

    start, end = _resolve_window()
    print(f"ES native window: {start} -> {end}", flush=True)

    http = make_retrying_session()
    already = _already_loaded_days(http, supabase_url, service_role, start, end) if skip_existing else set()
    if already:
        print(f"  {len(already)} ES days already in DB; will skip them", flush=True)

    # Selenium setup (imports deferred so non-ES runs don't need selenium installed).
    try:
        _make_driver()  # sanity
    except Exception as e:
        raise RuntimeError(f"Selenium/Chrome setup failed: {e}")

    driver = _make_driver()
    rows: List[dict] = []
    request_count = 0
    try:
        driver.get(ENAGAS_URL)
        _accept_cookies(driver)

        cur = start
        while cur <= end:
            if cur in already:
                cur += timedelta(days=1)
                continue
            print(f"ES | {cur}", flush=True)
            if _change_date(driver, cur):
                sample = _extract(driver, cur)
                if sample:
                    rows.append(_build_row(cur, sample, method_version))
                else:
                    print(f"  no data for {cur}", flush=True)
            request_count += 1
            time.sleep(SHORT_PAUSE + random.uniform(0, 1.5))
            if request_count and request_count % PAUSE_INTERVAL == 0:
                print(f"  long pause after {request_count} requests", flush=True)
                time.sleep(LONG_PAUSE + random.uniform(0, 10))
            cur += timedelta(days=1)
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    print(f"Built {len(rows)} ES native rows.")

    # Enagás already provides power from its widget; enrich the HH/industry split
    # via Eurostat shares (ES does not publish a direct HH/industry split). The
    # helper is idempotent: it keeps the power value Enagás gave us.
    efficiency = float(os.getenv("GAS_POWER_EFFICIENCY", "0.5"))
    entsoe_token = os.getenv("ENTSOE_API_TOKEN")
    print("Enriching ES rows with Eurostat HH/industry shares ...", flush=True)
    rows = enrich_rows_with_split(
        rows, country="ES", entsoe_token=entsoe_token,
        efficiency=efficiency, session=http, log_prefix="  ES ",
    )

    if rows:
        first, last = rows[0], rows[-1]
        print(f"  first: {first['gas_day']} total={first['total_mwh']/1000:.1f} GWh")
        print(f"  last : {last['gas_day']} total={last['total_mwh']/1000:.1f} GWh")

    if dry_run or not rows:
        if dry_run:
            print("Dry-run: not upserting.")
        return
    print(f"Upserting {len(rows)} ES native rows ...")
    upsert_rows(http, supabase_url, service_role, rows)
    print("Done.")


if __name__ == "__main__":
    sys.exit(main() or 0)
