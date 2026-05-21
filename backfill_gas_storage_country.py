"""Backfill per-country EU gas storage data from GIE AGSI+ into Supabase."""
import os, time
import requests
from dotenv import load_dotenv
from supabase import create_client

def _f(v):
    """Safe float conversion — handles None, empty string, and '-' from GIE API."""
    if v in (None, "", "-"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
GIE_KEY = os.getenv("GIE_API_KEY")

FROM_DATE = "2020-01-01"
TO_DATE   = "2026-05-21"
PAGE_SIZE = 3000

COUNTRIES = [
    "at", "be", "bg", "cz", "de", "dk", "es", "fr", "hr", "hu",
    "it", "lt", "lv", "nl", "pl", "pt", "ro", "se", "si", "sk",
]

def fetch_page(country: str, page: int):
    url = (f"https://agsi.gie.eu/api?type=country&country={country}"
           f"&from={FROM_DATE}&to={TO_DATE}&page={page}&size={PAGE_SIZE}")
    r = requests.get(url, headers={"x-key": GIE_KEY}, timeout=60)
    r.raise_for_status()
    return r.json()

def fetch_country(country: str):
    body = fetch_page(country, 1)
    records = body.get("data", [])
    last_page = body.get("last_page", 1)
    for p in range(2, last_page + 1):
        time.sleep(0.3)
        records.extend(fetch_page(country, p).get("data", []))
    return records

total_ok = 0
for country in COUNTRIES:
    print(f"\n-- {country.upper()} --")
    try:
        records = fetch_country(country)
        print(f"  Fetched {len(records)} records")
        if not records:
            continue
        rows = [
            {
                "gas_day":               r["gasDayStart"],
                "country":               country.upper(),
                "gas_in_storage_twh":    _f(r.get("gasInStorage")),
                "full_pct":              _f(r.get("full")),
                "trend_pct":             _f(r.get("trend")),
                "injection_twh":         _f(r.get("injection")),
                "withdrawal_twh":        _f(r.get("withdrawal")),
                "working_gas_volume_twh":_f(r.get("workingGasVolume")),
                "status":                r.get("status"),
                "source":                "gie_agsi",
            }
            for r in records if r.get("gasDayStart")
        ]
        chunk_size = 200
        ok = 0
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            supabase.table("gas_storage_country_daily").upsert(
                chunk, on_conflict="source,country,gas_day"
            ).execute()
            ok += len(chunk)
        print(f"  Upserted {ok} rows")
        total_ok += ok
        time.sleep(0.5)
    except Exception as e:
        print(f"  ERROR: {e}")

print(f"\nDone. Total rows upserted: {total_ok}")

