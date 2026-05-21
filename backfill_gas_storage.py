"""Fetch EU gas storage data from GIE AGSI+ and upload to Supabase."""
import os, math, time
import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
GIE_KEY = os.getenv("GIE_API_KEY")

FROM_DATE = "2020-01-01"
TO_DATE   = "2026-05-21"
PAGE_SIZE = 3000

def fetch_page(page: int):
    url = f"https://agsi.gie.eu/api?type=eu&from={FROM_DATE}&to={TO_DATE}&page={page}&size={PAGE_SIZE}"
    r = requests.get(url, headers={"x-key": GIE_KEY}, timeout=60)
    r.raise_for_status()
    return r.json()

print("Fetching EU gas storage from GIE AGSI+...")
body = fetch_page(1)
records = body.get("data", [])
last_page = body.get("last_page", 1)
print(f"  Page 1/{last_page}: {len(records)} records (total: {body.get('total')})")

for p in range(2, last_page + 1):
    time.sleep(0.5)
    page_data = fetch_page(p)
    chunk = page_data.get("data", [])
    records.extend(chunk)
    print(f"  Page {p}/{last_page}: {len(chunk)} records")

print(f"\nTotal fetched: {len(records)} records")

rows = [
    {
        "gas_day":               r["gasDayStart"],
        "gas_in_storage_twh":    float(r["gasInStorage"])  if r.get("gasInStorage")  not in (None, "") else None,
        "full_pct":              float(r["full"])           if r.get("full")           not in (None, "") else None,
        "trend_pct":             float(r["trend"])          if r.get("trend")          not in (None, "") else None,
        "injection_twh":         float(r["injection"])      if r.get("injection")      not in (None, "") else None,
        "withdrawal_twh":        float(r["withdrawal"])     if r.get("withdrawal")     not in (None, "") else None,
        "working_gas_volume_twh":float(r["workingGasVolume"]) if r.get("workingGasVolume") not in (None, "") else None,
        "status":                r.get("status"),
        "source":                "gie_agsi",
    }
    for r in records if r.get("gasDayStart")
]

print(f"Uploading {len(rows)} rows to Supabase...")
chunk_size = 200
ok = 0
for i in range(0, len(rows), chunk_size):
    chunk = rows[i:i + chunk_size]
    result = supabase.table("gas_storage_eu_daily").upsert(chunk, on_conflict="source,gas_day").execute()
    ok += len(chunk)
    print(f"  {ok}/{len(rows)} uploaded")

print(f"\nDone. {ok} rows upserted.")
if rows:
    latest = sorted(rows, key=lambda r: r["gas_day"], reverse=True)[0]
    print(f"Latest: {latest['gas_day']}  fill={latest['full_pct']}%  storage={latest['gas_in_storage_twh']} TWh")
