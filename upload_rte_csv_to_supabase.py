import csv
import os
import sys

from dotenv import load_dotenv
from supabase import create_client


def main():
    load_dotenv()

    supabase_url = os.getenv("SUPABASE_URL")
    service_role = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not supabase_url or not service_role:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env", file=sys.stderr)
        sys.exit(1)

    csv_path = os.getenv("RTE_FRANCE_CSV", "rte_france_mix_15min.csv")
    if not os.path.exists(csv_path):
        print(f"CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    source = os.getenv("RTE_UPLOAD_SOURCE", "rte_csv")
    zone_id = os.getenv("RTE_UPLOAD_ZONE", "FR")

    supabase = create_client(supabase_url, service_role)

    batch = []
    batch_size = int(os.getenv("RTE_UPLOAD_BATCH_SIZE", "500"))
    inserted = 0
    total = 0

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"ts", "renewable_percent", "renewable_mw", "non_renewable_mw", "total_mw"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            print(f"CSV missing columns: {sorted(missing)}", file=sys.stderr)
            sys.exit(1)

        for row in reader:
            total += 1
            ts = row["ts"]
            try:
                renewable_percent = float(row["renewable_percent"]) if row["renewable_percent"] else None
            except ValueError:
                renewable_percent = None

            def fnum(k):
                try:
                    return float(row[k]) if row.get(k) else None
                except ValueError:
                    return None

            payload = {
                "zone_id": zone_id,
                "country_code": zone_id,
                "ts": ts,
                "renewable_percent": renewable_percent,
                "carbon_intensity_g_per_kwh": None,
                "source": source,
                "raw": {
                    "totals": {
                        "renewable_mw": fnum("renewable_mw"),
                        "non_renewable_mw": fnum("non_renewable_mw"),
                        "total_mw": fnum("total_mw"),
                    }
                },
            }

            batch.append(payload)
            if len(batch) >= batch_size:
                supabase.table("energy_mix_snapshots").upsert(batch, on_conflict="source,zone_id,ts").execute()
                inserted += len(batch)
                print(f"Upserted {inserted} rows...")
                batch = []

        if batch:
            supabase.table("energy_mix_snapshots").upsert(batch, on_conflict="source,zone_id,ts").execute()
            inserted += len(batch)

    print(f"Done. Total rows processed: {total}. Rows upserted: {inserted}. Source='{source}', zone='{zone_id}'.")


if __name__ == "__main__":
    main()

