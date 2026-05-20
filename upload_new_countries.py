"""Upload tables + measures for the 4 new countries only."""
import os, csv, re
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
BASE_DIR = Path("data")
MEASURES_DIR = BASE_DIR / "measures"

NEW_COUNTRIES = {
    "Austria data":     ("AUT", "Austria"),
    "Cyprus data":      ("CYP", "Cyprus"),
    "Denmark data":     ("DNK", "Denmark"),
    "Netherlands data": ("NLD", "Netherlands"),
}

COLUMN_MAPPING = {
    "measure_id": "measure_id", "measure_name": "measure_name",
    "description": "description", "type_of_policy_or_measure": "type_of_policy_or_measure",
    "implementation_period": "implementation_period", "section": "section",
    "quantified_objectives": "quantified_objectives", "budget": "budget",
    "stakeholders": "stakeholders", "state_of_play": "state_of_play",
    "epbd_article_2a": "epbd_article_2a", "directive": "directive", "status": "status",
    "objective": "objective", "planned_budget_and_sources": "planned_budget_and_sources",
    "entities_responsible": "entities_responsible", "state_of_execution": "state_of_execution",
    "date_of_entry_into_force": "date_of_entry_into_force", "instrument_type": "instrument_type",
    "source": "source", "quantitative_target": "quantitative_target",
    "short_description": "short_description", "quantified_objective": "quantified_objective",
    "authorities_responsible": "authorities_responsible", "expected_impacts": "expected_impacts",
    "implementation_status": "implementation_status", "effective_date": "effective_date",
    "section_topic": "section_topic", "measure_number": "measure_number",
    "content": "content", "amending_legislation": "amending_legislation",
    "lead_institution": "lead_institution", "participating_institutions": "participating_institutions",
    "sources_of_funding": "sources_of_funding", "time_limit": "time_limit",
    "measure_category": "measure_category",
}

def get_or_create_country(code, name):
    r = supabase.table("countries").select("id").eq("code", code).execute()
    if r.data:
        print(f"  Country exists: {name} ({code})")
        return r.data[0]["id"]
    ins = supabase.table("countries").insert({"code": code, "name": name}).execute()
    print(f"  Created country: {name} ({code})")
    return ins.data[0]["id"]

def extract_meta(filename):
    name = filename.replace('.csv', '')
    m = re.search(r'Table[_\s]*(\d+(?:_\d+)?)', name, re.IGNORECASE)
    table_number = m.group(1) if m else None
    description = name[m.end():].strip('_').strip() if m else name
    has_ts = any(k in name.lower() for k in ['2024','2025','2030','2040','2050','timeline','projection','target','roadmap'])
    return table_number, description, has_ts

def upload_tables(country_id, name, folder):
    csv_files = sorted(folder.glob("*.csv"))
    print(f"\n  {name}: uploading {len(csv_files)} tables…")
    ok = err = 0
    for f in csv_files:
        try:
            with open(f, encoding='utf-8-sig') as fh:
                reader = csv.DictReader(fh)
                headers = list(reader.fieldnames or [])
                time_cols = [h for h in headers if re.search(r'\b(19|20)\d{2}\b', h)]
            table_number, description, has_ts = extract_meta(f.name)
            has_ts = has_ts or bool(time_cols)
            existing = supabase.table("data_tables").select("id").eq("country_id", country_id).eq("file_name", f.name).execute()
            if existing.data:
                table_id = existing.data[0]["id"]
                supabase.table("data_points").delete().eq("data_table_id", table_id).execute()
            else:
                res = supabase.table("data_tables").insert({
                    "country_id": country_id, "table_name": f.stem,
                    "table_description": description.replace("_", " ").title(),
                    "file_name": f.name, "table_number": table_number,
                    "original_filename": f.name, "has_time_series": has_ts,
                    "column_names": headers, "num_columns": len(headers),
                    "metadata": {"description": description, "time_columns": time_cols}
                }).execute()
                table_id = res.data[0]["id"]
            with open(f, encoding='utf-8-sig') as fh2:
                reader2 = csv.DictReader(fh2)
                rows = []
                for row in reader2:
                    rows.append({"data_table_id": table_id, "row_data": dict(row)})
                    if len(rows) >= 100:
                        supabase.table("data_points").insert(rows).execute()
                        rows = []
                if rows:
                    supabase.table("data_points").insert(rows).execute()
            ok += 1
        except Exception as e:
            print(f"    ERR {f.name}: {e}")
            err += 1
    print(f"  Tables: {ok} OK, {err} errors")

def upload_measures(country_id, code, measures_file):
    if not measures_file.exists():
        print(f"  No measures file: {measures_file.name}")
        return
    try:
        supabase.table("measures").delete().eq("country_id", country_id).execute()
        with open(measures_file, encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            csv_columns = list(reader.fieldnames or [])
            print(f"  Measures columns: {len(csv_columns)} columns")
            measures = []
            for row in reader:
                m = {"country_id": country_id}
                sv = row.get("stakeholders","") or row.get("entities_responsible","")
                ev = row.get("entities_responsible","") or row.get("stakeholders","")
                if sv: m["stakeholders"] = sv
                if ev: m["entities_responsible"] = ev
                for csv_col, db_col in COLUMN_MAPPING.items():
                    if csv_col in row and row[csv_col]:
                        m[db_col] = row[csv_col]
                unmapped = {c: row[c] for c in csv_columns if c not in COLUMN_MAPPING and row.get(c)}
                if unmapped:
                    m["additional_data"] = unmapped
                measures.append(m)
        if measures:
            # Normalize all rows to the same key set (PostgREST requires uniform keys per batch)
            all_keys = set().union(*[m.keys() for m in measures])
            measures = [{k: m.get(k) for k in all_keys} for m in measures]
            # Insert in chunks of 50 to stay under request limits
            chunk_size = 50
            uploaded = 0
            for i in range(0, len(measures), chunk_size):
                supabase.table("measures").insert(measures[i:i+chunk_size]).execute()
                uploaded += len(measures[i:i+chunk_size])
            print(f"  Measures: {uploaded} uploaded")
    except Exception as e:
        print(f"  ERR measures {code}: {e}")
        import traceback; traceback.print_exc()

print("Uploading 4 new countries…\n")
for folder_name, (code, name) in NEW_COUNTRIES.items():
    print(f"\n{'='*50}\n{name} ({code})")
    folder = BASE_DIR / folder_name
    if not folder.exists():
        print(f"  MISSING folder: {folder}")
        continue
    country_id = get_or_create_country(code, name)
    # Tables already uploaded — skip
    # upload_tables(country_id, name, folder)
    measures_file = MEASURES_DIR / f"{name.lower()}_measures.csv"
    upload_measures(country_id, code, measures_file)

print("\n\nAll done.")
