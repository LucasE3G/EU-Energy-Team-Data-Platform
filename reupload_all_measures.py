"""
Script to re-upload all measures with support for both stakeholders and entities_responsible columns
"""

import os
import csv
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

if not url or not key:
    print("Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

supabase: Client = create_client(url, key)

BASE_DIR = Path("data")
MEASURES_DIR = BASE_DIR / "measures"

COUNTRIES = {
    "belgium": "BEL",
    "bulgaria": "BGR",
    "croatia": "HRV",
    "finland": "FIN",
    "lithuania": "LTU",
    "romania": "ROU",
    "slovenia": "SVN",
    "spain": "ESP"
}

def get_country_id(country_code: str):
    """Get country ID from database"""
    try:
        result = supabase.table("countries").select("id").eq("code", country_code).execute()
        return result.data[0]["id"] if result.data else None
    except Exception as e:
        print(f"Error getting country ID for {country_code}: {e}")
        return None

def upload_measures(country_code: str, measures_file: Path):
    """Upload measures CSV to Supabase with support for both stakeholders and entities_responsible"""
    country_id = get_country_id(country_code)
    if not country_id:
        print(f"  [WARNING] Country {country_code} not found, skipping measures")
        return False
    
    try:
        # Delete existing measures for this country
        supabase.table("measures").delete().eq("country_id", country_id).execute()
        
        # Read and upload measures
        with open(measures_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            measures = []
            headers = reader.fieldnames or []
            
            print(f"  Columns found: {', '.join(headers)}")
            
            # Get all column names from CSV
            csv_columns = reader.fieldnames or []
            
            # Map of CSV column names to database column names
            column_mapping = {
                # Common columns
                "measure_category": "measure_category",
                "measure_id": "measure_id",
                "measure_name": "measure_name",
                "description": "description",
                "type_of_policy_or_measure": "type_of_policy_or_measure",
                "implementation_period": "implementation_period",
                # Belgium
                "quantified_objectives": "quantified_objectives",
                "budget": "budget",
                "stakeholders": "stakeholders",
                "state_of_play": "state_of_play",
                # Finland
                "epbd_article_2a": "epbd_article_2a",
                "directive": "directive",
                "status": "status",
                # Lithuania/Slovenia/Spain
                "objective": "objective",
                "planned_budget_and_sources": "planned_budget_and_sources",
                "entities_responsible": "entities_responsible",
                "state_of_execution": "state_of_execution",
                "date_of_entry_into_force": "date_of_entry_into_force",
                # Lithuania
                "instrument_type": "instrument_type",
                "source": "source",
                "quantitative_target": "quantitative_target",
                # Croatia
                "short_description": "short_description",
                "quantified_objective": "quantified_objective",
                "authorities_responsible": "authorities_responsible",
                "expected_impacts": "expected_impacts",
                "implementation_status": "implementation_status",
                "effective_date": "effective_date",
                # Bulgaria
                "section": "section",
                "section_topic": "section_topic",
                "measure_number": "measure_number",
                "content": "content",
                "amending_legislation": "amending_legislation",
                "lead_institution": "lead_institution",
                "participating_institutions": "participating_institutions",
                "sources_of_funding": "sources_of_funding",
                "time_limit": "time_limit"
            }
            
            for row in reader:
                measure = {
                    "country_id": country_id
                }
                
                # Handle both stakeholders and entities_responsible columns
                stakeholders_value = row.get("stakeholders", "") or row.get("entities_responsible", "")
                entities_responsible_value = row.get("entities_responsible", "") or row.get("stakeholders", "")
                if stakeholders_value:
                    measure["stakeholders"] = stakeholders_value
                if entities_responsible_value:
                    measure["entities_responsible"] = entities_responsible_value
                
                # Map all known columns
                for csv_col, db_col in column_mapping.items():
                    if csv_col in row and row[csv_col]:
                        measure[db_col] = row[csv_col]
                
                # Store any unmapped columns in additional_data JSONB
                unmapped_data = {}
                for csv_col in csv_columns:
                    if csv_col not in column_mapping and csv_col in row and row[csv_col]:
                        unmapped_data[csv_col] = row[csv_col]
                
                if unmapped_data:
                    measure["additional_data"] = unmapped_data
                
                measures.append(measure)
            
            if measures:
                supabase.table("measures").insert(measures).execute()
                print(f"  [OK] Uploaded {len(measures)} measures for {country_code}")
                return True
            else:
                print(f"  [WARNING] No measures found in {measures_file.name}")
                return False
    
    except Exception as e:
        print(f"  [ERROR] Error uploading measures for {country_code}: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("Re-uploading all measures with updated column support...\n")
    
    if not MEASURES_DIR.exists():
        print(f"[ERROR] Measures folder not found: {MEASURES_DIR}")
        return
    
    # Find all measure CSV files
    measure_files = list(MEASURES_DIR.glob("*.csv"))
    
    if not measure_files:
        print(f"[WARNING] No CSV files found in {MEASURES_DIR}")
        return
    
    print(f"Found {len(measure_files)} measure files\n")
    
    success_count = 0
    error_count = 0
    
    for measures_file in measure_files:
        # Extract country name from filename (e.g., belgium_measures.csv -> belgium)
        filename_lower = measures_file.stem.lower()
        country_name = None
        
        for name, code in COUNTRIES.items():
            if name in filename_lower:
                country_name = name
                country_code = code
                break
        
        if not country_name:
            print(f"[SKIP] Could not determine country from filename: {measures_file.name}")
            continue
        
        print(f"Processing: {measures_file.name} ({country_code})")
        if upload_measures(country_code, measures_file):
            success_count += 1
        else:
            error_count += 1
        print()
    
    print("="*80)
    print("Upload Summary")
    print("="*80)
    print(f"[OK] Successfully uploaded: {success_count} files")
    if error_count > 0:
        print(f"[ERROR] Errors: {error_count} files")
    print(f"Total files processed: {len(measure_files)}")

if __name__ == "__main__":
    main()
