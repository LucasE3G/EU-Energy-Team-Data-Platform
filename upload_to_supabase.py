"""
Script to upload CSV files from country folders to Supabase
Run this after setting up your Supabase project and running the schema SQL
"""

import os
import csv
import json
import re
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")  # Use service role key for admin operations

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Please set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in your .env file")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Base directory containing country folders
BASE_DIR = Path("data")

# Country mapping
COUNTRIES = {
    "Belgium data": "BEL",
    "Bulgaria data": "BGR",
    "Croatia data": "HRV",
    "Finland data": "FIN",
    "Lithuania data": "LTU",
    "Romania data": "ROU",
    "Slovenia data": "SVN",
    "Spain data": "ESP"
}

COUNTRY_NAMES = {
    "BEL": "Belgium",
    "BGR": "Bulgaria",
    "HRV": "Croatia",
    "FIN": "Finland",
    "LTU": "Lithuania",
    "ROU": "Romania",
    "SVN": "Slovenia",
    "ESP": "Spain"
}


def ensure_country_exists(country_code: str, country_name: str):
    """Ensure country exists in database, create if not"""
    try:
        result = supabase.table("countries").select("*").eq("code", country_code).execute()
        if len(result.data) == 0:
            supabase.table("countries").insert({
                "code": country_code,
                "name": country_name
            }).execute()
            print(f"✓ Created country: {country_name} ({country_code})")
        else:
            print(f"✓ Country exists: {country_name} ({country_code})")
        return result.data[0]["id"] if result.data else None
    except Exception as e:
        print(f"Error ensuring country {country_code}: {e}")
        return None


def get_country_id(country_code: str):
    """Get country ID from database"""
    try:
        result = supabase.table("countries").select("id").eq("code", country_code).execute()
        return result.data[0]["id"] if result.data else None
    except Exception as e:
        print(f"Error getting country ID for {country_code}: {e}")
        return None


def extract_metadata_from_filename(filename):
    """Extract metadata from CSV filename"""
    # Remove .csv extension
    name = filename.replace('.csv', '')
    
    # Try to extract table number (e.g., Table_1, Table_2_3, Table_3_7)
    table_match = re.search(r'Table[_\s]*(\d+(?:_\d+)?)', name, re.IGNORECASE)
    table_number = table_match.group(1) if table_match else None
    
    # Get description (everything after Table_X)
    if table_match:
        description = name[table_match.end():].strip('_').strip()
    else:
        description = name
    
    # Check if it contains time-related keywords
    has_time_series = any(keyword in name.lower() for keyword in [
        '2024', '2025', '2030', '2040', '2050', 
        'timeline', 'projection', 'target', 'roadmap'
    ])
    
    return {
        'table_number': table_number,
        'description': description,
        'has_time_series': has_time_series
    }


def upload_csv_data(country_code: str, country_folder: Path, file_path: Path):
    """Upload a single CSV file to Supabase"""
    country_id = get_country_id(country_code)
    if not country_id:
        print(f"⚠ Country {country_code} not found, skipping {file_path.name}")
        return
    
    file_name = file_path.name
    table_name = file_path.stem
    
    # Extract metadata from filename
    filename_metadata = extract_metadata_from_filename(file_name)
    
    try:
        # Read CSV to get column information
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames) if reader.fieldnames else []
            
            # Check for time-series columns
            time_columns = [h for h in headers if re.search(r'\b(19|20)\d{2}\b', h)]
            has_time_series_data = len(time_columns) > 0 or filename_metadata['has_time_series']
        
        # Check if table already exists
        existing = supabase.table("data_tables").select("*").eq("country_id", country_id).eq("file_name", file_name).execute()
        
        if existing.data:
            table_id = existing.data[0]["id"]
            print(f"  Table {file_name} already exists, updating data points...")
            # Delete existing data points
            supabase.table("data_points").delete().eq("data_table_id", table_id).execute()
        else:
            # Create data table entry with metadata
            table_result = supabase.table("data_tables").insert({
                "country_id": country_id,
                "table_name": table_name,
                "table_description": filename_metadata['description'].replace("_", " ").title(),
                "file_name": file_name,
                "table_number": filename_metadata['table_number'],
                "original_filename": file_name,
                "has_time_series": has_time_series_data,
                "column_names": headers,
                "num_columns": len(headers),
                "metadata": {
                    "description": filename_metadata['description'],
                    "time_columns": time_columns
                }
            }).execute()
            table_id = table_result.data[0]["id"]
            print(f"  ✓ Created table entry: {file_name} (Table {filename_metadata['table_number'] or 'N/A'})")
        
        # Read CSV and upload data points
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                # Convert row to JSONB format
                rows.append({
                    "data_table_id": table_id,
                    "row_data": row
                })
                
                # Batch insert every 100 rows
                if len(rows) >= 100:
                    supabase.table("data_points").insert(rows).execute()
                    rows = []
            
            # Insert remaining rows
            if rows:
                supabase.table("data_points").insert(rows).execute()
        
        print(f"  ✓ Uploaded {file_name}")
        
    except Exception as e:
        print(f"  ✗ Error uploading {file_name}: {e}")


def upload_measures(country_code: str, measures_file: Path):
    """Upload measures CSV to Supabase"""
    country_id = get_country_id(country_code)
    if not country_id:
        print(f"⚠ Country {country_code} not found, skipping measures")
        return
    
    try:
        # Delete existing measures for this country
        supabase.table("measures").delete().eq("country_id", country_id).execute()
        
        # Read and upload measures
        with open(measures_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            measures = []
            
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
                print(f"  ✓ Uploaded {len(measures)} measures for {country_code}")
    
    except Exception as e:
        print(f"  ✗ Error uploading measures for {country_code}: {e}")


def main():
    print("🚀 Starting CSV upload to Supabase...\n")
    
    # First, ensure all countries exist
    print("📋 Ensuring countries exist in database...")
    for folder_name, country_code in COUNTRIES.items():
        country_name = COUNTRY_NAMES.get(country_code, folder_name.replace(" data", ""))
        ensure_country_exists(country_code, country_name)
    
    print("\n📊 Uploading country data tables...")
    
    # Upload CSV files from each country folder
    for folder_name, country_code in COUNTRIES.items():
        country_folder = BASE_DIR / folder_name
        if not country_folder.exists():
            print(f"⚠ Folder not found: {country_folder}")
            continue
        
        print(f"\n📁 Processing {COUNTRY_NAMES.get(country_code, folder_name)}...")
        csv_files = list(country_folder.glob("*.csv"))
        
        for csv_file in csv_files:
            upload_csv_data(country_code, country_folder, csv_file)
            time.sleep(0.1)  # Small delay to avoid rate limiting
    
    # Upload measures
    print("\n📋 Uploading measures...")
    measures_folder = BASE_DIR / "measures"
    if measures_folder.exists():
        for measures_file in measures_folder.glob("*.csv"):
            country_name = measures_file.stem.split("_")[0].capitalize()
            # Find country code from name
            country_code = None
            for code, name in COUNTRY_NAMES.items():
                if name.lower() == country_name.lower():
                    country_code = code
                    break
            
            if country_code:
                print(f"\n📋 Processing measures for {country_name}...")
                upload_measures(country_code, measures_file)
    
    print("\n✅ Upload complete!")


if __name__ == "__main__":
    main()
