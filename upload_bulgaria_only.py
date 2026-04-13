"""
Script to upload/update Bulgaria CSV files to Supabase
Specifically for updating missing GHG emission tables
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
COUNTRY_CODE = "BGR"
COUNTRY_NAME = "Bulgaria"
COUNTRY_FOLDER = BASE_DIR / "Bulgaria data"


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
    
    # Try to extract table number (e.g., Table_1, Table_2_3, Table_No26, Table_No27)
    table_match = re.search(r'Table[_\s]*(?:No|N)?[_\s]*(\d+(?:_\d+)?)', name, re.IGNORECASE)
    table_number = table_match.group(1) if table_match else None
    
    # Get description (everything after Table_X or Table_NoX)
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


def upload_csv_data(country_code: str, file_path: Path):
    """Upload a single CSV file to Supabase"""
    country_id = get_country_id(country_code)
    if not country_id:
        print(f"[WARNING] Country {country_code} not found, skipping {file_path.name}")
        return False
    
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
            print(f"  [UPDATE] Updating existing table: {file_name}")
            # Delete existing data points
            supabase.table("data_points").delete().eq("data_table_id", table_id).execute()
            # Update table metadata
            supabase.table("data_tables").update({
                "table_name": table_name,
                "table_description": filename_metadata['description'].replace("_", " ").title(),
                "table_number": filename_metadata['table_number'],
                "has_time_series": has_time_series_data,
                "column_names": headers,
                "num_columns": len(headers),
                "metadata": {
                    "description": filename_metadata['description'],
                    "time_columns": time_columns
                }
            }).eq("id", table_id).execute()
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
            print(f"  [OK] Created new table: {file_name} (Table {filename_metadata['table_number'] or 'N/A'})")
        
        # Read CSV and upload data points
        row_count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = []
            for row in reader:
                # Convert row to JSONB format
                rows.append({
                    "data_table_id": table_id,
                    "row_data": row
                })
                row_count += 1
                
                # Batch insert every 100 rows
                if len(rows) >= 100:
                    supabase.table("data_points").insert(rows).execute()
                    rows = []
            
            # Insert remaining rows
            if rows:
                supabase.table("data_points").insert(rows).execute()
        
        print(f"  [OK] Uploaded {row_count} rows from {file_name}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error uploading {file_name}: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("Starting Bulgaria data upload/update to Supabase...\n")
    
    if not COUNTRY_FOLDER.exists():
        print(f"ERROR: Folder '{COUNTRY_FOLDER}' not found!")
        print(f"   Expected path: {COUNTRY_FOLDER.absolute()}")
        return
    
    # Get country ID
    country_id = get_country_id(COUNTRY_CODE)
    if not country_id:
        print(f"ERROR: Country {COUNTRY_NAME} ({COUNTRY_CODE}) not found in database!")
        return
    
    print(f"[OK] Found country: {COUNTRY_NAME} (ID: {country_id})\n")
    
    # Find all CSV files
    csv_files = list(COUNTRY_FOLDER.glob("*.csv"))
    
    if not csv_files:
        print(f"WARNING: No CSV files found in {COUNTRY_FOLDER}")
        return
    
    print(f"Found {len(csv_files)} CSV files\n")
    
    # Sort files for consistent processing
    csv_files.sort(key=lambda x: x.name)
    
    # Upload each CSV file
    success_count = 0
    error_count = 0
    
    for csv_file in csv_files:
        print(f"Processing: {csv_file.name}")
        if upload_csv_data(COUNTRY_CODE, csv_file):
            success_count += 1
        else:
            error_count += 1
        print()  # Empty line for readability
        time.sleep(0.1)  # Small delay to avoid rate limiting
    
    # Summary
    print("="*80)
    print("Upload Summary")
    print("="*80)
    print(f"[OK] Successfully uploaded/updated: {success_count} files")
    if error_count > 0:
        print(f"[ERROR] Errors: {error_count} files")
    print(f"Total files processed: {len(csv_files)}")
    print("\nBulgaria data upload complete!")


if __name__ == "__main__":
    main()
