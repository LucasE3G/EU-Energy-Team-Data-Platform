"""
Verify Bulgaria GHG emission tables after upload
"""

import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_ANON_KEY")

supabase: Client = create_client(url, key)

print("Verifying Bulgaria GHG emission tables...\n")

# Get Bulgaria country ID
bulgaria = supabase.table("countries").select("id").eq("code", "BGR").execute()
if not bulgaria.data:
    print("Bulgaria not found!")
    exit(1)

bulgaria_id = bulgaria.data[0]["id"]

# Search for GHG/emission/carbon tables
tables = supabase.table("data_tables")\
    .select("*")\
    .eq("country_id", bulgaria_id)\
    .execute()

# Filter for GHG-related tables
ghg_keywords = ['emission', 'ghg', 'co2', 'carbon', 'greenhouse']
ghg_tables = [t for t in tables.data if any(
    kw in (t.get('table_description') or '').lower() or 
    kw in (t.get('table_name') or '').lower()
    for kw in ghg_keywords
)]

print(f"Found {len(ghg_tables)} GHG-related tables:\n")

for table in sorted(ghg_tables, key=lambda x: x.get('table_number', '')):
    print(f"Table {table.get('table_number', 'N/A')}: {table.get('table_description', 'N/A')}")
    print(f"  File: {table.get('file_name', 'N/A')}")
    print(f"  Has Time Series: {table.get('has_time_series', False)}")
    
    # Get sample data
    sample = supabase.table("data_points")\
        .select("row_data")\
        .eq("data_table_id", table['id'])\
        .limit(1)\
        .execute()
    
    if sample.data:
        row = sample.data[0]['row_data']
        cols = list(row.keys())[:10]
        print(f"  Columns: {', '.join(cols[:5])}...")
        
        # Check for year columns
        year_cols = [col for col in cols if any(year in str(col) for year in ['2023', '2030', '2040', '2050', '2020'])]
        if year_cols:
            print(f"  Year columns: {', '.join(year_cols[:3])}...")
    print()

# Specifically check Table 22 (new GHG targets table)
table_22 = [t for t in ghg_tables if '22' in str(t.get('table_number', '')) and 'greenhouse' in (t.get('table_description') or '').lower()]
if table_22:
    print("\n" + "="*80)
    print("NEW TABLE FOUND: Table 22 - GHG Emission Savings Targets")
    print("="*80)
    t22 = table_22[0]
    print(f"Description: {t22.get('table_description')}")
    print(f"Has Time Series: {t22.get('has_time_series')}")
    
    # Get all data
    data = supabase.table("data_points")\
        .select("row_data")\
        .eq("data_table_id", t22['id'])\
        .execute()
    
    print(f"\nData rows ({len(data.data)}):")
    for i, row in enumerate(data.data[:5], 1):
        print(f"\n  Row {i}:")
        for key, value in row['row_data'].items():
            print(f"    {key}: {value}")
