"""
Script to find specific Belgium and Bulgaria GHG tables
"""

import os
from supabase import create_client, Client
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Initialize Supabase client
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')

if not url or not key:
    print("Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

supabase: Client = create_client(url, key)

print("Searching for Belgium and Bulgaria GHG tables...\n")

# Get countries
countries_response = supabase.table('countries').select('*').execute()
countries = {c['name']: c for c in countries_response.data}

# Search Belgium tables
print("="*80)
print("BELGIUM - Searching for GHG emission tables")
print("="*80)

belgium_id = countries.get('Belgium', {}).get('id')
if belgium_id:
    # Search Belgium tables with emission keywords
    belgium_tables_all = supabase.table('data_tables')\
        .select('*')\
        .eq('country_id', belgium_id)\
        .execute()
    
    # Filter in Python
    belgium_tables_filtered = [t for t in belgium_tables_all.data if any(
        kw in (t.get('table_description') or '').lower() or 
        kw in (t.get('table_name') or '').lower()
        for kw in ['emission', 'ghg', 'co2', 'carbon', 'greenhouse']
    )]
    
    print(f"\nFound {len(belgium_tables_filtered)} Belgium tables with emission-related keywords:\n")
    
    for table in belgium_tables_filtered:
        print(f"  Table {table.get('table_number', 'N/A')}: {table.get('table_description', 'N/A')}")
        print(f"    Has Time Series: {table.get('has_time_series', False)}")
        print(f"    File: {table.get('file_name', 'N/A')}")
        
        # Get sample data
        sample = supabase.table('data_points')\
            .select('row_data')\
            .eq('data_table_id', table['id'])\
            .limit(1)\
            .execute()
        
        if sample.data:
            row = sample.data[0]['row_data']
            cols = list(row.keys())[:8]
            print(f"    Sample columns: {', '.join(cols)}")
        print()

# Search Bulgaria tables
print("\n" + "="*80)
print("BULGARIA - Searching for Table N26 (Annual Operational Carbon Emissions)")
print("="*80)

bulgaria_id = countries.get('Bulgaria', {}).get('id')
if bulgaria_id:
    # Search by table number 26
    bulgaria_tables_num = supabase.table('data_tables')\
        .select('*')\
        .eq('country_id', bulgaria_id)\
        .ilike('table_number', '%26%')\
        .execute()
    
    # Search by keywords - get all Bulgaria tables and filter
    bulgaria_tables_all = supabase.table('data_tables')\
        .select('*')\
        .eq('country_id', bulgaria_id)\
        .execute()
    
    # Filter for carbon/emission keywords
    bulgaria_tables_kw_filtered = [t for t in bulgaria_tables_all.data if any(
        kw in (t.get('table_description') or '').lower() or 
        kw in (t.get('table_name') or '').lower()
        for kw in ['annual operational carbon', 'carbon emissions', 'operational carbon', 'carbon']
    )]
    
    # Combine and deduplicate
    all_bulgaria = {}
    for table in bulgaria_tables_num.data + bulgaria_tables_kw_filtered:
        all_bulgaria[table['id']] = table
    
    print(f"\nFound {len(all_bulgaria)} Bulgaria tables:\n")
    
    for table in all_bulgaria.values():
        print(f"  Table {table.get('table_number', 'N/A')}: {table.get('table_description', 'N/A')}")
        print(f"    Has Time Series: {table.get('has_time_series', False)}")
        print(f"    File: {table.get('file_name', 'N/A')}")
        
        # Get sample data
        sample = supabase.table('data_points')\
            .select('row_data')\
            .eq('data_table_id', table['id'])\
            .limit(1)\
            .execute()
        
        if sample.data:
            row = sample.data[0]['row_data']
            cols = list(row.keys())[:10]
            print(f"    Sample columns: {', '.join(cols)}")
            
            # Check for year columns
            year_cols = [col for col in cols if any(year in str(col) for year in ['2023', '2030', '2040', '2050', '2020'])]
            if year_cols:
                print(f"    Year columns found: {', '.join(year_cols)}")
        print()

# Also check all Bulgaria tables to see what's available
print("\n" + "="*80)
print("BULGARIA - All tables (for reference)")
print("="*80)

if bulgaria_id:
    all_bg_tables = supabase.table('data_tables')\
        .select('table_number, table_description, has_time_series')\
        .eq('country_id', bulgaria_id)\
        .order('table_number')\
        .execute()
    
    print(f"\nTotal Bulgaria tables: {len(all_bg_tables.data)}\n")
    for table in all_bg_tables.data[:30]:  # Show first 30
        print(f"  Table {table.get('table_number', 'N/A')}: {table.get('table_description', 'N/A')[:60]}...")
        if table.get('has_time_series'):
            print(f"    [TIME-SERIES]")
