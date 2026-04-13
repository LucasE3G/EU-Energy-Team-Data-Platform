"""
Python script to export ALL table names from Supabase to CSV
This bypasses the 100-row limit by fetching in batches
"""

import csv
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_SERVICE_ROLE_KEY') or os.getenv('SUPABASE_ANON_KEY')

if not url or not key:
    print("Error: SUPABASE_URL and SUPABASE_KEY must be set in .env file")
    exit(1)

supabase: Client = create_client(url, key)

print("Fetching all tables from Supabase...")

# Fetch all tables with pagination
all_tables = []
page_size = 1000
offset = 0
has_more = True

while has_more:
    response = supabase.table('data_tables')\
        .select('*, countries(name, code)')\
        .order('country_id', desc=False)\
        .order('table_number', desc=False)\
        .range(offset, offset + page_size - 1)\
        .execute()
    
    if response.data:
        all_tables.extend(response.data)
        print(f"Fetched {len(all_tables)} tables so far...")
        
        # If we got fewer than page_size, we've reached the end
        if len(response.data) < page_size:
            has_more = False
        else:
            offset += page_size
    else:
        has_more = False

print(f"\nTotal tables found: {len(all_tables)}")

# Flatten the data for CSV export
csv_data = []
for table in all_tables:
    country = table.get('countries', {}) if isinstance(table.get('countries'), dict) else {}
    csv_data.append({
        'country_name': country.get('name', ''),
        'country_code': country.get('code', ''),
        'table_number': table.get('table_number', ''),
        'table_name': table.get('table_name', ''),
        'table_description': table.get('table_description', ''),
        'file_name': table.get('file_name', ''),
        'original_filename': table.get('original_filename', ''),
        'has_time_series': table.get('has_time_series', False),
        'num_columns': table.get('num_columns', ''),
        'created_at': table.get('created_at', '')
    })

# Write to CSV
output_file = 'table_names_export.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as f:
    if csv_data:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)
        print(f"\n✓ Successfully exported {len(csv_data)} tables to {output_file}")
    else:
        print("\n✗ No tables found to export")
