"""
Script to analyze GHG emission tables across countries
and assess their comparability
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

print("Analyzing GHG emission tables across countries...\n")

# Keywords to search for GHG-related tables
ghg_keywords = [
    'emission', 'ghg', 'co2', 'greenhouse', 'carbon', 'co2eq', 'co2 eq'
]

# Target table descriptions from user
target_tables = {
    'Belgium': [
        'Projections Emissions Per Unit Area',
        'Operational Greenhouse Gas Emissions Of Buildings By Building Type',
        'Operational Greenhouse Gas Emissions',
        'emission', 'ghg', 'co2'  # Broader search for Belgium
    ],
    'Finland': [
        'Changes In The Energy Consumption And Emissions Of The Residential Building',
        'Energy Consumption And Co2 Emissionsof The Non-Residential Building Stock'
    ],
    'Lithuania': [
        'Ghg Emission Reduction Targets',
        'Estimated Operational Ghg Emissions Reduction'
    ],
    'Romania': [
        'Expected Annual Ghg Emissions Total'
    ],
    'Slovenia': [
        'Greenhouse Gas Emissions Of Buildings In Observed Years'
    ],
    'Spain': [
        'Co2Eq Emissions Per Use And Stages For Decarbonising'
    ],
    'Bulgaria': [
        'Annual Operational Carbon Emissions',  # Table N26
        'table n26', 'n26', '26', 'carbon emissions', 'operational carbon'
    ]
}

# Fetch all countries
countries_response = supabase.table('countries').select('*').execute()
countries = {c['name']: c for c in countries_response.data}
country_codes = {c['name']: c['code'] for c in countries_response.data}

print("Countries found:", list(countries.keys()))
print("\n" + "="*80 + "\n")

# Fetch all tables
all_tables_response = supabase.table('data_tables').select('*, countries(name, code)').execute()
all_tables = all_tables_response.data

# Find matching tables
found_tables = {}
for country_name, table_descriptions in target_tables.items():
    country_code = country_codes.get(country_name, '')
    found_tables[country_name] = []
    
    for desc in table_descriptions:
        # Search for tables matching description
        matches = []
        for table in all_tables:
            country = table.get('countries', {}) if isinstance(table.get('countries'), dict) else {}
            table_country = country.get('name', '')
            
            if country_name.lower() in table_country.lower() or country_code.lower() in str(table.get('country_id', '')):
                table_desc = (table.get('table_description') or '').lower()
                table_name = (table.get('table_name') or '').lower()
                search_term = desc.lower()
                
                # Check if description matches
                if search_term in table_desc or search_term in table_name:
                    matches.append(table)
                # Also check table numbers for Bulgaria (N26 only)
                elif ('n26' in search_term or search_term == '26' or 'annual operational carbon' in search_term.lower()) and '26' in str(table.get('table_number', '')):
                    matches.append(table)
                # For Bulgaria, also search by keywords
                elif country_name == 'Bulgaria' and any(kw in table_desc.lower() or kw in table_name.lower() for kw in ['annual operational carbon', 'carbon emissions', 'operational carbon']):
                    matches.append(table)
        
        found_tables[country_name].extend(matches)

# Now analyze each found table
results = {}

for country_name, tables in found_tables.items():
    if not tables:
        print(f"[NOT FOUND] {country_name}: No matching tables found")
        continue
    
    print(f"\n{'='*80}")
    print(f"[{country_name}]")
    print(f"{'='*80}")
    
    results[country_name] = []
    
    results[country_name] = []
    
    for table in tables:
        table_id = table['id']
        table_desc = table.get('table_description', 'N/A')
        table_num = table.get('table_number', 'N/A')
        has_time_series = table.get('has_time_series', False)
        column_names = table.get('column_names', [])
        
        print(f"\n  Table: {table_desc}")
        print(f"  Table Number: {table_num}")
        print(f"  Has Time Series: {has_time_series}")
        
        # Fetch sample data points
        data_response = supabase.table('data_points')\
            .select('row_data')\
            .eq('data_table_id', table_id)\
            .limit(5)\
            .execute()
        
        sample_rows = data_response.data[:3] if data_response.data else []
        
        # Analyze structure
        time_columns = []
        value_columns = []
        unit_info = {}
        
        if sample_rows:
            first_row = sample_rows[0]['row_data']
            all_keys = list(first_row.keys())
            
            # Find time columns (2030, 2040, 2050, etc.)
            for key in all_keys:
                key_lower = key.lower()
                if any(year in key for year in ['2030', '2040', '2050', '2020', '2025']):
                    time_columns.append(key)
                # Find value columns with units
                if any(unit in key_lower for unit in ['kgco2eq', 'co2', 'ghg', 'emission', 'kt', 'tco2']):
                    value_columns.append(key)
                    # Extract unit info
                    if 'kgco2eq' in key_lower or 'kg co2' in key_lower:
                        unit_info['unit'] = 'kgCO2eq/m2'
                        if '/m2' in key_lower or 'per m2' in key_lower or 'm2' in key_lower or '/m' in key_lower:
                            unit_info['type'] = 'per_area'
                        else:
                            unit_info['type'] = 'per_area'  # Default assumption
                    elif 'mtco2eq' in key_lower or 'mt co2' in key_lower:
                        unit_info['unit'] = 'Mt CO2 eq'
                        unit_info['type'] = 'total'
                    elif 'kt' in key_lower or 'kt co2' in key_lower:
                        unit_info['unit'] = 'kt CO2 eq'
                        unit_info['type'] = 'total'
                    elif 'tco2' in key_lower or 'tonnes co2eq' in key_lower or 'tonnes co2' in key_lower:
                        unit_info['unit'] = 't CO2 eq'
                        unit_info['type'] = 'total'
            
            print(f"  Time Columns Found: {time_columns[:5]}")
            print(f"  Value Columns Found: {value_columns[:5]}")
            print(f"  Unit Type: {unit_info.get('unit', 'Unknown')} ({unit_info.get('type', 'Unknown')})")
            
            # Show sample data structure
            print(f"\n  Sample Row Structure:")
            for key, value in list(first_row.items())[:8]:
                print(f"    {key}: {value}")
        
        # Count total rows
        count_response = supabase.table('data_points')\
            .select('id', count='exact')\
            .eq('data_table_id', table_id)\
            .execute()
        
        row_count = count_response.count if hasattr(count_response, 'count') else len(data_response.data)
        print(f"  Total Rows: {row_count}")
        
        results[country_name].append({
            'table_id': table_id,
            'table_number': table_num,
            'description': table_desc,
            'has_time_series': has_time_series,
            'time_columns': time_columns,
            'value_columns': value_columns,
            'unit_info': unit_info,
            'row_count': row_count,
            'sample_structure': sample_rows[0]['row_data'] if sample_rows else {}
        })

# Summary analysis
print(f"\n\n{'='*80}")
print("[COMPARABILITY ANALYSIS]")
print(f"{'='*80}\n")

# Group by unit type
per_area_tables = []
total_tables = []
unknown_tables = []

for country, tables in results.items():
    for table in tables:
        unit_type = table['unit_info'].get('type', 'unknown')
        if unit_type == 'per_area':
            per_area_tables.append((country, table))
        elif unit_type == 'total':
            total_tables.append((country, table))
        else:
            unknown_tables.append((country, table))

print("[PER AREA] Tables with kgCO2eq/m2 units:")
for country, table in per_area_tables:
    print(f"  - {country}: {table['description']} (Table {table['table_number']})")

print("\n[TOTAL] Tables with kt CO2 eq units:")
for country, table in total_tables:
    print(f"  - {country}: {table['description']} (Table {table['table_number']})")

print("\n[UNKNOWN] Tables with unknown units:")
for country, table in unknown_tables:
    print(f"  - {country}: {table['description']} (Table {table['table_number']})")

# Check time periods
print("\n\n[TIME PERIODS] Analysis:")
time_periods = {}
for country, tables in results.items():
    for table in tables:
        time_cols = table['time_columns']
        if time_cols:
            periods = []
            for col in time_cols:
                if '2030' in col:
                    periods.append('2030')
                if '2040' in col:
                    periods.append('2040')
                if '2050' in col:
                    periods.append('2050')
            if periods:
                key = '-'.join(sorted(set(periods)))
                if key not in time_periods:
                    time_periods[key] = []
                time_periods[key].append(f"{country}: {table['description']}")

for period, tables_list in time_periods.items():
    print(f"\n  Period: {period}")
    for table_info in tables_list:
        print(f"    - {table_info}")

# Save results to JSON
with open('ghg_tables_analysis.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False, default=str)

print(f"\n\n[SAVED] Detailed analysis saved to: ghg_tables_analysis.json")
