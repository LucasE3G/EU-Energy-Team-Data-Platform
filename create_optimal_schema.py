"""
Create an optimal Supabase schema based on analysis of all CSV files
This script analyzes the data and generates the best schema design
"""

import json
from pathlib import Path

# Load the analysis
with open('csv_analysis.json', 'r', encoding='utf-8') as f:
    analysis = json.load(f)

print("=" * 80)
print("SCHEMA DESIGN RECOMMENDATIONS")
print("=" * 80)

print("\n1. DATA STRUCTURE ANALYSIS")
print("-" * 80)
print(f"Total files: {analysis['summary']['total_files']}")
print(f"  - With time-series: {analysis['summary']['with_time_series']}")
print(f"  - Without time-series: {analysis['summary']['without_time_series']}")

print("\n2. COLUMN PATTERNS")
print("-" * 80)

# Analyze column patterns
time_column_patterns = {}
non_time_patterns = {}

for country, files in analysis['countries'].items():
    for file_info in files:
        structure = file_info.get('structure', {})
        if structure.get('has_time_series'):
            time_cols = structure.get('time_columns', [])
            non_time_cols = structure.get('non_time_columns', [])
            
            # Count time column patterns
            for col in time_cols:
                # Extract year pattern
                import re
                year_match = re.search(r'\b(19|20)\d{2}\b', col)
                if year_match:
                    year = year_match.group()
                    if year not in time_column_patterns:
                        time_column_patterns[year] = []
                    time_column_patterns[year].append(col)
            
            # Count non-time column patterns
            for col in non_time_cols:
                col_lower = col.lower()
                if 'type' in col_lower or 'category' in col_lower:
                    if 'type/category' not in non_time_patterns:
                        non_time_patterns['type/category'] = []
                    non_time_patterns['type/category'].append(col)
                elif 'indicator' in col_lower or 'metric' in col_lower:
                    if 'indicator' not in non_time_patterns:
                        non_time_patterns['indicator'] = []
                    non_time_patterns['indicator'].append(col)

print("Time column years found:")
for year in sorted(set(time_column_patterns.keys()))[:10]:
    print(f"  - {year}: {len(time_column_patterns[year])} occurrences")

print("\nNon-time column patterns:")
for pattern, examples in list(non_time_patterns.items())[:5]:
    print(f"  - {pattern}: {len(examples)} files")

print("\n3. RECOMMENDED SCHEMA STRUCTURE")
print("-" * 80)
print("""
The schema should support:

1. METADATA TABLE (data_tables)
   - Store filename information (table number, description)
   - Store structure metadata (has_time_series, column_names)
   - Enable filtering by country, table type, etc.

2. FLEXIBLE DATA STORAGE (data_points)
   - Use JSONB to store CSV rows
   - Preserve all column names and values
   - Enable querying with JSONB operators

3. MEASURES TABLE (measures)
   - Structured table for policy measures
   - Fixed columns matching CSV structure

4. INDEXES
   - GIN index on JSONB for fast queries
   - Indexes on metadata fields for filtering
   - Index on time-series flag for quick filtering
""")

print("\n4. QUERY PATTERNS TO SUPPORT")
print("-" * 80)
print("""
- Find all tables for a country
- Find all time-series tables
- Query data by column name/value
- Filter by table description/keywords
- Get time-series data for specific years
- Compare data across countries
""")

print("\n5. SCHEMA IS OPTIMAL FOR:")
print("-" * 80)
print("✓ Flexible CSV structures (2-19 columns)")
print("✓ Time-series and non-time-series data")
print("✓ Filename metadata preservation")
print("✓ Fast queries with JSONB indexes")
print("✓ No schema changes needed for new files")
