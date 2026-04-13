"""
Test script to show how your CSV data will be stored in the database
Run this to see examples before uploading
"""

import csv
import json
from pathlib import Path

def show_csv_structure(csv_path):
    """Show how a CSV file will be stored"""
    print(f"\n{'='*60}")
    print(f"File: {csv_path.name}")
    print(f"{'='*60}")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        print(f"\nColumns: {', '.join(headers)}")
        
        print("\nFirst 3 rows as they will be stored in JSONB:")
        for i, row in enumerate(reader):
            if i >= 3:
                break
            print(f"\nRow {i+1}:")
            print(json.dumps(row, indent=2, ensure_ascii=False))
    
    print(f"\n{'='*60}\n")

# Test with sample files
BASE_DIR = Path(r"C:\Users\LucasDeschênes\coding\extract data")

test_files = [
    BASE_DIR / "Belgium data" / "Table_1_Number_of_public_buildings_certified_by_type.csv",
    BASE_DIR / "Belgium data" / "Table_7_Targets_annual_residential_renovation_rates.csv",
    BASE_DIR / "Belgium data" / "Table_15_Projections_emissions_per_unit_area.csv",
]

for file_path in test_files:
    if file_path.exists():
        show_csv_structure(file_path)
