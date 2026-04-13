"""
Analyze all measures CSV files to find all unique columns
"""

import csv
from pathlib import Path

measures_dir = Path("data/measures")
all_columns = set()

print("Analyzing measures CSV files...\n")

for csv_file in measures_dir.glob("*.csv"):
    print(f"{csv_file.name}:")
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            columns = list(reader.fieldnames)
            print(f"  Columns: {columns}")
            all_columns.update(columns)
    except Exception as e:
        print(f"  Error: {e}")
    print()

print("="*80)
print("All unique columns found across all measure files:")
print("="*80)
for col in sorted(all_columns):
    print(f"  - {col}")
