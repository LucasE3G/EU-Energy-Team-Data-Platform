"""
Analyze all CSV files to understand their structure and create a better schema
"""

import csv
import json
from pathlib import Path
import re
from collections import defaultdict

BASE_DIR = Path("data")

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
        'has_time_series': has_time_series,
        'original_filename': filename
    }

def analyze_csv_structure(csv_path):
    """Analyze a single CSV file"""
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Try to detect delimiter
            sample = f.read(1024)
            f.seek(0)
            
            sniffer = csv.Sniffer()
            delimiter = sniffer.sniff(sample).delimiter
            
            reader = csv.DictReader(f, delimiter=delimiter)
            headers = reader.fieldnames
            
            # Check for time-series columns
            time_columns = []
            non_time_columns = []
            
            for header in headers:
                # Check if column name contains years
                if re.search(r'\b(19|20)\d{2}\b', header):
                    time_columns.append(header)
                else:
                    non_time_columns.append(header)
            
            # Read first few rows to understand data
            sample_rows = []
            for i, row in enumerate(reader):
                if i >= 3:
                    break
                sample_rows.append(row)
            
            return {
                'headers': headers,
                'time_columns': time_columns,
                'non_time_columns': non_time_columns,
                'has_time_series': len(time_columns) > 0,
                'num_columns': len(headers),
                'sample_rows': sample_rows
            }
    except Exception as e:
        return {'error': str(e)}

def analyze_all_csvs():
    """Analyze all CSV files in the data directory"""
    results = {
        'countries': defaultdict(list),
        'measures': [],
        'summary': {
            'total_files': 0,
            'with_time_series': 0,
            'without_time_series': 0,
            'column_counts': defaultdict(int)
        }
    }
    
    # Analyze country data folders
    country_folders = [
        'Belgium data', 'Bulgaria data', 'Croatia data', 
        'Finland data', 'Lithuania data', 'Romania data',
        'Slovenia data', 'Spain data'
    ]
    
    for folder_name in country_folders:
        folder_path = BASE_DIR / folder_name
        if not folder_path.exists():
            continue
        
        country_code = folder_name.replace(' data', '').upper()
        if country_code == 'BELGIUM':
            country_code = 'BEL'
        elif country_code == 'BULGARIA':
            country_code = 'BGR'
        elif country_code == 'CROATIA':
            country_code = 'HRV'
        elif country_code == 'FINLAND':
            country_code = 'FIN'
        elif country_code == 'LITHUANIA':
            country_code = 'LTU'
        elif country_code == 'ROMANIA':
            country_code = 'ROU'
        elif country_code == 'SLOVENIA':
            country_code = 'SVN'
        elif country_code == 'SPAIN':
            country_code = 'ESP'
        
        csv_files = list(folder_path.glob('*.csv'))
        
        for csv_file in csv_files:
            metadata = extract_metadata_from_filename(csv_file.name)
            structure = analyze_csv_structure(csv_file)
            
            file_info = {
                'filename': csv_file.name,
                'path': str(csv_file),
                'metadata': metadata,
                'structure': structure
            }
            
            results['countries'][country_code].append(file_info)
            results['summary']['total_files'] += 1
            
            if structure.get('has_time_series'):
                results['summary']['with_time_series'] += 1
            else:
                results['summary']['without_time_series'] += 1
            
            if 'num_columns' in structure:
                results['summary']['column_counts'][structure['num_columns']] += 1
    
    # Analyze measures
    measures_folder = BASE_DIR / 'measures'
    if measures_folder.exists():
        for csv_file in measures_folder.glob('*.csv'):
            structure = analyze_csv_structure(csv_file)
            results['measures'].append({
                'filename': csv_file.name,
                'structure': structure
            })
    
    return results

if __name__ == '__main__':
    print("Analyzing CSV files...\n")
    results = analyze_all_csvs()
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total CSV files: {results['summary']['total_files']}")
    print(f"Files with time-series data: {results['summary']['with_time_series']}")
    print(f"Files without time-series: {results['summary']['without_time_series']}")
    print(f"\nColumn count distribution:")
    for count, num_files in sorted(results['summary']['column_counts'].items()):
        print(f"  {count} columns: {num_files} files")
    
    print("\n" + "=" * 80)
    print("BY COUNTRY")
    print("=" * 80)
    for country, files in results['countries'].items():
        print(f"\n{country}: {len(files)} files")
        time_series_count = sum(1 for f in files if f['structure'].get('has_time_series'))
        print(f"  - {time_series_count} with time-series, {len(files) - time_series_count} without")
    
    # Save detailed results
    with open('csv_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print("\n[OK] Detailed analysis saved to csv_analysis.json")
    
    # Show examples
    print("\n" + "=" * 80)
    print("EXAMPLE: Time-series table")
    print("=" * 80)
    for country, files in results['countries'].items():
        for file_info in files:
            if file_info['structure'].get('has_time_series'):
                print(f"\nFile: {file_info['filename']}")
                print(f"Metadata: {file_info['metadata']}")
                print(f"Time columns: {file_info['structure'].get('time_columns', [])}")
                print(f"Non-time columns: {file_info['structure'].get('non_time_columns', [])}")
                break
        if any(f['structure'].get('has_time_series') for f in files):
            break
    
    print("\n" + "=" * 80)
    print("EXAMPLE: Non-time-series table")
    print("=" * 80)
    for country, files in results['countries'].items():
        for file_info in files:
            if not file_info['structure'].get('has_time_series'):
                print(f"\nFile: {file_info['filename']}")
                print(f"Metadata: {file_info['metadata']}")
                print(f"Columns: {file_info['structure'].get('headers', [])}")
                break
        if any(not f['structure'].get('has_time_series') for f in files):
            break
