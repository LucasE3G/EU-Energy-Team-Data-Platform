# Export Table Names from Supabase to CSV

## Method 1: Using Python Script (Recommended - Gets ALL Tables)

**This method bypasses the 100-row limit:**

1. **Install dependencies** (if not already installed):
   ```bash
   pip install supabase python-dotenv
   ```

2. **Run the Python script**:
   ```bash
   python export_all_tables.py
   ```

3. **Output**: The script will create `table_names_export.csv` with ALL tables

## Method 2: Using Supabase SQL Editor

**Note**: Supabase SQL Editor may show only 100 rows, but CSV export should include all rows.

1. **Open Supabase Dashboard**
   - Go to your Supabase project dashboard
   - Navigate to **SQL Editor** in the left sidebar

2. **Run the SQL Query**
   - Copy the SQL query from `export_table_names.sql`
   - Paste it into the SQL Editor
   - Click **Run** to execute the query

3. **Export Results**
   - After the query runs, you'll see the results in a table (may show only 100)
   - Click the **Download** button (usually at the top right of the results table)
   - Select **CSV** format
   - **The CSV export should include ALL rows**, not just the 100 displayed
   - The file will be downloaded to your computer

**If CSV export still only has 100 rows**, use Method 1 (Python script) instead.

## Method 2: Using Supabase CLI

If you have Supabase CLI installed:

```bash
# Set your Supabase project URL and API key
export SUPABASE_URL="your-project-url"
export SUPABASE_KEY="your-service-role-key"

# Run the query and save to CSV
supabase db query --sql "SELECT c.name AS country_name, c.code AS country_code, dt.table_number, dt.table_name, dt.table_description, dt.file_name, dt.original_filename, dt.has_time_series, dt.num_columns, dt.created_at FROM data_tables dt LEFT JOIN countries c ON dt.country_id = c.id ORDER BY c.name, dt.table_number, dt.table_name;" --output csv > table_names.csv
```

## Method 3: Using Python Script

Create a Python script to export:

```python
import csv
from supabase import create_client, Client

# Initialize Supabase client
url = "your-supabase-url"
key = "your-supabase-key"
supabase: Client = create_client(url, key)

# Query data
response = supabase.table('data_tables').select('*, countries(name, code)').execute()

# Write to CSV
with open('table_names.csv', 'w', newline='', encoding='utf-8') as f:
    if response.data:
        writer = csv.DictWriter(f, fieldnames=response.data[0].keys())
        writer.writeheader()
        writer.writerows(response.data)
```

## Query Options

The `export_table_names.sql` file contains three query options:

1. **Option 1 (Default)**: Full table metadata including all fields
2. **Option 2**: Simplified version with row counts
3. **Option 3**: Includes column names as JSON

Choose the option that best fits your needs by uncommenting the desired query.
