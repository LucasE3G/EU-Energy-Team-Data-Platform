# Final Supabase Schema Design

## Analysis Summary

After analyzing **399 CSV files** from 8 countries:

- **187 files** have time-series data (years as columns)
- **212 files** have non-time-series data (simple tables)
- Column counts range from **2 to 19 columns**
- Filenames contain critical metadata (table numbers, descriptions)

## Schema Design Rationale

### 1. **JSONB for Data Storage** ✓

**Why JSONB is optimal:**
- Each CSV has different column structures
- Some have 2 columns, others have 19
- Column names vary significantly
- Time-series columns have different year formats
- No schema changes needed when adding new files

**Example structures handled:**
```
Simple: {"Category": "ECOLE", "Number": "5936"}
Time-series: {"Indicator": "...", "2024-2030": "50984", "2031-2040": "89630"}
Complex: {"Sector": "...", "2030 [kgCO2eq/m2.year]": "11.64", "2030 [%]": "-24%", ...}
```

### 2. **Metadata Extraction from Filenames** ✓

Filenames contain essential information:
- `Table_1_Number_of_public_buildings_certified_by_type.csv`
- `Table_7_Targets_annual_residential_renovation_rates.csv`
- `Table_15_Projections_emissions_per_unit_area.csv`

**Captured metadata:**
- `table_number`: "1", "7", "15", "2_3", "3_7"
- `description`: Extracted from filename
- `has_time_series`: Detected from columns
- `column_names`: Array of all columns
- `num_columns`: Column count

### 3. **Schema Structure**

```
countries
├── id, code, name
│
data_tables (metadata)
├── id, country_id, table_name
├── table_number, original_filename
├── has_time_series, column_names, num_columns
└── metadata (JSONB for time columns, etc.)
│
data_points (actual data)
├── id, data_table_id
└── row_data (JSONB - flexible structure)
│
measures (policy measures)
└── Structured columns matching CSV
```

## Query Capabilities

### Find tables by type
```sql
-- All time-series tables
SELECT * FROM data_tables WHERE has_time_series = true;

-- Tables with specific keywords
SELECT * FROM data_tables 
WHERE table_description ILIKE '%renovation%';
```

### Query JSONB data
```sql
-- Simple column lookup
SELECT row_data->>'Category' FROM data_points 
WHERE data_table_id = 1;

-- Time-series data
SELECT row_data->>'2030 [TWh]' FROM data_points 
WHERE data_table_id = 10;

-- Filter by value
SELECT * FROM data_points 
WHERE row_data->>'Category' = 'ECOLE';
```

### Cross-country comparison
```sql
-- Compare same table across countries
SELECT c.name, dt.table_name, COUNT(dp.id) as row_count
FROM data_tables dt
JOIN countries c ON dt.country_id = c.id
JOIN data_points dp ON dt.id = dp.data_table_id
WHERE dt.table_number = '7'
GROUP BY c.name, dt.table_name;
```

## Performance Optimizations

1. **GIN Index on JSONB**: Fast queries on JSONB data
2. **Indexes on metadata**: Quick filtering by country, table type
3. **Index on time-series flag**: Fast filtering of time-series tables
4. **Index on table_number**: Quick lookup by table number

## Benefits

✅ **Flexible**: Handles any CSV structure (2-19 columns)
✅ **Queryable**: PostgreSQL JSONB with efficient indexes
✅ **Metadata-rich**: Filename information preserved
✅ **Scalable**: No migrations needed for new files
✅ **Type-aware**: Distinguishes time-series vs non-time-series
✅ **Searchable**: Full-text search on descriptions

## Migration Steps

1. Run `supabase_schema.sql` in Supabase SQL Editor
2. Run `python upload_to_supabase.py` to upload all data
3. Verify data with queries
4. Start building visualizations!
