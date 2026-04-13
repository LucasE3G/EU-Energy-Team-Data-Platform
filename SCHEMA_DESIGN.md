# Database Schema Design Explanation

## Overview

The schema is designed to handle **399 CSV files** with varying structures:
- **187 files** with time-series data (years as columns: 2024-2030, 2031-2040, etc.)
- **212 files** without time-series data (simple key-value or multi-column tables)
- Column counts ranging from **2 to 19 columns**

## Key Design Decisions

### 1. JSONB for Flexible Data Storage

**Why JSONB?**
- Each CSV has different columns (2-19 columns)
- Some have time-series columns, some don't
- Column names vary significantly between files
- JSONB allows storing any CSV structure without schema changes

**Example Storage:**
```json
{
  "Category": "ECOLE",
  "Number": "5936"
}
```

or

```json
{
  "Part of the park studied": "Total residential park",
  "Indicator": "Number of buildings to be renovated annually",
  "2024-2030": "50984",
  "2031-2040": "89630",
  "2041-2050": "96564"
}
```

### 2. Filename Metadata Extraction

The **filename contains critical information** about what the data represents:
- `Table_1_Number_of_public_buildings_certified_by_type.csv`
- `Table_7_Targets_annual_residential_renovation_rates.csv`
- `Table_15_Projections_emissions_per_unit_area.csv`

**Stored Metadata:**
- `table_number`: "1", "7", "15", "2_3", "3_7" (extracted from filename)
- `description`: "Number_of_public_buildings_certified_by_type" (from filename)
- `has_time_series`: Boolean (detected from columns or filename)
- `column_names`: Array of all column names
- `num_columns`: Count of columns

### 3. Schema Structure

```
countries
  ├── id, code, name
  
data_tables (metadata about each CSV)
  ├── id, country_id, table_name
  ├── table_number, original_filename
  ├── has_time_series, column_names, num_columns
  └── metadata (JSONB for additional info)
  
data_points (actual CSV rows)
  ├── id, data_table_id
  └── row_data (JSONB - flexible structure)
  
measures (policy measures)
  ├── id, country_id
  └── measure fields (structured)
```

## Querying Examples

### Find all time-series tables for a country
```sql
SELECT * FROM data_tables 
WHERE country_id = 1 AND has_time_series = true;
```

### Get data from a specific table
```sql
SELECT row_data FROM data_points 
WHERE data_table_id = 123;
```

### Query JSONB data
```sql
-- Find rows where Category = 'ECOLE'
SELECT * FROM data_points 
WHERE row_data->>'Category' = 'ECOLE';

-- Get 2030 values from time-series data
SELECT row_data->>'2030 [kgCO2eq/m2.year]' 
FROM data_points 
WHERE data_table_id = 15;
```

### Find tables by description
```sql
SELECT * FROM data_tables 
WHERE table_description ILIKE '%renovation%';
```

## Benefits

1. **Flexible**: Handles any CSV structure without schema changes
2. **Queryable**: PostgreSQL JSONB with GIN indexes for fast queries
3. **Metadata-rich**: Filename information preserved and searchable
4. **Scalable**: Can add new CSV files without migration
5. **Type-aware**: Can distinguish time-series vs non-time-series tables

## Future Enhancements

- Add data type detection (numbers, percentages, dates)
- Create views for common query patterns
- Add full-text search on descriptions
- Create materialized views for aggregated data
