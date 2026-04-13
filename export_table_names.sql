-- SQL Query to Export All Table Names from Supabase
-- Run this in Supabase SQL Editor and export results as CSV
-- Note: Supabase SQL Editor may limit results, but CSV export should include all rows

-- Option 1: Basic table names with country info (ALL TABLES - no limit)
SELECT 
    c.name AS country_name,
    c.code AS country_code,
    dt.table_number,
    dt.table_name,
    dt.table_description,
    dt.file_name,
    dt.original_filename,
    dt.has_time_series,
    dt.num_columns,
    dt.created_at
FROM data_tables dt
LEFT JOIN countries c ON dt.country_id = c.id
ORDER BY c.name, dt.table_number, dt.table_name
LIMIT 10000; -- Set high limit to ensure all tables are included

-- Option 2: Simplified version (just essential info)
-- SELECT 
--     c.name AS country_name,
--     c.code AS country_code,
--     dt.table_number,
--     dt.table_description,
--     dt.has_time_series,
--     COUNT(dp.id) AS row_count
-- FROM data_tables dt
-- LEFT JOIN countries c ON dt.country_id = c.id
-- LEFT JOIN data_points dp ON dt.id = dp.data_table_id
-- GROUP BY c.name, c.code, dt.table_number, dt.table_description, dt.has_time_series
-- ORDER BY c.name, dt.table_number;

-- Option 3: With column names (if you want to see what columns each table has)
-- SELECT 
--     c.name AS country_name,
--     c.code AS country_code,
--     dt.table_number,
--     dt.table_description,
--     dt.has_time_series,
--     dt.column_names::text AS column_names,
--     dt.num_columns
-- FROM data_tables dt
-- LEFT JOIN countries c ON dt.country_id = c.id
-- ORDER BY c.name, dt.table_number;
