-- Fix Romania Table 2_7_1 year format
-- Convert ONLY these 3 specific entries:
--   "2.03K" → "2030"
--   "2.04K" → "2040"
--   "2.05K" → "2050"
-- This fixes the data so it matches properly with the chart queries

-- First, let's find the table ID for Romania's Table 2_7_1
-- Run this query to find the table:
-- SELECT dt.id, dt.table_number, dt.table_description, dt.file_name, c.name, c.code
-- FROM data_tables dt
-- JOIN countries c ON dt.country_id = c.id
-- WHERE c.code = 'ROU' 
-- AND (dt.table_description LIKE '%Expected_Annual_GHG_Emissions_Total%' 
--      OR dt.file_name LIKE '%2_7_1%Expected_Annual_GHG_Emissions_Total%');

-- Update ONLY the 3 specific Year column values in the row_data JSONB
-- This updates ONLY data points for Romania Table 2_7_1 with the exact values "2.03K", "2.04K", "2.05K"

UPDATE data_points
SET row_data = jsonb_set(
    row_data,
    '{Year}',
    CASE 
        WHEN row_data->>'Year' = '2.03K' OR row_data->>'Year' LIKE '2.03K%' THEN '"2030"'
        WHEN row_data->>'Year' = '2.04K' OR row_data->>'Year' LIKE '2.04K%' THEN '"2040"'
        WHEN row_data->>'Year' = '2.05K' OR row_data->>'Year' LIKE '2.05K%' THEN '"2050"'
        ELSE row_data->'Year'
    END
)
WHERE data_table_id IN (
    SELECT dt.id
    FROM data_tables dt
    JOIN countries c ON dt.country_id = c.id
    WHERE c.code = 'ROU' 
    AND (
        dt.table_description LIKE '%Expected_Annual_GHG_Emissions_Total%'
        OR dt.file_name LIKE '%2_7_1%Expected_Annual_GHG_Emissions_Total%'
        OR (dt.table_number = '2_7' AND dt.table_description LIKE '%1_Expected_Annual_GHG_Emissions_Total%')
    )
)
AND row_data->>'Year' IS NOT NULL
AND (
    row_data->>'Year' = '2.03K' 
    OR row_data->>'Year' LIKE '2.03K%'
    OR row_data->>'Year' = '2.04K' 
    OR row_data->>'Year' LIKE '2.04K%'
    OR row_data->>'Year' = '2.05K' 
    OR row_data->>'Year' LIKE '2.05K%'
);

-- Verify the update
-- Run this to check the results before and after:
-- SELECT dp.row_data->>'Year' as year_value, COUNT(*) as count
-- FROM data_points dp
-- JOIN data_tables dt ON dp.data_table_id = dt.id
-- JOIN countries c ON dt.country_id = c.id
-- WHERE c.code = 'ROU' 
-- AND (
--     dt.table_description LIKE '%Expected_Annual_GHG_Emissions_Total%'
--     OR dt.file_name LIKE '%2_7_1%Expected_Annual_GHG_Emissions_Total%'
-- )
-- GROUP BY dp.row_data->>'Year'
-- ORDER BY dp.row_data->>'Year';
