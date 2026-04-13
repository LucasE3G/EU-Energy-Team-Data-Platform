# Schema Design Explanation

## Current Approach: JSONB Storage

The schema uses **JSONB** to store CSV rows because:

### Why JSONB?
1. **Flexibility**: Each CSV file has different columns
   - Table_1: `Category, Number`
   - Table_7: `Part of park, Indicator, 2024-2030, 2031-2040, 2041-2050`
   - Table_15: `Sector, Type, 2030 [kgCO2eq/m2.year], 2030 [%], 2040 [...], ...`

2. **Query Performance**: PostgreSQL JSONB with GIN indexes allows fast queries:
   ```sql
   -- Find all rows where a specific column equals a value
   SELECT * FROM data_points 
   WHERE row_data->>'Category' = 'ECOLE';
   
   -- Find rows with time-series data
   SELECT row_data->>'2024-2030' FROM data_points 
   WHERE data_table_id = 123;
   ```

3. **No Schema Changes**: Adding new CSV files doesn't require schema changes

### How Data is Stored

Example: Table_1 row becomes:
```json
{
  "Category": "ECOLE",
  "Number": "5936"
}
```

Example: Table_7 row becomes:
```json
{
  "Part of the park studied": "Total residential park",
  "Indicator": "Number of buildings to be renovated annually",
  "2024-2030": "50984",
  "2031-2040": "89630",
  "2041-2050": "96564"
}
```

## Alternative: Structured Schema

If you prefer a more structured approach, we could:
- Create separate tables for each CSV structure type
- Use a key-value table for flexible columns
- Normalize time-series data into separate rows

Would you like me to create an alternative schema?
