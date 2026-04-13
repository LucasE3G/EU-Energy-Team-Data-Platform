# Action Required - Fix Dashboard Charts and Measures

## Issue 1: Dashboard Still Shows Old GHG Chart

The dashboard is still showing the old broken chart instead of the two new charts. This is likely because:
1. Browser cache is showing old JavaScript
2. The chart container needs to be cleared properly

### Solution:
1. **Hard refresh your browser**: Press `Ctrl+F5` (Windows) or `Cmd+Shift+R` (Mac) to clear cache
2. **Check browser console**: Open Developer Tools (F12) and look for:
   - Messages like "Found X tables for GHG Emission Reductions"
   - Any errors in red
3. **Verify the charts are loading**: You should see TWO separate charts:
   - "GHG Emission Reductions (Per Area)" with Y-axis labeled "kgCO2eq/m²"
   - "GHG Emission Reductions (Total)" with Y-axis labeled "kt CO2 eq"

## Issue 2: Measures Table Missing entities_responsible Column

The measures table schema was based on Belgium data which uses "stakeholders", but other countries use "entities_responsible".

### Solution Steps:

#### Step 1: Add Column to Database
Run this SQL in Supabase SQL Editor:
```sql
ALTER TABLE measures 
ADD COLUMN IF NOT EXISTS entities_responsible TEXT;
```

Or use the file: `add_entities_responsible_column.sql`

#### Step 2: Re-upload All Measures
Run this command:
```bash
python reupload_all_measures.py
```

This will:
- Read all measure CSV files
- Handle both `stakeholders` and `entities_responsible` columns
- Populate both fields appropriately
- Re-upload all measures to Supabase

## What Was Fixed in Code

### 1. Measures Upload (`upload_to_supabase.py`)
- Now handles both `stakeholders` and `entities_responsible` columns
- Maps data to both fields for compatibility

### 2. Frontend Stakeholder Mapping (`app.js`)
- Updated to check both columns
- Uses `entities_responsible` when `stakeholders` is not available

### 3. Dashboard GHG Charts (`app.js`)
- Split into two charts: Per Area and Total
- Improved column detection for various formats
- Better handling of Bulgaria Table 22 (uses "Total" row)
- Proper unit labels on Y-axis
- Fixed time period extraction (no duplicates)

## Verification Steps

After completing the steps above:

1. **Check Dashboard Charts**:
   - Should see TWO GHG charts (not one)
   - Y-axis should show proper units (kgCO2eq/m² or kt CO2 eq)
   - X-axis should show only 2030, 2040, 2050 (no duplicates)
   - Bulgaria should show data (not zero)

2. **Check Stakeholder Mapping**:
   - Navigate to any country's "Stakeholders" tab
   - Should see stakeholders/entities for all countries
   - Belgium should show from "stakeholders" column
   - Other countries should show from "entities_responsible" column

3. **Check Console Logs**:
   - Open browser console (F12)
   - Look for messages about tables found and charts created
   - Should see: "Found X tables for GHG Emission Reductions (Per Area)"
   - Should see: "Found X tables for GHG Emission Reductions (Total)"

## If Charts Still Don't Show

1. Check browser console for errors
2. Verify tables exist in Supabase with correct table numbers
3. Check that tables have `has_time_series = true`
4. Verify data points exist for those tables
