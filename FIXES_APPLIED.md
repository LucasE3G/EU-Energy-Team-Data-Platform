# Fixes Applied

## 1. Measures Table - Added entities_responsible Column Support

### Schema Update
- Added `entities_responsible TEXT` column to measures table schema
- Created `add_entities_responsible_column.sql` to add the column to existing database

### Upload Script Update
- Updated `upload_to_supabase.py` to handle both `stakeholders` and `entities_responsible` columns
- Now maps both columns: if CSV has `entities_responsible`, it populates both fields
- Created `reupload_all_measures.py` to re-upload all measures with the new column support

### Frontend Update
- Updated `loadStakeholderMapping()` to select both columns
- Updated `parseStakeholders()` to use `entities_responsible` when `stakeholders` is not available
- Updated `showMeasureDetail()` to display the correct field

## 2. Dashboard GHG Charts - Fixed Data Extraction

### Chart Categories
- Split into two separate charts:
  - **GHG Emission Reductions (Per Area)** - kgCO2eq/m²
  - **GHG Emission Reductions (Total)** - kt CO2 eq

### Data Extraction Improvements
- Enhanced column detection to find per-area columns (handles various formats)
- Improved handling of Bulgaria Table 22 (prefers "Total" row)
- Better filtering of text values vs numeric values
- Proper unit conversion (tonnes→kt, Mt→kt)
- Fixed time period extraction (only 2030, 2040, 2050 - no duplicates)

### Chart Rendering
- Added proper unit labels on Y-axis
- Clear container before loading new charts
- Better error handling and console logging

## Next Steps

1. **Run SQL migration**: Execute `add_entities_responsible_column.sql` in Supabase SQL Editor
2. **Re-upload measures**: Run `python reupload_all_measures.py` to update all measures with new column support
3. **Clear browser cache**: Hard refresh (Ctrl+F5) to ensure new JavaScript loads
4. **Check console**: Look for debug messages showing which tables are found and charts created
