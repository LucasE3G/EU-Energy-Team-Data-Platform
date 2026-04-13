# GHG Emission Tables - Final Analysis

## Confirmed Tables

### Belgium ✅
- **Table 15: Projections Emissions Per Unit Area**
  - Units: **kgCO2eq/m2.year** (per area)
  - Time periods: 2030, 2040, 2050
  - Status: ✅ **CONFIRMED** - This is the correct table for Belgium per-area emissions
  - Structure: Sector × Building Type × Years (2030, 2040, 2050)

**Note**: Belgium Table 15 is per-area only. If the chart shows Belgium with total emissions (kt CO2 eq), it's likely:
- Aggregating incorrectly, OR
- Using a different Belgium table that wasn't found

### Bulgaria ✅
- **Table N26: Annual Operational Carbon Emissions 2023**
  - Units: **BOTH** - Total (MtCO2eq) AND Per Area (kgCO2eq/(m2.y))
  - Time period: **2023 baseline only** (not projections)
  - Status: ✅ **FOUND** - Has baseline 2023 data
  - Structure: Building Type × Total emissions × Per area intensity

- **Table N27: Reduction Of Annual Operational Carbon Emissions**
  - Units: **kgCO2eq/(m2.y)** (reduction values)
  - Time period: **Not time-series** (reduction factors, not projections)
  - Status: ✅ **FOUND** - Contains reduction factors
  - Structure: Building Type × Reduction values

**Key Insight**: 
- N26 provides **baseline 2023** data (both total and per-area)
- N27 provides **reduction factors** (per-area reductions)
- **Can calculate projections**: N26 baseline × N27 reductions = projected emissions for 2030/2040/2050
- However, N27 is NOT time-series, so it may need assumptions or additional data to project to specific years

## Updated Comparability Assessment

### Group A: Per-Area Emissions (kgCO2eq/m2) - DIRECTLY COMPARABLE
✅ **Belgium Table 15**: 2030, 2040, 2050 projections
✅ **Romania Table 2_7**: Per-area column (2030, 2040, 2050)
✅ **Spain Table 2_3**: Per-area column (2030, 2040, 2050)
✅ **Bulgaria Table N26**: Baseline 2023 (can calculate projections using N27)
⚠️ **Croatia**: Needs verification

### Group B: Total Emissions (kt/Mt CO2 eq) - COMPARABLE AFTER UNIT CONVERSION
✅ **Finland**: Tables 2_6 + 2_7 (residential + non-residential) - kt CO2 eq
✅ **Lithuania**: Tables 63 + 64 - tonnes CO2eq (÷1000 = kt)
✅ **Slovenia**: Table 14 - kt CO2 eq
✅ **Romania Table 2_7**: Total column - MtCO2eq (×1000 = kt)
✅ **Spain Table 2_3**: Total column - MtCO2eq (×1000 = kt)
✅ **Bulgaria Table N26**: Total column - MtCO2eq (×1000 = kt) - baseline 2023 only

## Chart Recommendations

### Option 1: Per-Area Emissions Chart (kgCO2eq/m2)
**Countries with 2030/2040/2050 projections:**
- Belgium Table 15 ✅
- Romania Table 2_7 (per-area) ✅
- Spain Table 2_3 (per-area) ✅
- Croatia (if verified) ⚠️

**Bulgaria**: 
- Has baseline 2023 data (N26)
- Has reduction factors (N27)
- **Challenge**: N27 is not time-series, so projecting to 2030/2040/2050 requires:
  - Assumptions about reduction timeline, OR
  - Additional data about reduction schedule

### Option 2: Total Emissions Chart (kt CO2 eq - normalized)
**Countries with 2030/2040/2050 projections:**
- Finland (Tables 2_6 + 2_7) ✅
- Lithuania (Tables 63 + 64) ✅
- Slovenia Table 14 ✅
- Romania Table 2_7 (total) ✅
- Spain Table 2_3 (total) ✅

**Bulgaria**: 
- Has baseline 2023 total (N26)
- **Challenge**: No direct projections - would need to calculate from reductions

## Bulgaria Calculation Approach

To use Bulgaria N26 for 2030/2040/2050 projections:

1. **Using N26 baseline + N27 reductions:**
   ```
   Projected emissions (2030/2040/2050) = N26_baseline × (1 - N27_reduction_factor)
   ```
   - **Issue**: N27 doesn't specify which year the reduction applies to
   - **Assumption needed**: Apply reduction evenly across years, or use reduction schedule

2. **Alternative**: If Bulgaria has other tables with projections, use those instead

## Current Chart Issue

**Problem**: The chart shows Belgium vs Slovenia comparing total emissions (kt CO2 eq), but:
- Belgium Table 15 is **per-area** (kgCO2eq/m2), not total
- This suggests either:
  1. Wrong Belgium table is being used
  2. Belgium data is being incorrectly converted/aggregated
  3. There's another Belgium table with total emissions that wasn't found

**Solution**: 
- For per-area comparison: Use Belgium Table 15 ✅
- For total emissions comparison: Need to find Belgium's total emissions table OR use per-area data only if building area is available

## Action Items

1. ✅ **Belgium Table 15 confirmed** - Use for per-area emissions
2. ✅ **Bulgaria N26 found** - Has baseline 2023 data (both total and per-area)
3. ✅ **Bulgaria N27 found** - Has reduction factors (but not time-series)
4. ⚠️ **Bulgaria projections** - Need to determine how to calculate 2030/2040/2050 from N26+N27
5. ⚠️ **Belgium total emissions** - Chart shows total but Table 15 is per-area - investigate
6. ⚠️ **Croatia verification** - Confirm which table has per-area data

## Next Steps

1. Update chart code to use Belgium Table 15 for per-area comparison
2. Determine Bulgaria projection calculation method (N26 + N27)
3. Create separate charts for per-area vs total emissions
4. Add unit conversion logic (tonnes→kt, Mt→kt)
5. Investigate why Belgium chart shows total emissions when Table 15 is per-area
