# GHG Emission Tables Comparability Analysis

## Summary

Based on analysis of GHG emission tables across countries, here's the comparability assessment:

## Tables Found by Country

### Belgium
- **Table 15: Projections Emissions Per Unit Area**
  - Units: **kgCO2eq/m2.year** (per area)
  - Time periods: 2030, 2040, 2050
  - Structure: Sector (Residential/Non-residential) × Building Type × Years
  - ✅ **COMPARABLE** with other per-area tables

- **Table (needs verification): Operational Greenhouse Gas Emissions Of Buildings By Building Type**
  - Status: Need to verify exact table number/name

### Finland
- **Table 2_6: Changes In The Energy Consumption And Emissions Of The Residential Building**
  - Units: **kt CO2 eq** (total emissions)
  - Time periods: 2020, 2025, 2030, 2035, 2040, 2045, 2050
  - Structure: Percentage changes over time
  - ⚠️ **NOT DIRECTLY COMPARABLE** with per-area tables (different unit type)

- **Table 2_7: Energy Consumption And Co2 Emissionsof The Non-Residential Building Stock**
  - Units: **kt CO2 eq** (total emissions)
  - Time periods: 2020, 2025, 2030, 2035, 2040, 2045, 2050
  - Structure: Percentage changes over time
  - ⚠️ **NOT DIRECTLY COMPARABLE** with per-area tables (different unit type)
  - ✅ **COMPARABLE** with other total emission tables (Slovenia, Lithuania)

### Lithuania
- **Table 63: Ghg Emission Reduction Targets**
  - Units: **tonnes CO2eq** (total emissions)
  - Time periods: 2030, 2040, 2050
  - Structure: Building Type × Years
  - ✅ **COMPARABLE** with other total emission tables (convert tonnes to kt)

- **Table 64: Estimated Operational Ghg Emissions Reduction**
  - Units: **tonnes CO2eq** (total emissions)
  - Time periods: 2030, 2040, 2050
  - Structure: Building Type × Years
  - ✅ **COMPARABLE** with other total emission tables (convert tonnes to kt)

### Romania
- **Table 2_7: Expected Annual Ghg Emissions Total**
  - Units: **Both** - Total (MtCO2eq/year) AND Per Area (kgCO2eq/m2/year)
  - Time periods: 2020 (baseline), 2030, 2040, 2050
  - Structure: Year × Total emissions × Per area intensity
  - ✅ **COMPARABLE** - Has both unit types! Can be used for both comparisons

### Slovenia
- **Table 14: Greenhouse Gas Emissions Of Buildings In Observed Years**
  - Units: **kt CO2 eq** (total emissions)
  - Time periods: 2023, 2030, 2040, 2050
  - Structure: Building Purpose × Years
  - ✅ **COMPARABLE** with other total emission tables

### Spain
- **Table 2_3: Co2Eq Emissions Per Use And Stages For Decarbonising**
  - Units: **Both** - Total (MtCO2eq) AND Per Area (kgCO2eq/(m² y))
  - Time periods: 2023, 2030, 2040, 2050
  - Structure: Building Type × Total emissions × Per area intensity
  - ✅ **COMPARABLE** - Has both unit types! Can be used for both comparisons

### Bulgaria
- **Status**: Tables N26 and N27 not found in database
- **Action Needed**: Verify table numbers or search by different criteria

## Comparability Groups

### Group 1: Per-Area Emissions (kgCO2eq/m2)
**Can be directly compared:**
- ✅ Belgium Table 15
- ✅ Romania Table 2_7 (per-area column)
- ✅ Spain Table 2_3 (per-area column)
- ✅ Croatia (mentioned by user, needs verification)
- ✅ Bulgaria (if N26/N27 have per-area data)

**Common timeframe**: 2030, 2040, 2050

### Group 2: Total Emissions (kt/Mt CO2 eq)
**Can be compared after unit conversion:**
- ✅ Finland Table 2_6 (residential) - kt CO2 eq
- ✅ Finland Table 2_7 (non-residential) - kt CO2 eq
- ✅ Lithuania Table 63 - tonnes CO2eq (convert: 1 kt = 1000 tonnes)
- ✅ Lithuania Table 64 - tonnes CO2eq (convert: 1 kt = 1000 tonnes)
- ✅ Slovenia Table 14 - kt CO2 eq
- ✅ Romania Table 2_7 (total column) - Mt CO2eq (convert: 1 Mt = 1000 kt)
- ✅ Spain Table 2_3 (total column) - Mt CO2eq (convert: 1 Mt = 1000 kt)

**Common timeframe**: 2030, 2040, 2050 (some include 2020, 2023, 2025)

## Recommendations for Chart Creation

### Option 1: Separate Charts by Unit Type
Create two separate comparison charts:
1. **Per-Area Emissions Chart** (kgCO2eq/m2)
   - Belgium, Romania (per-area), Spain (per-area), Croatia, Bulgaria
   
2. **Total Emissions Chart** (kt CO2 eq - normalized)
   - Finland (residential + non-residential), Lithuania, Slovenia, Romania (total), Spain (total)

### Option 2: Normalize All to Per-Area
If building area data is available, convert total emissions to per-area:
- Formula: `kgCO2eq/m2 = (kt CO2 eq × 1,000,000) / (building_area_m2 × 1000)`
- **Challenge**: Need building area data for each country/year

### Option 3: Show Both Metrics Side-by-Side
For countries with both metrics (Romania, Spain), show both:
- Left Y-axis: Total emissions (kt CO2 eq)
- Right Y-axis: Per-area emissions (kgCO2eq/m2)

## Data Points Currently Used

Based on the chart shown (Belgium vs Slovenia):
- Currently comparing **total emissions** (kt CO2 eq) for Belgium and Slovenia
- Belgium shows two distinct trends (possibly two different data series)
- Slovenia shows flat/zero values

**Issue**: Belgium Table 15 is **per-area** (kgCO2eq/m2), not total emissions. The current chart may be:
1. Using wrong Belgium table
2. Aggregating Belgium data incorrectly
3. Missing the "Operational GHG Emissions" table that has total emissions

## Action Items

1. ✅ **Verify Belgium's "Operational GHG Emissions" table** - Find the table with total emissions
2. ✅ **Find Bulgaria tables N26 and N27** - Search by table number or different keywords
3. ✅ **Check Croatia tables** - User mentioned Croatia has kgCO2eq/m2 data
4. ✅ **Normalize units** - Convert tonnes to kt, Mt to kt for consistent comparison
5. ✅ **Create separate chart functions** - One for per-area, one for total emissions
6. ✅ **Add unit conversion** - Implement conversion logic in chart rendering

## Next Steps

1. Search for Belgium's "Operational GHG Emissions" table
2. Find Bulgaria N26/N27 tables
3. Update chart rendering to handle unit conversions
4. Create two separate comparison views (per-area vs total)
5. Add unit labels clearly on charts
