# GHG Emission Tables - Comparability Assessment

## Your Questions Answered

### 1. Are these tables comparable?

**YES, but they need to be grouped by unit type:**

#### Group A: Per-Area Emissions (kgCO2eq/m2) - DIRECTLY COMPARABLE
- ✅ **Belgium**: Table 15 - "Projections Emissions Per Unit Area"
- ✅ **Romania**: Table 2_7 - "Expected Annual Ghg Emissions Total" (has per-area column)
- ✅ **Spain**: Table 2_3 - "Co2Eq Emissions Per Use And Stages" (has per-area column)
- ✅ **Croatia**: (You mentioned it has kgCO2eq/m2 - needs verification)
- ✅ **Bulgaria**: Tables N26/N27 (needs to be found and verified)

#### Group B: Total Emissions (kt/Mt CO2 eq) - COMPARABLE AFTER UNIT CONVERSION
- ✅ **Finland**: Table 2_6 (residential) + Table 2_7 (non-residential) = **kt CO2 eq**
- ✅ **Lithuania**: Table 63 + Table 64 = **tonnes CO2eq** (1 kt = 1000 tonnes)
- ✅ **Slovenia**: Table 14 = **kt CO2 eq**
- ✅ **Romania**: Table 2_7 (has total column) = **Mt CO2eq** (1 Mt = 1000 kt)
- ✅ **Spain**: Table 2_3 (has total column) = **Mt CO2eq** (1 Mt = 1000 kt)

### 2. Which data points are currently used in charts?

**Current Issue Identified:**
- The chart showing Belgium vs Slovenia is comparing **total emissions** (kt CO2 eq)
- However, Belgium Table 15 is **per-area** (kgCO2eq/m2), NOT total emissions
- This suggests either:
  1. Wrong Belgium table is being used
  2. Belgium data is being incorrectly aggregated
  3. Missing Belgium table: "Operational Greenhouse Gas Emissions Of Buildings By Building Type" (which should have total emissions)

**What SHOULD be used:**
- For **per-area comparison**: Belgium Table 15, Romania (per-area), Spain (per-area)
- For **total emissions comparison**: Need to find Belgium's total emissions table, Slovenia Table 14, Finland Tables 2_6+2_7, Lithuania Tables 63+64

### 3. Can they make a chart together?

**YES, but with two approaches:**

#### Approach 1: Two Separate Charts (RECOMMENDED)
1. **Per-Area Emissions Chart** (kgCO2eq/m2)
   - Belgium Table 15
   - Romania Table 2_7 (per-area column)
   - Spain Table 2_3 (per-area column)
   - Croatia (if verified)
   - Bulgaria N26/N27 (if found and verified)

2. **Total Emissions Chart** (kt CO2 eq - normalized)
   - Belgium: Need to find total emissions table
   - Finland: Tables 2_6 + 2_7 (combine residential + non-residential)
   - Lithuania: Tables 63 + 64 (convert tonnes to kt)
   - Slovenia: Table 14
   - Romania: Table 2_7 (total column, convert Mt to kt)
   - Spain: Table 2_3 (total column, convert Mt to kt)

#### Approach 2: Dual-Axis Chart (for countries with both metrics)
- Romania and Spain have BOTH total and per-area data
- Can show both metrics on same chart with dual Y-axes

## Detailed Table Analysis

### Belgium
- **Table 15**: Per-area (kgCO2eq/m2.year) ✅ Found
- **"Operational GHG Emissions"**: Total emissions - ❓ **NEEDS TO BE FOUND**
  - Search criteria: "Operational", "Greenhouse Gas", "Buildings By Building Type"
  - Likely table number: 20 or 21 (based on your mention)

### Finland
- **Table 2_6**: Residential buildings - Total (kt CO2 eq) ✅ Found
- **Table 2_7**: Non-residential buildings - Total (kt CO2 eq) ✅ Found
- **Note**: Need to combine both tables for total Finland emissions

### Lithuania
- **Table 63**: GHG Emission Reduction Targets - Total (tonnes CO2eq) ✅ Found
- **Table 64**: Estimated Operational GHG Emissions Reduction - Total (tonnes CO2eq) ✅ Found
- **Conversion**: 1 kt = 1000 tonnes (divide by 1000 to get kt)

### Romania
- **Table 2_7**: Has BOTH metrics ✅ Found
  - Total: MtCO2eq/year (convert to kt: multiply by 1000)
  - Per-area: kgCO2eq/m2/year
- **Perfect for dual comparison!**

### Slovenia
- **Table 14**: Total emissions (kt CO2 eq) ✅ Found
- Time periods: 2023, 2030, 2040, 2050

### Spain
- **Table 2_3**: Has BOTH metrics ✅ Found
  - Total: MtCO2eq (convert to kt: multiply by 1000)
  - Per-area: kgCO2eq/(m² y)
- **Perfect for dual comparison!**

### Bulgaria
- **Tables N26 and N27**: ❌ **NOT FOUND**
- **Action needed**: 
  - Search by table numbers 26, 27
  - Search by keywords: "emission", "ghg", "co2"
  - Check if they exist with different naming

### Croatia
- **Status**: You mentioned Croatia has kgCO2eq/m2 data
- **Action needed**: Identify exact table number/name

## Common Timeframe Confirmation

✅ **YES** - Most tables share 2030, 2040, 2050:
- Belgium: 2030, 2040, 2050 ✅
- Finland: 2030, 2040, 2050 (+ 2020, 2025, 2035, 2045) ✅
- Lithuania: 2030, 2040, 2050 ✅
- Romania: 2030, 2040, 2050 (+ 2020 baseline) ✅
- Slovenia: 2030, 2040, 2050 (+ 2023) ✅
- Spain: 2030, 2040, 2050 (+ 2023) ✅

## Unit Conversion Reference

| Unit | Conversion to kt CO2 eq |
|------|------------------------|
| tonnes CO2eq | ÷ 1000 = kt CO2 eq |
| kt CO2 eq | = kt CO2 eq (no conversion) |
| Mt CO2eq | × 1000 = kt CO2 eq |
| kgCO2eq/m2 | Cannot convert to total without building area |

## Recommendations

1. ✅ **Find Belgium's total emissions table** - Search for "Operational GHG Emissions" or tables 20/21
2. ✅ **Find Bulgaria N26/N27** - Search by table number or emission keywords
3. ✅ **Verify Croatia table** - Identify exact table with per-area data
4. ✅ **Create two chart functions**:
   - `renderPerAreaGHGChart()` - For kgCO2eq/m2 comparison
   - `renderTotalGHGChart()` - For kt CO2 eq comparison (with unit conversion)
5. ✅ **Add unit conversion logic** - Convert tonnes→kt, Mt→kt automatically
6. ✅ **Combine Finland tables** - Sum residential + non-residential for total
7. ✅ **Add clear unit labels** - Show units prominently on charts

## Next Steps

1. Run enhanced search for missing tables (Belgium total emissions, Bulgaria N26/N27)
2. Update chart rendering code to handle unit conversions
3. Create separate comparison views for per-area vs total emissions
4. Test with actual data to verify comparability
