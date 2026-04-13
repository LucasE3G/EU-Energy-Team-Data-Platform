# Spain NBRP – Tables missed or wrong (from your document map)

Based on your list of tables that **should** exist in the document, below is the status of each.  
**“Missed”** = no dedicated `Table_X_Y_Z_...csv` file (or only a `*_text_fallback.csv` with raw text).  
**“Wrong / incomplete”** = CSV exists but structure or data is wrong (e.g. multi-column merge).

## Tables you asked about (1.7.1, 1.8.1, 2.1.x, 2.2.x, 2.3.x)

| Table | Title (short) | In index? | CSV status |
|-------|----------------|-----------|------------|
| **1.7.1** | Evolution of relevant indicators of energy poverty (2018–2024) | Yes (p.132) | **Missed** – re-run extraction to get text-fallback CSV |
| **1.8.1** | Primary energy transition factors in force in 2023 (MITERD) | **No** – caption not detected | **Missed** |
| **2.1.1** | Evolution of dwellings renovated (43 % least efficient) | Yes (p.149) | **Missed** – re-run for text-fallback |
| **2.1.2** | Trajectory of annual renovations of residential stock | Yes (p.149) | **Missed** – re-run for text-fallback |
| **2.1.3** | (Area exclusions – check number) | No | **Missed** |
| **2.1.4** | Area concerned by exclusions | No | **Missed** |
| **2.2.1** | Primary/final energy consumption targets, milestones | Yes (p.153) | **Missed** – re-run for text-fallback |
| **2.2.2** | Percentages of consumption by EPBD services by use | Yes (p.154) | **Fixed from index** (parser added) |
| **2.2.3** | Primary/final energy in residential sector by services | Yes (p.155) | **Missed** – re-run for text-fallback |
| **2.2.4** | Primary/final energy of tertiary sector by services | Yes (p.155) | **Missed** – re-run for text-fallback |
| **2.2.5** | Energy savings by use and stages for decarbonising | Yes (p.155) | **Missed** – re-run for text-fallback |
| **2.2.6** | Renewable targets | **No** – caption not detected | **Missed** |
| **2.2.7** | Targets for deployment of solar energy in buildings | Yes (p.157) | **Missed** – re-run for text-fallback |
| **2.3.1** | Areas for calculation of emissions | Yes (p.159) | **Missed** – re-run for text-fallback |
| **2.3.2** | CO2-eq emissions per use and stages | Yes (p.159) | **Missed** – re-run for text-fallback |
| **2.3.3** | Emission savings per use and milestones (% vs 2023) | Yes (p.160) | **Fixed from index** (parser added) |

**Notes:**  
- **1.8.1** and **2.2.6** are not in `spain_table_index.csv` (caption regex may not match the PDF wording).  
- **2.1.3**, **2.1.4** are not in the index.  
- Re-running **run_spain_extraction** will now save **text-fallback** CSVs (region above caption as one column “Text”) for tables where no grid table is detected, so you get at least the raw text for 1.7.1, 2.1.1, 2.1.2, 2.2.1, 2.2.3–2.2.5, 2.2.7, 2.3.1–2.3.3.  
- **2.2.2** and **2.3.3** are written from the index by **fix_spain_tables_from_index.py** (run after extraction).

---

## Section 1.1 (Figure 1.1.1 → Table 1.1.34)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| (Figure 1.1.1) | Proportion of buildings by age | No table CSV (figure) |
| 1.1.1 | Stratification by age and use (No of buildings) | **Fixed from index** |
| 1.1.2 | Stratification by age and use (built area) | **Fixed from index** |
| 1.1.3 | Number of properties by use and time of construction | **Wrong** – merged columns; index has no full data |
| 1.1.4 | Number of properties by use without industrial/warehouse | **Fixed from index** |
| 1.1.5 | Summary of the Spanish building stock | **Missed** – no CSV; index has caption only |
| 1.1.6 | Distribution of residential sector by no of buildings and time | **Fixed from index** |
| 1.1.7 | Distribution of sector residential by area and time | **Missed** |
| 1.1.8 | Distribution of residential sector by number of dwellings and time | **Present** – check if complete (was 2 rows) |
| 1.1.9 | Distribution of residential sector by age and type | **Missed** |
| 1.1.10 | Distribution of residential sector by use (main/non-main) | **Missed** |
| 1.1.11 | Distribution by Autonomous Community and type in 2020 | **Present** |
| 1.1.12 | Number of main dwellings by year of construction and floor area | **Missed** |
| 1.1.13 | Distribution by climatic zone and time (No of buildings) | **Missed** |
| 1.1.14 | Distribution by climatic zone and time (area, m2) | **Missed** |
| 1.1.15 | Distribution by climatic zone and time (No of dwellings) | **Missed** |
| 1.1.16 | No of demolition visas and m2 demolished per year | **Missed** |
| 1.1.17 | Distribution of tertiary sector by number of buildings and time | **Present** – check if complete |
| 1.1.18 | Distribution of tertiary sector by area (m2) and time | **Missed** |
| 1.1.19 | Distribution of tertiary sector by use and size (no of) | **Missed** |
| 1.1.20 | Distribution of tertiary sector by use and size (m²) | **Missed** |
| 1.1.21 | Distribution of tertiary by climatic zone and time (No of buildings) | **Present** |
| 1.1.22 | Distribution of tertiary by climatic zone and time (area, m2) | **Missed** |
| 1.1.23 | Distribution of building stock by energy performance class | **Present** |
| 1.1.24 | ECCN (no. buildings and m2) | **Missed** |
| 1.1.25 | ECCN disaggregating new and existing | **Missed** |
| 1.1.26 | Worst performing buildings (floor area and number) | **Missed** (or page-based) |
| 1.1.27 | Worst-performing residential buildings from time to | **Missed** |
| 1.1.28 | Worst-performing residential by climate zone (No of buildings) | **Missed** |
| 1.1.29 | Worst performing residential by time (floor area, m2) | **Missed** |
| 1.1.30 | Worst-performing residential by climatic zone (floor area, m2) | **Present** |
| 1.1.31 | 43 % least efficient by climatic zone | **Missed** |
| 1.1.32 | Buildings and residential use floor area by construction period | **Missed** |
| 1.1.33 | Buildings and tertiary use floor area by construction period | **Missed** |
| 1.1.34 | Total buildings and floor area excluded | **Missed** |

---

## Section 1.2 (Table 1.2.1 → 1.2.11)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 1.2.1 | Number of EWCs registered by Autonomous Communities (2014-2023) | **Missed** |
| 1.2.2 | Classification in consumption of total EWCs by Autonomous Community | **Present** |
| 1.2.3 | Classification in consumption of residential buildings by AC | **Missed** |
| 1.2.4 | Qualification in emissions from residential buildings by AC | **Missed** |
| 1.2.5 | Distribution of main dwellings by conservation status | **Missed** |
| 1.2.6 | Distribution of main dwellings by conservation status | **Missed** |
| 1.2.7 | Breakdown of final consumption by residential services | **Missed** |
| 1.2.8 | Classification in consumption of tertiary buildings by AC | **Missed** |
| 1.2.9 | Rating on emissions from tertiary buildings by AC | **Missed** |
| 1.2.10 | Number of certificates by building type | **Missed** |
| 1.2.11 | Number of certificates according to energy classification | **Missed** (or page-based) |

---

## Section 1.3 (Table 1.3.1 → 1.3.4)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 1.3.1 | Remediation depth levels set out in the EPBD | **Missed** |
| 1.3.2 | Building renovation levels or rates (residential or tertiary) | **Missed** |
| 1.3.3 | Renovation rates for residential and tertiary | **Missed** |
| 1.3.4 | No of total renovations and equivalent deep renovations | **Missed** |

---

## Section 1.4 (Table 1.4.1 → 1.4.7)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 1.4.1 | Average pass-by factors for calculation years 2020 and 2023 | **Missed** |
| 1.4.2 | Final and primary energy consumption by building type | **Missed** |
| 1.4.3 | Final and primary energy consumption by building type and service | **Missed** |
| 1.4.4 | Final and primary energy consumption by building type | **Missed** |
| 1.4.5 | Built area of main dwellings | **Missed** |
| 1.4.6 | Average primary energy consumption of residential stock | **Missed** |
| 1.4.7 | Share of renewable energy | **Missed** |

---

## Section 1.5 (Table 1.5.1 → 1.5.3)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 1.5.1 | Primary energy transition factors and average emissions (2020, 2023) | **Missed** |
| 1.5.2 | Surface area of the built stock by building type | **Missed** |
| 1.5.3 | Emission values by Building type | **Missed** |

---

## Section 1.6

No tables.

---

## Section 1.7

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 1.7.1 | Evolution of relevant indicators of energy poverty (2018–2024) | **Missed** |

---

## Section 1.8

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 1.8.1 | Primary energy transition factors in force in 2023 | **Missed** |

---

## Section 2.1 (Table 2.1.1 → 2.1.4)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 2.1.1 | Evolution of number of dwellings renovated and rehabilitation (43 % least efficient) | **Missed** |
| 2.1.2 | Trajectory of annual renovations of the residential stock | **Missed** (re-run for text-fallback) |
| 2.1.3 | (Area concerned by exclusions – check exact number) | **Missed** |
| 2.1.4 | Area concerned by exclusions | **Missed** |

---

## Section 2.2 (Table 2.2.1 → 2.2.7)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 2.2.1 | Primary and final energy consumption targets, by use and milestones | **Missed** |
| 2.2.2 | Percentages of consumption by EPBD services by use | **Fixed from index** |
| 2.2.3 | Primary and final energy consumption in residential sector by services | **Missed** |
| 2.2.4 | Primary and final energy consumption of tertiary sector by services | **Missed** |
| 2.2.5 | Energy savings by use and stages for decarbonising the fleet | **Missed** |
| 2.2.6 | Renewable targets | **Missed** (not in index) |
| 2.2.7 | Targets for deployment of solar energy in buildings | **Missed** |

---

## Section 2.3 (Table 2.3.1 → 2.3.3)

| Table   | Title (short) | Status |
|---------|----------------|--------|
| 2.3.1 | Areas taken into account for the calculation of emissions | **Missed** |
| 2.3.2 | (CO2-eq emissions per use – check exact title) | **Missed** |
| 2.3.3 | Emission savings per use and milestones (% compared to 2023) | **Fixed from index** |

---

## Summary

- **Fixed from index (correct):** 1.1.1, 1.1.2, 1.1.4, 1.1.6, **2.2.2**, **2.3.3**  
- **Present but verify:** 1.1.8, 1.1.11, 1.1.17, 1.1.21, 1.1.23, 1.1.30, 1.2.2  
- **Wrong / incomplete:** 1.1.3  
- **Missed (no dedicated CSV):** All others listed above. Re-running extraction now produces **text-fallback** CSVs (`*_text_fallback.csv`) for tables in the index where no grid is detected (e.g. 1.7.1, 2.1.1, 2.1.2, 2.2.1, 2.2.3–2.2.5, 2.2.7, 2.3.1–2.3.3).
- **Not in caption index** (cannot auto-extract): 1.8.1, 2.1.3, 2.1.4, 2.2.6 – caption wording in the PDF may differ; add manually or adjust `list_spain_table_captions.py` if needed.

Many “missed” tables may exist only as **page-based CSVs** (e.g. `54.1.csv`, `88.1.csv`). To get proper named CSVs for them you would need to either: run extraction again with better detection/naming, or add a step that matches page-based CSVs to table numbers using the caption index and renames/duplicates them.
