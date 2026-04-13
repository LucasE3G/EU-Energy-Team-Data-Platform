# Spain NBRP table extraction – notes

## Table map (document structure)

- **Section 1.1:** First = Figure 1.1.1 (proportion of buildings by age). Last = Table 1.1.34 (Total buildings and floor area excluded).
- **Section 1.2:** First = Table 1.2.1 (Number of EWCs registered…). Last = Table 1.2.11 (Number of certificates according to energy classification).
- **Section 1.3:** First = Table 1.3.1 (Remediation depth levels…). Last = Table 1.3.4 (No of total renovations and equivalent deep renovations…).
- **Section 1.4:** First = Table 1.4.1 (Average pass-by factors…). Last = Table 1.4.7 (Share of renewable energy).
- **Section 1.5:** First = Table 1.5.1 (Primary energy transition factors…). Last = Table 1.5.3 (Emission values by Building type).
- **Section 1.6:** No tables.
- **Section 1.7:** Table 1.7.1 (Evolution of the relevant indicators of energy poverty…).
- **Section 1.8:** Table 1.8.1 (Primary energy transition factors in force in 2023).
- **Section 2.1:** Table 2.1.1 through Table 2.1.4.
- **Section 2.2:** Table 2.2.1 through Table 2.2.7.
- **Section 2.3:** Table 2.3.1 through Table 2.3.3.

## Why Table 1.1.1 was “missed”

- **Table 1. 1. 1.** (Stratification of the building stock by age and use – No of buildings) is on **page 48**. Its data was captured in `spain_table_index.csv` when building the caption list.
- No separate CSV was created because the extractor did not detect a table on that page (or it was filtered as non-data). **Fix:** Correct CSV created from the index: `Table_1_1_1_...csv`.

## Table 1.1.5 (Summary of the Spanish building stock) – missed

- **Table 1. 1. 5.** is on **page 54**. The index only stores the caption (“Table 1. 1. 5. Summary of the Spanish building stock”), not the full table data, so it cannot be rebuilt from the index. The extractor did not produce a dedicated CSV for it (page may have been skipped or content merged with another table). To include it, add the table data manually from the PDF or improve extraction for that page.

## Why Tables 1.1.2, 1.1.3, 1.1.4, 1.1.6 had wrong or incomplete data (multi-column issue)

- These tables have **many time-period columns** and in the PDF have **no vertical lines** – only horizontal lines and spacing – plus multi-row headers. pdfplumber often **merges adjacent columns** and concatenates values in one cell, so extracted CSVs had too few columns and wrong structure.

## What was done

- **Table 1.1.1:** CSV created from index (page 48).
- **Table 1.1.2:** CSV replaced with correct columns from index (page 50).
- **Table 1.1.4:** CSV created from index (page 52).
- **Table 1.1.6:** Corrupted CSV overwritten with correct structure from index (page 54): `Table_1_1_6_Distribution_of_the_residential_sector_by_number_of_buildings_and_time_...csv` (no of buildings, &lt; 1940 … TOTAL; rows Multi-family, Single-family, TOTAL).
- **Table 1.1.3:** Index has only caption; merged-column CSV remains. Correct manually or re-extract if needed.
- **Table 1.1.5:** Not in index with table data; add manually from PDF if needed.

## Number format

- Numbers use **period (.) as thousands separator** (e.g. `1.664.048` = 1,664,048). Decimals, if any, use comma. CSVs keep the same format as in the source.
