# Update Measures Schema and Data

## Overview
The measures table schema has been updated to include all columns from all country-specific CSV files. Previously, many columns were being filtered out during upload.

## Steps Required

### 1. Update Database Schema
Run the SQL script `add_all_measures_columns.sql` in your Supabase SQL Editor to add all missing columns to the `measures` table.

### 2. Re-upload All Measures Data
Run the Python script to re-upload all measures with the new column mapping:

```bash
python reupload_all_measures.py
```

This script will:
- Read all measure CSV files from `data/measures/`
- Map CSV columns to database columns dynamically
- Store any unmapped columns in the `additional_data` JSONB field
- Preserve all data including:
  - Finland: `directive`, `description`, `status`, `epbd_article_2a`
  - Lithuania/Slovenia/Spain: `objective`, `planned_budget_and_sources`, `date_of_entry_into_force`, `state_of_execution`
  - Croatia: `short_description`, `quantified_objective`, `authorities_responsible`, `expected_impacts`, `implementation_status`, `effective_date`
  - Bulgaria: `section`, `section_topic`, `measure_number`, `content`, `amending_legislation`, `lead_institution`, `participating_institutions`, `sources_of_funding`, `time_limit`

### 3. Verify Frontend Display
After re-uploading, refresh your browser (hard refresh: Ctrl+Shift+R) to see all the new fields displayed in:
- Measure detail modals (click any measure)
- Measure listings
- Stakeholder mapping

## New Columns Added

### Finland-specific:
- `epbd_article_2a` (TEXT)
- `directive` (TEXT)
- `status` (TEXT)

### Lithuania/Slovenia/Spain-specific:
- `objective` (TEXT)
- `planned_budget_and_sources` (TEXT)
- `state_of_execution` (TEXT)
- `date_of_entry_into_force` (TEXT)

### Lithuania-specific:
- `instrument_type` (TEXT)
- `source` (TEXT)
- `quantitative_target` (TEXT)

### Croatia-specific:
- `short_description` (TEXT)
- `quantified_objective` (TEXT)
- `authorities_responsible` (TEXT)
- `expected_impacts` (TEXT)
- `implementation_status` (TEXT)
- `effective_date` (TEXT)

### Bulgaria-specific:
- `section` (TEXT)
- `section_topic` (TEXT)
- `measure_number` (VARCHAR(50))
- `content` (TEXT)
- `amending_legislation` (TEXT)
- `lead_institution` (TEXT)
- `participating_institutions` (TEXT)
- `sources_of_funding` (TEXT)
- `time_limit` (TEXT)

### Flexible storage:
- `additional_data` (JSONB) - for any columns not explicitly mapped

## Status Detection Improvements

The `getMeasureStatus()` function now checks:
1. `status` field (Finland) - uses explicit status value
2. `state_of_execution` field (Lithuania/Slovenia/Spain) - uses explicit execution state
3. `implementation_period` or `time_limit` - falls back to date-based analysis

This ensures accurate status indicators for all countries.
