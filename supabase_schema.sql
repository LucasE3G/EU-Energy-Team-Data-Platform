-- Supabase Database Schema for Renovation Plans Data
-- Run this SQL in your Supabase SQL Editor

-- Countries table
CREATE TABLE IF NOT EXISTS countries (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Data tables (for storing CSV data)
CREATE TABLE IF NOT EXISTS data_tables (
    id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES countries(id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    table_description TEXT,
    file_name VARCHAR(255) NOT NULL,
    -- Metadata extracted from filename
    table_number VARCHAR(50), -- e.g., "1", "2_3", "3_7"
    original_filename VARCHAR(255) NOT NULL, -- Full original filename
    -- Data structure metadata
    has_time_series BOOLEAN DEFAULT FALSE,
    column_names JSONB, -- Array of column names for quick reference
    num_columns INTEGER,
    -- Additional metadata
    metadata JSONB, -- Store any additional metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(country_id, file_name)
);

-- Data points (flexible structure for CSV data)
CREATE TABLE IF NOT EXISTS data_points (
    id SERIAL PRIMARY KEY,
    data_table_id INTEGER REFERENCES data_tables(id) ON DELETE CASCADE,
    row_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Measures table
CREATE TABLE IF NOT EXISTS measures (
    id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES countries(id) ON DELETE CASCADE,
    -- Common columns
    measure_category VARCHAR(255),
    measure_id VARCHAR(50),
    measure_name TEXT,
    description TEXT,
    -- Belgium-specific
    quantified_objectives TEXT,
    type_of_policy_or_measure TEXT,
    budget TEXT,
    stakeholders TEXT,
    state_of_play TEXT,
    implementation_period TEXT,
    -- Finland-specific
    epbd_article_2a TEXT,
    directive TEXT,
    status TEXT,
    -- Lithuania/Slovenia/Spain-specific
    objective TEXT,
    planned_budget_and_sources TEXT,
    entities_responsible TEXT,
    state_of_execution TEXT,
    date_of_entry_into_force TEXT,
    -- Lithuania-specific
    instrument_type TEXT,
    source TEXT,
    quantitative_target TEXT,
    -- Croatia-specific
    short_description TEXT,
    quantified_objective TEXT,
    authorities_responsible TEXT,
    expected_impacts TEXT,
    implementation_status TEXT,
    effective_date TEXT,
    -- Bulgaria-specific
    section TEXT,
    section_topic TEXT,
    measure_number VARCHAR(50),
    content TEXT,
    amending_legislation TEXT,
    lead_institution TEXT,
    participating_institutions TEXT,
    sources_of_funding TEXT,
    time_limit TEXT,
    -- Flexible storage for any other columns
    additional_data JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_data_points_table_id ON data_points(data_table_id);
CREATE INDEX IF NOT EXISTS idx_data_points_row_data ON data_points USING GIN(row_data);
CREATE INDEX IF NOT EXISTS idx_measures_country_id ON measures(country_id);
CREATE INDEX IF NOT EXISTS idx_measures_category ON measures(measure_category);
CREATE INDEX IF NOT EXISTS idx_data_tables_country_id ON data_tables(country_id);
CREATE INDEX IF NOT EXISTS idx_data_tables_table_number ON data_tables(table_number);
CREATE INDEX IF NOT EXISTS idx_data_tables_has_time_series ON data_tables(has_time_series);
CREATE INDEX IF NOT EXISTS idx_data_tables_column_names ON data_tables USING GIN(column_names);

-- Enable Row Level Security (RLS)
ALTER TABLE countries ENABLE ROW LEVEL SECURITY;
ALTER TABLE data_tables ENABLE ROW LEVEL SECURITY;
ALTER TABLE data_points ENABLE ROW LEVEL SECURITY;
ALTER TABLE measures ENABLE ROW LEVEL SECURITY;

-- Create policies to allow public read access (adjust as needed)
CREATE POLICY "Allow public read access" ON countries FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON data_tables FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON data_points FOR SELECT USING (true);
CREATE POLICY "Allow public read access" ON measures FOR SELECT USING (true);
