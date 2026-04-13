-- Add entities_responsible column to measures table if it doesn't exist
-- Run this in Supabase SQL Editor

ALTER TABLE measures 
ADD COLUMN IF NOT EXISTS entities_responsible TEXT;
