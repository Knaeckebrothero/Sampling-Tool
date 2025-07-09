-- Simplified database schema for the sampling tool

-- Main data table for financial records
-- This table is created dynamically when importing CSV data
-- The structure below matches the example_data.csv format

CREATE TABLE IF NOT EXISTS financial_data (
                                              id INTEGER,
                                              key_figure TEXT,
                                              value REAL,
                                              date TEXT,
                                              legal_form TEXT
);

-- Note: When importing via pandas, column names are cleaned:
-- "key figure" becomes "key_figure"
-- "legal form" becomes "legal_form"
-- Spaces and hyphens in column names are replaced with underscores

-- The actual table structure will match whatever CSV you import
-- This is just an example based on the typical format
