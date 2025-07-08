-- Database schema for the sampling tool

-- Main data table for financial records
-- This is a flexible schema that can accommodate various CSV structures
-- In practice, you might want to define specific columns based on your data
CREATE TABLE IF NOT EXISTS financial_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Add your specific columns here based on your CSV structure
    -- Example columns (adjust based on your actual data):
    transaction_id TEXT,
    account_number TEXT,
    transaction_date DATE,
    amount REAL,
    currency TEXT,
    description TEXT,
    category TEXT,
    vendor TEXT,
    department TEXT,
    cost_center TEXT,
    project_code TEXT,
    approval_status TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Configurations table for saving filter and rule sets
CREATE TABLE IF NOT EXISTS configurations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    config_json TEXT NOT NULL,  -- JSON containing filters and rules
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sampling history table to track sampling runs
CREATE TABLE IF NOT EXISTS sampling_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    config_id INTEGER,
    sample_count INTEGER NOT NULL,
    summary_json TEXT,  -- JSON containing summary statistics
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (config_id) REFERENCES configurations(id) ON DELETE SET NULL
);

-- Sampling results table to store actual sampled records
CREATE TABLE IF NOT EXISTS sampling_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    history_id INTEGER NOT NULL,
    rule_name TEXT NOT NULL,
    data_json TEXT NOT NULL,  -- JSON containing the sampled record
    FOREIGN KEY (history_id) REFERENCES sampling_history(id) ON DELETE CASCADE
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_financial_data_date ON financial_data(transaction_date);
CREATE INDEX IF NOT EXISTS idx_financial_data_amount ON financial_data(amount);
CREATE INDEX IF NOT EXISTS idx_financial_data_category ON financial_data(category);
CREATE INDEX IF NOT EXISTS idx_configurations_name ON configurations(name);
CREATE INDEX IF NOT EXISTS idx_sampling_history_config ON sampling_history(config_id);
CREATE INDEX IF NOT EXISTS idx_sampling_results_history ON sampling_results(history_id);

-- Trigger to update the updated_at timestamp for configurations
CREATE TRIGGER IF NOT EXISTS update_configurations_timestamp
    AFTER UPDATE ON configurations
BEGIN
    UPDATE configurations SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Example view for common queries
CREATE VIEW IF NOT EXISTS v_recent_samples AS
SELECT
    sh.id as history_id,
    sh.created_at as sample_date,
    sh.sample_count,
    c.name as config_name,
    c.description as config_description
FROM sampling_history sh
LEFT JOIN configurations c ON sh.config_id = c.id
ORDER BY sh.created_at DESC
LIMIT 100;
