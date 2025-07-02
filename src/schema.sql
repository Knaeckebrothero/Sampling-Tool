-- schema.sql
-- Database schema for the sampling tool

-- Table for storing imported datasets metadata
CREATE TABLE IF NOT EXISTS datasets (
                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                        name TEXT NOT NULL UNIQUE,
                                        table_name TEXT NOT NULL UNIQUE,
                                        original_filename TEXT,
                                        row_count INTEGER,
                                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for storing column metadata
CREATE TABLE IF NOT EXISTS dataset_columns (
                                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                                               dataset_id INTEGER NOT NULL,
                                               column_name TEXT NOT NULL,
                                               column_type TEXT NOT NULL,
                                               column_index INTEGER,
                                               FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    UNIQUE(dataset_id, column_name)
    );

-- Table for storing global filters
CREATE TABLE IF NOT EXISTS global_filters (
                                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                                              dataset_id INTEGER NOT NULL,
                                              column_name TEXT NOT NULL,
                                              column_type TEXT NOT NULL,
                                              filter_config TEXT NOT NULL,  -- JSON
                                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                              FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
    );

-- Table for storing sampling rules
CREATE TABLE IF NOT EXISTS sampling_rules (
                                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                                              dataset_id INTEGER NOT NULL,
                                              name TEXT NOT NULL,
                                              column_name TEXT NOT NULL,
                                              column_type TEXT NOT NULL,
                                              filter_config TEXT NOT NULL,  -- JSON
                                              sample_count INTEGER NOT NULL,
                                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                              FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
    );

-- Table for storing sampling configurations
CREATE TABLE IF NOT EXISTS sampling_configs (
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                dataset_id INTEGER NOT NULL,
                                                name TEXT NOT NULL,
                                                config_data TEXT NOT NULL,  -- JSON with filters and rules
                                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
    );

-- Table for storing sample results history
CREATE TABLE IF NOT EXISTS sample_results (
                                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                                              dataset_id INTEGER NOT NULL,
                                              config_id INTEGER,
                                              rule_name TEXT NOT NULL,
                                              sample_data TEXT NOT NULL,  -- JSON
                                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                              created_by TEXT,
                                              FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    FOREIGN KEY (config_id) REFERENCES sampling_configs(id) ON DELETE SET NULL
    );

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_columns_dataset ON dataset_columns(dataset_id);
CREATE INDEX IF NOT EXISTS idx_filters_dataset ON global_filters(dataset_id);
CREATE INDEX IF NOT EXISTS idx_rules_dataset ON sampling_rules(dataset_id);
CREATE INDEX IF NOT EXISTS idx_configs_dataset ON sampling_configs(dataset_id);
CREATE INDEX IF NOT EXISTS idx_results_dataset ON sample_results(dataset_id);

-- Trigger to update the updated_at timestamp
CREATE TRIGGER IF NOT EXISTS update_datasets_timestamp
    AFTER UPDATE ON datasets
BEGIN
UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;
