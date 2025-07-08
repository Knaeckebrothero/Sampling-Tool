import sqlite3
import pandas as pd
import sys
import os
import argparse
import logging


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def create_schema(conn, logger):
    """Create database schema."""
    schema = """
             -- Main data table
             CREATE TABLE IF NOT EXISTS financial_data (
                                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                 -- Columns will be added dynamically based on CSV
                                                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
             );

             -- Configurations table
             CREATE TABLE IF NOT EXISTS configurations (
                                                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                           name TEXT NOT NULL UNIQUE,
                                                           description TEXT,
                                                           config_json TEXT NOT NULL,
                                                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                           updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
             );

             -- Sampling history table
             CREATE TABLE IF NOT EXISTS sampling_history (
                                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                             config_id INTEGER,
                                                             sample_count INTEGER NOT NULL,
                                                             summary_json TEXT,
                                                             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                             FOREIGN KEY (config_id) REFERENCES configurations(id) ON DELETE SET NULL
                 );

             -- Sampling results table
             CREATE TABLE IF NOT EXISTS sampling_results (
                                                             id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                             history_id INTEGER NOT NULL,
                                                             rule_name TEXT NOT NULL,
                                                             data_json TEXT NOT NULL,
                                                             FOREIGN KEY (history_id) REFERENCES sampling_history(id) ON DELETE CASCADE
                 );

             -- Create indexes
             CREATE INDEX IF NOT EXISTS idx_financial_data_created ON financial_data(created_at);
             CREATE INDEX IF NOT EXISTS idx_configurations_name ON configurations(name);
             CREATE INDEX IF NOT EXISTS idx_sampling_history_config ON sampling_history(config_id);
             CREATE INDEX IF NOT EXISTS idx_sampling_results_history ON sampling_results(history_id); \
             """

    try:
        conn.executescript(schema)
        conn.commit()
        logger.info("Database schema created successfully")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error creating schema: {e}")
        return False

def import_csv_to_db(conn, csv_path, delimiter=';', logger=None):
    """Import CSV data into the financial_data table."""
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path, delimiter=delimiter)
        logger.info(f"Read {len(df)} rows from CSV")

        # Clean column names
        df.columns = [
            ''.join(c for c in col.strip().replace(' ', '_').replace('-', '_')
                    if c.isalnum() or c == '_')
            for col in df.columns
        ]

        # Drop the existing financial_data table to recreate with new columns
        conn.execute("DROP TABLE IF EXISTS financial_data")

        # Import to database (this will create the table with proper columns)
        df.to_sql('financial_data', conn, if_exists='replace', index=False)

        # Add the id column as primary key
        conn.execute("""
            CREATE TABLE financial_data_new AS 
            SELECT rowid as id, * FROM financial_data
        """)
        conn.execute("DROP TABLE financial_data")
        conn.execute("ALTER TABLE financial_data_new RENAME TO financial_data")

        logger.info(f"Imported {len(df)} records successfully")
        return True

    except Exception as e:
        logger.error(f"Error importing CSV: {e}")
        return False

def main():
    """Main initialization function."""
    logger = setup_logging()

    parser = argparse.ArgumentParser(description='Initialize the sampling tool database')
    parser.add_argument('--db-path', default='./data/sampling.db', help='Database path')
    parser.add_argument('--csv-path', help='CSV file to import')
    parser.add_argument('--delimiter', default=';', help='CSV delimiter')
    parser.add_argument('--reset', action='store_true', help='Reset database')

    args = parser.parse_args()

    # Create data directory if needed
    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

    # Handle reset
    if args.reset and os.path.exists(args.db_path):
        os.remove(args.db_path)
        logger.info("Removed existing database")

    # Connect to database
    try:
        conn = sqlite3.connect(args.db_path)
        logger.info(f"Connected to database: {args.db_path}")
    except sqlite3.Error as e:
        logger.error(f"Failed to connect: {e}")
        return 1

    # Create schema
    if not create_schema(conn, logger):
        return 1

    # Import CSV if provided
    if args.csv_path:
        if not import_csv_to_db(conn, args.csv_path, args.delimiter, logger):
            return 1
    else:
        logger.info("No CSV provided - database initialized empty")

    # Show summary
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM financial_data")
    count = cursor.fetchone()[0]
    logger.info(f"\nDatabase ready with {count} records")

    conn.close()
    return 0

if __name__ == "__main__":
    sys.exit(main())
