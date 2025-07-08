import json
import argparse
import logging
import os
import sqlite3
import sys
import pandas as pd
import csv
from datetime import datetime

def setup_logging(verbose=False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Initialize the sampling tool database.')
    parser.add_argument(
        '--db-path',
        default='./data/sampling.db',
        help='Path to the SQLite database file'
    )
    parser.add_argument(
        '--schema-path',
        default='./schema.sql',
        help='Path to the schema SQL file'
    )
    parser.add_argument(
        '--csv-path',
        help='Path to CSV file with initial data'
    )
    parser.add_argument(
        '--delimiter',
        default=';',
        help='CSV delimiter (default: semicolon)'
    )
    parser.add_argument(
        '--force-reset',
        action='store_true',
        help='Force reset the database (deletes existing data)'
    )
    parser.add_argument(
        '--create-dynamic-schema',
        action='store_true',
        help='Create table schema dynamically based on CSV columns'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    return parser.parse_args()

def ensure_directory_exists(path):
    """Ensure the directory for the given file path exists."""
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        return True
    return False

def detect_column_type(values):
    """Detect SQL column type based on sample values."""
    # Remove None/empty values
    clean_values = [v for v in values if v and str(v).strip()]

    if not clean_values:
        return "TEXT"

    # Check if all values are numeric
    try:
        for val in clean_values[:20]:  # Check first 20 values
            float(str(val).replace(',', '.').replace(' ', ''))
        return "REAL"
    except:
        pass

    # Check if values look like dates
    date_patterns = ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%m/%d/%Y']
    date_count = 0
    for val in clean_values[:20]:
        for pattern in date_patterns:
            try:
                datetime.strptime(str(val), pattern)
                date_count += 1
                break
            except:
                pass

    if date_count > len(clean_values[:20]) * 0.5:
        return "DATE"

    # Default to TEXT
    return "TEXT"

def create_dynamic_table_from_csv(conn, csv_path, table_name, delimiter, logger):
    """Create table dynamically based on CSV structure."""
    try:
        # Read CSV header and sample data
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            headers = reader.fieldnames

            # Read sample rows for type detection
            sample_data = []
            for i, row in enumerate(reader):
                sample_data.append(row)
                if i >= 100:  # Read up to 100 rows
                    break

        if not headers:
            raise ValueError("CSV file has no headers")

        # Detect column types
        column_definitions = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]

        for header in headers:
            # Clean column name (remove special characters, spaces)
            clean_name = header.strip().replace(' ', '_').replace('-', '_')
            clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')

            # Get sample values for this column
            values = [row.get(header) for row in sample_data]
            sql_type = detect_column_type(values)

            column_definitions.append(f"{clean_name} {sql_type}")
            logger.debug(f"Column '{header}' -> '{clean_name}' ({sql_type})")

        # Add timestamp
        column_definitions.append("created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

        # Create table
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_definitions)}
        )
        """

        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(create_table_sql)
        conn.commit()

        logger.info(f"Created dynamic table '{table_name}' with {len(headers)} columns")
        return True

    except Exception as e:
        logger.error(f"Error creating dynamic table: {e}")
        return False

def execute_sql_file(conn, filepath, logger):
    """Execute SQL commands from a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as sql_file:
            sql_script = sql_file.read()
            conn.executescript(sql_script)
            conn.commit()
            logger.info(f"Successfully executed SQL from {filepath}")
            return True
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        conn.rollback()
    except IOError as e:
        logger.error(f"I/O error: {e}")
    return False

def import_csv_data(conn, csv_path, table_name, delimiter, logger):
    """Import data from CSV file."""
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path, delimiter=delimiter)

        # Clean column names to match database
        df.columns = [
            ''.join(c for c in col.strip().replace(' ', '_').replace('-', '_')
                   if c.isalnum() or c == '_')
            for col in df.columns
        ]

        # Import to database
        df.to_sql(table_name, conn, if_exists='append', index=False)
        logger.info(f"Imported {len(df)} records from {csv_path}")
        return True

    except Exception as e:
        logger.error(f"Error importing CSV: {e}")
        return False

def create_example_configuration(conn, logger):
    """Create an example configuration."""
    try:
        example_config = {
            "global_filters": [
                {
                    "column": "amount",
                    "column_type": "number",
                    "filter_config": {
                        "min": 1000,
                        "max": 50000
                    }
                }
            ],
            "sampling_rules": [
                {
                    "name": "High Value Transactions",
                    "column": "amount",
                    "column_type": "number",
                    "filter_config": {
                        "min": 10000
                    },
                    "sample_count": 5
                },
                {
                    "name": "Medium Value Transactions",
                    "column": "amount",
                    "column_type": "number",
                    "filter_config": {
                        "min": 1000,
                        "max": 10000
                    },
                    "sample_count": 10
                }
            ]
        }

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO configurations (name, description, config_json)
            VALUES (?, ?, ?)
        """, (
            "Example Configuration",
            "Sample configuration for demonstration",
            json.dumps(example_config)
        ))
        conn.commit()

        logger.info("Created example configuration")
        return True

    except sqlite3.Error as e:
        logger.error(f"Error creating example configuration: {e}")
        return False

def verify_database(conn, logger):
    """Verify database contents."""
    cursor = conn.cursor()

    # Check tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    logger.info(f"Database tables: {[table[0] for table in tables]}")

    # Check record counts
    for table in ['financial_data', 'configurations']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"Table '{table}' contains {count} records")
        except sqlite3.Error:
            pass

    return True

def initialize_database(db_path, schema_path, csv_path, delimiter,
                       force_reset, create_dynamic, logger):
    """Initialize the database with schema and optional data."""
    if ensure_directory_exists(db_path):
        logger.info(f"Created directory for database")

    # Handle force reset
    if force_reset and os.path.exists(db_path):
        try:
            os.remove(db_path)
            logger.warning(f"Deleted existing database")
        except OSError as e:
            logger.error(f"Failed to delete database: {e}")
            return False

    # Connect to database
    try:
        conn = sqlite3.connect(db_path)
        logger.info(f"Connected to database at {db_path}")
    except sqlite3.Error as e:
        logger.error(f"Failed to connect: {e}")
        return False

    try:
        # Create dynamic schema if requested and CSV provided
        if create_dynamic and csv_path:
            if not create_dynamic_table_from_csv(conn, csv_path, 'financial_data',
                                                delimiter, logger):
                return False
        else:
            # Execute standard schema
            if not execute_sql_file(conn, schema_path, logger):
                return False

        # Always create configuration tables
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS configurations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                description TEXT,
                config_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS sampling_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER,
                sample_count INTEGER NOT NULL,
                summary_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (config_id) REFERENCES configurations(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS sampling_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                history_id INTEGER NOT NULL,
                rule_name TEXT NOT NULL,
                data_json TEXT NOT NULL,
                FOREIGN KEY (history_id) REFERENCES sampling_history(id) ON DELETE CASCADE
            );
        """)

        # Import CSV data if provided
        if csv_path and os.path.exists(csv_path):
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM financial_data")
            count = cursor.fetchone()[0]

            if count == 0:
                if not import_csv_data(conn, csv_path, 'financial_data', delimiter, logger):
                    logger.warning("Failed to import CSV data, continuing anyway")
            else:
                logger.info(f"Table 'financial_data' already contains {count} records")

        # Create example configuration
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM configurations")
        if cursor.fetchone()[0] == 0:
            create_example_configuration(conn, logger)

        # Verify database
        verify_database(conn, logger)

        conn.close()
        logger.info("Database initialization completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error during initialization: {e}")
        conn.close()
        return False

def main():
    args = parse_args()
    logger = setup_logging(args.verbose)

    logger.info("Starting database initialization")
    logger.info(f"Database path: {args.db_path}")

    # Validate paths
    if not args.create_dynamic_schema and not os.path.exists(args.schema_path):
        logger.error(f"Schema file not found: {args.schema_path}")
        return 1

    if args.csv_path and not os.path.exists(args.csv_path):
        logger.error(f"CSV file not found: {args.csv_path}")
        return 1

    # Initialize database
    success = initialize_database(
        args.db_path,
        args.schema_path,
        args.csv_path,
        args.delimiter,
        args.force_reset,
        args.create_dynamic_schema,
        logger
    )

    if success:
        logger.info("\n" + "="*50)
        logger.info("Database initialization completed!")
        logger.info("You can now run the sampling tool.")
        logger.info("="*50)

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
