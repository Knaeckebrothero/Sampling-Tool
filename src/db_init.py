# db_init.py
import argparse
import logging
import os
import sqlite3
import sys
import csv
from datetime import datetime
from database import SamplingDatabase


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
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
        help='Path to CSV file with initial data to import'
    )
    parser.add_argument(
        '--csv-delimiter',
        default=';',
        help='CSV file delimiter (default: ;)'
    )
    parser.add_argument(
        '--dataset-name',
        help='Name for the imported dataset'
    )
    parser.add_argument(
        '--force-reset',
        action='store_true',
        help='Force reset the database (deletes existing data)'
    )
    return parser.parse_args()

def ensure_directory_exists(path):
    """Ensure the directory for the given file path exists."""
    directory = os.path.dirname(path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
        return True
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

def detect_column_type(values):
    """Detect the type of a column based on its values."""
    if not values:
        return 'text'

    # Try to parse as dates
    date_formats = [
        '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',
        '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
        '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
    ]
    date_count = 0
    for value in values[:20]:  # Check first 20 values
        if not value:
            continue
        for fmt in date_formats:
            try:
                datetime.strptime(value, fmt)
                date_count += 1
                break
            except:
                pass

    if date_count > len(values[:20]) * 0.5:
        return 'date'

    # Try to parse as numbers
    number_count = 0
    for value in values[:20]:
        if not value:
            continue
        try:
            float(value.replace(',', '.').replace(' ', ''))
            number_count += 1
        except:
            pass

    if number_count > len(values[:20]) * 0.5:
        return 'number'

    return 'text'

def import_csv_to_database(db: SamplingDatabase, csv_path: str, delimiter: str, dataset_name: str, logger):
    """Import CSV data into the database."""
    try:
        # Read CSV file to detect structure
        with open(csv_path, 'r', encoding='utf-8') as file:
            # Read sample for type detection
            sample_reader = csv.DictReader(file, delimiter=delimiter)
            sample_data = []
            for i, row in enumerate(sample_reader):
                sample_data.append(row)
                if i >= 100:  # Read up to 100 rows for type detection
                    break

            if not sample_data:
                logger.error("No data found in CSV file")
                return False

            # Get column names and detect types
            column_names = list(sample_data[0].keys())
            columns = []

            for col in column_names:
                values = [row[col] for row in sample_data]
                col_type = detect_column_type(values)
                columns.append((col, col_type))

            logger.info(f"Detected {len(columns)} columns")

            # Create dataset table
            table_name = db.create_dataset_table(dataset_name, columns)

            # Read full file and insert data
            file.seek(0)
            reader = csv.DictReader(file, delimiter=delimiter)

            # Parse values based on detected types
            parsed_data = []
            for row in reader:
                parsed_row = {}
                for col_name, col_type in columns:
                    value = row[col_name]
                    if not value:
                        parsed_row[col_name] = None
                    elif col_type == 'number':
                        try:
                            parsed_row[col_name] = float(value.replace(',', '.').replace(' ', ''))
                        except:
                            parsed_row[col_name] = None
                    elif col_type == 'date':
                        date_formats = [
                            '%d-%m-%Y', '%d/%m/%Y', '%d.%m.%Y',
                            '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d',
                            '%m/%d/%Y', '%m-%d-%Y', '%m.%d.%Y',
                        ]
                        parsed = None
                        for fmt in date_formats:
                            try:
                                parsed = datetime.strptime(value, fmt)
                                break
                            except:
                                pass
                        parsed_row[col_name] = parsed
                    else:
                        parsed_row[col_name] = value

                parsed_data.append(parsed_row)

            # Insert data in batches
            batch_size = 1000
            for i in range(0, len(parsed_data), batch_size):
                batch = parsed_data[i:i + batch_size]
                db.insert_data_rows(table_name, column_names, batch)

            logger.info(f"Successfully imported {len(parsed_data)} rows from {csv_path}")
            return True

    except Exception as e:
        logger.error(f"Error importing CSV: {e}")
        return False

def insert_example_data(db: SamplingDatabase, logger):
    """Insert example dataset with sample financial data."""
    try:
        # Define columns for example financial data
        columns = [
            ('TransactionID', 'text'),
            ('Date', 'date'),
            ('Account', 'text'),
            ('Description', 'text'),
            ('Amount', 'number'),
            ('Category', 'text'),
            ('Status', 'text')
        ]

        # Create dataset table
        dataset_name = 'Example Financial Data'
        table_name = db.create_dataset_table(dataset_name, columns)

        # Create sample data
        sample_data = [
            {
                'TransactionID': 'TRX001',
                'Date': datetime(2024, 1, 15),
                'Account': 'Checking',
                'Description': 'Office Supplies',
                'Amount': 125.50,
                'Category': 'Operations',
                'Status': 'Completed'
            },
            {
                'TransactionID': 'TRX002',
                'Date': datetime(2024, 1, 16),
                'Account': 'Credit Card',
                'Description': 'Client Dinner',
                'Amount': 250.00,
                'Category': 'Entertainment',
                'Status': 'Pending'
            },
            {
                'TransactionID': 'TRX003',
                'Date': datetime(2024, 1, 17),
                'Account': 'Checking',
                'Description': 'Software License',
                'Amount': 1200.00,
                'Category': 'IT',
                'Status': 'Completed'
            }
        ]

        # Insert sample data
        column_names = [col[0] for col in columns]
        db.insert_data_rows(table_name, column_names, sample_data)

        logger.info("Inserted example financial data successfully")
        return True

    except Exception as e:
        logger.error(f"Error inserting example data: {e}")
        return False

def initialize_database(db_path, schema_path, csv_path, csv_delimiter, dataset_name, force_reset, logger):
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

    # Connect to database using our SamplingDatabase class
    try:
        db = SamplingDatabase(db_path)
        logger.info(f"Database connection established")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        return False

    try:
        # Check if database has any datasets
        datasets = db.list_datasets()

        if len(datasets) == 0:
            # Import CSV data if provided
            if csv_path and os.path.exists(csv_path):
                if not dataset_name:
                    dataset_name = os.path.splitext(os.path.basename(csv_path))[0]

                if not import_csv_to_database(db, csv_path, csv_delimiter, dataset_name, logger):
                    return False
            else:
                # Insert example data
                if not insert_example_data(db, logger):
                    return False
        else:
            logger.info(f"Database already contains {len(datasets)} dataset(s)")

        # List final datasets
        datasets = db.list_datasets()
        logger.info("Available datasets:")
        for ds in datasets:
            logger.info(f"  - {ds['name']} ({ds['row_count']} rows)")

        db.close()
        logger.info("Database initialization completed")
        return True

    except Exception as e:
        logger.error(f"Error during initialization: {e}")
        db.close()
        return False

def main():
    logger = setup_logging()
    args = parse_args()

    logger.info("Starting database initialization")

    # Validate paths
    if args.schema_path and not os.path.exists(args.schema_path):
        logger.warning(f"Schema file not found: {args.schema_path}")

    if args.csv_path and not os.path.exists(args.csv_path):
        logger.error(f"CSV file not found: {args.csv_path}")
        return 1

    # Initialize database
    success = initialize_database(
        args.db_path,
        args.schema_path,
        args.csv_path,
        args.csv_delimiter,
        args.dataset_name,
        args.force_reset,
        logger
    )

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
