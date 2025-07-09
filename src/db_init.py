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


def import_csv_to_db(conn, csv_path, delimiter=';', logger=None):
    """Import CSV data into the financial_data table."""
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path, delimiter=delimiter)
        logger.info(f"Read {len(df)} rows from CSV")
        logger.info(f"Original columns: {list(df.columns)}")

        # Store original dtypes for debugging
        original_dtypes = df.dtypes.to_dict()

        # Clean column names - remove spaces and special characters
        df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]
        logger.info(f"Cleaned columns: {list(df.columns)}")

        # Create a copy to preserve original data
        df_converted = df.copy()

        # Convert European number format (comma as decimal separator) to standard format
        for col in df_converted.columns:
            if df_converted[col].dtype == 'object':
                # Skip if column appears to be non-numeric
                sample_values = df_converted[col].dropna().head(5)
                logger.info(f"Processing column '{col}': sample values = {list(sample_values)}")

                # Check if this looks like numeric data with European format
                numeric_pattern_found = False
                for val in sample_values:
                    if isinstance(val, str) and (',' in val or '.' in val):
                        # Check if it contains numbers
                        if any(c.isdigit() for c in val):
                            numeric_pattern_found = True
                            break

                if numeric_pattern_found:
                    try:
                        # Create a copy of the series for conversion
                        converted_series = df_converted[col].copy()

                        # Replace European format: dots (thousand separator) removed, comma becomes dot
                        converted_series = converted_series.apply(lambda x:
                                                                  str(x).replace('.', '').replace(',', '.') if isinstance(x, str) else x
                                                                  )

                        # Try to convert to numeric
                        converted_numeric = pd.to_numeric(converted_series, errors='coerce')

                        # Only replace if we successfully converted some values
                        if converted_numeric.notna().any():
                            df_converted[col] = converted_numeric
                            logger.info(f"  Converted '{col}' to numeric type")
                        else:
                            logger.info(f"  Kept '{col}' as text (conversion failed)")
                    except Exception as e:
                        logger.warning(f"  Could not convert '{col}': {e}")
                else:
                    logger.info(f"  Kept '{col}' as text (no numeric pattern found)")

        # Show final data types
        logger.info("Final data types:")
        for col, dtype in df_converted.dtypes.items():
            logger.info(f"  {col}: {dtype}")

        # Show a sample of the data to be imported
        logger.info("Sample of data to be imported:")
        logger.info(df_converted.head(3).to_string())

        # Drop existing table if it exists
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS financial_data")
        conn.commit()

        # Import to database
        df_converted.to_sql('financial_data', conn, if_exists='replace', index=False)

        # Verify the import
        cursor.execute("SELECT * FROM financial_data LIMIT 1")
        sample_row = cursor.fetchone()
        if sample_row:
            logger.info(f"Sample row from database: {sample_row}")

        # Get actual column count
        cursor.execute("PRAGMA table_info(financial_data)")
        columns = cursor.fetchall()
        logger.info(f"Database table has {len(columns)} columns: {[c[1] for c in columns]}")

        logger.info(f"Import completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error importing CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
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

    # Import CSV if provided
    if args.csv_path:
        if not import_csv_to_db(conn, args.csv_path, args.delimiter, logger):
            return 1

        # Show summary
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM financial_data")
        count = cursor.fetchone()[0]
        logger.info(f"\nDatabase ready with {count} records")
    else:
        logger.info("No CSV provided - database initialized empty")
        logger.info("To import data, run:")
        logger.info(f"  python db_init.py --csv-path your_data.csv --delimiter ';'")

    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
