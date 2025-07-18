import sqlite3
import pandas as pd
import sys
import os
import argparse
import logging
from typing import Optional, Dict, List


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def execute_schema(conn, schema_path, logger):
    """Execute SQL schema file."""
    try:
        with open(schema_path, 'r') as f:
            schema = f.read()
            # SQLite doesn't support UNIQUEIDENTIFIER, replace with TEXT
            schema = schema.replace('UNIQUEIDENTIFIER', 'TEXT')
            # SQLite doesn't support NVARCHAR, replace with VARCHAR
            schema = schema.replace('NVARCHAR', 'VARCHAR')
            
            cursor = conn.cursor()
            cursor.executescript(schema)
            conn.commit()
            logger.info("Database schema created/updated successfully")
            return True
    except Exception as e:
        logger.error(f"Error executing schema: {e}")
        return False


def get_table_mapping(csv_filename: str) -> Optional[str]:
    """Map CSV filename to table name."""
    mappings = {
        'tabelle1_kundenstamm.csv': 'kundenstamm',
        'tabelle2_softfact_vw.csv': 'softfact_vw',
        'tabelle3_kontodaten_vw.csv': 'kontodaten_vw'
    }
    
    # Case-insensitive matching
    csv_lower = csv_filename.lower()
    for pattern, table in mappings.items():
        if pattern in csv_lower:
            return table
    
    return None


def import_csv_to_table(conn, csv_path, table_name, delimiter=';', logger=None):
    """Import CSV data into specified table."""
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path, delimiter=delimiter, encoding='utf-8-sig')
        logger.info(f"Read {len(df)} rows from {csv_path}")
        logger.info(f"Original columns: {list(df.columns)}")
        
        # Skip the data type row (row 2 in the CSVs)
        if len(df) > 2:
            # Check if second row contains data type info
            second_row = df.iloc[1]
            if any(str(val).lower() in ['varchar', 'nvarchar', 'int', 'bigint', 'date', 'decimal', 'char', 'uniqueidentifier'] 
                   for val in second_row.values):
                logger.info("Detected data type row, removing it")
                df = df.drop(df.index[1])
                df = df.reset_index(drop=True)
        
        # Skip empty rows (rows 3 and 4 in the CSVs)
        df = df.dropna(how='all')
        logger.info(f"After removing empty rows: {len(df)} rows")
        
        # Clean column names - convert to lowercase and replace special characters
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_').replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss') for col in df.columns]
        logger.info(f"Cleaned columns: {list(df.columns)}")
        
        # Handle BOM character in first column name if present
        if df.columns[0].startswith('\ufeff'):
            df.columns = [df.columns[0].replace('\ufeff', '')] + list(df.columns[1:])
        
        # Convert data types based on column names and content
        df_converted = df.copy()
        
        for col in df_converted.columns:
            if df_converted[col].dtype == 'object':
                sample_values = df_converted[col].dropna().head(5)
                
                # Try to detect and convert dates
                if 'datum' in col or 'date' in col or col == 'stichtag':
                    try:
                        df_converted[col] = pd.to_datetime(df_converted[col], format='%d.%m.%Y', errors='coerce')
                        logger.info(f"  Converted '{col}' to datetime")
                    except:
                        pass
                
                # Try to detect and convert numeric fields
                elif any(keyword in col for keyword in ['nummer', 'id', 'pk', 'count', 'amount']):
                    try:
                        # For European format numbers
                        if any(',' in str(val) for val in sample_values if pd.notna(val)):
                            converted = df_converted[col].apply(lambda x: 
                                str(x).replace('.', '').replace(',', '.') if isinstance(x, str) else x
                            )
                            df_converted[col] = pd.to_numeric(converted, errors='coerce')
                        else:
                            df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce')
                        logger.info(f"  Converted '{col}' to numeric")
                    except:
                        pass
        
        # Show sample of data to be imported
        logger.info("Sample of data to be imported:")
        logger.info(df_converted.head(3).to_string())
        
        # Drop existing data from table (but keep structure)
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
        
        # Import to database
        df_converted.to_sql(table_name, conn, if_exists='append', index=False)
        
        # Verify the import
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        logger.info(f"Successfully imported {count} records to {table_name}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error importing CSV to {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Main initialization function."""
    logger = setup_logging()
    
    parser = argparse.ArgumentParser(description='Initialize the sampling tool database')
    parser.add_argument('--db-path', default='./data/sampling.db', help='Database path')
    parser.add_argument('--schema-path', default='./schema.sql', help='Schema SQL file path')
    parser.add_argument('--csv-path', help='CSV file to import')
    parser.add_argument('--csv-dir', help='Directory containing CSV files to import')
    parser.add_argument('--table', help='Target table name (auto-detected if not specified)')
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
        conn.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {args.db_path}")
    except sqlite3.Error as e:
        logger.error(f"Failed to connect: {e}")
        return 1
    
    # Execute schema
    schema_path = args.schema_path
    if not os.path.isabs(schema_path):
        # Try to find schema.sql relative to this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(script_dir, 'schema.sql')
    
    if os.path.exists(schema_path):
        if not execute_schema(conn, schema_path, logger):
            return 1
    else:
        logger.warning(f"Schema file not found at {schema_path}")
        logger.info("Creating minimal table structure...")
        # Create minimal tables
        cursor = conn.cursor()
        cursor.executescript("""
            CREATE TABLE IF NOT EXISTS kundenstamm (pk TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS softfact_vw (pk TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS kontodaten_vw (pk TEXT PRIMARY KEY);
        """)
        conn.commit()
    
    # Import CSV(s)
    if args.csv_dir:
        # Import all CSVs from directory
        csv_files = [f for f in os.listdir(args.csv_dir) if f.lower().endswith('.csv')]
        logger.info(f"Found {len(csv_files)} CSV files in {args.csv_dir}")
        
        for csv_file in csv_files:
            csv_path = os.path.join(args.csv_dir, csv_file)
            table_name = get_table_mapping(csv_file)
            
            if table_name:
                logger.info(f"\nImporting {csv_file} to table {table_name}")
                import_csv_to_table(conn, csv_path, table_name, args.delimiter, logger)
            else:
                logger.warning(f"Could not determine table for {csv_file}, skipping")
    
    elif args.csv_path:
        # Import single CSV
        table_name = args.table
        if not table_name:
            # Try to auto-detect from filename
            table_name = get_table_mapping(os.path.basename(args.csv_path))
        
        if table_name:
            if not import_csv_to_table(conn, args.csv_path, table_name, args.delimiter, logger):
                return 1
        else:
            logger.error("Could not determine target table. Please specify --table")
            return 1
    
    # Show summary
    cursor = conn.cursor()
    logger.info("\nDatabase summary:")
    for table in ['kundenstamm', 'softfact_vw', 'kontodaten_vw']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"  {table}: {count} records")
        except sqlite3.Error:
            logger.info(f"  {table}: table not found or empty")
    
    conn.close()
    
    if not args.csv_path and not args.csv_dir:
        logger.info("\nTo import data:")
        logger.info("  Single file: python db_init.py --csv-path file.csv")
        logger.info("  Directory: python db_init.py --csv-dir ./new_tables/")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())