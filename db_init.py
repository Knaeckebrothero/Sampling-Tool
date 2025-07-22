import sqlite3
import pandas as pd
import sys
import os
import logging
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


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
        with open(schema_path, 'r', encoding='utf-8') as f:
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


def import_csv_to_table(conn, csv_path, table_name, delimiter=';', logger=None):
    """Import CSV data into specified table."""
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path, delimiter=delimiter, encoding='utf-8-sig')
        logger.info(f"Read {len(df)} rows from {csv_path}")
        logger.info(f"Original columns: {list(df.columns)}")
        
        # Skip empty rows
        df = df.dropna(how='all')
        logger.info(f"After removing empty rows: {len(df)} rows")
        
        # Clean column names - convert to lowercase and replace special characters
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_')
                     .replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss') 
                     for col in df.columns]
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
                        # First try German date format
                        df_converted[col] = pd.to_datetime(df_converted[col], format='%Y-%m-%d', errors='coerce')
                        # If that didn't work well, try other format
                        if df_converted[col].isna().sum() > len(df_converted) * 0.5:
                            df_converted[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
                        logger.info(f"  Converted '{col}' to datetime")
                    except:
                        pass
                
                # Try to detect and convert numeric fields
                # Skip 'pk' column and guid as they should remain text
                elif col not in ['pk', 'guid'] and any(keyword in col for keyword in ['nummer', 'id', 'count', 'amount']):
                    try:
                        # Check if all non-null values can be converted to numeric
                        test_numeric = pd.to_numeric(df_converted[col], errors='coerce')
                        if test_numeric.notna().sum() > 0:
                            df_converted[col] = test_numeric
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
    
    # Use environment variable for database path with fallback
    db_path = os.getenv('DB_PATH', './sampling.db')
    schema_path = './src/schema.sql'
    sample_data_dir = './sample_data'
    
    # Hardcoded mapping of CSV files to tables
    csv_table_mapping = {
        'Kundenstamm.csv': 'kundenstamm',
        'Softfact.csv': 'softfact_vw',
        'Kontodaten.csv': 'kontodaten_vw'
    }
    
    # Create data directory if needed
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    # Remove existing database for clean initialization
    if os.path.exists(db_path):
        os.remove(db_path)
        logger.info("Removed existing database for clean initialization")
    
    # Connect to database
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {db_path}")
    except sqlite3.Error as e:
        logger.error(f"Failed to connect: {e}")
        return 1
    
    # Execute schema
    if os.path.exists(schema_path):
        logger.info(f"Loading schema from {schema_path}")
        if not execute_schema(conn, schema_path, logger):
            logger.error("Failed to execute schema")
            return 1
    else:
        logger.error(f"Schema file not found at {schema_path}")
        logger.info("Please ensure the schema.sql file is in the src/ directory")
        return 1
    
    # Import sample data
    logger.info(f"\nImporting sample data from {sample_data_dir}")
    
    if not os.path.exists(sample_data_dir):
        logger.error(f"Sample data directory not found: {sample_data_dir}")
        return 1
    
    success_count = 0
    for csv_file, table_name in csv_table_mapping.items():
        csv_path = os.path.join(sample_data_dir, csv_file)
        
        if os.path.exists(csv_path):
            logger.info(f"\nImporting {csv_file} to table {table_name}")
            if import_csv_to_table(conn, csv_path, table_name, delimiter=';', logger=logger):
                success_count += 1
        else:
            logger.warning(f"CSV file not found: {csv_path}")
    
    # Show summary
    cursor = conn.cursor()
    logger.info("\n" + "="*50)
    logger.info("Database initialization summary:")
    logger.info("="*50)
    
    for table in ['kundenstamm', 'softfact_vw', 'kontodaten_vw']:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            logger.info(f"\n{table}:")
            logger.info(f"  Records: {count}")
            logger.info(f"  Columns: {len(columns)}")
        except sqlite3.Error as e:
            logger.error(f"\n{table}: Error - {e}")
    
    conn.close()
    
    if success_count == len(csv_table_mapping):
        logger.info("\n✓ Database initialization completed successfully!")
        logger.info(f"  Database location: {os.path.abspath(db_path)}")
    else:
        logger.warning(f"\n⚠ Database initialization completed with issues: {success_count}/{len(csv_table_mapping)} tables imported")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())