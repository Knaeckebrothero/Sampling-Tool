import os
import pyodbc
import logging
import pandas as pd
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
log = logging.getLogger(__name__)


class Database:
    """
    MS SQL Server database manager for the sampling tool.
    """

    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection."""
        log.debug("Initializing MS SQL Server connection...")
        # Load MS SQL configuration from environment
        self._server = os.getenv('MSSQL_SERVER', 'localhost')
        self._database = os.getenv('MSSQL_DATABASE', 'SamplingDB')
        self._driver = os.getenv('MSSQL_DRIVER', 'ODBC Driver 17 for SQL Server')
        
        self._conn: Optional[pyodbc.Connection] = None
        self.cursor: Optional[pyodbc.Cursor] = None
        self.connect()
        log.info("MS SQL Server database initialized.")

    def connect(self):
        """Establish connection to MS SQL Server."""
        try:
            # Build connection string for Windows Authentication
            conn_str = (
                f"DRIVER={{{self._driver}}};"
                f"SERVER={self._server};"
                f"DATABASE={self._database};"
                f"Trusted_Connection=yes;"
            )
            
            # Connect to MS SQL Server
            self._conn = pyodbc.connect(conn_str)
            self.cursor = self._conn.cursor()
            log.info(f"Connected to MS SQL Server: {self._server}/{self._database}")
            
            # Note: Tables are assumed to already exist in MS SQL Server
            
        except pyodbc.Error as e:
            log.error(f"Failed to connect to MS SQL Server: {e}")
            raise

    def _row_to_dict(self, row):
        """Convert a pyodbc Row object to a dictionary."""
        if not row:
            return None
        columns = [column[0] for column in self.cursor.description]
        return dict(zip(columns, row))

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            log.debug("Database connection closed.")

    def get_table_columns(self, table_name: str = "kundenstamm") -> List[str]:
        """Get column names for a table."""
        query = """
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ? 
            ORDER BY ORDINAL_POSITION
        """
        result = self.cursor.execute(query, (table_name,)).fetchall()
        columns = [row[0] for row in result if row[0] not in ['index']]
        log.debug(f"Table columns: {columns}")
        return columns

    def get_column_info(self, table_name: str = "kundenstamm") -> Dict[str, str]:
        """Get column information including data types."""
        query = """
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """
        result = self.cursor.execute(query, (table_name,)).fetchall()
        return {row[0]: row[1] for row in result if row[0] not in ['index']}

    def get_all_data(self, table_name: str = "kundenstamm", limit: Optional[int] = None) -> List[Dict]:
        """Retrieve all data from the table."""
        if limit:
            query = f"SELECT TOP {limit} * FROM {table_name}"
        else:
            query = f"SELECT * FROM {table_name}"

        result = self.cursor.execute(query).fetchall()
        return [self._row_to_dict(row) for row in result]

    def get_sample_data(self, table_name: str = "kundenstamm", limit: int = 100) -> List[Dict]:
        """Get a sample of data for preview/type detection."""
        return self.get_all_data(table_name, limit)

    def get_filtered_data(self, table_name: str = "kundenstamm",
                          where_clause: str = "", params: Optional[tuple] = None) -> List[Dict]:
        """Get filtered data based on WHERE clause."""
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        if params:
            result = self.cursor.execute(query, params).fetchall()
        else:
            result = self.cursor.execute(query).fetchall()

        return [self._row_to_dict(row) for row in result]

    def get_row_count(self, table_name: str = "kundenstamm") -> int:
        """Get count of rows."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.cursor.execute(query).fetchone()
        return result[0] if result else 0

    def import_csv_data(self, csv_path: str, table_name: str = "kundenstamm",
                        delimiter: str = ';', truncate: bool = False):
        """Import data from CSV file into the MS SQL database."""
        try:
            # Read CSV with pandas
            df = pd.read_csv(csv_path, delimiter=delimiter)
            log.info(f"Read {len(df)} rows from CSV file")
            log.info(f"Original columns: {list(df.columns)}")

            # Clean column names - remove spaces and special characters
            original_columns = list(df.columns)
            df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]
            log.info(f"Cleaned columns: {list(df.columns)}")

            # Create a copy for conversion
            df_converted = df.copy()

            # Convert European number format (comma as decimal separator) to standard format
            for col in df_converted.columns:
                if df_converted[col].dtype == 'object':
                    # Check if this looks like numeric data
                    sample_values = df_converted[col].dropna().head(5)

                    # Check for numeric pattern
                    numeric_pattern_found = False
                    for val in sample_values:
                        if isinstance(val, str) and any(c.isdigit() for c in val):
                            if ',' in val or '.' in val:
                                numeric_pattern_found = True
                                break

                    if numeric_pattern_found:
                        try:
                            # Convert European format
                            converted_series = df_converted[col].apply(lambda x:
                                                                       str(x).replace('.', '').replace(',', '.') if isinstance(x, str) else x
                                                                       )

                            # Try to convert to numeric
                            converted_numeric = pd.to_numeric(converted_series, errors='coerce')

                            # Only use if we converted some values successfully
                            if converted_numeric.notna().any():
                                df_converted[col] = converted_numeric
                                log.info(f"Converted column '{col}' to numeric")
                        except Exception as e:
                            log.warning(f"Could not convert column '{col}': {e}")

            # Truncate the existing table if requested
            if truncate:
                self.cursor.execute(f"TRUNCATE TABLE {table_name}")
                self._conn.commit()
                log.info(f"Truncated table {table_name}")

            # Prepare data for bulk insert
            columns = list(df_converted.columns)
            placeholders = ','.join(['?' for _ in columns])
            columns_str = ','.join([f"[{col}]" for col in columns])  # Bracket column names for MS SQL
            
            # Convert DataFrame to list of tuples for executemany
            data_tuples = [tuple(row) for row in df_converted.values]
            
            # Bulk insert using executemany
            insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            self.cursor.executemany(insert_query, data_tuples)
            self._conn.commit()
            
            # Verify the import
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            log.info(f"Successfully imported {count} records to {table_name}")

            # Log table structure
            columns_query = """
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = ?
            """
            self.cursor.execute(columns_query, (table_name,))
            col_count = self.cursor.fetchone()[0]
            log.info(f"Table now has {col_count} columns")

            if count == 0:
                log.error("Warning: Table exists but no data was imported!")
                # Try to debug
                self.cursor.execute(f"SELECT TOP 1 * FROM {table_name}")
                sample = self.cursor.fetchone()
                log.info(f"Sample row: {sample}")

        except Exception as e:
            log.error(f"Error importing CSV data: {e}")
            import traceback
            log.error(traceback.format_exc())
            raise

    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            AND TABLE_CATALOG = ?
        """
        result = self.cursor.execute(query, (self._database,)).fetchall()
        return [row[0] for row in result]
    
    def get_production_tables(self) -> List[str]:
        """Get list of the three production tables."""
        return ['kundenstamm', 'softfact_vw', 'kontodaten_vw']
    
    def get_joined_data(self, base_table: str = "kundenstamm", 
                       join_tables: Optional[List[str]] = None,
                       join_conditions: Optional[Dict[str, str]] = None,
                       where_clause: str = "", 
                       params: Optional[tuple] = None,
                       limit: Optional[int] = None) -> List[Dict]:
        """Get data with joins across multiple tables."""
        # Handle TOP clause for MS SQL
        if limit:
            query = f"SELECT TOP {limit} * FROM {base_table}"
        else:
            query = f"SELECT * FROM {base_table}"
        
        # Add joins if specified
        if join_tables and join_conditions:
            for table in join_tables:
                if table in join_conditions:
                    query += f" LEFT JOIN {table} ON {join_conditions[table]}"
        
        # Add where clause
        if where_clause:
            query += f" WHERE {where_clause}"
        
        # Execute query
        if params:
            result = self.cursor.execute(query, params).fetchall()
        else:
            result = self.cursor.execute(query).fetchall()
        
        return [self._row_to_dict(row) for row in result]
    
    def get_table_relationships(self) -> Dict[str, Dict[str, str]]:
        """Get common join relationships between tables."""
        return {
            'kundenstamm': {
                'softfact_vw': 'kundenstamm.kundennummer = softfact_vw.kundennummer AND kundenstamm.banknummer = softfact_vw.banknummer',
                'kontodaten_vw': 'kundenstamm.personennummer_pseudonym = kontodaten_vw.personennummer_pseudonym AND kundenstamm.banknummer = kontodaten_vw.banknummer'
            },
            'softfact_vw': {
                'kontodaten_vw': 'softfact_vw.personennummer_pseudonym = kontodaten_vw.personennummer_pseudonym'
            }
        }
    
    def verify_table_structure(self, table_name: str, expected_columns: List[str]) -> bool:
        """Verify that a table has the expected columns."""
        actual_columns = self.get_table_columns(table_name)
        missing = set(expected_columns) - set(actual_columns)
        if missing:
            log.warning(f"Table {table_name} is missing columns: {missing}")
            return False
        return True
    
    @classmethod
    def get_instance(cls, db_path: Optional[str] = None):
        """Simple factory method to get database instance."""
        return cls(db_path)
