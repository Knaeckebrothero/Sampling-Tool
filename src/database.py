import os
import sqlite3
import logging
import pandas as pd
from typing import Optional, List, Dict, Any

# Set up logging
log = logging.getLogger(__name__)


class Database:
    """
    Simplified database manager for the sampling tool.
    """

    def __init__(self, db_path: str = "./data/sampling.db"):
        """Initialize database connection."""
        log.debug("Initializing database connection...")
        self._path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self.connect()
        log.info("Database initialized.")

    def connect(self):
        """Establish connection to the database."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self._path), exist_ok=True)

            # Connect to database
            self._conn = sqlite3.connect(self._path)
            self._conn.row_factory = sqlite3.Row  # Enable column access by name
            self.cursor = self._conn.cursor()
            log.info(f"Connected to database at {self._path}")

            # Create table if it doesn't exist
            self._create_table()

        except sqlite3.Error as e:
            log.error(f"Failed to connect to database: {e}")
            raise

    def _create_table(self):
        """Create the financial_data table if it doesn't exist."""
        try:
            # Check if table exists
            self.cursor.execute("""
                                SELECT name FROM sqlite_master
                                WHERE type='table' AND name='financial_data'
                                """)
            if not self.cursor.fetchone():
                # Create a minimal table - it will be replaced when importing CSV
                self.cursor.execute("""
                                    CREATE TABLE financial_data (
                                                                    id INTEGER PRIMARY KEY
                                    )
                                    """)
                self._conn.commit()
                log.debug("Created empty financial_data table")
        except sqlite3.Error as e:
            log.error(f"Error creating table: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            log.debug("Database connection closed.")

    def get_table_columns(self, table_name: str = "financial_data") -> List[str]:
        """Get column names for a table."""
        query = f"PRAGMA table_info({table_name})"
        result = self.cursor.execute(query).fetchall()
        columns = [row['name'] for row in result if row['name'] not in ['index']]
        log.debug(f"Table columns: {columns}")
        return columns

    def get_column_info(self, table_name: str = "financial_data") -> Dict[str, str]:
        """Get column information including data types."""
        query = f"PRAGMA table_info({table_name})"
        result = self.cursor.execute(query).fetchall()
        return {row['name']: row['type'] for row in result if row['name'] not in ['index']}

    def get_all_data(self, table_name: str = "financial_data", limit: Optional[int] = None) -> List[Dict]:
        """Retrieve all data from the table."""
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"

        result = self.cursor.execute(query).fetchall()
        return [dict(row) for row in result]

    def get_sample_data(self, table_name: str = "financial_data", limit: int = 100) -> List[Dict]:
        """Get a sample of data for preview/type detection."""
        return self.get_all_data(table_name, limit)

    def get_filtered_data(self, table_name: str = "financial_data",
                          where_clause: str = "", params: Optional[tuple] = None) -> List[Dict]:
        """Get filtered data based on WHERE clause."""
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        if params:
            result = self.cursor.execute(query, params).fetchall()
        else:
            result = self.cursor.execute(query).fetchall()

        return [dict(row) for row in result]

    def get_row_count(self, table_name: str = "financial_data") -> int:
        """Get count of rows."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.cursor.execute(query).fetchone()
        return result['count'] if result else 0

    def import_csv_data(self, csv_path: str, table_name: str = "financial_data",
                        delimiter: str = ';', truncate: bool = False):
        """Import data from CSV file into the database."""
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

            # Drop the existing table if truncate is True
            if truncate:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                self._conn.commit()
                log.info(f"Dropped existing table {table_name}")

            # Import to database
            df_converted.to_sql(table_name, self._conn, if_exists='replace', index=False)
            self._conn.commit()

            # Verify the import
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = self.cursor.fetchone()[0]
            log.info(f"Successfully imported {count} records to {table_name}")

            # Log table structure
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            log.info(f"Table now has {len(columns)} columns")

            if count == 0:
                log.error("Warning: Table was created but no data was imported!")
                # Try to debug
                self.cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
                sample = self.cursor.fetchone()
                log.info(f"Sample row: {sample}")

        except Exception as e:
            log.error(f"Error importing CSV data: {e}")
            import traceback
            log.error(traceback.format_exc())
            raise

    @classmethod
    def get_instance(cls, db_path: str = "./data/sampling.db"):
        """Simple factory method to get database instance."""
        return cls(db_path)
