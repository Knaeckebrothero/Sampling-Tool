# database.py
import os
import sqlite3
import logging
import json
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime


# Set up logging
log = logging.getLogger(__name__)


class Singleton:
    """
    Ensures that a class has only one instance and provides a global point of access to that instance.
    """
    _instance = None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        """
        Provides a thread-safe singleton instance of the class.
        """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance


class SamplingDatabase(Singleton):
    """
    Manage SQLite database interactions for the sampling tool.
    Handles dynamic table creation based on imported CSV data.
    """

    def __init__(self, db_path: str = "./sampling.db"):
        """
        Initialize database connection.

        :param db_path: Path to the SQLite database file
        """
        log.debug("Initializing database connection...")
        self._path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self.connect()
        self._initialize_system_tables()
        log.info("Database initialized.")

    def __del__(self):
        """Clean up database connection on object destruction."""
        self.close()
        log.debug("Database object destroyed.")

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

        except sqlite3.Error as e:
            log.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            log.debug("Database connection closed.")

    def _initialize_system_tables(self):
        """Initialize system tables for metadata, filters, and rules."""
        try:
            # Table for storing imported datasets metadata
            self.cursor.execute("""
                                CREATE TABLE IF NOT EXISTS datasets (
                                                                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                        name TEXT NOT NULL UNIQUE,
                                                                        table_name TEXT NOT NULL UNIQUE,
                                                                        original_filename TEXT,
                                                                        row_count INTEGER,
                                                                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                )
                                """)

            # Table for storing column metadata
            self.cursor.execute("""
                                CREATE TABLE IF NOT EXISTS dataset_columns (
                                                                               id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                               dataset_id INTEGER NOT NULL,
                                                                               column_name TEXT NOT NULL,
                                                                               column_type TEXT NOT NULL,
                                                                               column_index INTEGER,
                                                                               FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                                    UNIQUE(dataset_id, column_name)
                                    )
                                """)

            # Table for storing global filters
            self.cursor.execute("""
                                CREATE TABLE IF NOT EXISTS global_filters (
                                                                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                              dataset_id INTEGER NOT NULL,
                                                                              column_name TEXT NOT NULL,
                                                                              column_type TEXT NOT NULL,
                                                                              filter_config TEXT NOT NULL,  -- JSON
                                                                              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                              FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                                    )
                                """)

            # Table for storing sampling rules
            self.cursor.execute("""
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
                                    )
                                """)

            # Table for storing sampling configurations
            self.cursor.execute("""
                                CREATE TABLE IF NOT EXISTS sampling_configs (
                                                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                                dataset_id INTEGER NOT NULL,
                                                                                name TEXT NOT NULL,
                                                                                config_data TEXT NOT NULL,  -- JSON with filters and rules
                                                                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                                FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
                                    )
                                """)

            # Create indexes
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_columns_dataset ON dataset_columns(dataset_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_filters_dataset ON global_filters(dataset_id)")
            self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_rules_dataset ON sampling_rules(dataset_id)")

            self._conn.commit()
            log.debug("System tables initialized successfully")

        except sqlite3.Error as e:
            log.error(f"Error initializing system tables: {e}")
            self._conn.rollback()
            raise

    def create_dataset_table(self, dataset_name: str, columns: List[Tuple[str, str]]) -> str:
        """
        Create a new table for dataset with dynamic columns.

        :param dataset_name: Name of the dataset
        :param columns: List of (column_name, column_type) tuples
        :return: Generated table name
        """
        # Generate safe table name
        table_name = f"data_{dataset_name.lower().replace(' ', '_').replace('-', '_')}"
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')

        try:
            # Create the data table
            column_defs = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
            for col_name, col_type in columns:
                if col_type == 'number':
                    sql_type = 'REAL'
                elif col_type == 'date':
                    sql_type = 'TEXT'  # Store dates as ISO format strings
                else:
                    sql_type = 'TEXT'

                # Make column name SQL-safe
                safe_col_name = f'"{col_name}"'
                column_defs.append(f"{safe_col_name} {sql_type}")

            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
            self.cursor.execute(create_table_sql)

            # Insert dataset metadata
            self.cursor.execute("""
                                INSERT INTO datasets (name, table_name)
                                VALUES (?, ?)
                                """, (dataset_name, table_name))

            dataset_id = self.cursor.lastrowid

            # Insert column metadata
            for idx, (col_name, col_type) in enumerate(columns):
                self.cursor.execute("""
                                    INSERT INTO dataset_columns (dataset_id, column_name, column_type, column_index)
                                    VALUES (?, ?, ?, ?)
                                    """, (dataset_id, col_name, col_type, idx))

            self._conn.commit()
            log.info(f"Created table '{table_name}' for dataset '{dataset_name}'")
            return table_name

        except sqlite3.Error as e:
            log.error(f"Error creating dataset table: {e}")
            self._conn.rollback()
            raise

    def insert_data_rows(self, table_name: str, columns: List[str], data: List[Dict[str, Any]]):
        """
        Insert multiple rows of data into a dataset table.

        :param table_name: Name of the table
        :param columns: List of column names
        :param data: List of dictionaries containing row data
        """
        if not data:
            return

        try:
            # Prepare insert query
            safe_columns = [f'"{col}"' for col in columns]
            placeholders = ','.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {table_name} ({','.join(safe_columns)}) VALUES ({placeholders})"

            # Prepare data for insertion
            rows = []
            for row_dict in data:
                row_values = []
                for col in columns:
                    value = row_dict.get(col)
                    # Convert datetime objects to ISO format strings
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    row_values.append(value)
                rows.append(tuple(row_values))

            # Bulk insert
            self.cursor.executemany(insert_sql, rows)

            # Update row count in datasets table
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]

            self.cursor.execute("""
                                UPDATE datasets
                                SET row_count = ?, updated_at = CURRENT_TIMESTAMP
                                WHERE table_name = ?
                                """, (row_count, table_name))

            self._conn.commit()
            log.info(f"Inserted {len(data)} rows into {table_name}")

        except sqlite3.Error as e:
            log.error(f"Error inserting data rows: {e}")
            self._conn.rollback()
            raise

    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a dataset.

        :param dataset_name: Name of the dataset
        :return: Dataset information or None
        """
        try:
            self.cursor.execute("""
                                SELECT id, name, table_name, original_filename, row_count, created_at
                                FROM datasets
                                WHERE name = ?
                                """, (dataset_name,))

            row = self.cursor.fetchone()
            if row:
                return dict(row)
            return None

        except sqlite3.Error as e:
            log.error(f"Error getting dataset info: {e}")
            raise

    def get_dataset_columns(self, dataset_id: int) -> List[Dict[str, Any]]:
        """
        Get column information for a dataset.

        :param dataset_id: Dataset ID
        :return: List of column information
        """
        try:
            self.cursor.execute("""
                                SELECT column_name, column_type, column_index
                                FROM dataset_columns
                                WHERE dataset_id = ?
                                ORDER BY column_index
                                """, (dataset_id,))

            return [dict(row) for row in self.cursor.fetchall()]

        except sqlite3.Error as e:
            log.error(f"Error getting dataset columns: {e}")
            raise

    def get_all_data(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all data from a dataset table.

        :param table_name: Name of the table
        :return: List of rows as dictionaries
        """
        try:
            self.cursor.execute(f"SELECT * FROM {table_name} ORDER BY id")
            rows = self.cursor.fetchall()

            # Convert Row objects to dictionaries
            result = []
            for row in rows:
                row_dict = dict(row)
                # Remove the id column as it's not part of the original data
                row_dict.pop('id', None)

                # Convert ISO date strings back to datetime objects
                for key, value in row_dict.items():
                    if isinstance(value, str) and value:
                        # Try to parse as datetime
                        try:
                            row_dict[key] = datetime.fromisoformat(value)
                        except:
                            pass  # Keep as string if not a valid datetime

                result.append(row_dict)

            return result

        except sqlite3.Error as e:
            log.error(f"Error retrieving data: {e}")
            raise

    def get_filtered_data(self, table_name: str, where_clause: str, params: Tuple) -> List[Dict[str, Any]]:
        """
        Retrieve filtered data from a dataset table.

        :param table_name: Name of the table
        :param where_clause: SQL WHERE clause
        :param params: Parameters for the WHERE clause
        :return: List of rows as dictionaries
        """
        try:
            query = f"SELECT * FROM {table_name} WHERE {where_clause} ORDER BY id"
            self.cursor.execute(query, params)
            rows = self.cursor.fetchall()

            # Convert Row objects to dictionaries
            result = []
            for row in rows:
                row_dict = dict(row)
                row_dict.pop('id', None)

                # Convert ISO date strings back to datetime objects
                for key, value in row_dict.items():
                    if isinstance(value, str) and value:
                        try:
                            row_dict[key] = datetime.fromisoformat(value)
                        except:
                            pass

                result.append(row_dict)

            return result

        except sqlite3.Error as e:
            log.error(f"Error retrieving filtered data: {e}")
            raise

    def save_global_filters(self, dataset_id: int, filters: List[Dict[str, Any]]):
        """
        Save global filters for a dataset.

        :param dataset_id: Dataset ID
        :param filters: List of filter dictionaries
        """
        try:
            # Clear existing filters
            self.cursor.execute("DELETE FROM global_filters WHERE dataset_id = ?", (dataset_id,))

            # Insert new filters
            for filter_obj in filters:
                self.cursor.execute("""
                                    INSERT INTO global_filters (dataset_id, column_name, column_type, filter_config)
                                    VALUES (?, ?, ?, ?)
                                    """, (
                                        dataset_id,
                                        filter_obj['column'],
                                        filter_obj['column_type'],
                                        json.dumps(filter_obj['filter_config'])
                                    ))

            self._conn.commit()
            log.info(f"Saved {len(filters)} global filters")

        except sqlite3.Error as e:
            log.error(f"Error saving global filters: {e}")
            self._conn.rollback()
            raise

    def save_sampling_rules(self, dataset_id: int, rules: List[Dict[str, Any]]):
        """
        Save sampling rules for a dataset.

        :param dataset_id: Dataset ID
        :param rules: List of rule dictionaries
        """
        try:
            # Clear existing rules
            self.cursor.execute("DELETE FROM sampling_rules WHERE dataset_id = ?", (dataset_id,))

            # Insert new rules
            for rule in rules:
                self.cursor.execute("""
                                    INSERT INTO sampling_rules
                                    (dataset_id, name, column_name, column_type, filter_config, sample_count)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """, (
                                        dataset_id,
                                        rule['name'],
                                        rule['column'],
                                        rule['column_type'],
                                        json.dumps(rule['filter_config']),
                                        rule['sample_count']
                                    ))

            self._conn.commit()
            log.info(f"Saved {len(rules)} sampling rules")

        except sqlite3.Error as e:
            log.error(f"Error saving sampling rules: {e}")
            self._conn.rollback()
            raise

    def load_global_filters(self, dataset_id: int) -> List[Dict[str, Any]]:
        """
        Load global filters for a dataset.

        :param dataset_id: Dataset ID
        :return: List of filter dictionaries
        """
        try:
            self.cursor.execute("""
                                SELECT column_name, column_type, filter_config
                                FROM global_filters
                                WHERE dataset_id = ?
                                ORDER BY id
                                """, (dataset_id,))

            filters = []
            for row in self.cursor.fetchall():
                filters.append({
                    'column': row['column_name'],
                    'column_type': row['column_type'],
                    'filter_config': json.loads(row['filter_config'])
                })

            return filters

        except sqlite3.Error as e:
            log.error(f"Error loading global filters: {e}")
            raise

    def load_sampling_rules(self, dataset_id: int) -> List[Dict[str, Any]]:
        """
        Load sampling rules for a dataset.

        :param dataset_id: Dataset ID
        :return: List of rule dictionaries
        """
        try:
            self.cursor.execute("""
                                SELECT name, column_name, column_type, filter_config, sample_count
                                FROM sampling_rules
                                WHERE dataset_id = ?
                                ORDER BY id
                                """, (dataset_id,))

            rules = []
            for row in self.cursor.fetchall():
                rules.append({
                    'name': row['name'],
                    'column': row['column_name'],
                    'column_type': row['column_type'],
                    'filter_config': json.loads(row['filter_config']),
                    'sample_count': row['sample_count']
                })

            return rules

        except sqlite3.Error as e:
            log.error(f"Error loading sampling rules: {e}")
            raise

    def save_sampling_config(self, dataset_id: int, config_name: str, filters: List[Dict], rules: List[Dict]):
        """
        Save a complete sampling configuration.

        :param dataset_id: Dataset ID
        :param config_name: Name for the configuration
        :param filters: List of global filters
        :param rules: List of sampling rules
        """
        try:
            config_data = {
                'global_filters': filters,
                'sampling_rules': rules
            }

            self.cursor.execute("""
                                INSERT INTO sampling_configs (dataset_id, name, config_data)
                                VALUES (?, ?, ?)
                                """, (dataset_id, config_name, json.dumps(config_data)))

            self._conn.commit()
            log.info(f"Saved sampling configuration '{config_name}'")

        except sqlite3.Error as e:
            log.error(f"Error saving sampling config: {e}")
            self._conn.rollback()
            raise

    def list_datasets(self) -> List[Dict[str, Any]]:
        """
        List all available datasets.

        :return: List of dataset information
        """
        try:
            self.cursor.execute("""
                                SELECT id, name, table_name, row_count, created_at
                                FROM datasets
                                ORDER BY created_at DESC
                                """)

            return [dict(row) for row in self.cursor.fetchall()]

        except sqlite3.Error as e:
            log.error(f"Error listing datasets: {e}")
            raise

    def delete_dataset(self, dataset_id: int):
        """
        Delete a dataset and all associated data.

        :param dataset_id: Dataset ID
        """
        try:
            # Get table name
            self.cursor.execute("SELECT table_name FROM datasets WHERE id = ?", (dataset_id,))
            row = self.cursor.fetchone()

            if row:
                table_name = row['table_name']

                # Drop the data table
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

                # Delete dataset record (cascade will handle related records)
                self.cursor.execute("DELETE FROM datasets WHERE id = ?", (dataset_id,))

                self._conn.commit()
                log.info(f"Deleted dataset with ID {dataset_id}")

        except sqlite3.Error as e:
            log.error(f"Error deleting dataset: {e}")
            self._conn.rollback()
            raise