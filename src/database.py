import os
import sqlite3
import logging
import json
import pandas as pd
from typing import Optional, Union, List, Tuple, Any, Dict
from datetime import datetime

# Set up logging
log = logging.getLogger(__name__)


class Singleton:
    """Simple singleton pattern - included directly in database.py"""
    _instance = None

    @classmethod
    def get_instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance


class Database(Singleton):
    """
    Manage SQLite database interactions for the sampling tool.
    """

    def __init__(self, db_path: str = "./data/sampling.db"):
        """
        Initialize database connection.
        
        :param db_path: Path to the SQLite database file
        """
        log.debug("Initializing database connection...")
        self._path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self.connect()
        log.info("Database initialized.")

    def __del__(self):
        """Clean up database connection on object destruction."""
        self.close()
        log.debug("Database object destroyed.")

    def connect(self):
        """Establish connection to the database and verify schema."""
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self._path), exist_ok=True)

            # Connect to database
            self._conn = sqlite3.connect(self._path)
            self._conn.row_factory = sqlite3.Row  # Enable column access by name
            self.cursor = self._conn.cursor()
            log.info(f"Connected to database at {self._path}")

            # Verify required tables exist
            self._verify_tables()

        except sqlite3.Error as e:
            log.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            log.debug("Database connection closed.")

    def _verify_tables(self, required_tables: List[str] = None):
        """
        Verify that required tables exist in the database.
        
        :param required_tables: List of required table names
        :raises RuntimeError: If required tables are missing
        """
        if required_tables is None:
            # Define required tables for the sampling tool
            required_tables = ['financial_data', 'configurations', 'sampling_results', 'sampling_history']

        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            existing_tables = [row[0] for row in self.cursor.fetchall()]

            missing_tables = [table for table in required_tables if table not in existing_tables]
            if missing_tables:
                log.error(f"Required tables are missing: {missing_tables}")
                log.error("Please run the database initialization script first!")
                raise RuntimeError(f"Required tables are missing: {missing_tables}")
            else:
                log.debug(f"All required tables exist: {required_tables}")

        except sqlite3.Error as e:
            log.error(f"Error verifying tables: {e}")
            raise

    def query(self, query: str, params: Optional[Union[tuple, list]] = None) -> List[sqlite3.Row]:
        """
        Execute a SELECT query and return results.
        
        :param query: SQL query string
        :param params: Query parameters
        :return: List of rows
        """
        try:
            if params:
                log.debug(f"Executing query: {query} with params: {params}")
                self.cursor.execute(query, params)
            else:
                log.debug(f"Executing query: {query}")
                self.cursor.execute(query)

            result = self.cursor.fetchall()
            log.debug(f"Query returned {len(result)} records")
            return result

        except sqlite3.Error as e:
            log.error(f"Error executing query: {e}")
            log.debug(f"Query was: {query}")
            if params:
                log.debug(f"Params were: {params}")
            raise

    def insert(self, query: str, params: Optional[Any] = None) -> int:
        """
        Execute an INSERT query and return the last inserted row ID.
        
        :param query: SQL insert query
        :param params: Query parameters
        :return: ID of the last inserted row
        """
        try:
            if params:
                log.debug(f"Executing insert: {query} with params: {params}")
                self.cursor.execute(query, params)
            else:
                log.debug(f"Executing insert: {query}")
                self.cursor.execute(query)

            self._conn.commit()
            return self.cursor.lastrowid

        except sqlite3.Error as e:
            log.error(f"Error executing insert: {e}")
            self._conn.rollback()
            raise

    def update(self, query: str, params: Optional[Any] = None) -> int:
        """
        Execute an UPDATE query and return the number of affected rows.
        
        :param query: SQL update query
        :param params: Query parameters
        :return: Number of affected rows
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            self._conn.commit()
            return self.cursor.rowcount

        except sqlite3.Error as e:
            log.error(f"Error executing update: {e}")
            self._conn.rollback()
            raise

    def delete(self, query: str, params: Optional[Any] = None) -> int:
        """
        Execute a DELETE query and return the number of affected rows.
        
        :param query: SQL delete query
        :param params: Query parameters
        :return: Number of affected rows
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            self._conn.commit()
            return self.cursor.rowcount

        except sqlite3.Error as e:
            log.error(f"Error executing delete: {e}")
            self._conn.rollback()
            raise

    def execute_script(self, script: str):
        """
        Execute a SQL script (multiple statements).
        
        :param script: SQL script
        """
        try:
            self._conn.executescript(script)
            self._conn.commit()
            log.debug("Script executed successfully")

        except sqlite3.Error as e:
            log.error(f"Error executing script: {e}")
            self._conn.rollback()
            raise

    # ===== Sampling Tool Specific Methods =====

    def get_table_columns(self, table_name: str = "financial_data") -> List[str]:
        """
        Get column names for a table.
        
        :param table_name: Name of the table
        :return: List of column names
        """
        query = f"PRAGMA table_info({table_name})"
        result = self.query(query)
        return [row['name'] for row in result if row['name'] != 'id']

    def get_column_info(self, table_name: str = "financial_data") -> Dict[str, str]:
        """
        Get column information including data types.
        
        :param table_name: Name of the table
        :return: Dictionary of column names to SQL types
        """
        query = f"PRAGMA table_info({table_name})"
        result = self.query(query)
        return {row['name']: row['type'] for row in result if row['name'] != 'id'}

    def get_all_data(self, table_name: str = "financial_data", limit: Optional[int] = None) -> List[Dict]:
        """
        Retrieve all data from the main table.
        
        :param table_name: Name of the table
        :param limit: Optional limit on number of records
        :return: List of dictionaries containing the data
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"

        result = self.query(query)
        return [dict(row) for row in result]

    def get_sample_data(self, table_name: str = "financial_data", limit: int = 100) -> List[Dict]:
        """
        Get a sample of data for preview/type detection.
        
        :param table_name: Name of the table
        :param limit: Number of rows to retrieve
        :return: List of dictionaries
        """
        return self.get_all_data(table_name, limit)

    def get_filtered_data(self, table_name: str = "financial_data",
                          where_clause: str = "", params: Optional[tuple] = None) -> List[Dict]:
        """
        Get filtered data based on WHERE clause.
        
        :param table_name: Name of the table
        :param where_clause: SQL WHERE clause (without WHERE keyword)
        :param params: Query parameters
        :return: List of dictionaries
        """
        query = f"SELECT * FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.query(query, params)
        return [dict(row) for row in result]

    def get_distinct_values(self, column: str, table_name: str = "financial_data",
                            limit: int = 100) -> List[Any]:
        """
        Get distinct values for a column.
        
        :param column: Column name
        :param table_name: Table name
        :param limit: Maximum number of distinct values
        :return: List of distinct values
        """
        query = f"SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL ORDER BY {column} LIMIT ?"
        result = self.query(query, (limit,))
        return [row[column] for row in result]

    def get_row_count(self, table_name: str = "financial_data",
                      where_clause: str = "", params: Optional[tuple] = None) -> int:
        """
        Get count of rows, optionally with filter.
        
        :param table_name: Table name
        :param where_clause: Optional WHERE clause
        :param params: Query parameters
        :return: Row count
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = self.query(query, params)
        return result[0]['count'] if result else 0

    def save_configuration(self, name: str, config_data: dict, description: str = "") -> int:
        """
        Save a sampling configuration (filters and rules).
        
        :param name: Configuration name
        :param config_data: Dictionary containing filters and rules
        :param description: Optional description
        :return: ID of saved configuration
        """
        query = """
                INSERT INTO configurations (name, description, config_json, created_at)
                VALUES (?, ?, ?, ?) \
                """
        return self.insert(query, (
            name,
            description,
            json.dumps(config_data),
            datetime.now().isoformat()
        ))

    def load_configuration(self, config_id: int) -> Optional[Dict]:
        """
        Load a saved configuration.
        
        :param config_id: Configuration ID
        :return: Configuration data or None
        """
        query = "SELECT * FROM configurations WHERE id = ?"
        result = self.query(query, (config_id,))

        if result:
            row = dict(result[0])
            row['config_data'] = json.loads(row['config_json'])
            return row
        return None

    def list_configurations(self) -> List[Dict]:
        """
        List all saved configurations.
        
        :return: List of configuration summaries
        """
        query = """
                SELECT id, name, description, created_at, updated_at
                FROM configurations
                ORDER BY updated_at DESC \
                """
        result = self.query(query)
        return [dict(row) for row in result]

    def save_sampling_results(self, config_id: int, results: List[Dict],
                              summary: Dict) -> int:
        """
        Save sampling results.
        
        :param config_id: Configuration ID used for sampling
        :param results: List of sampled records
        :param summary: Summary information
        :return: Sampling history ID
        """
        # Insert into sampling history
        history_query = """
                        INSERT INTO sampling_history (config_id, sample_count, summary_json, created_at)
                        VALUES (?, ?, ?, ?) \
                        """
        history_id = self.insert(history_query, (
            config_id,
            len(results),
            json.dumps(summary),
            datetime.now().isoformat()
        ))

        # Insert individual results
        if results:
            for result in results:
                result_query = """
                               INSERT INTO sampling_results (history_id, rule_name, data_json)
                               VALUES (?, ?, ?) \
                               """
                # Extract rule name and prepare data
                rule_name = result.get('_rule_name', 'Unknown')
                data = {k: v for k, v in result.items() if k != '_rule_name'}

                self.insert(result_query, (
                    history_id,
                    rule_name,
                    json.dumps(data, default=str)  # Convert dates/objects to strings
                ))

        return history_id

    def get_sampling_history(self, limit: int = 50) -> List[Dict]:
        """
        Get sampling history.
        
        :param limit: Number of recent entries to retrieve
        :return: List of sampling history entries
        """
        query = """
                SELECT sh.*, c.name as config_name
                FROM sampling_history sh
                         LEFT JOIN configurations c ON sh.config_id = c.id
                ORDER BY sh.created_at DESC
                    LIMIT ? \
                """
        result = self.query(query, (limit,))
        return [dict(row) for row in result]

    def get_sampling_results(self, history_id: int) -> List[Dict]:
        """
        Get results for a specific sampling run.
        
        :param history_id: Sampling history ID
        :return: List of sampling results
        """
        query = """
                SELECT * FROM sampling_results
                WHERE history_id = ?
                ORDER BY id \
                """
        result = self.query(query, (history_id,))

        results = []
        for row in result:
            data = json.loads(row['data_json'])
            data['_rule_name'] = row['rule_name']
            results.append(data)

        return results

    def import_csv_data(self, csv_path: str, table_name: str = "financial_data",
                        truncate: bool = False):
        """
        Import data from CSV file into the database.
        
        :param csv_path: Path to CSV file
        :param table_name: Target table name
        :param truncate: Whether to clear existing data first
        """
        try:
            if truncate:
                self.delete(f"DELETE FROM {table_name}")
                log.info(f"Truncated table {table_name}")

            df = pd.read_csv(csv_path, delimiter=';')  # Assuming semicolon delimiter
            df.to_sql(table_name, self._conn, if_exists='append', index=False)
            log.info(f"Imported {len(df)} records from {csv_path} to {table_name}")

        except Exception as e:
            log.error(f"Error importing CSV data: {e}")
            raise

    def get_column_statistics(self, column: str, table_name: str = "financial_data") -> Dict:
        """
        Get statistics for a numeric column.
        
        :param column: Column name
        :param table_name: Table name
        :return: Dictionary with min, max, avg, count
        """
        query = f"""
        SELECT 
            MIN(CAST({column} AS REAL)) as min_val,
            MAX(CAST({column} AS REAL)) as max_val,
            AVG(CAST({column} AS REAL)) as avg_val,
            COUNT(*) as count
        FROM {table_name}
        WHERE {column} IS NOT NULL
        """
        result = self.query(query)
        if result:
            return dict(result[0])
        return {}
