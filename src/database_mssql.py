import os
import sqlite3
import logging
import pandas as pd
from typing import Optional, List, Dict, Any, Union
from dotenv import load_dotenv
from enum import Enum

# SQL Server specific imports
try:
    import pyodbc
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    logging.warning("pyodbc not installed. MS SQL Server support unavailable.")

# Load environment variables
load_dotenv()

# Set up logging
log = logging.getLogger(__name__)


class DatabaseType(Enum):
    """Supported database types"""
    SQLITE = "sqlite"
    MSSQL = "mssql"


class Row:
    """
    Custom row class that provides both index and key-based access to row data.
    Similar to sqlite3.Row but for pyodbc.
    """
    def __init__(self, cursor_description, row_data):
        self._description = cursor_description
        self._data = row_data
        self._keys = [column[0] for column in cursor_description]
    
    def __getitem__(self, key):
        if isinstance(key, int):
            return self._data[key]
        elif isinstance(key, str):
            try:
                index = self._keys.index(key)
                return self._data[index]
            except ValueError:
                raise KeyError(f"No such column: {key}")
        else:
            raise TypeError(f"Invalid key type: {type(key)}")
    
    def __len__(self):
        return len(self._data)
    
    def __iter__(self):
        """Make Row objects work with dict() constructor - iterate over key-value pairs"""
        return iter(self.items())
    
    def keys(self):
        return self._keys
    
    def values(self):
        return list(self._data)
    
    def items(self):
        return zip(self._keys, self._data)
    
    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError):
            return default
    
    def __contains__(self, key):
        return key in self._keys
    
    def __repr__(self):
        items = ', '.join(f"{k}={repr(v)}" for k, v in zip(self._keys, self._data))
        return f"Row({{{items}}})"


class RowFactoryCursor:
    """
    Cursor wrapper that converts pyodbc rows to dictionary-like Row objects.
    """
    def __init__(self, cursor):
        self._cursor = cursor
        self.description = cursor.description
    
    def execute(self, query, params=None):
        if params:
            result = self._cursor.execute(query, params)
        else:
            result = self._cursor.execute(query)
        self.description = self._cursor.description
        return self
    
    def executemany(self, query, params):
        return self._cursor.executemany(query, params)
    
    def executescript(self, script):
        return self._cursor.executescript(script)
    
    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._cursor.description:
            return Row(self._cursor.description, row)
        return row
    
    def fetchall(self):
        rows = self._cursor.fetchall()
        if not rows:
            return []
        if self._cursor.description:
            return [Row(self._cursor.description, row) for row in rows]
        return rows
    
    def fetchmany(self, size=None):
        if size is None:
            rows = self._cursor.fetchmany()
        else:
            rows = self._cursor.fetchmany(size)
        if not rows:
            return []
        if self._cursor.description:
            return [Row(self._cursor.description, row) for row in rows]
        return rows
    
    def __getattr__(self, name):
        # Delegate all other attributes to the underlying cursor
        return getattr(self._cursor, name)
    
    def __iter__(self):
        return self
    
    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Database:
    """
    Database manager supporting both SQLite and MS SQL Server.
    """

    def __init__(self, db_type: Optional[str] = None, connection_params: Optional[Dict] = None):
        """
        Initialize database connection.

        Args:
            db_type: 'sqlite' or 'mssql' (defaults to env var DB_TYPE or 'sqlite')
            connection_params: Connection parameters for MS SQL Server
        """
        log.debug("Initializing database connection...")

        # Determine database type
        self.db_type = DatabaseType(db_type or os.getenv('DB_TYPE', 'sqlite'))

        # Connection objects
        self._conn: Optional[Union[sqlite3.Connection, pyodbc.Connection]] = None
        self.cursor: Optional[Union[sqlite3.Cursor, pyodbc.Cursor]] = None

        # Connection parameters
        self.connection_params = connection_params or {}

        # Connect to database
        self.connect()
        log.info(f"Database initialized ({self.db_type.value}).")

    def connect(self):
        """Establish connection to the database."""
        try:
            if self.db_type == DatabaseType.SQLITE:
                self._connect_sqlite()
            elif self.db_type == DatabaseType.MSSQL:
                self._connect_mssql()
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

        except Exception as e:
            log.error(f"Failed to connect to database: {e}")
            raise

    def _connect_sqlite(self):
        """Connect to SQLite database."""
        db_path = self.connection_params.get('db_path') or os.getenv('SQLITE_DB_PATH', './sampling.db')

        # Ensure directory exists
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # Connect to database
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row  # Enable column access by name
        self.cursor = self._conn.cursor()
        log.info(f"Connected to SQLite database at {db_path}")

        # Create table if it doesn't exist
        self._create_sqlite_tables()

    def _connect_mssql(self):
        """Connect to MS SQL Server database."""
        if not MSSQL_AVAILABLE:
            raise ImportError("pyodbc is required for MS SQL Server support. Install it with: pip install pyodbc")

        # Build connection string based on authentication method
        auth_method = self.connection_params.get('auth_method') or os.getenv('MSSQL_AUTH_METHOD', 'sql')

        if auth_method == 'windows':
            # Windows Authentication
            server = self.connection_params.get('server') or os.getenv('MSSQL_SERVER', 'localhost')
            database = self.connection_params.get('database') or os.getenv('MSSQL_DATABASE', 'SamplingDB')
            driver = self.connection_params.get('driver') or os.getenv('MSSQL_DRIVER', 'ODBC Driver 17 for SQL Server')

            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
            )

        else:  # SQL Authentication
            server = self.connection_params.get('server') or os.getenv('MSSQL_SERVER', 'localhost')
            database = self.connection_params.get('database') or os.getenv('MSSQL_DATABASE', 'SamplingDB')
            username = self.connection_params.get('username') or os.getenv('MSSQL_USERNAME', 'sa')
            password = self.connection_params.get('password') or os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd')
            driver = self.connection_params.get('driver') or os.getenv('MSSQL_DRIVER', 'ODBC Driver 17 for SQL Server')

            connection_string = (
                f"DRIVER={{{driver}}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )

        # Additional connection options
        if self.connection_params.get('encrypt', True):
            connection_string += "Encrypt=yes;"
        if self.connection_params.get('trust_server_certificate', True):
            connection_string += "TrustServerCertificate=yes;"

        # Connect
        self._conn = pyodbc.connect(connection_string)
        # Wrap cursor with our RowFactoryCursor to provide dictionary-like access
        self.cursor = RowFactoryCursor(self._conn.cursor())
        log.info(f"Connected to MS SQL Server: {server}/{database}")

        # Check if tables exist
        self._check_mssql_tables()

    def _create_sqlite_tables(self):
        """Create SQLite tables if they don't exist."""
        try:
            schema_path = os.path.join(os.path.dirname(__file__), 'schema.sql')
            if os.path.exists(schema_path):
                with open(schema_path, 'r') as f:
                    schema = f.read()
                    # SQLite adaptations
                    schema = schema.replace('UNIQUEIDENTIFIER', 'TEXT')
                    schema = schema.replace('NVARCHAR', 'VARCHAR')
                    self.cursor.executescript(schema)
                    self._conn.commit()
                    log.info("SQLite schema created/updated")
            else:
                # Create minimal tables
                self._create_minimal_tables_sqlite()
        except Exception as e:
            log.error(f"Error creating SQLite tables: {e}")
            raise

    def _create_minimal_tables_sqlite(self):
        """Create minimal SQLite tables."""
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS kundenstamm (
                                                                       pk TEXT PRIMARY KEY,
                                                                       banknummer VARCHAR(20),
                                                                       kundennummer VARCHAR(20),
                                                                       stichtag DATE
                            )
                            """)
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS softfact_vw (
                                                                       pk TEXT PRIMARY KEY,
                                                                       banknummer VARCHAR(20),
                                                                       kundennummer VARCHAR(20),
                                                                       stichtag DATE
                            )
                            """)
        self.cursor.execute("""
                            CREATE TABLE IF NOT EXISTS kontodaten_vw (
                                                                         pk TEXT PRIMARY KEY,
                                                                         banknummer VARCHAR(20),
                                                                         personennummer_pseudonym BIGINT,
                                                                         stichtag DATE
                            )
                            """)
        self._conn.commit()
        log.debug("Created minimal SQLite table structure")

    def _check_mssql_tables(self):
        """Check if required tables exist in MS SQL Server."""
        required_tables = ['kundenstamm', 'softfact_vw', 'kontodaten_vw']

        query = """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW' \
                """

        self.cursor.execute(query)
        existing_tables = [row[0].lower() for row in self.cursor.fetchall()]

        missing_tables = [t for t in required_tables if t not in existing_tables]

        if missing_tables:
            log.warning(f"Missing tables/views in MS SQL Server: {missing_tables}")
            log.info("For development, you may want to create these tables using the init script.")
        else:
            log.info("All required tables/views found in MS SQL Server")

    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            log.debug("Database connection closed.")

    def get_table_columns(self, table_name: str = "kundenstamm") -> List[str]:
        """Get column names for a table."""
        if self.db_type == DatabaseType.SQLITE:
            query = f"PRAGMA table_info({table_name})"
            result = self.cursor.execute(query).fetchall()
            if hasattr(result[0], 'keys'):  # Row factory enabled
                columns = [row['name'] for row in result if row['name'] not in ['index']]
            else:
                columns = [row[1] for row in result if row[1] not in ['index']]
        else:  # MS SQL Server
            query = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                    ORDER BY ORDINAL_POSITION \
                    """
            result = self.cursor.execute(query, table_name).fetchall()
            columns = [row[0] for row in result]

        log.debug(f"Table columns for {table_name}: {columns}")
        return columns

    def get_column_info(self, table_name: str = "kundenstamm") -> Dict[str, str]:
        """Get column information including data types."""
        if self.db_type == DatabaseType.SQLITE:
            query = f"PRAGMA table_info({table_name})"
            result = self.cursor.execute(query).fetchall()
            if hasattr(result[0], 'keys'):
                return {row['name']: row['type'] for row in result if row['name'] not in ['index']}
            else:
                return {row[1]: row[2] for row in result if row[1] not in ['index']}
        else:  # MS SQL Server
            query = """
                    SELECT COLUMN_NAME, DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ? \
                    """
            result = self.cursor.execute(query, table_name).fetchall()
            return {row[0]: row[1] for row in result}

    def get_all_data(self, table_name: str = "kundenstamm", limit: Optional[int] = None) -> List[Dict]:
        """Retrieve all data from the table."""
        if self.db_type == DatabaseType.SQLITE:
            query = f"SELECT * FROM {table_name}"
            if limit:
                query += f" LIMIT {limit}"
        else:  # MS SQL Server
            if limit:
                query = f"SELECT TOP {limit} * FROM {table_name}"
            else:
                query = f"SELECT * FROM {table_name}"

        result = self.cursor.execute(query).fetchall()

        # Convert to list of dicts
        if hasattr(result[0] if result else None, 'keys'):
            # Row objects (both SQLite with row_factory and our custom Row class)
            return [dict(row) for row in result]
        else:
            # Plain tuples (shouldn't happen with our wrapper, but kept for safety)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in result]

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

        # Convert to list of dicts
        if hasattr(result[0] if result else None, 'keys'):
            # Row objects (both SQLite with row_factory and our custom Row class)
            return [dict(row) for row in result]
        else:
            # Plain tuples (shouldn't happen with our wrapper, but kept for safety)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in result]

    def get_row_count(self, table_name: str = "kundenstamm") -> int:
        """Get count of rows."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.cursor.execute(query).fetchone()

        if hasattr(result, 'keys'):
            # Row objects (both SQLite with row_factory and our custom Row class)
            return result['count'] if result else 0
        else:
            # Plain tuple (shouldn't happen with our wrapper, but kept for safety)
            return result[0] if result else 0

    def import_csv_data(self, csv_path: str, table_name: str = "kundenstamm",
                        delimiter: str = ';', truncate: bool = False):
        """Import data from CSV file into the database."""
        try:
            # Read CSV with pandas
            df = pd.read_csv(csv_path, delimiter=delimiter)
            log.info(f"Read {len(df)} rows from CSV file")

            # Clean column names
            df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]

            # Handle European number format
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        # Convert European format
                        converted = df[col].str.replace('.', '').str.replace(',', '.')
                        df[col] = pd.to_numeric(converted, errors='ignore')
                    except:
                        pass

            # Import to database
            if self.db_type == DatabaseType.SQLITE:
                # Drop table if truncate
                if truncate:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                    self._conn.commit()

                # Use pandas to_sql
                df.to_sql(table_name, self._conn, if_exists='replace' if truncate else 'append', index=False)
            else:  # MS SQL Server
                # Use bulk insert or iterate
                if truncate:
                    self.cursor.execute(f"TRUNCATE TABLE {table_name}")
                    self._conn.commit()

                # Insert data
                # For better performance with large datasets, consider using bulk insert
                for _, row in df.iterrows():
                    placeholders = ','.join(['?' for _ in range(len(row))])
                    query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    self.cursor.execute(query, tuple(row))

                self._conn.commit()

            log.info(f"Successfully imported {len(df)} records to {table_name}")

        except Exception as e:
            log.error(f"Error importing CSV data: {e}")
            raise

    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database."""
        if self.db_type == DatabaseType.SQLITE:
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        else:  # MS SQL Server
            query = """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW'
                    ORDER BY TABLE_NAME \
                    """

        result = self.cursor.execute(query).fetchall()

        if hasattr(result[0] if result else None, 'keys'):
            # Row objects
            if self.db_type == DatabaseType.SQLITE:
                return [row['name'] for row in result]
            else:
                return [row['TABLE_NAME'] for row in result]
        else:
            # Plain tuples
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
        # Build query
        if self.db_type == DatabaseType.MSSQL and limit:
            query = f"SELECT TOP {limit} * FROM {base_table}"
        else:
            query = f"SELECT * FROM {base_table}"

        # Add joins
        if join_tables and join_conditions:
            for table in join_tables:
                if table in join_conditions:
                    query += f" LEFT JOIN {table} ON {join_conditions[table]}"

        # Add where clause
        if where_clause:
            query += f" WHERE {where_clause}"

        # Add limit for SQLite
        if self.db_type == DatabaseType.SQLITE and limit:
            query += f" LIMIT {limit}"

        # Execute query
        if params:
            result = self.cursor.execute(query, params).fetchall()
        else:
            result = self.cursor.execute(query).fetchall()

        # Convert to list of dicts
        if hasattr(result[0] if result else None, 'keys'):
            # Row objects (both SQLite with row_factory and our custom Row class)
            return [dict(row) for row in result]
        else:
            # Plain tuples (shouldn't happen with our wrapper, but kept for safety)
            columns = [desc[0] for desc in self.cursor.description]
            return [dict(zip(columns, row)) for row in result]

    def get_table_relationships(self) -> Dict[str, Dict[str, str]]:
        """Get common join relationships between tables."""
        # These are the standard relationships - same for both databases
        return {
            'kundenstamm': {
                'softfact_vw': 'kundenstamm.kundennummer = softfact_vw.kundennummer AND kundenstamm.banknummer = softfact_vw.banknummer',
                'kontodaten_vw': 'kundenstamm.personennummer_pseudonym = kontodaten_vw.personennummer_pseudonym AND kundenstamm.banknummer = kontodaten_vw.banknummer'
            },
            'softfact_vw': {
                'kontodaten_vw': 'softfact_vw.personennummer_pseudonym = kontodaten_vw.personennummer_pseudonym'
            }
        }

    @classmethod
    def get_instance(cls, db_type: Optional[str] = None, connection_params: Optional[Dict] = None):
        """Factory method to get database instance."""
        return cls(db_type, connection_params)