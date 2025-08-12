"""
Test script to verify database connection
Tests both SQLite and MS SQL Server connections
"""

import os
import sys
from dotenv import load_dotenv

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.database_mssql import Database, DatabaseType

# Load environment variables
load_dotenv()


def test_sqlite():
    """Test SQLite connection."""
    print("\n" + "="*50)
    print("Testing SQLite Connection...")
    print("="*50)

    try:
        db = Database(db_type='sqlite')

        # Test basic operations
        tables = db.get_all_tables()
        print(f"✅ Connected to SQLite")
        print(f"   Tables found: {tables}")

        # Test data retrieval
        if 'kundenstamm' in tables:
            count = db.get_row_count('kundenstamm')
            print(f"   Records in kundenstamm: {count}")

            if count > 0:
                sample = db.get_sample_data('kundenstamm', 1)
                print(f"   Sample record retrieved: {len(sample)} row(s)")

        db.close()
        return True

    except Exception as e:
        print(f"❌ SQLite connection failed: {e}")
        return False


def test_mssql_docker():
    """Test MS SQL Server connection (Docker/Development)."""
    print("\n" + "="*50)
    print("Testing MS SQL Server Connection (Docker)...")
    print("="*50)

    try:
        # Use development credentials
        connection_params = {
            'server': os.getenv('MSSQL_SERVER', 'localhost'),
            'database': os.getenv('MSSQL_DATABASE', 'SamplingDB'),
            'username': os.getenv('MSSQL_USERNAME', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd'),
            'auth_method': 'sql'
        }

        db = Database(db_type='mssql', connection_params=connection_params)

        # Test basic operations
        tables = db.get_all_tables()
        print(f"✅ Connected to MS SQL Server")
        print(f"   Server: {connection_params['server']}")
        print(f"   Database: {connection_params['database']}")
        print(f"   Tables found: {tables}")

        # Test data retrieval
        production_tables = db.get_production_tables()
        for table in production_tables:
            if table in tables:
                count = db.get_row_count(table)
                print(f"   Records in {table}: {count}")

        db.close()
        return True

    except Exception as e:
        print(f"❌ MS SQL Server connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_mssql_windows_auth():
    """Test MS SQL Server connection with Windows Authentication."""
    print("\n" + "="*50)
    print("Testing MS SQL Server Connection (Windows Auth)...")
    print("="*50)

    # Get server from user input
    server = input("Enter your DWH server name (or press Enter to skip): ").strip()

    if not server:
        print("⏭️  Skipping Windows Authentication test")
        return None

    database = input("Enter database name (default: DataWarehouse): ").strip() or "DataWarehouse"

    try:
        connection_params = {
            'server': server,
            'database': database,
            'auth_method': 'windows'
        }

        db = Database(db_type='mssql', connection_params=connection_params)

        # Test basic operations
        tables = db.get_all_tables()
        print(f"✅ Connected to MS SQL Server with Windows Authentication")
        print(f"   Server: {server}")
        print(f"   Database: {database}")
        print(f"   Tables found: {len(tables)} tables")

        # Show first few tables
        if tables:
            print(f"   Sample tables: {tables[:5]}")

        db.close()
        return True

    except Exception as e:
        print(f"❌ Windows Authentication connection failed: {e}")
        return False


def check_drivers():
    """Check available ODBC drivers."""
    print("\n" + "="*50)
    print("Checking ODBC Drivers...")
    print("="*50)

    try:
        import pyodbc
        drivers = pyodbc.drivers()

        if drivers:
            print("Available ODBC drivers:")
            for driver in drivers:
                if 'SQL' in driver.upper():
                    print(f"  ✅ {driver}")
                else:
                    print(f"  - {driver}")
        else:
            print("❌ No ODBC drivers found")

        # Check for recommended drivers
        recommended = ['ODBC Driver 17 for SQL Server', 'ODBC Driver 18 for SQL Server']
        for rec_driver in recommended:
            if rec_driver in drivers:
                print(f"\n✅ Recommended driver '{rec_driver}' is installed")
                break
        else:
            print("\n⚠️  No recommended SQL Server drivers found.")
            print("   Install ODBC Driver 17 for SQL Server from:")
            print("   https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")

    except ImportError:
        print("❌ pyodbc is not installed. Run: pip install pyodbc")


def main():
    """Run all connection tests."""
    print("\n🔧 DATABASE CONNECTION TEST SUITE")

    # Check drivers first
    check_drivers()

    # Test SQLite
    sqlite_ok = test_sqlite()

    # Test MS SQL Server (Docker)
    mssql_docker_ok = test_mssql_docker()

    # Test MS SQL Server (Windows Auth) - optional
    mssql_windows = test_mssql_windows_auth()

    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    print(f"SQLite:              {'✅ PASS' if sqlite_ok else '❌ FAIL'}")
    print(f"MS SQL (Docker):     {'✅ PASS' if mssql_docker_ok else '❌ FAIL'}")
    if mssql_windows is not None:
        print(f"MS SQL (Windows):    {'✅ PASS' if mssql_windows else '❌ FAIL'}")

    # Recommendations
    print("\n📝 RECOMMENDATIONS:")
    if not mssql_docker_ok:
        print("1. Start Docker container: docker-compose up -d")
        print("2. Initialize database: python init_mssql_db.py")

    if mssql_windows == False:
        print("1. Check your DWH server name and network connection")
        print("2. Ensure you have appropriate read permissions")
        print("3. Verify ODBC Driver is installed")


if __name__ == "__main__":
    main()
