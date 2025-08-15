#!/usr/bin/env python3
"""
Windows Authentication Test Script for Production Data Warehouse
=================================================================
This is a standalone script to test Windows authentication against
production MS SQL Server databases with encrypted connections.

Requirements:
- pyodbc (pip install pyodbc)
- Windows credentials with access to the DWH
- ODBC Driver for SQL Server installed

Usage:
    python test_windows_auth_production.py

Author: Claude
Date: 2025-08-15
"""

import sys
import os
import getpass
import socket
from datetime import datetime

# Check for pyodbc
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    print("ERROR: pyodbc is not installed.")
    print("Please install it with: pip install pyodbc")
    sys.exit(1)


def print_header(text, char="="):
    """Print a formatted header."""
    width = 70
    print(f"\n{char * width}")
    print(f"{text:^{width}}")
    print(f"{char * width}")


def print_section(text):
    """Print a section header."""
    print(f"\n{'─' * 50}")
    print(f"► {text}")
    print(f"{'─' * 50}")


def get_available_drivers():
    """Get list of available ODBC drivers."""
    drivers = pyodbc.drivers()
    sql_drivers = [d for d in drivers if 'SQL' in d.upper()]
    return sql_drivers


def select_driver():
    """Let user select an ODBC driver."""
    drivers = get_available_drivers()
    
    if not drivers:
        print("❌ No SQL Server ODBC drivers found!")
        print("Please install an ODBC driver for SQL Server.")
        return None
    
    print("\nAvailable SQL Server ODBC Drivers:")
    for i, driver in enumerate(drivers, 1):
        recommended = ""
        if "18" in driver:
            recommended = " (Latest - Mandatory encryption)"
        elif "17" in driver:
            recommended = " (Recommended - Good compatibility)"
        print(f"  {i}. {driver}{recommended}")
    
    # Default to Driver 17 if available, otherwise the first one
    default = next((d for d in drivers if "17" in d), drivers[0])
    default_idx = drivers.index(default) + 1
    
    while True:
        choice = input(f"\nSelect driver [1-{len(drivers)}] (default: {default_idx}): ").strip()
        
        if not choice:
            return default
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(drivers):
                return drivers[idx]
            else:
                print(f"Please enter a number between 1 and {len(drivers)}")
        except ValueError:
            print("Please enter a valid number")


def get_connection_params():
    """Interactively get connection parameters from user."""
    print_section("Connection Configuration")
    
    # Server
    print("\nEnter the DWH server address:")
    print("Examples:")
    print("  - dwh-server.company.com")
    print("  - 10.0.0.100")
    print("  - server.domain.local,1433")
    
    server = input("Server: ").strip()
    if not server:
        print("❌ Server is required!")
        return None
    
    # Database
    print("\nEnter the database name:")
    print("(Press Enter for default: master)")
    
    database = input("Database: ").strip() or "master"
    
    # Driver
    print("\nSelect ODBC Driver:")
    driver = select_driver()
    if not driver:
        return None
    
    # Encryption settings
    print("\nEncryption Settings:")
    print("1. Enable encryption with certificate validation (Production)")
    print("2. Enable encryption, trust server certificate (Testing)")
    print("3. Disable encryption (Not recommended)")
    
    enc_choice = input("Select [1-3] (default: 2): ").strip() or "2"
    
    if enc_choice == "1":
        encrypt = True
        trust_cert = False
    elif enc_choice == "2":
        encrypt = True
        trust_cert = True
    else:
        encrypt = False
        trust_cert = True
        print("⚠️  WARNING: Encryption disabled - connection will not be secure!")
    
    # Timeout
    print("\nConnection timeout in seconds (default: 30):")
    timeout = input("Timeout: ").strip() or "30"
    
    return {
        'server': server,
        'database': database,
        'driver': driver,
        'encrypt': encrypt,
        'trust_cert': trust_cert,
        'timeout': timeout
    }


def build_connection_string(params):
    """Build the connection string for Windows authentication."""
    conn_str = (
        f"DRIVER={{{params['driver']}}};"
        f"SERVER={params['server']};"
        f"DATABASE={params['database']};"
        f"Trusted_Connection=yes;"  # Windows Authentication
    )
    
    # Add encryption settings
    if params['encrypt']:
        conn_str += "Encrypt=yes;"
    else:
        conn_str += "Encrypt=no;"
    
    if params['trust_cert']:
        conn_str += "TrustServerCertificate=yes;"
    else:
        conn_str += "TrustServerCertificate=no;"
    
    # Add timeout
    conn_str += f"Connection Timeout={params['timeout']};"
    
    return conn_str


def test_connection(conn_str, params):
    """Test the database connection."""
    print_section("Testing Connection")
    
    print(f"Server:     {params['server']}")
    print(f"Database:   {params['database']}")
    print(f"Driver:     {params['driver']}")
    print(f"Encryption: {'Enabled' if params['encrypt'] else 'Disabled'}")
    print(f"Trust Cert: {'Yes' if params['trust_cert'] else 'No'}")
    print(f"Auth:       Windows (Integrated)")
    print(f"User:       {getpass.getuser()}@{socket.gethostname()}")
    
    print("\nConnecting...")
    
    try:
        # Establish connection
        conn = pyodbc.connect(conn_str)
        print("✅ Connection successful!")
        
        cursor = conn.cursor()
        
        # Test 1: Check connection security
        print_section("Security Status")
        
        security_query = """
        SELECT 
            session_id,
            auth_scheme,
            encrypt_option,
            protocol_type,
            client_net_address,
            local_net_address
        FROM sys.dm_exec_connections 
        WHERE session_id = @@SPID
        """
        
        try:
            cursor.execute(security_query)
            row = cursor.fetchone()
            
            if row:
                print(f"Session ID:  {row[0]}")
                print(f"Auth Scheme: {row[1]}")
                print(f"Encrypted:   {row[2]}")
                print(f"Protocol:    {row[3]}")
                print(f"Client IP:   {row[4]}")
                print(f"Server IP:   {row[5]}")
                
                if row[2] == 'TRUE':
                    print("\n🔒 Connection is ENCRYPTED")
                else:
                    print("\n⚠️  Connection is NOT encrypted")
                
                if row[1] in ['KERBEROS', 'NTLM']:
                    print(f"✅ Windows Authentication confirmed ({row[1]})")
                else:
                    print(f"⚠️  Unexpected auth scheme: {row[1]}")
                    
        except Exception as e:
            print(f"Could not query security status: {e}")
        
        # Test 2: Simple query
        print_section("Query Test")
        
        test_query = """
        SELECT 
            @@SERVERNAME as server_name,
            @@VERSION as version,
            DB_NAME() as database_name,
            SYSTEM_USER as login_name,
            USER_NAME() as user_name,
            GETDATE() as current_time
        """
        
        print("Executing test query...")
        cursor.execute(test_query)
        row = cursor.fetchone()
        
        if row:
            print(f"Server Name: {row[0]}")
            print(f"Version:     {str(row[1]).split(chr(10))[0][:60]}...")
            print(f"Database:    {row[2]}")
            print(f"Login:       {row[3]}")
            print(f"DB User:     {row[4]}")
            print(f"Server Time: {row[5]}")
            print("\n✅ Query execution successful!")
        
        # Test 3: List tables (optional)
        print_section("Database Objects")
        
        choice = input("\nList available tables/views? (y/N): ").strip().lower()
        
        if choice == 'y':
            tables_query = """
            SELECT TOP 20
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME
            """
            
            cursor.execute(tables_query)
            rows = cursor.fetchall()
            
            if rows:
                print("\nTop 20 Tables/Views:")
                print(f"{'Schema':<15} {'Name':<30} {'Type':<10}")
                print("-" * 60)
                for row in rows:
                    schema = row[0] or 'dbo'
                    name = row[1][:30]
                    ttype = 'TABLE' if row[2] == 'BASE TABLE' else 'VIEW'
                    print(f"{schema:<15} {name:<30} {ttype:<10}")
                
                # Count total
                count_query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES"
                cursor.execute(count_query)
                total = cursor.fetchone()[0]
                print(f"\nTotal objects: {total}")
            else:
                print("No tables/views found (check permissions)")
        
        # Test 4: Custom query (optional)
        print_section("Custom Query (Optional)")
        
        print("Enter a custom SQL query to test (or press Enter to skip):")
        print("Example: SELECT COUNT(*) FROM your_table")
        custom_query = input("SQL> ").strip()
        
        if custom_query:
            try:
                cursor.execute(custom_query)
                
                # Check if it's a SELECT query
                if custom_query.upper().strip().startswith('SELECT'):
                    rows = cursor.fetchall()
                    if rows:
                        # Print column names
                        columns = [column[0] for column in cursor.description]
                        print("\nResults:")
                        print(" | ".join(columns))
                        print("-" * 60)
                        
                        # Print first 10 rows
                        for i, row in enumerate(rows[:10]):
                            print(" | ".join(str(val) for val in row))
                            if i == 9 and len(rows) > 10:
                                print(f"... ({len(rows) - 10} more rows)")
                    else:
                        print("Query returned no results")
                else:
                    print(f"Query executed successfully. Rows affected: {cursor.rowcount}")
                    
            except Exception as e:
                print(f"❌ Query failed: {e}")
        
        # Close connection
        conn.close()
        
        return True
        
    except pyodbc.Error as e:
        print(f"\n❌ Connection failed!")
        print(f"Error: {e}")
        
        # Provide helpful error messages
        error_str = str(e).lower()
        
        if "login failed" in error_str:
            print("\n📝 Troubleshooting:")
            print("1. Verify your Windows account has access to the SQL Server")
            print("2. Check if the server name is correct")
            print("3. Ensure the database name exists")
            print("4. Verify network connectivity to the server")
            
        elif "ssl" in error_str or "certificate" in error_str or "encrypt" in error_str:
            print("\n📝 Troubleshooting:")
            print("1. Try enabling 'Trust Server Certificate' option")
            print("2. Check if the server has a valid SSL certificate")
            print("3. Try using a different ODBC driver version")
            print("4. Contact your DBA about SSL/TLS configuration")
            
        elif "network" in error_str or "timeout" in error_str:
            print("\n📝 Troubleshooting:")
            print("1. Check network connectivity to the server")
            print("2. Verify the server address and port")
            print("3. Check firewall rules (port 1433)")
            print("4. Try increasing the timeout value")
            
        elif "driver" in error_str:
            print("\n📝 Troubleshooting:")
            print("1. Install the Microsoft ODBC Driver for SQL Server")
            print("2. Download from: https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server")
            
        return False


def main():
    """Main function."""
    print_header("Windows Authentication Test for Production DWH")
    
    print("\nThis script will test Windows authentication against")
    print("your production SQL Server with encrypted connections.")
    print("\nPrerequisites:")
    print("  ✓ Windows account with SQL Server access")
    print("  ✓ ODBC Driver for SQL Server installed")
    print("  ✓ Network access to the DWH server")
    
    # Get connection parameters
    params = get_connection_params()
    
    if not params:
        print("\n❌ Configuration cancelled")
        return 1
    
    # Build connection string
    conn_str = build_connection_string(params)
    
    # Test connection
    success = test_connection(conn_str, params)
    
    # Summary
    print_header("Test Summary", "=")
    
    if success:
        print("\n✅ SUCCESS: Windows authentication is working!")
        print("\nYour configuration:")
        print(f"  Server:   {params['server']}")
        print(f"  Database: {params['database']}")
        print(f"  Driver:   {params['driver']}")
        
        if params['encrypt']:
            print("\n🔒 Connection is encrypted")
        
        print("\nYou can now use these settings in your application.")
        
        # Show example .env configuration
        print("\nExample .env configuration:")
        print("─" * 50)
        print(f"MSSQL_SERVER={params['server']}")
        print(f"MSSQL_DATABASE={params['database']}")
        print(f"MSSQL_AUTH_METHOD=windows")
        print(f"MSSQL_DRIVER={params['driver']}")
        print(f"MSSQL_ENCRYPT={'true' if params['encrypt'] else 'false'}")
        print(f"MSSQL_TRUST_CERT={'true' if params['trust_cert'] else 'false'}")
        print("─" * 50)
        
        return 0
    else:
        print("\n❌ FAILED: Could not connect with Windows authentication")
        print("\nPlease check the error messages above and try again.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)