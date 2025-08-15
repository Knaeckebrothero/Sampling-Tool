"""
Test script to verify encrypted database connections
Tests various encryption configurations for MS SQL Server
"""

import os
import sys
import time
from dotenv import load_dotenv

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.database_mssql import Database, DatabaseType

# Load environment variables
load_dotenv()


def print_header(text):
    """Print a formatted header."""
    print("\n" + "="*60)
    print(f" {text}")
    print("="*60)


def test_unencrypted_connection():
    """Test connection without encryption."""
    print_header("Testing UNENCRYPTED Connection")
    
    try:
        connection_params = {
            'server': os.getenv('MSSQL_SERVER', 'localhost'),
            'database': os.getenv('MSSQL_DATABASE', 'SamplingDB'),
            'username': os.getenv('MSSQL_USERNAME', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd'),
            'auth_method': 'sql',
            'encrypt': False,  # Explicitly disable encryption
            'trust_server_certificate': True
        }
        
        print("Connecting with encryption DISABLED...")
        db = Database(db_type='mssql', connection_params=connection_params)
        
        # Get connection info
        info = db.get_connection_info()
        print(f"✅ Connected successfully")
        print(f"   Encrypted: {'Yes' if info.get('encrypted') else 'No'}")
        
        if not info.get('encrypted'):
            print("⚠️  WARNING: Connection is NOT encrypted!")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_encrypted_self_signed():
    """Test encrypted connection with self-signed certificate."""
    print_header("Testing ENCRYPTED Connection (Self-Signed Cert)")
    
    try:
        connection_params = {
            'server': os.getenv('MSSQL_SERVER', 'localhost'),
            'database': os.getenv('MSSQL_DATABASE', 'SamplingDB'),
            'username': os.getenv('MSSQL_USERNAME', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd'),
            'auth_method': 'sql',
            'encrypt': True,  # Enable encryption
            'trust_server_certificate': True  # Trust self-signed cert
        }
        
        print("Connecting with encryption ENABLED (trusting server cert)...")
        db = Database(db_type='mssql', connection_params=connection_params)
        
        # Get connection info
        info = db.get_connection_info()
        print(f"✅ Connected successfully")
        print(f"   Encrypted: {'Yes' if info.get('encrypted') else 'No'}")
        
        if info.get('encrypted'):
            print("🔒 Connection is ENCRYPTED (self-signed cert)")
        
        # Test a simple query
        result = db.test_connection()
        print(f"   Query test: {'Passed' if result else 'Failed'}")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def test_encrypted_cert_validation():
    """Test encrypted connection with certificate validation."""
    print_header("Testing ENCRYPTED Connection (Cert Validation)")
    
    try:
        connection_params = {
            'server': os.getenv('MSSQL_SERVER', 'localhost'),
            'database': os.getenv('MSSQL_DATABASE', 'SamplingDB'),
            'username': os.getenv('MSSQL_USERNAME', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd'),
            'auth_method': 'sql',
            'encrypt': True,  # Enable encryption
            'trust_server_certificate': False  # Validate certificate
        }
        
        print("Connecting with encryption ENABLED (validating cert)...")
        print("Note: This will likely fail with self-signed certificates")
        
        db = Database(db_type='mssql', connection_params=connection_params)
        
        # Get connection info
        info = db.get_connection_info()
        print(f"✅ Connected successfully")
        print(f"   Encrypted: {'Yes' if info.get('encrypted') else 'No'}")
        print("🔒 Connection is ENCRYPTED with valid certificate!")
        
        db.close()
        return True
        
    except Exception as e:
        if "certificate" in str(e).lower() or "ssl" in str(e).lower():
            print(f"⚠️  Expected failure: {e}")
            print("   This is normal for self-signed certificates")
            print("   For production, use a valid certificate from a trusted CA")
        else:
            print(f"❌ Connection failed: {e}")
        return False


def test_windows_auth_encrypted():
    """Test Windows authentication with encryption."""
    print_header("Testing Windows Auth with Encryption")
    
    server = input("Enter your DWH server (or press Enter to skip): ").strip()
    
    if not server:
        print("⏭️  Skipping Windows Auth test")
        return None
    
    database = input("Enter database name (default: DataWarehouse): ").strip() or "DataWarehouse"
    
    try:
        connection_params = {
            'server': server,
            'database': database,
            'auth_method': 'windows',
            'encrypt': True,  # Enable encryption
            'trust_server_certificate': False  # Validate certificate (production)
        }
        
        print("Connecting with Windows Auth + Encryption...")
        db = Database(db_type='mssql', connection_params=connection_params)
        
        # Get connection info
        info = db.get_connection_info()
        print(f"✅ Connected successfully")
        print(f"   Server: {server}")
        print(f"   Database: {database}")
        print(f"   Encrypted: {'Yes' if info.get('encrypted') else 'No'}")
        
        if info.get('encrypted'):
            print("🔒 Connection is ENCRYPTED with Windows Authentication!")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


def performance_comparison():
    """Compare performance of encrypted vs unencrypted connections."""
    print_header("Performance Comparison")
    
    connection_params_base = {
        'server': os.getenv('MSSQL_SERVER', 'localhost'),
        'database': os.getenv('MSSQL_DATABASE', 'SamplingDB'),
        'username': os.getenv('MSSQL_USERNAME', 'sa'),
        'password': os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd'),
        'auth_method': 'sql',
        'trust_server_certificate': True
    }
    
    # Test unencrypted
    print("\nTesting unencrypted performance...")
    try:
        params = connection_params_base.copy()
        params['encrypt'] = False
        
        start = time.time()
        db = Database(db_type='mssql', connection_params=params)
        
        # Run test queries
        for _ in range(10):
            db.cursor.execute("SELECT 1").fetchone()
        
        unencrypted_time = time.time() - start
        db.close()
        
        print(f"   Unencrypted: {unencrypted_time:.3f} seconds")
    except Exception as e:
        print(f"   Failed: {e}")
        unencrypted_time = None
    
    # Test encrypted
    print("\nTesting encrypted performance...")
    try:
        params = connection_params_base.copy()
        params['encrypt'] = True
        
        start = time.time()
        db = Database(db_type='mssql', connection_params=params)
        
        # Run test queries
        for _ in range(10):
            db.cursor.execute("SELECT 1").fetchone()
        
        encrypted_time = time.time() - start
        db.close()
        
        print(f"   Encrypted: {encrypted_time:.3f} seconds")
    except Exception as e:
        print(f"   Failed: {e}")
        encrypted_time = None
    
    # Compare
    if unencrypted_time and encrypted_time:
        overhead = ((encrypted_time - unencrypted_time) / unencrypted_time) * 100
        print(f"\n📊 Encryption overhead: {overhead:.1f}%")
        print("   Note: Overhead is typically minimal for modern hardware")


def check_driver_support():
    """Check ODBC driver encryption support."""
    print_header("ODBC Driver Encryption Support")
    
    try:
        import pyodbc
        drivers = pyodbc.drivers()
        
        sql_drivers = [d for d in drivers if 'SQL' in d.upper()]
        
        if not sql_drivers:
            print("❌ No SQL Server ODBC drivers found")
            return
        
        print("Available SQL Server drivers:")
        for driver in sql_drivers:
            print(f"  • {driver}")
            
            # Check version-specific features
            if "18" in driver:
                print("    ✅ ODBC Driver 18: Mandatory encryption by default")
                print("    ℹ️  Requires explicit Encrypt=no to disable")
            elif "17" in driver:
                print("    ✅ ODBC Driver 17: Optional encryption")
                print("    ℹ️  Good balance of compatibility and security")
            elif "13" in driver or "11" in driver:
                print("    ⚠️  Older driver: Limited TLS support")
                print("    ℹ️  Consider upgrading for better security")
            
    except ImportError:
        print("❌ pyodbc not installed")


def main():
    """Run all encryption tests."""
    print("\n🔐 MS SQL SERVER ENCRYPTION TEST SUITE")
    print("Testing various encryption configurations...")
    
    # Check drivers
    check_driver_support()
    
    # Run tests
    results = {}
    
    # Test 1: Unencrypted
    results['unencrypted'] = test_unencrypted_connection()
    
    # Test 2: Encrypted with self-signed
    results['encrypted_self_signed'] = test_encrypted_self_signed()
    
    # Test 3: Encrypted with cert validation
    results['encrypted_validated'] = test_encrypted_cert_validation()
    
    # Test 4: Performance comparison
    performance_comparison()
    
    # Test 5: Windows Auth (optional)
    windows_result = test_windows_auth_encrypted()
    if windows_result is not None:
        results['windows_encrypted'] = windows_result
    
    # Summary
    print_header("TEST SUMMARY")
    
    print("\nConnection Tests:")
    print(f"  Unencrypted:           {'✅ PASS' if results.get('unencrypted') else '❌ FAIL'}")
    print(f"  Encrypted (Self-Sign): {'✅ PASS' if results.get('encrypted_self_signed') else '❌ FAIL'}")
    print(f"  Encrypted (Validated): {'⚠️  EXPECTED FAIL' if not results.get('encrypted_validated') else '✅ PASS'}")
    
    if 'windows_encrypted' in results:
        print(f"  Windows + Encrypted:   {'✅ PASS' if results['windows_encrypted'] else '❌ FAIL'}")
    
    print("\n📝 RECOMMENDATIONS:")
    print("1. For Development: Use encrypt=true, trust_server_certificate=true")
    print("2. For Production: Use encrypt=true, trust_server_certificate=false")
    print("3. For DWH: Use Windows auth + encryption with valid certificates")
    print("4. Always use ODBC Driver 17 or 18 for best security")
    
    print("\n🔒 Security Best Practices:")
    print("• Never disable encryption in production")
    print("• Use valid certificates from trusted CAs in production")
    print("• Regularly update ODBC drivers for security patches")
    print("• Monitor connection logs for security anomalies")


if __name__ == "__main__":
    main()