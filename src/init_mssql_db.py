"""
MS SQL Server Database Initialization Script
Creates the database and tables for the sampling tool in MS SQL Server
"""

import os
import sys
import pyodbc
import logging
import pandas as pd
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def create_database_if_not_exists(connection, db_name):
    """Create database if it doesn't exist."""
    try:
        cursor = connection.cursor()
        # Set autocommit to True for CREATE DATABASE
        connection.autocommit = True
        
        # Check if database exists
        cursor.execute(f"""
            IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{db_name}')
            BEGIN
                CREATE DATABASE [{db_name}]
            END
        """)
        
        # Reset autocommit
        connection.autocommit = False
        log.info(f"Database '{db_name}' ready")
    except Exception as e:
        log.error(f"Error creating database: {e}")
        raise


def create_tables(connection):
    """Create the production tables/views in MS SQL Server."""
    cursor = connection.cursor()
    
    # Drop existing tables if they exist (for development)
    drop_statements = """
        IF OBJECT_ID('dbo.kontodaten_vw', 'U') IS NOT NULL DROP TABLE dbo.kontodaten_vw;
        IF OBJECT_ID('dbo.softfact_vw', 'U') IS NOT NULL DROP TABLE dbo.softfact_vw;
        IF OBJECT_ID('dbo.kundenstamm', 'U') IS NOT NULL DROP TABLE dbo.kundenstamm;
    """

    for statement in drop_statements.split(';'):
        if statement.strip():
            cursor.execute(statement)
    connection.commit()

    # Create kundenstamm table
    cursor.execute("""
                   CREATE TABLE dbo.kundenstamm (
                                                    pk NVARCHAR(152) PRIMARY KEY,
                                                    banknummer VARCHAR(20),
                                                    kundennummer VARCHAR(20),
                                                    stichtag DATE,
                                                    personennummer_pseudonym BIGINT,
                                                    kundennummer_fusionierter_kunde VARCHAR(20),
                                                    banknummer_fusionierter_kunde VARCHAR(20),
                                                    art_kundenstammvertrag INT,
                                                    geburtsdatum_gruendungsdatum_pseudonym BIGINT,
                                                    geburtsort_pseudonym BIGINT,
                                                    person_angelegt_am DATE,
                                                    rechtsform DECIMAL(29),
                                                    rechtsformauspraegung DECIMAL(3),
                                                    rechtsform_binaer VARCHAR(25),
                                                    rechtsformauspraegung_beschreibung_1 VARCHAR(200),
                                                    rechtsformauspraegung_beschreibung_2 VARCHAR(400),
                                                    grundform VARCHAR(25),
                                                    staatsangehoerigkeit_nationalitaet_bezeichnung_pseudonym BIGINT,
                                                    ausstellende_behoerde_ausweis VARCHAR(50),
                                                    ausstellungsdatum_ausweis DATE,
                                                    ausweisart VARCHAR(40),
                                                    ausweiskopie_vorhanden CHAR(1),
                                                    ausweisnummer_pseudonym BIGINT,
                                                    eingetragen_am DATE,
                                                    gueltig_bis_ausweis DATE,
                                                    legitimation_geprueft_am DATE,
                                                    legitimationsverfahren VARCHAR(25),
                                                    ort_registergericht VARCHAR(32),
                                                    registerart DECIMAL(3),
                                                    registernummer_pseudonym BIGINT,
                                                    vorname_fuer_par_24c_kwg_pseudonym BIGINT,
                                                    nachname_fuer_par_24c_kwg_pseudonym BIGINT,
                                                    firmenname_fuer_par_24c_kwg_pseudonym BIGINT,
                                                    nachname_pseudonym BIGINT,
                                                    vorname_pseudonym BIGINT,
                                                    vollstaendiger_name_pseudonym BIGINT,
                                                    risikoklasse_nach_gwg INT,
                                                    person_ist_pep CHAR(1),
                                                    letzte_bearbeitung_wirtschaftlich_berechtigte DATE,
                                                    aktualitaet_der_kundendaten_wurde_ueberprueft DATE,
                                                    strasse_pseudonym BIGINT,
                                                    postleitzahl_pseudonym BIGINT,
                                                    ort_pseudonym BIGINT,
                                                    land_bezeichnung_pseudonym BIGINT
                   )
                   """)
    log.info("Created table: kundenstamm")

    # Create softfact_vw table
    cursor.execute("""
                   CREATE TABLE dbo.softfact_vw (
                                                    pk NVARCHAR(203) PRIMARY KEY,
                                                    banknummer VARCHAR(20),
                                                    banknummer_fusionierter_kunde VARCHAR(20),
                                                    stichtag DATE,
                                                    feststellung_wirtschaftlich_berechtigter INT,
                                                    guid UNIQUEIDENTIFIER,
                                                    kundennummer VARCHAR(20),
                                                    kundennummer_fusionierter_kunde VARCHAR(20),
                                                    personennummer_pseudonym BIGINT,
                                                    personennummer_2_kunde_pseudonym BIGINT,
                                                    rollentyp INT,
                                                    softfact_laufende_nummer INT,
                                                    softfactartschluessel VARCHAR(11),
                                                    softfacttyp DECIMAL(2),
                                                    schluesselart VARCHAR(250),
                                                    softfactartbezeichnung VARCHAR(250),
                                                    statistikschluessel VARCHAR(250)
                   )
                   """)
    log.info("Created table: softfact_vw")

    # Create kontodaten_vw table
    cursor.execute("""
                   CREATE TABLE dbo.kontodaten_vw (
                                                      pk NVARCHAR(152) PRIMARY KEY,
                                                      guid UNIQUEIDENTIFIER,
                                                      banknummer VARCHAR(20),
                                                      banknummer_fusionierter_kunde VARCHAR(20),
                                                      stichtag DATE,
                                                      personennummer_pseudonym BIGINT,
                                                      kontonummer_pseudonym BIGINT,
                                                      kundennummer_fusionierter_kunde BIGINT,
                                                      kontoeroeffnung DATE,
                                                      konto_fuer_fremde_rechnung CHAR(1),
                                                      anderkonto CHAR(1),
                                                      treuhandkonto CHAR(1),
                                                      aufloesungskennzeichen CHAR(1),
                                                      kontoaenderungsdatum DATE,
                                                      geschaeftsart DECIMAL(3),
                                                      spartenschluessel VARCHAR(2)
                   )
                   """)
    log.info("Created table: kontodaten_vw")

    # Create indexes for better performance
    indexes = [
        "CREATE INDEX idx_kundenstamm_stichtag ON dbo.kundenstamm(stichtag)",
        "CREATE INDEX idx_kundenstamm_banknummer ON dbo.kundenstamm(banknummer)",
        "CREATE INDEX idx_kundenstamm_kundennummer ON dbo.kundenstamm(kundennummer)",
        "CREATE INDEX idx_softfact_stichtag ON dbo.softfact_vw(stichtag)",
        "CREATE INDEX idx_softfact_banknummer ON dbo.softfact_vw(banknummer)",
        "CREATE INDEX idx_softfact_kundennummer ON dbo.softfact_vw(kundennummer)",
        "CREATE INDEX idx_kontodaten_stichtag ON dbo.kontodaten_vw(stichtag)",
        "CREATE INDEX idx_kontodaten_banknummer ON dbo.kontodaten_vw(banknummer)",
        "CREATE INDEX idx_kontodaten_personennummer ON dbo.kontodaten_vw(personennummer_pseudonym)"
    ]

    for idx_query in indexes:
        try:
            cursor.execute(idx_query)
        except pyodbc.ProgrammingError as e:
            if "There is already an index" not in str(e):
                log.warning(f"Index creation warning: {e}")

    connection.commit()
    log.info("Created indexes")


def import_csv_to_table(connection, csv_path, table_name, delimiter=';'):
    """Import CSV data into specified table."""
    import pandas as pd
    
    try:
        # Read CSV with pandas
        df = pd.read_csv(csv_path, delimiter=delimiter, encoding='utf-8-sig')
        log.info(f"Read {len(df)} rows from {csv_path}")
        log.info(f"Original columns: {list(df.columns)}")
        
        # Skip empty rows
        df = df.dropna(how='all')
        log.info(f"After removing empty rows: {len(df)} rows")
        
        # Clean column names - convert to lowercase and replace special characters
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_')
                     .replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss') 
                     for col in df.columns]
        log.info(f"Cleaned columns: {list(df.columns)}")
        
        # Handle BOM character in first column name if present
        if df.columns[0].startswith('\ufeff'):
            df.columns = [df.columns[0].replace('\ufeff', '')] + list(df.columns[1:])
        
        # Convert data types based on column names and content
        df_converted = df.copy()
        
        for col in df_converted.columns:
            if df_converted[col].dtype == 'object':
                # Try to detect and convert dates
                if 'datum' in col or 'date' in col or col == 'stichtag':
                    try:
                        # First try standard format
                        df_converted[col] = pd.to_datetime(df_converted[col], format='%Y-%m-%d', errors='coerce')
                        # If that didn't work well, try German format
                        if df_converted[col].isna().sum() > len(df_converted) * 0.5:
                            df_converted[col] = pd.to_datetime(df[col], format='%d.%m.%Y', errors='coerce')
                        log.info(f"  Converted '{col}' to datetime")
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
                            log.info(f"  Converted '{col}' to numeric")
                    except:
                        pass
        
        # Clear existing data
        cursor = connection.cursor()
        cursor.execute(f"DELETE FROM dbo.{table_name}")
        connection.commit()
        
        # Get column names from the database table
        cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}' 
            ORDER BY ORDINAL_POSITION
        """)
        db_columns = [row[0].lower() for row in cursor.fetchall()]
        
        # Make sure dataframe columns match database columns
        df_converted = df_converted[[col for col in df_converted.columns if col in db_columns]]
        
        # Insert data in batches
        batch_size = 1000
        total_rows = len(df_converted)
        
        for i in range(0, total_rows, batch_size):
            batch = df_converted.iloc[i:i+batch_size]
            
            # Create insert statement
            columns = ','.join(batch.columns)
            placeholders = ','.join(['?' for _ in batch.columns])
            insert_query = f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})"
            
            # Execute batch insert
            for _, row in batch.iterrows():
                # Convert NaN/None to None for SQL NULL
                values = [None if pd.isna(val) else val for val in row.values]
                cursor.execute(insert_query, values)
            
            if (i + batch_size) % 5000 == 0 or i + batch_size >= total_rows:
                connection.commit()
                log.info(f"  Inserted {min(i + batch_size, total_rows)}/{total_rows} rows")
        
        # Verify the import
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        count = cursor.fetchone()[0]
        log.info(f"Successfully imported {count} records to {table_name}")
        
        return True
        
    except Exception as e:
        log.error(f"Error importing CSV to {table_name}: {e}")
        import traceback
        log.error(traceback.format_exc())
        return False


def insert_sample_data(connection):
    """Import data from CSV files."""
    import pandas as pd
    
    sample_data_dir = './sample_data'
    
    # Mapping of CSV files to tables
    csv_table_mapping = {
        'Kundenstamm.csv': 'kundenstamm',
        'Softfact.csv': 'softfact_vw',
        'Kontodaten.csv': 'kontodaten_vw'
    }
    
    log.info(f"\nImporting data from {sample_data_dir}")
    
    if not os.path.exists(sample_data_dir):
        log.error(f"Sample data directory not found: {sample_data_dir}")
        log.info("Creating minimal sample data instead...")
        insert_minimal_sample_data(connection)
        return
    
    success_count = 0
    for csv_file, table_name in csv_table_mapping.items():
        csv_path = os.path.join(sample_data_dir, csv_file)
        
        if os.path.exists(csv_path):
            log.info(f"\nImporting {csv_file} to table {table_name}")
            if import_csv_to_table(connection, csv_path, table_name, delimiter=';'):
                success_count += 1
        else:
            log.warning(f"CSV file not found: {csv_path}")
    
    if success_count == len(csv_table_mapping):
        log.info("\nAll CSV files imported successfully!")
    else:
        log.warning(f"\nImported {success_count}/{len(csv_table_mapping)} CSV files")


def insert_minimal_sample_data(connection):
    """Insert minimal sample data for testing when CSV files are not available."""
    import uuid
    from datetime import datetime, timedelta
    import random
    
    cursor = connection.cursor()

    # Sample data for kundenstamm with more complete data
    sample_customers = []
    rechtsformen = ['GmbH', 'AG', 'GmbH & Co. KG', 'OHG', 'Einzelunternehmen', 'e.K.']
    
    for i in range(100):
        pk = f"CUST_{i:06d}"
        banknummer = f"{random.randint(10000000, 99999999)}"
        kundennummer = f"{random.randint(100000, 999999)}"
        stichtag = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
        personennummer = random.randint(1000000000, 9999999999)

        sample_customers.append((
            pk, banknummer, kundennummer, stichtag, personennummer,
            None, None, random.randint(1, 5), 
            random.randint(19500101, 20051231),  # geburtsdatum
            random.randint(1000, 9999),  # geburtsort
            stichtag, 
            random.randint(1, 10),  # rechtsform
            random.randint(1, 3),  # rechtsformauspraegung
            random.choice(['Natürlich', 'Juristisch']),  # rechtsform_binaer
            random.choice(rechtsformen),  # rechtsformauspraegung_beschreibung_1
            f"Beschreibung für {random.choice(rechtsformen)}",  # rechtsformauspraegung_beschreibung_2
            random.choice(['Grundform A', 'Grundform B']),  # grundform
            random.randint(1000, 9999),  # staatsangehoerigkeit
            f"Behörde {random.randint(1, 50)}",  # ausstellende_behoerde
            (datetime.now() - timedelta(days=random.randint(365, 3650))).date(),  # ausstellungsdatum
            random.choice(['Personalausweis', 'Reisepass']),  # ausweisart
            random.choice(['Y', 'N']),  # ausweiskopie_vorhanden
            random.randint(100000000, 999999999),  # ausweisnummer
            stichtag,  # eingetragen_am
            (datetime.now() + timedelta(days=random.randint(365, 3650))).date(),  # gueltig_bis
            stichtag,  # legitimation_geprueft_am
            random.choice(['VideoIdent', 'PostIdent', 'Persönlich']),  # legitimationsverfahren
            f"Stadt {random.randint(1, 100)}",  # ort_registergericht
            random.randint(1, 5),  # registerart
            random.randint(100000, 999999),  # registernummer
            random.randint(1000, 9999),  # vorname_fuer_par_24c
            random.randint(1000, 9999),  # nachname_fuer_par_24c
            random.randint(1000, 9999),  # firmenname_fuer_par_24c
            random.randint(1000, 9999),  # nachname
            random.randint(1000, 9999),  # vorname
            random.randint(1000, 9999),  # vollstaendiger_name
            random.randint(1, 3),  # risikoklasse_nach_gwg
            random.choice(['Y', 'N']),  # person_ist_pep
            stichtag,  # letzte_bearbeitung_wirtschaftlich_berechtigte
            stichtag,  # aktualitaet_der_kundendaten
            random.randint(1000, 9999),  # strasse
            random.randint(10000, 99999),  # postleitzahl
            random.randint(1000, 9999),  # ort
            random.randint(1, 200)  # land_bezeichnung
        ))

    # Insert customers
    placeholders = ','.join(['?' for _ in range(44)])
    insert_query = f"INSERT INTO dbo.kundenstamm VALUES ({placeholders})"

    for customer in sample_customers:
        cursor.execute(insert_query, customer)

    connection.commit()
    log.info(f"Inserted {len(sample_customers)} sample customers")

    # Sample data for softfact_vw
    sample_softfacts = []
    for i in range(200):
        pk = f"SF_{i:06d}"
        banknummer = f"{random.randint(10000000, 99999999)}"
        kundennummer = f"{random.randint(100000, 999999)}"
        stichtag = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
        guid = str(uuid.uuid4())

        sample_softfacts.append((
            pk, banknummer, None, stichtag, random.randint(1, 5), guid,
            kundennummer, None, random.randint(1000000000, 9999999999), 
            random.randint(1000000000, 9999999999),
            random.randint(1, 10), random.randint(1, 100), 
            f"SF{random.randint(100, 999)}",
            random.randint(1, 5), 
            f"Schlüsselart {random.randint(1, 20)}",
            f"Softfactart {random.randint(1, 20)}",
            f"STAT{random.randint(100, 999)}"
        ))

    # Insert softfacts
    placeholders = ','.join(['?' for _ in range(17)])
    insert_query = f"INSERT INTO dbo.softfact_vw VALUES ({placeholders})"

    for softfact in sample_softfacts:
        cursor.execute(insert_query, softfact)

    connection.commit()
    log.info(f"Inserted {len(sample_softfacts)} sample softfacts")
    
    # Sample data for kontodaten_vw
    sample_kontos = []
    for i in range(150):
        pk = f"KTO_{i:06d}"
        guid = str(uuid.uuid4())
        banknummer = f"{random.randint(10000000, 99999999)}"
        stichtag = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
        personennummer = random.randint(1000000000, 9999999999)
        kontonummer = random.randint(1000000000, 9999999999)
        
        sample_kontos.append((
            pk, guid, banknummer, None, stichtag, personennummer,
            kontonummer, random.randint(100000, 999999),
            (datetime.now() - timedelta(days=random.randint(365, 7300))).date(),  # kontoeroeffnung
            random.choice(['Y', 'N']),  # konto_fuer_fremde_rechnung
            random.choice(['Y', 'N']),  # anderkonto
            random.choice(['Y', 'N']),  # treuhandkonto
            random.choice(['Y', 'N', None]),  # aufloesungskennzeichen
            stichtag,  # kontoaenderungsdatum
            random.randint(1, 20),  # geschaeftsart
            f"{random.randint(10, 99)}"  # spartenschluessel
        ))
    
    # Insert kontos
    placeholders = ','.join(['?' for _ in range(16)])
    insert_query = f"INSERT INTO dbo.kontodaten_vw VALUES ({placeholders})"
    
    for konto in sample_kontos:
        cursor.execute(insert_query, konto)
    
    connection.commit()
    log.info(f"Inserted {len(sample_kontos)} sample kontodaten")


def main():
    """Main initialization function."""
    # Get connection parameters from environment or use defaults
    server = os.getenv('MSSQL_SERVER', 'localhost')
    database = os.getenv('MSSQL_DATABASE', 'SamplingDB')
    username = os.getenv('MSSQL_USERNAME', 'sa')
    password = os.getenv('MSSQL_PASSWORD', 'YourStrong@Passw0rd')
    driver = os.getenv('MSSQL_DRIVER', 'ODBC Driver 17 for SQL Server')

    try:
        # First connect to master to create database
        master_conn_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE=master;"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )

        master_conn = pyodbc.connect(master_conn_string)

        # Create database
        create_database_if_not_exists(master_conn, database)

        master_conn.close()

        # Now connect to the new database
        conn_string = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )

        conn = pyodbc.connect(conn_string)

        # Create tables
        create_tables(conn)

        # Always insert sample data from CSV files
        insert_sample_data(conn)

        conn.close()

        log.info(f"\n✅ MS SQL Server database initialized successfully!")
        log.info(f"   Server: {server}")
        log.info(f"   Database: {database}")
        log.info(f"\nYou can now use the sampling tool with MS SQL Server.")

    except Exception as e:
        log.error(f"Initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
