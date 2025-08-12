"""
MS SQL Server Database Initialization Script
Creates the database and tables for the sampling tool in MS SQL Server
"""

import os
import sys
import pyodbc
import logging
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


def insert_sample_data(connection):
    """Insert some sample data for testing."""
    import uuid
    from datetime import datetime, timedelta
    import random
    
    cursor = connection.cursor()

    # Sample data for kundenstamm
    sample_customers = []
    for i in range(100):
        pk = f"CUST_{i:06d}"
        banknummer = f"BNK{random.randint(100, 999)}"
        kundennummer = f"KND{i:06d}"
        stichtag = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
        personennummer = random.randint(1000000, 9999999)

        sample_customers.append((
            pk, banknummer, kundennummer, stichtag, personennummer,
            None, None, random.randint(1, 5), None, None,
            stichtag, None, None, random.choice(['GmbH', 'AG', 'Person']),
            None, None, None, None, None, None,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
            None, None, None, None, random.choice(['Y', 'N']),
            None, None, None, None, None, None
        ))

    # Insert customers
    placeholders = ','.join(['?' for _ in range(44)])  # 44 columns
    insert_query = f"INSERT INTO dbo.kundenstamm VALUES ({placeholders})"

    for customer in sample_customers:
        cursor.execute(insert_query, customer)

    connection.commit()
    log.info(f"Inserted {len(sample_customers)} sample customers")

    # Sample data for softfact_vw
    sample_softfacts = []
    for i in range(50):
        pk = f"SF_{i:06d}"
        banknummer = f"BNK{random.randint(100, 999)}"
        kundennummer = f"KND{random.randint(0, 99):06d}"
        stichtag = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
        guid = str(uuid.uuid4())

        sample_softfacts.append((
            pk, banknummer, None, stichtag, None, guid,
            kundennummer, None, random.randint(1000000, 9999999), None,
            random.randint(1, 10), None, f"SF{random.randint(100, 999)}",
            random.randint(1, 5), None, None, None
        ))

    # Insert softfacts
    placeholders = ','.join(['?' for _ in range(17)])
    insert_query = f"INSERT INTO dbo.softfact_vw VALUES ({placeholders})"

    for softfact in sample_softfacts:
        cursor.execute(insert_query, softfact)

    connection.commit()
    log.info(f"Inserted {len(sample_softfacts)} sample softfacts")


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

        # Optionally insert sample data
        response = input("\nDo you want to insert sample data for testing? (y/n): ")
        if response.lower() == 'y':
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
