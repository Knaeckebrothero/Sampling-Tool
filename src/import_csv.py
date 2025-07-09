#!/usr/bin/env python
"""
Alternative import method that should work more reliably
"""

import pandas as pd
import sqlite3
import os
import sys

def safe_import_csv(csv_path, db_path='./data/sampling.db', delimiter=';'):
    """Import CSV using a more reliable method"""
    print(f"Importing {csv_path} to {db_path}")

    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    # Read CSV
    print("Reading CSV...")
    df = pd.read_csv(csv_path, delimiter=delimiter)
    print(f"Found {len(df)} rows and {len(df.columns)} columns")

    # Clean column names
    df.columns = [col.strip().replace(' ', '_').replace('-', '_') for col in df.columns]
    print(f"Columns: {list(df.columns)}")

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop existing table
    cursor.execute("DROP TABLE IF EXISTS financial_data")
    conn.commit()

    # Create table with proper columns
    print("Creating table...")

    # Build CREATE TABLE statement
    column_defs = []
    for col in df.columns:
        # Determine column type
        if df[col].dtype == 'int64':
            col_type = 'INTEGER'
        elif df[col].dtype == 'float64':
            col_type = 'REAL'
        else:
            # Check if it's a numeric column with European format
            sample = df[col].dropna().head()
            if any(isinstance(v, str) and ',' in v and any(c.isdigit() for c in v) for v in sample):
                col_type = 'REAL'
            else:
                col_type = 'TEXT'

        column_defs.append(f"{col} {col_type}")

    create_table_sql = f"CREATE TABLE financial_data ({', '.join(column_defs)})"
    print(f"SQL: {create_table_sql}")
    cursor.execute(create_table_sql)
    conn.commit()

    # Prepare data for insertion
    print("Preparing data...")
    data_to_insert = []

    for _, row in df.iterrows():
        row_data = []
        for col in df.columns:
            value = row[col]

            # Handle NaN/None
            if pd.isna(value):
                row_data.append(None)
            # Convert European numbers
            elif isinstance(value, str) and ',' in value and any(c.isdigit() for c in value):
                try:
                    # Remove thousand separators and convert decimal comma
                    cleaned = value.replace('.', '').replace(',', '.')
                    row_data.append(float(cleaned))
                except:
                    row_data.append(value)  # Keep as string if conversion fails
            else:
                row_data.append(value)

        data_to_insert.append(tuple(row_data))

    # Insert data
    print(f"Inserting {len(data_to_insert)} rows...")
    placeholders = ','.join(['?' for _ in df.columns])
    insert_sql = f"INSERT INTO financial_data VALUES ({placeholders})"

    cursor.executemany(insert_sql, data_to_insert)
    conn.commit()

    # Verify
    cursor.execute("SELECT COUNT(*) FROM financial_data")
    count = cursor.fetchone()[0]
    print(f"✓ Successfully imported {count} rows")

    # Show sample
    cursor.execute("SELECT * FROM financial_data LIMIT 3")
    print("\nSample data:")
    for row in cursor.fetchall():
        print(f"  {row}")

    # Show table structure
    cursor.execute("PRAGMA table_info(financial_data)")
    print("\nTable structure:")
    for col in cursor.fetchall():
        print(f"  {col[1]}: {col[2]}")

    conn.close()
    print("\n✓ Import complete!")

if __name__ == "__main__":
    csv_file = sys.argv[1] if len(sys.argv) > 1 else "example_data.csv"

    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found")
        print("Usage: python fix_import.py [csv_file]")
        sys.exit(1)

    safe_import_csv(csv_file)
