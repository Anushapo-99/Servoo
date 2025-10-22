
"""
STEP 3: LOAD CLEANED DATA INTO SQL MASTER TABLE
-
Creates SQLite master database and loads cleaned Servoo product data.

Purpose
-------
Load a cleaned product CSV into a SQLite "master" database table. This module
reads a pre-cleaned CSV file into a pandas DataFrame, augments it with two
metadata columns, enforces a specific column order, creates a SQLite table
(if needed), and writes the DataFrame into the target database.

"""

# IMPORTS
import sqlite3
import pandas as pd
from datetime import datetime

# CONFIG 
CSV_FILE = "/home/anusha/Desktop/Servoo/DATA_ENGINEERING/Data/Servoo-Product-Data-Cleaned.csv"
DB_FILE = "/home/anusha/Desktop/Servoo/DATA_ENGINEERING/Data/servoo_master.db"
TABLE_NAME = "products"

"""Configuration
-------------
The module uses three top-level configuration constants which can be adjusted:
- CSV_FILE: path to the cleaned CSV input file.
- DB_FILE: path to the SQLite database file to create / modify.
- TABLE_NAME: name of the target table inside the SQLite database."""

# Read cleaned CSV data
df = pd.read_csv(CSV_FILE)
"""1. Read the cleaned CSV file at CSV_FILE into a pandas DataFrame."""

# Add additional columns
df["Source_File"] = df.get("Supplier", "Unknown")
df["Last_Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


"""2. Add two columns:
    - Source_File: copied from the "Supplier" column when present, otherwise "Unknown".
    - Last_Updated: timestamp of when the script ran (format YYYY-MM-DD HH:MM:SS)."""


# Ensure correct column order and names
expected_columns = [
    "Product_ID",
    "Product_Name",
    "Serial_Number",
    "Supplier",
    "Weight_Quantity",
    "Packaging_Type",
    "Units_Per_Carton",
    "Source_File",
    "Last_Updated"
]
df = df[expected_columns]

""""3. Reorder/select columns to match the expected schema. The script expects the
    following columns to exist in the CSV before reordering:
      - Product_ID
      - Product_Name
      - Serial_Number
      - Supplier
      - Weight_Quantity
      - Packaging_Type
      - Units_Per_Carton
    If any of the expected columns are missing, selecting the expected order will
    raise a KeyError."""

# Connect to SQLite and create table
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    Product_ID TEXT PRIMARY KEY,
    Product_Name TEXT,
    Serial_Number TEXT,
    Supplier TEXT,
    Weight_Quantity TEXT,
    Packaging_Type TEXT,
    Units_Per_Carton TEXT,
    Source_File TEXT,
    Last_Updated TEXT
)
""")

"""4. Connect to a SQLite database file at DB_FILE and create the target table
    TABLE_NAME if it does not already exist. The table schema is:
      - Product_ID TEXT PRIMARY KEY
      - Product_Name TEXT
      - Serial_Number TEXT
      - Supplier TEXT
      - Weight_Quantity TEXT
      - Packaging_Type TEXT
      - Units_Per_Carton TEXT
      - Source_File TEXT
      - Last_Updated TEXT"""


# Insert data into table
df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)


conn.commit()
conn.close()


"""5. Write the DataFrame into the database table using pandas.DataFrame.to_sql with
    if_exists="replace" and index=False.
    Note: using "replace" will drop and recreate the table (so any prior table contents will be lost)."""

print(f"✅ Data successfully loaded into {DB_FILE} -> Table: {TABLE_NAME}")
