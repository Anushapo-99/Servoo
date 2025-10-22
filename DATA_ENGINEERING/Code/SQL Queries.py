"""
STEP 4: SQL ANALYSIS & REPORTING
---------------------------------
Performs analysis on Servoo master product database and saves results
to both a CSV file and a new database table.
"""

# IMPORTS
import sqlite3
import pandas as pd
from datetime import datetime

#CONFIG 
DB_FILE = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_ENGINEERING_TASK/Data/servoo_master.db"
TABLE_NAME = "products"
OUTPUT_CSV = "/home/anusha/Desktop/sevoo_task/servoo_task/DATA_ENGINEERING_TASK/Data/Servoo_SQL_Report.csv"
OUTPUT_TABLE = "analysis_report"

#CONNECT 
conn = sqlite3.connect(DB_FILE)

#QUERIES 
queries = {
    "Total number of products": f"SELECT COUNT(*) AS total_products FROM {TABLE_NAME};",

    "Number of CTN vs NON-CTN products": f"""
        SELECT Packaging_Type, COUNT(*) AS count
        FROM {TABLE_NAME}
        GROUP BY Packaging_Type;
    """,

    "Total units available": f"""
        SELECT 
            SUM(
                CASE 
                    WHEN Packaging_Type='CTN' 
                         AND Units_Per_Carton NOT IN ('N/A', '', '0', 'NULL')
                    THEN CAST(Units_Per_Carton AS INTEGER)
                    ELSE 1
                END
            ) AS total_units
        FROM {TABLE_NAME};
    """,

    "Duplicate Serial_Numbers": f"""
        SELECT Serial_Number, COUNT(*) AS count
        FROM {TABLE_NAME}
        GROUP BY Serial_Number
        HAVING COUNT(*) > 1;
    """,

    "Missing Serial_Numbers": f"""
        SELECT COUNT(*) AS missing_serials
        FROM {TABLE_NAME}
        WHERE Serial_Number IS NULL OR TRIM(Serial_Number) = '';
    """,

    "Supplier-wise product counts": f"""
        SELECT Supplier, COUNT(*) AS total_products
        FROM {TABLE_NAME}
        GROUP BY Supplier
        ORDER BY total_products DESC;
    """,

    "Products appearing in multiple catalogs": f"""
        SELECT Serial_Number, COUNT(DISTINCT Supplier) AS supplier_count
        FROM {TABLE_NAME}
        GROUP BY Serial_Number
        HAVING supplier_count > 1;
    """,

    "Unique products (only in one catalog)": f"""
        SELECT COUNT(*) AS unique_products
        FROM (
            SELECT Serial_Number
            FROM {TABLE_NAME}
            GROUP BY Serial_Number
            HAVING COUNT(DISTINCT Supplier) = 1
        );
    """
}

#EXECUTE & SAVE 
results = []  # to store all results for CSV + DB

for desc, query in queries.items():
    print(f"\n🔹 {desc}")
    df_result = pd.read_sql_query(query, conn)
    print(df_result.to_string(index=False))

    # Convert each result to a text summary format for saving
    summary = df_result.to_dict(orient="records")
    results.append({
        "Query_Description": desc,
        "Query": query.strip(),
        "Result": str(summary)
    })

#SAVE TO CSV 
df_summary = pd.DataFrame(results)
df_summary["Generated_At"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_summary.to_csv(OUTPUT_CSV, index=False)

#SAVE TO DATABASE 
df_summary.to_sql(OUTPUT_TABLE, conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print("\n✅ Analysis complete!")
print(f"📁 CSV saved at: {OUTPUT_CSV}")
print(f"🗃️ Results also stored in table '{OUTPUT_TABLE}' inside {DB_FILE}")

