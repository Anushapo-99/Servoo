# Import necessary libraries
"""
Module purpose
--------------
This module implements a lightweight ETL pipeline to ingest multiple supplier CSV
files, standardize column names, extract product packaging metadata (weight/quantity,
units-per-carton and packaging type), consolidate records, assign global product IDs,
and write a single cleaned output CSV.

Intended usage
--------------
- Configure `input_files` (a mapping of supplier name -> CSV filepath) and
    `output_file`.
- Run the script as a standalone module. Each configured CSV will be read,
    transformed, and appended to a combined DataFrame which is saved to `output_file`.
- The consolidated output columns are:
        Product_ID, Product_Name, Serial_Number, Supplier,
        Weight_Quantity, Packaging_Type, Units_Per_Carton
Dependencies
------------
- pandas (pd)
- numpy (np)
- re
- os
Top-level behavior summary
--------------------------
1. Iterate over configured `input_files`. Missing files are skipped with a warning.
2. Read each CSV into a DataFrame and normalize column names via `standardize_columns`.
3. Ensure required columns ('Product_Name', 'Serial_Number') exist; if missing they are
     created with None values.
4. Extract features:
     - `Weight_Quantity` via `extract_weight_quantity`
     - `Units_Per_Carton` via `extract_units_per_carton`
     - `Packaging_Type` via `detect_packaging_type`, except for the "Amal Trading"
         supplier where a provided 'Unit' column is used (CTN vs NON-CTN)
5. Keep selected columns, append to a global list and concat into `final_df`.
6. Deduplicate by (Product_Name, Supplier), reset index and assign Product_ID values
     in the form "product_<n>".
7. Write consolidated CSV to `output_file` and print a brief summary.

Config & customization points
-----------------------------
- input_files: dict[str, str] mapping supplier display names to file paths.
- output_file: path for the consolidated CSV.
- Adjust or extend:
        * Column mapping inside `standardize_columns` to handle more supplier column names.
        * Regex patterns inside extraction functions to support additional formats.
        * Packaging classification rules to respect more packaging types.
Error handling & logging
------------------------
- Missing input files: printed warning and skipped.
- Missing Product_Name column after standardization: raises ValueError.
- CSV read errors (encoding, malformed CSV) will propagate as pandas exceptions.
- Minimal console logging is provided via print statements; replace with logging
    for production use.
Examples
--------
- To add a new supplier, add an entry to `input_files` mapping (name -> CSV path).
- To change output location, update the `output_file` variable.
- Run the module (e.g., python Merging_code.py) to produce the consolidated CSV.
Limitations & caveats
---------------------
- Extraction logic is heuristic-driven; edge cases exist and manual review may be
    necessary for unusual naming conventions.
- Volume-to-mass conversions are approximations and may not hold for non-water densities.
- Numeric rounding to integer grams may hide fractional weights; adjust if needed.
- Deduplication is based only on (Product_Name, Supplier). If global deduplication
    across suppliers is required, add normalized matching logic (e.g., fuzzy matching).
Author / Maintainer notes
-------------------------
- Keep regexes maintainable and add unit test coverage for the extraction functions
    to catch regressions when new product name formats are encountered.
- Consider extracting the pipeline into re-usable functions and adding CLI args
    for input/output configuration and verbose logging."""


# IMPORTS
import pandas as pd
import os
import re
import numpy as np

#CONFIG - INPUT FILES 
input_files = {
    "Amal Trading": "servoo_task/common_files/Amal Trading - Sheet1 (2).csv",
    "Future": "/home/anusha/Desktop/sevoo_task/servoo_task/common_files/CATALOG - FUTURE.csv",
    "Red Frozen": "/home/anusha/Desktop/sevoo_task/servoo_task/common_files/CATALOG-RED-FROZEN.csv",
    "Chettiot": "/home/anusha/Desktop/sevoo_task/servoo_task/common_files/CATALOG-CHETTIOT-csv.csv"
}

# HELPER FUNCTIONS 

def extract_weight_quantity(name: str):
    """
    Extract and standardize weight/quantity values from product names.
    - Handles forms like: "340GX24", "400GX20", "2.5KGX 4", "15.9KG1", "500 ML"
    - Converts KG -> G (×1000), L/LTR -> G (×1000), ML -> G (×1)
    - Returns standardized grams as string, e.g. "400G" or None if not found.
    """
    if pd.isna(name):
        return None

    text = str(name).upper().strip()

    # Regex finds the first occurrence of a number + unit.
    # Uses a lookahead to allow immediate X or digits after the unit (handles "340GX24").
    weight_re = re.compile(
        r'(\d+(?:\.\d+)?)\s*(KG|KGS|G|GM|GR|GRAM|GRAMS|L|LTR|LITRE|ML|M L|LT)(?=[\s\*×X0-9\W]|$)',
        flags=re.IGNORECASE
    )

    match = weight_re.search(text)
    if not match:
        return None

    try:
        value = float(match.group(1))
    except Exception:
        return None

    unit = match.group(2).upper()

    # Normalize units and convert to grams
    if unit in {"KG", "KGS"}:
        grams = value * 1000
    elif unit in {"G", "GM", "GR", "GRAM", "GRAMS"}:
        grams = value
    elif unit in {"L", "LTR", "LITRE"}:
        grams = value * 1000  # treat 1 L as ~1000 g
    elif unit in {"ML", "M L"}:
        grams = value * 1     # treat 1 mL as ~1 g
    else:
        grams = value

    # Return integer grams with "G"
    return f"{int(round(grams))}G"

"""extract_weight_quantity(name: str) -> Optional[str]
        - Purpose: Find and standardize first weight/volume-like token in a product name.
        - Behavior:
                * Uses a regex to capture a numeric value optionally followed by a unit
                    (KG, G, GM, GR, GRAM(S), L, LTR, LITRE, ML, etc.). Allows decimal values.
                * Handles patterns with immediate suffixes (e.g., "340GX24") via a lookahead.
                * Unit conversions:
                        KG -> grams (×1000)
                        L/LTR/LITRE -> grams (×1000) (approximate, treats 1 L ≈ 1000 g)
                        ML -> grams (×1) (treats 1 mL ≈ 1 g)
                        G/GM/... -> grams (×1)
                * Rounds to nearest integer and returns a string like "400G".
                * Returns None when no match or on parse failure.
        - Limitations:
                * Volume-to-mass conversions assume water-like density (1 L ≈ 1000 g).
                * Does not interpret compound forms beyond the first weight token."""

#  UNITS PER CARTON EXTRACTION 
def extract_units_per_carton(name: str):
    """
    Extract number of units per carton from product names.

    Handles formats like:
        - '4 X 18 X 56G' → 72
        - '24X500 ML CTN' → 24
        - '(1.5 ML*12)' → 12
        - '96PCS' or '12PC' → 96 or 12
        - '6*2.5L FAMILY CTN' → 6
        - '7 UP 150 ML X 30' → 30
        - '100 TEABAGS', '100 BAGS', '100 TEA BAGS' → 100
    """
    if pd.isna(name) or not isinstance(name, str):
        return None

    name = name.upper().strip()

    # --- Case 1: PCS / PC patterns ---
    pcs_match = re.search(r'(\d+)\s*(PCS|PC|PS)\b', name)
    if pcs_match:
        return int(pcs_match.group(1))

    # --- Case 2: (1.5 ML*12) type pattern ---
    bracket_match = re.search(r'\(\s*[\d\.]+\s*(?:ML|L|G|KG)?\s*[*×X]\s*(\d+)\s*\)', name)
    if bracket_match:
        return int(bracket_match.group(1))

    # --- Case 3: Patterns like 6*2.5L or 6×2.25LTR ---
    before_unit_match = re.search(r'(\d+)\s*[*×X]\s*[\d\.]+\s*(?:ML|L|LTR|G|KG|MIL|OZ)', name)
    if before_unit_match:
        return int(before_unit_match.group(1))

    # --- Case 4: Multiple X patterns (4 X 18 X 56G) ---
    x_numbers = re.findall(r'(\d+)\s*[X×]\s*(?=\d+)', name)
    if len(x_numbers) >= 2:
        nums = list(map(int, x_numbers))
        return int(np.prod(nums))
    elif len(x_numbers) == 1:
        return int(x_numbers[0])

    # --- Case 5: Single X pattern like 24X500 ML ---
    single_x_match = re.search(r'\b(\d+)\s*[X×]\s*\d*\s*(?:KG|G|GM|L|ML)\b', name)
    if single_x_match:
        return int(single_x_match.group(1))

    # --- Case 6: Numbers followed by BAG(S), TEABAG(S), UNIT(S) etc. ---
    bag_match = re.search(r'\b(\d+)\s*(BAG|BAGS|TEABAG|TEABAGS|TEA BAGS|STICKS)\b', name)
    if bag_match:
        return int(bag_match.group(1))
     
        # --- Case 7: PLUS pattern like 36+6 ---
    plus_match = re.search(r'(\d+)\s*\+\s*(\d+)', name)
    if plus_match:
        return int(plus_match.group(1)) + int(plus_match.group(2))
   
    # --- Case 8: Catch-all fallback for * × X patterns ---
    star_after_match = re.search(r'[*×X]\s*(\d+)', name)
    if star_after_match:
        return int(star_after_match.group(1))

    return None

"""extract_units_per_carton(name: str) -> Optional[int]
        - Purpose: Infer the number of units contained in a carton/package from product text.
        - Recognized patterns (non-exhaustive):
                * "96PCS", "12PC" -> numeric value before PCS/PC
                * "(1.5 ML*12)" -> captures the trailing multiplier inside parentheses
                * "6*2.5L" or "6×2.25LTR" -> captures the leading multiplier before a unit
                * "4 X 18 X 56G" -> multiplies the successive numbers around X/× to compute total
                * "24X500 ML" -> leading number before X
                * "100 TEABAGS" / "100 BAGS" -> numeric before bag-like tokens
                * "36+6" -> sum of both sides of plus
                * fallback: capture digit following a star/X symbol
        - Returns an integer count when a pattern is matched; otherwise None.
        - Limitations:
                * Heuristic-based and may misinterpret ambiguous strings.
                * Focuses on common commercial formatting but is not exhaustive."""


#  PACKAGING TYPE DETECTION 
def detect_packaging_type(name: str):
    """
    Detect packaging type as either 'CTN' (carton) or 'NON-CTN'.
    Works for abbreviations like CTN, CARTON, CARTONS, etc.
    Maps all other packaging types (BAG, BOX, BTL, CAN, DOZ, etc.) to 'NON-CTN'.
    """
    if pd.isna(name):
        return "NON-CTN"

    name = str(name).strip().upper()

    carton_keywords = {"CTN", "CARTON", "CARTONS"}
    return "CTN" if any(k in name for k in carton_keywords) else "NON-CTN"

"""detect_packaging_type(name: str) -> str
        - Purpose: Classify packaging as 'CTN' (carton) vs 'NON-CTN'.
        - Behavior:
                * If input is NaN, returns "NON-CTN".
                * If any of the keywords {'CTN', 'CARTON', 'CARTONS'} appear (case-insensitive),
                    returns "CTN".
                * All other values map to "NON-CTN".
        - Note: Supplier-specific overrides may be applied in the pipeline (e.g., using
            an explicit 'Unit' column for Amal Trading)."""


#  COLUMN STANDARDIZATION FUNCTION 
def standardize_columns(df: pd.DataFrame, supplier_name: str):
    """Standardize inconsistent column names."""
    col_map = {
        'Item Name': 'Product_Name',
        'PRODUCT TITLE': 'Product_Name',
        'Name': 'Product_Name',
        'Product_Name': 'Product_Name',
        'SERIAL NUMBER': 'Serial_Number',
        'SL NO': 'Serial_Number',
        'Serial_Number': 'Serial_Number',
        'Unit': 'Unit'  # 🔹 NEW: ensure "Unit" column keeps consistent name
    }
    df = df.rename(columns={col: col_map.get(col, col) for col in df.columns})
    if 'Product_Name' not in df.columns:
        raise ValueError(f"❌ Product name column missing in {supplier_name} file")
    df['Supplier'] = supplier_name
    return df

"""
standardize_columns(df: pandas.DataFrame, supplier_name: str) -> pandas.DataFrame
        - Purpose: Normalize common, inconsistent column names across supplier files and
            tag each row with the supplier name.
        - Behavior:
                * Renames columns using a mapping (e.g., 'Item Name' -> 'Product_Name',
                    'SERIAL NUMBER' -> 'Serial_Number').
                * Ensures a 'Product_Name' column exists; if not present after renaming,
                    raises ValueError with a clear message including the supplier name.
                * Adds a 'Supplier' column populated with the given supplier_name.
        - Returns: the renamed DataFrame."""


#  MAIN ETL PIPELINE 
all_data = []


# PROCESS EACH INPUT FILE
for supplier, file_path in input_files.items():
    if not os.path.exists(file_path):
        print(f"⚠️ File not found: {file_path}")
        continue

    print(f"📥 Processing {supplier} ...")
    df = pd.read_csv(file_path)

    # Standardize columns
    df = standardize_columns(df, supplier)

    # Ensure required columns
    for col in ['Serial_Number', 'Product_Name']:
        if col not in df.columns:
            df[col] = None

    # Feature extraction
    df['Weight_Quantity'] = df['Product_Name'].apply(extract_weight_quantity)
    df['Units_Per_Carton'] = df['Product_Name'].apply(extract_units_per_carton)
     # 🔹 CHANGE #1: Packaging_Type handling
    if supplier == "Amal Trading" and 'Unit' in df.columns:
        # Directly use the 'Unit' column for packaging type
        df['Packaging_Type'] = df['Unit'].apply(lambda x: "CTN" if str(x).strip().upper() == "CTN" else "NON-CTN")
    else:
        # For all others, detect from product name
        df['Packaging_Type'] = df['Product_Name'].apply(detect_packaging_type)

    # Keep only needed columns for now
    df_clean = df[['Product_Name', 'Serial_Number', 'Supplier',
                   'Weight_Quantity', 'Packaging_Type', 'Units_Per_Carton']]
    all_data.append(df_clean)


#  COMBINE ALL FILES 
"""Concatenate all supplier DataFrames, deduplicate, reset index"""
final_df = pd.concat(all_data, ignore_index=True)
final_df.drop_duplicates(subset=['Product_Name', 'Supplier'], inplace=True)
final_df.reset_index(drop=True, inplace=True)


#  ADD GLOBAL UNIQUE PRODUCT IDs 
"""Assign unique Product_IDs in the form "product_<n>"."""
final_df['Product_ID'] = [f"product_{i+1}" for i in range(len(final_df))]

# Reorder columns
final_df = final_df[['Product_ID', 'Product_Name', 'Serial_Number', 'Supplier',
                     'Weight_Quantity', 'Packaging_Type', 'Units_Per_Carton']]

#  SAVE OUTPUT 
""" output_file: str
        - Purpose: File path for saving the consolidated cleaned product CSV.
        - Usage: The final DataFrame is written to this path as a CSV without index."""
output_file = "/home/anusha/Desktop/Servoo/DATA_ENGINEERING/Data/Servoo_Cleaned_Products.csv"
final_df.to_csv(output_file, index=False)
print(f"\n✅ Consolidated file saved as: {output_file}")


#  SUMMARY 
print("\n📊 Sample Output:")
print(final_df.head(10))
print(f"\n🧾 Total Cleaned Products: {len(final_df)}")