import pandas as pd
import os
import re
import uuid
import numpy as np

# === CONFIG ===
input_files = {
    "Amal Trading": "servoo_task/common_files/Amal Trading - Sheet1 (2).csv",
    "Future": "/home/anusha/Desktop/sevoo_task/servoo_task/common_files/CATALOG - FUTURE.csv",
    "Red Frozen": "/home/anusha/Downloads/CATALOG-RED-FROZEN.csv",
    "Chettiot": "/home/anusha/Downloads/CATALOG-CHETTIOT-csv.csv"
}

# Optional: set working directory if running from elsewhere
# os.chdir("/path/to/your/folder")

# === HELPER FUNCTIONS ===
def extract_weight_quantity(name: str):
    """Extract weight or quantity pattern like '1KG', '500G', '2L' from the product name."""
    if pd.isna(name):
        return None
    match = re.search(r"(\d+(?:\.\d+)?\s?(?:KG|g|G|GM|L|ML|ml|l|gm|kg))", name, re.IGNORECASE)
    return match.group(1).upper().replace(" ", "") if match else None


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
    """
    if pd.isna(name) or not isinstance(name, str):
        return None

    name = name.upper().strip()

    # --- Case 1: PCS / PC patterns ---
    pcs_match = re.search(r'(\d+)\s*(PCS|PC)\b', name)
    if pcs_match:
        return int(pcs_match.group(1))

    # --- Case 2: (1.5 ML*12) type pattern ---
    bracket_match = re.search(r'\(\s*[\d\.]+\s*(?:ML|L|G|KG)?\s*[*×X]\s*(\d+)\s*\)', name)
    if bracket_match:
        return int(bracket_match.group(1))

    # --- Case 3: A pattern like 6*2.5L or 6×2.25LTR ---
    before_unit_match = re.search(r'(\d+)\s*[*×X]\s*[\d\.]+\s*(?:ML|L|LTR|G|KG)', name)
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

    # --- Case 6: Catch-all fallback ---
    star_after_match = re.search(r'[*×X]\s*(\d+)', name)
    if star_after_match:
        return int(star_after_match.group(1))

    return None


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



# def standardize_columns(df: pd.DataFrame, supplier_name: str):
#     """Map inconsistent column names and standardize structure."""
#     col_map = {
#         'Item Name': 'Product_Name',
#         'Product_Name': 'Product_Name',
#         'PRODUCT TITLE': 'Product_Name',
#         'Name': 'Product_Name',
#         'SERIAL NUMBER': 'Serial_Number',
#         'Serial_Number': 'Serial_Number',
#         'SL NO': 'Serial_Number'
#     }

#     df = df.rename(columns={col: col_map.get(col, col) for col in df.columns})
#     if 'Product_Name' not in df.columns:
#         raise ValueError(f"Product name column missing in {supplier_name} file")

#     df['Supplier'] = supplier_name
#     return df




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

# === MAIN ETL PIPELINE ===
all_data = []



# for supplier, file_path in input_files.items():
#     if not os.path.exists(file_path):
#         print(f"⚠️ File not found: {file_path}")
#         continue

#     print(f"📥 Processing {supplier} ...")
#     df = pd.read_csv(file_path)

#     # Standardize column names
#     df = standardize_columns(df, supplier)

#     # Ensure essential columns exist
#     for col in ['Serial_Number', 'Product_Name']:
#         if col not in df.columns:
#             df[col] = None

#     # Extract weight, carton info, packaging type
#     df['Weight_Quantity'] = df['Product_Name'].apply(extract_weight_quantity)
#     df['Units_Per_Carton'] = df['Product_Name'].apply(extract_units_per_carton)
#     df['Packaging_Type'] = df['Product_Name'].apply(detect_packaging_type)
#     # Add sequential product IDs
#     df['Product_ID'] = [f"product_{i+1}" for i in range(len(df))]

#     # Keep only required columns
#     df_clean = df[['Product_ID', 'Product_Name', 'Serial_Number', 'Supplier',
#                 'Weight_Quantity', 'Packaging_Type', 'Units_Per_Carton']]

#     # Append cleaned data
#     all_data.append(df_clean)
   



# # === COMBINE AND CLEAN ===
# final_df = pd.concat(all_data, ignore_index=True)
# final_df.drop_duplicates(subset=['Product_Name', 'Supplier'], inplace=True)
# final_df.reset_index(drop=True, inplace=True)

# # === SAVE OUTPUT ===
# output_file = "Servoo_Cleaned_Products1.csv"
# final_df.to_csv(output_file, index=False)
# print(f"✅ Consolidated file saved as: {output_file}")

# # === OPTIONAL: Summary ===
# print("\nSample Output:")
# print(final_df.head(10))
# print(f"\nTotal Cleaned Products: {len(final_df)}")





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


# === COMBINE ALL FILES ===
final_df = pd.concat(all_data, ignore_index=True)
final_df.drop_duplicates(subset=['Product_Name', 'Supplier'], inplace=True)
final_df.reset_index(drop=True, inplace=True)

# === ADD GLOBAL UNIQUE PRODUCT IDs ===
final_df['Product_ID'] = [f"product_{i+1}" for i in range(len(final_df))]

# Reorder columns
final_df = final_df[['Product_ID', 'Product_Name', 'Serial_Number', 'Supplier',
                     'Weight_Quantity', 'Packaging_Type', 'Units_Per_Carton']]

# === SAVE OUTPUT ===
output_file = "Servoo_Cleaned_Products_Final1.csv"
final_df.to_csv(output_file, index=False)
print(f"\n✅ Consolidated file saved as: {output_file}")

# === SUMMARY ===
print("\n📊 Sample Output:")
print(final_df.head(10))
print(f"\n🧾 Total Cleaned Products: {len(final_df)}")