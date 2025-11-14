import os
import shutil
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
ONE_DRIVE_FILE_PATH = os.getenv("ONE_DRIVE_FILE_PATH")

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

excel_file = "expenses.xlsx"
destination = os.path.join(os.getcwd(), excel_file)

# Copy the content of
# source to destination

try:
    shutil.copy(ONE_DRIVE_FILE_PATH, destination)
    print("File copied successfully.")

# If source and destination are same
except shutil.SameFileError:
    print("Source and destination represents the same file.")

# If there is any permission issue
except PermissionError:
    print("Permission denied.")

# For other errors
except:
    print("Error occurred while copying file.")

required_columns = ["data", "mês", "comerciante", "# talão", "categoria", "item sueco", "item", "quantidade", "unidade", "sek"]

preview = pd.read_excel(destination, header=None, nrows=10)
header_row_index = None
row_str = []

for i, row in preview.iterrows():
    row_str = row.astype(str).str.lower().tolist()
    print(f"Row {i}: {row_str}")
    if any(h in row_str for h in required_columns):
        header_row_index = i
        break

if header_row_index is None:
    raise ValueError("❌ Could not detect header row in Excel file.")

print(f"✅ Detected header row at index: {header_row_index}")

df = pd.read_excel(excel_file, header=header_row_index)

df.columns = df.columns.str.strip().str.lower()

df = pd.read_excel(excel_file)

print(f"Columns: {df.columns}")

print("🔍 Detecting columns...")
print(f"Headers in line 3: {row_str}")

col_map = {
    "Data": row_str.index("data"),
    "Comerciante": row_str.index("comerciante"),
    "# talão": row_str.index("# talão"),
    "Categoria": row_str.index("categoria"),
    "Item Sueco": row_str.index("item sueco"),
    "Item": row_str.index("item"),
    "Quantidade": row_str.index("quantidade"),
    "Unidade": row_str.index("unidade"),
    "SEK": row_str.index("sek"),
}

print("📊 Preview of data:")
print(col_map)

#if not all(col_map.values()):
#    raise ValueError(f"❌ Missing one or more required columns. Found: {col_map}")

print(f"✅ Detected column mapping: {col_map}")

documents = []
# Loop starting at row index 2
for row_index in range(2, len(df)):
    row = df.iloc[row_index]
    print("Processing row:", row.tolist()[:14])

    # Skip completely empty rows
    if row.isna().all():
        continue

    # ------- Parse Values Safely -------
    # Date
    try:
        date = pd.to_datetime(row[col_map["Data"]])
    except Exception:
        date = datetime.now()

    # SEK
    try:
        sek = round(float(row[col_map["SEK"]]), 2)
    except Exception:
        sek = 0.0

    # Receipt number
    try:
        receipt_number = int(row[col_map["# talão"]])
    except Exception:
        receipt_number = None
    
    # ------- Construct Document -------
    doc = {
        "date": date,
        "seller": str(row[col_map["Comerciante"]]) if not pd.isna(row[col_map["Comerciante"]]) else "",
        "receipt_number": receipt_number,
        "category": str(row[col_map["Categoria"]]) if not pd.isna(row[col_map["Categoria"]]) else "",
        "item_swedish": str(row[col_map["Item Sueco"]]) if not pd.isna(row[col_map["Item Sueco"]]) else "",
        "item": str(row[col_map["Item"]]) if not pd.isna(row[col_map["Item"]]) else "",
        "quantity": str(row[col_map["Quantidade"]]) if not pd.isna(row[col_map["Quantidade"]]) else "",
        "unit": str(row[col_map["Unidade"]]) if not pd.isna(row[col_map["Unidade"]]) else "",
        "SEK": sek,
        "created_at": datetime.now()
    }

    documents.append(doc)

if documents:
    result = collection.insert_many(documents)
    print(f"✅ Successfully inserted {len(result.inserted_ids)} records into MongoDB.")
else:
    print("⚠️ No data found to insert.")