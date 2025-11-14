import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# --- Load environment variables ---
load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME", "finance_tracker")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "expenses")

# --- Connect to MongoDB ---
client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# --- Ask for input ---
print("Enter a new expense:")
amount = float(input("Amount: "))
category = input("Category (e.g., Food, Transport, Rent): ")
seller = input("Seller: ")
receipt_number = input("Receipt Number: ")
item_swedish = input("Item in swedish: ")
item = input("Item: ")
quantity = int(input("Quantity: "))
unit = input("Unit (e.g., pcs, kg, liters): ")
date_str = input("Date (YYYY-MM-DD) [press Enter for today]: ")

# Use today's date if none provided
if not date_str.strip():
    date = datetime.now()
else:
    date = datetime.strptime(date_str, "%Y-%m-%d")

# --- Create the document ---
expense_doc = {
    "date": date,
    "seller": seller,
    "receipt_number": receipt_number,
    "category": category,
    "item_swedish": item_swedish,
    "item": item,
    "amount": amount,
    "quantity": quantity,
    "unit": unit,
    "created_at": datetime.now()
}

# --- Insert into MongoDB ---
result = collection.insert_one(expense_doc)

print(f"✅ Expense saved successfully with ID: {result.inserted_id}")