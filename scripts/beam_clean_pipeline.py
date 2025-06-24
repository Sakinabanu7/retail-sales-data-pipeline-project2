import apache_beam as beam
import pandas as pd
import json
import re
from datetime import datetime
from apache_beam.options.pipeline_options import PipelineOptions

# ------------------- CLEAN CUSTOMER DATA -------------------
def load_clean_customers():
    df = pd.read_csv("customers_sample.csv").drop_duplicates()
    df = df.dropna(subset=["CustomerID", "CustomerName", "Email", "Phone"])

    def clean_phone(phone):
        digits = re.sub(r"\D", "", str(phone))
        return digits if len(digits) == 10 else None

    df["Phone"] = df["Phone"].apply(clean_phone)
    df = df.dropna(subset=["Phone"])
    df["CustomerName"] = df["CustomerName"].astype(str).str.strip().str[:10]
    return df.set_index("CustomerID").to_dict(orient="index")

# ------------------- CLEAN PRODUCT DATA -------------------
def load_clean_products():
    df = pd.read_csv("products_sample.csv").drop_duplicates()
    df = df.dropna(subset=["ProductID", "ProductName", "Price"])
    df["ProductName"] = df["ProductName"].astype(str).str.strip().str[:10]
    return df.set_index("ProductID").to_dict(orient="index")

# Load cleaned lookup dictionaries
customer_dict = load_clean_customers()
product_dict = load_clean_products()

# ------------------- CLEAN SALES RECORD -------------------
def clean_and_merge_sales(record):
    try:
        # Normalize alternate key names (API-style)
        if "id" in record:
            record["SaleID"] = record.get("id")
            record["CustomerID"] = record.get("value1")
            record["ProductID"] = record.get("value2")
            record["SaleDate"] = record.get("date")
            record["Quantity"] = record.get("value3") or record.get("category")
            record["TotalAmount"] = record.get("amount")

        # Check required fields
        if not all([record.get("CustomerID"), record.get("ProductID"), record.get("Quantity"), record.get("TotalAmount")]):
            return None

        quantity = int(float(record["Quantity"]))
        amount = float(record["TotalAmount"])

        try:
            sale_date = datetime.strptime(record["SaleDate"], "%Y-%m-%d").strftime("%Y-%m-%d")
        except:
            sale_date = "1970-01-01"

        customer = customer_dict.get(record["CustomerID"])
        product = product_dict.get(record["ProductID"])
        if not customer or not product:
            return None  # Enforce FK match

        return f'{record["SaleID"]},{record["CustomerID"]},{customer["CustomerName"]},{customer["Email"]},{customer["Phone"]},' \
               f'{record["ProductID"]},{product["ProductName"]},{product["Category"]},{sale_date},{quantity},{amount}'

    except Exception as e:
        print("Error cleaning record:", e)
        return None

# ------------------- BEAM PIPELINE -------------------
def run():
    options = PipelineOptions(
        runner='DirectRunner',  # LOCAL execution
        save_main_session=True
    )

    # ✅ Fix: load sales JSON array properly
    with open("sales_sample.json", "r") as f:
        sales_data = json.load(f)  # Load the entire JSON array

    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Load Sales Records' >> beam.Create(sales_data)
            | 'Clean & Merge Records' >> beam.Map(clean_and_merge_sales)
            | 'Drop Invalids' >> beam.Filter(lambda x: x is not None)
            | 'Write Cleaned Output' >> beam.io.WriteToText("final_cleaned_sales", file_name_suffix=".csv", shard_name_template="")
        )

if __name__ == '__main__':
    run()
clean_and_merge_sales