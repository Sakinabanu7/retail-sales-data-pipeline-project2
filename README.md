 
 
# Retail Sales Data Pipeline – GCP Data Engineering Project

## Objective

This project simulates the implementation of an end-to-end data pipeline for retail sales using **Apache Beam** within a **Google Cloud Platform (GCP)** architecture. It demonstrates data ingestion, cleaning, transformation, validation, and storage in a scalable, production-ready manner.

---

## Architecture Overview

```

\[CSV / API / On-Prem SQL Server]
⬇
GCS (Bronze Layer)
⬇
Apache Beam on Dataflow (Transformation)
⬇
GCS (Silver Layer)
⬇
Cloud SQL (PostgreSQL)
⬇
Looker/Tableau

```

- **Bronze Layer (GCS)**: Stores raw, unmodified ingested data.
- **Silver Layer (GCS)**: Contains cleansed and transformed data.
- **Cloud SQL**: Final curated destination for reporting and analysis.
- **Visualization Tools**: Looker or Tableau connect to Cloud SQL.

---

## Tools & Technologies

| Tool/Service        | Role                                          |
|---------------------|-----------------------------------------------|
| **Apache Beam**     | Distributed data processing and transformation |
| **Google Cloud Storage (GCS)** | Stores raw and cleaned data          |
| **Cloud SQL (PostgreSQL)**     | Stores final processed data           |
| **Pub/Sub + Cloud Scheduler**  | Automate scheduled pipeline triggers  |
| **Python (pandas, psycopg2)**  | Local development and DB integration  |
| **Draw.io**         | Architecture diagram                         |
| **Looker/Tableau**  | BI reporting and dashboards                   |

---

## Datasets

- `sales_sample.json` – Raw sales data (mixed schema structure)
- `customers_sample.csv` – Customer details (lookup)
- `products_sample.csv` – Product details (lookup)

---

## Data Cleaning & Transformation (Apache Beam)

### Features:

- Parsed mixed field formats (e.g., `SaleID` vs `id`, `value1`, etc.)
- Joined customer and product data via lookup tables
- Handled missing fields (`CustomerID`, `ProductID`, etc.)
- Applied default values or skipped invalid rows
- Cleaned phone numbers using regex
- Normalized sale dates to `YYYY-MM-DD`
- Wrote clean, structured output to `final_cleaned_sales.csv` (Silver Layer)

### Issues Faced & Resolved:

| Issue | Solution |
|-------|----------|
| Inconsistent schema in JSON | Used conditional key parsing (`id`, `value1`, etc.) |
| Missing or null values | Applied filtering and default date (`1970-01-01`) |
| Invalid foreign keys | Validated against lookup dictionaries |
| Duplicates in lookup data | Dropped using pandas `.drop_duplicates()` |
| Non-standard phone numbers | Regex cleaning to 10-digit format |
| Data type mismatches | Explicit type casting (int, float, str, date) |

---

## Data Validation Checks

- ✅ Null checks on critical fields
- ✅ Type conversions for Quantity, Amount, and Dates
- ✅ FK validation with lookup dictionaries
- ✅ Format checks on customer name, phone, and product name
- ✅ Logging for failed rows during transformation

---

## Output: Cleaned Silver Layer CSV

Example Row:
```

31.0,51.0,John Smith,[john@example.com](mailto:john@example.com),1234567890,3,Tablet,Electronics,2023-10-31,4,3936.25

````

Saved as: `final_cleaned_sales.csv`

---

## Target Table Schema (Cloud SQL Simulation in PostgreSQL)

```sql
CREATE TABLE sales_cleaned (
    SaleID INT,
    CustomerID INT,
    CustomerName VARCHAR(100),
    Email VARCHAR(100),
    Phone VARCHAR(20),
    ProductID INT,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    SaleDate DATE,
    Quantity INT,
    TotalAmount DECIMAL(10,2)
);
````

---

## 🚀 Pipeline Execution Flow

### Step 1: Transform Raw Data with Beam

```bash
python beam_clean_pipeline.py
```

* Loads raw JSON (`sales_sample.json`)
* Cleans and merges with lookup tables
* Outputs `final_cleaned_sales.csv` (Silver Layer)

---

### Step 2: Load Cleaned Data into PostgreSQL


python load_to_postgres.py
```

* Skips CSV header
* Inserts rows into `sales_cleaned` table using Beam's `ParDo` and `psycopg2`

---

## Optional BI Layer

* Connect `sales_cleaned` table to **Looker** or **Tableau**
* Create dashboards for:

  * Sales trends over time
  * Top-selling products and categories
  * Top customers by revenue
  * Sales by country or city (if location data is added)

---

## Interview Discussion Points

| Topic           | Talking Point                                                             |
| --------------- | ------------------------------------------------------------------------- |
| GCP Design      | “Bronze, Silver, and Cloud SQL layers provide separation of concerns.”    |
| Apache Beam     | “I used `Map`, `Filter`, and `ParDo` transforms to clean and load data.”  |
| Data Validation | “Ensured type safety, FK checks, and cleaned dirty inputs.”               |
| Reusability     | “Input files can change without altering core logic.”                     |
| Local Testing   | “I validated logic locally using DirectRunner before cloud deployment.”   |
| Real-World Prep | “The pipeline can be deployed to GCP by changing runner and I/O targets.” |

---

## Project Files

| File                      | Purpose                          |
| ------------------------- | -------------------------------- |
| `beam_clean_pipeline.py`  | Cleans raw sales data using Beam |
| `load_to_postgres.py`     | Loads clean CSV to PostgreSQL    |
| `sales_sample.json`       | Source sales data                |
| `customers_sample.csv`    | Customer lookup                  |
| `products_sample.csv`     | Product lookup                   |
| `final_cleaned_sales.csv` | Silver-layer output              |
| `README.md`               | Project documentation            |

---

## Summary

This project showcases the design of a modular, scalable, and production-ready data pipeline using GCP principles. The structure supports seamless migration to cloud-based execution via Dataflow and Cloud SQL, while maintaining flexibility and clarity in local development.

```
 
