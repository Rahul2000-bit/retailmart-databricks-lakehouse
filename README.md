# RetailMart Lakehouse — Azure Databricks End-to-End Project

## 🏢 Project Overview
A production-grade Data Lakehouse built for **RetailMart India**, an e-commerce retail company. This project implements a complete data engineering pipeline on **Azure Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold), governed by **Unity Catalog**.

---

## 🏗️ Architecture
Source System (CSV Files)

↓

ADLS Gen2 Storage (raw/)

↓

[BRONZE LAYER]  — Raw ingestion via Auto Loader (Structured Streaming)

↓

[SILVER LAYER]  — Cleaned & transformed Delta tables (Batch + DLT)

↓

[GOLD LAYER]    — Business-ready aggregated tables

↓

[WORKFLOWS]     — Orchestrated & scheduled daily pipeline

---

## ⚙️ Tech Stack

| Technology | Usage |
|---|---|
| Azure Databricks (Premium) | Core compute platform |
| Azure Data Lake Storage Gen2 | Raw data storage |
| Unity Catalog | Data governance & access control |
| Delta Lake | Storage format for all layers |
| Auto Loader | Incremental file ingestion |
| Structured Streaming | Bronze layer streaming pipeline |
| Delta Live Tables (DLT) | Declarative Silver layer pipeline |
| Databricks Workflows | Pipeline orchestration & scheduling |
| PySpark & Spark SQL | Data transformation |
| Azure Service Principal | Secure ADLS authentication |

---

## 📁 Project Structure
retailmart-databricks-lakehouse/

├── Notebooks/

│   ├── 01_setup_catalog.sql       — Unity Catalog setup (Bronze/Silver/Gold schemas)

│   ├── 02_bronze_ingestion.py     — Auto Loader streaming ingestion to Bronze

│   ├── 03_silver_transformation.py— Batch transformations to Silver layer

│   ├── 04_gold_layer.sql          — Business aggregations to Gold layer

│   ├── 05_delta_features.sql      — Delta Lake features (Time Travel, Optimize, Vacuum)

│   └── 06_dlt_silver_pipeline.sql — Delta Live Tables pipeline with data quality

├── data/

│   ├── customers.csv              — Customer master data (20 records)

│   ├── orders.csv                 — Orders transactional data (30 records)

│   └── products.csv               — Product catalog (15 records)

└── README.md

---

## 🥉 Bronze Layer
Raw data ingested **as-is** from ADLS Gen2 using **Auto Loader** (CloudFiles).

| Table | Source | Records |
|---|---|---|
| `bronze.customers_raw` | `raw/customers/customers.csv` | 20 |
| `bronze.orders_raw` | `raw/orders/orders.csv` | 30 |
| `bronze.products_raw` | `raw/products/products.csv` | 15 |

Metadata columns added: `_ingested_at`, `_source_file`

---

## 🥈 Silver Layer
Cleaned, typed and transformed tables built from Bronze.

| Table | Key Transformations |
|---|---|
| `silver.customers_cleaned` | Dedup, phone cleaning, date casting |
| `silver.orders_enriched` | Null filtering, discounted_amount calculation, date casting |
| `silver.products_cleaned` | Dedup, profit_margin_pct calculation |

### Delta Live Tables (DLT) Pipeline
Silver layer also implemented as a **DLT streaming pipeline** with data quality expectations:

| Table | Constraints |
|---|---|
| `orders_silver_dlt` | `total_amount > 0`, `order_date IS NOT NULL` |
| `products_silver_dlt` | `product_id IS NOT NULL`, `price > 0` |
| `customers_silver_dlt` | `customer_id IS NOT NULL`, `LENGTH(phone) = 10` |

---

## 🥇 Gold Layer
Business-ready aggregated tables for reporting and dashboards.

| Table | Business Question |
|---|---|
| `gold.sales_summary_by_category` | Revenue, orders and profit by product category |
| `gold.customer_order_summary` | Customer spend analysis with segment ranking |
| `gold.monthly_sales_trend` | Month-over-month revenue growth analysis |

---

## ⚡ Delta Lake Features Demonstrated
- **Time Travel** — Query historical table versions
- **OPTIMIZE** — File compaction for faster queries
- **VACUUM** — Removing old data files
- **Schema Evolution** — Adding new columns with ALTER TABLE
- **DESCRIBE HISTORY** — Full audit trail of table changes

---

## 🔄 Workflow Orchestration
Automated daily pipeline using **Databricks Workflows**:
retailmart_daily_pipeline (Scheduled: 6:00 AM IST)

├── Task 1: 02_bronze_ingestion

├── Task 2: 03_silver_transformation  (depends on Task 1)

└── Task 3: 04_gold_layer             (depends on Task 2)

---

## 🔐 Security
- Azure Service Principal with OAuth 2.0 authentication
- Unity Catalog for fine-grained access control
- ADLS Gen2 with hierarchical namespace

---

## 👨‍💻 Author
**Rahul K Ranjith**
Data Engineer | Azure Databricks | PySpark | Delta Lake
