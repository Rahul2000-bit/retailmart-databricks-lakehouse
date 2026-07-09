# Azure Databricks Lakehouse Projects

Two enterprise-grade Data Lakehouse projects built on **Azure Databricks** demonstrating end-to-end data engineering capabilities across Retail Analytics and Banking Fraud Detection domains.

---

## 🏪 Project 1 — RetailMart India: Retail Analytics Lakehouse

### Overview
A production-grade Data Lakehouse built for **RetailMart India**, an e-commerce retail company. This project implements a complete data engineering pipeline on **Azure Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold), governed by **Unity Catalog**.

### Architecture
Source CSVs → ADLS Gen2 → Bronze → Silver → Gold → Workflows

### Tech Stack
| Technology | Usage |
|---|---|
| Azure Databricks Premium | Core compute platform |
| Azure Data Lake Storage Gen2 | Raw data storage |
| Unity Catalog | Data governance & access control |
| Delta Lake | Storage format for all layers |
| Auto Loader | Incremental file ingestion |
| Structured Streaming | Bronze layer streaming pipeline |
| Delta Live Tables | Declarative Silver layer pipeline |
| Databricks Workflows | Pipeline orchestration & scheduling |
| PySpark & Spark SQL | Data transformation |
| Azure Service Principal | Secure ADLS authentication |

### Bronze Layer
Raw data ingested as-is from ADLS Gen2 using Auto Loader (CloudFiles).

| Table | Source | Records |
|---|---|---|
| `bronze.customers_raw` | `raw/customers/customers.csv` | 20 |
| `bronze.orders_raw` | `raw/orders/orders.csv` | 30 |
| `bronze.products_raw` | `raw/products/products.csv` | 15 |

Metadata columns added: `_ingested_at`, `_source_file`

### Silver Layer
Cleaned, typed and transformed tables built from Bronze.

| Table | Key Transformations |
|---|---|
| `silver.customers_cleaned` | Dedup, phone cleaning, date casting |
| `silver.orders_enriched` | Null filtering, discounted_amount calculation, date casting |
| `silver.products_cleaned` | Dedup, profit_margin_pct calculation |

### DLT Pipeline
Silver layer implemented as DLT streaming pipeline with data quality expectations:

| Table | Constraints |
|---|---|
| `orders_silver_dlt` | `total_amount > 0`, `order_date IS NOT NULL` |
| `products_silver_dlt` | `product_id IS NOT NULL`, `price > 0` |
| `customers_silver_dlt` | `customer_id IS NOT NULL`, `LENGTH(phone) = 10` |

### Gold Layer
| Table | Business Question |
|---|---|
| `gold.sales_summary_by_category` | Revenue, orders and profit by product category |
| `gold.customer_order_summary` | Customer spend analysis with segment ranking |
| `gold.monthly_sales_trend` | Month-over-month revenue growth analysis |

### Delta Lake Features Demonstrated
- Time Travel — Query historical table versions
- OPTIMIZE — File compaction for faster queries
- VACUUM — Removing old data files
- Schema Evolution — Adding new columns with ALTER TABLE
- DESCRIBE HISTORY — Full audit trail of table changes

### Workflow Orchestration
retailmart_daily_pipeline (Scheduled: 6:00 AM IST)
├── Task 1: 02_bronze_ingestion
├── Task 2: 03_silver_transformation  (depends on Task 1)
└── Task 3: 04_gold_layer             (depends on Task 2)

### Project Structure
RetailMart/
├── Notebooks/
│   ├── 01_setup_catalog.sql
│   ├── 02_bronze_ingestion.py
│   ├── 03_silver_transformation.py
│   ├── 04_gold_layer.sql
│   ├── 05_delta_features.sql
│   └── 06_dlt_silver_pipeline.sql
└── data/
├── customers.csv
├── orders.csv
└── products.csv

---

## 🏦 Project 2 — FinShield Bank: Fraud Detection Lakehouse

### Overview
A production-grade Fraud Detection Data Lakehouse for **FinShield Bank** processing daily banking transactions across ATM, Online, Card and Wallet channels with real-time fraud detection, RBI regulatory reporting and enterprise-grade data quality management.

### Architecture
12 Source Systems → ADLS Gen2 → Bronze (12 tables)
↓
Silver (6 tables)
Watermark + MERGE/Upsert
↓
Gold (5 tables)
Fraud Detection + RBI Reports
↓
DLT Pipeline + Workflows

### Tech Stack
| Technology | Usage |
|---|---|
| Azure Databricks Premium | Core compute platform |
| Azure Data Lake Storage Gen2 | Raw data storage |
| Unity Catalog | Data governance & lineage |
| Delta Lake | Storage format with CDC |
| Auto Loader | 12-table dynamic loop ingestion |
| MERGE/Upsert | Silver layer incremental writes |
| Watermark Processing | Incremental bronze reads |
| Partition Reprocessing | Gold layer optimization |
| Delta Live Tables | Streaming fraud detection pipeline |
| Databricks Workflows | 5-task orchestrated pipeline |
| PySpark & Spark SQL | Transformations |
| Azure Service Principal | Secure ADLS authentication |

### Bronze Layer
Dynamic loop-based Auto Loader ingesting 12 source tables with schema evolution support.

| Table | Source |
|---|---|
| `bronze.customers_raw` | Customer master data |
| `bronze.accounts_raw` | Bank accounts |
| `bronze.branches_raw` | Branch master |
| `bronze.transactions_raw` | Core transactions |
| `bronze.atm_transactions_raw` | ATM withdrawals |
| `bronze.online_transactions_raw` | Net banking/UPI |
| `bronze.card_transactions_raw` | Credit/Debit card |
| `bronze.wallet_transactions_raw` | PhonePe/Paytm/GPay |
| `bronze.merchants_raw` | Merchant master |
| `bronze.exchange_rates_raw` | Currency rates |
| `bronze.fraud_rules_raw` | Known fraud patterns |
| `bronze.blacklist_raw` | Blacklisted entities |
| `bronze.kyc_status_raw` | KYC verification |

### Silver Layer
Watermark-based incremental reads + MERGE/Upsert writes for efficient processing.

| Table | Key Transformations |
|---|---|
| `silver.customers_cleansed` | Name standardization, phone cleaning, PAN/email validation, date format fixing |
| `silver.accounts_cleansed` | Orphan removal, negative balance flag, IFSC validation |
| `silver.transactions_cleansed` | Dedup, null/negative filtering, currency conversion, blacklist flag |
| `silver.unified_transactions` | Union of ATM + Online + Card + Wallet channels |
| `silver.merchants_enriched` | Blacklist and high-risk merchant flagging |
| `silver.kyc_validated` | KYC expiry check and validity flag |

### Gold Layer
| Table | Purpose |
|---|---|
| `gold.fraud_alerts` | Real-time fraud detection with 7 rules and scoring |
| `gold.customer_risk_profile` | Risk scoring per customer (Low/Medium/High) |
| `gold.branch_performance` | Branch-wise transaction analysis |
| `gold.monthly_transaction_summary` | Month-over-month revenue trends |
| `gold.regulatory_report` | RBI compliance report with purpose codes |

### Fraud Detection Rules
| Rule | Description | Score |
|---|---|---|
| Blacklisted Account | Transaction from blacklisted account | +1 |
| Future Dated | Transaction date in future | +1 |
| High Value Night | Amount > ₹5L between 10 PM and 5 AM | +1 |
| Foreign Currency | Non-INR transaction on domestic account | +1 |
| Blacklisted Merchant | Transaction at blacklisted merchant | +1 |
| Velocity Check | More than 3 transactions within 1 hour | +1 |
| High Risk Merchant | Transaction at high-risk merchant | +1 |

### Production Scenarios Handled
| Scenario | Solution Implemented |
|---|---|
| New daily file arriving | Auto Loader + Checkpoint-based tracking |
| Late arriving data | Watermark + MERGE + Gold partition reprocessing |
| Schema evolution | Auto schema detection + mergeSchema + spark.conf |
| New source system onboarded | Dynamic loop extension + Union chain |
| Bad data spike (40% NULL) | DQ checks + Quarantine table + Pipeline alerts |
| Duplicate file upload | Checkpoint + Window dedup + MERGE protection |
| New fraud rules added | Gold layer extension — Velocity + High Risk Merchant |
| Regulatory requirement change | Purpose code derivation + RBI report update |

### DLT Pipeline
transactions_dlt (Bronze → streaming with 3 expectations: DROP invalid)
↓
fraud_alerts_dlt (transactions_dlt → fraud flagging with WARN expectation)

### Workflow Orchestration
finshield_daily_pipeline (Scheduled: 5:00 AM IST)
├── Task 1: 01_bronze_ingestion
├── Task 2: 02_silver_cleansing    (depends on Task 1)
├── Task 3: 03_silver_enrichment   (depends on Task 2)
├── Task 4: 04_gold_layer          (depends on Task 3)
└── Task 5: 05_dlt_pipeline        (depends on Task 4)

### Security & Governance
- Azure Service Principal with OAuth 2.0 for ADLS access
- Unity Catalog for fine-grained table and column access control
- CDC (Change Data Feed) enabled for audit trail
- Pipeline control table for state management
- Quarantine table for bad record tracking
- Pipeline alerts table for operational monitoring

### Project Structure
FinShield/
├── Notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleansing.py
│   ├── 03_silver_enrichment.py
│   ├── 04_gold_layer.py
│   ├── 05_dlt_pipeline.sql
│   └── 06_scenario_handling.py
└── data/
├── customers.csv
├── accounts.csv
├── branches.csv
├── transactions.csv
├── transactions_day2.csv
├── transactions_late.csv
├── transactions_day3_schema.csv
├── transactions_day4_baddata.csv
├── wallet_transactions.csv
├── atm_transactions.csv
├── online_transactions.csv
├── card_transactions.csv
├── merchants.csv
├── exchange_rates.csv
├── fraud_rules.csv
├── blacklist.csv
└── kyc_status.csv

---

## 👨‍💻 Author
**Rahul K Ranjith**
Data Engineer | Azure Databricks | PySpark | Delta Lake | Unity Catalog
