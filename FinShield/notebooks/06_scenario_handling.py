# Databricks notebook source
# MAGIC %sql
# MAGIC -- Enable Change Data Feed on transactions bronze table
# MAGIC ALTER TABLE finshield_bank.bronze.transactions_raw
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = True);
# MAGIC
# MAGIC -- Verify it's enabled
# MAGIC SHOW TBLPROPERTIES finshield_bank.bronze.transactions_raw;

# COMMAND ----------

from pyspark.sql.functions import *   


df_cdc = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 6)
    .table("finshield_bank.bronze.transactions_raw")
)

display(df_cdc.select(
    "transaction_id",
    "amount", 
    "transaction_date",
    "_change_type",
    "_commit_version",
    "_commit_timestamp"
).orderBy("_commit_version"))

# COMMAND ----------

# In production — only process new inserts

df_new_inserts = df_cdc.filter(col("_change_type") == "insert")
df_updates = df_cdc.filter(col("_change_type") == "update_postimage")
df_deletes = df_cdc.filter(col("_change_type") == "delete")

print(f"New inserts: {df_new_inserts.count()}")
print(f"Updates: {df_updates.count()}")
print(f"Deletes: {df_deletes.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finshield_bank.bronze.pipeline_control (
# MAGIC     pipeline_name STRING,
# MAGIC     table_name STRING,
# MAGIC     last_processed_version LONG,
# MAGIC     last_processed_timestamp TIMESTAMP,
# MAGIC     last_run_status STRING,
# MAGIC     records_processed LONG,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from pyspark.sql.functions import *

#Get current version of bronze transactions
current_version = spark.sql("""
    SELECT MAX(version) as current_version 
    FROM (DESCRIBE HISTORY finshield_bank.bronze.transactions_raw)
""").collect()[0]["current_version"]

records_processed = df_new_inserts.count()

spark.sql(f"""
    MERGE INTO finshield_bank.bronze.pipeline_control AS target
    USING (SELECT 
        'finshield_daily_pipeline' as pipeline_name,
        'transactions_raw' as table_name,
        {current_version} as last_processed_version,
        current_timestamp() as last_processed_timestamp,
        'SUCCESS' as last_run_status,
        {records_processed} as records_processed,
        current_timestamp() as updated_at
    ) AS source
    ON target.pipeline_name = source.pipeline_name 
    AND target.table_name = source.table_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Control table updated — Version: {current_version}, Records: {records_processed}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM finshield_bank.bronze.pipeline_control

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS finshield_bank.bronze.pipeline_alerts (
# MAGIC     alert_id STRING,
# MAGIC     alert_type STRING,
# MAGIC     severity STRING,
# MAGIC     table_name STRING,
# MAGIC     alert_message STRING,
# MAGIC     records_affected LONG,
# MAGIC     detected_at TIMESTAMP,
# MAGIC     resolved_at TIMESTAMP,
# MAGIC     status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from datetime import datetime, date
from pyspark.sql.functions import *
import uuid

def detect_and_log_late_data(table_name):
    today = date.today()
    
    result = spark.sql(f"""
        SELECT 
            COUNT(*) as record_count,
            MIN(transaction_date) as earliest_txn_date,
            MAX(_ingested_at) as latest_ingestion
        FROM finshield_bank.bronze.{table_name}
        WHERE DATE(_ingested_at) = '{today}'
    """).collect()[0]
    
    if result["earliest_txn_date"] and str(result["earliest_txn_date"]) < str(today):
        days_late = (today - result["earliest_txn_date"]).days
        alert_message = f"Late data detected! Transactions from {result['earliest_txn_date']} arrived today. {days_late} day(s) late. {result['record_count']} records affected."
        
        # Log alert to table
        spark.sql(f"""
            INSERT INTO finshield_bank.bronze.pipeline_alerts VALUES (
                '{str(uuid.uuid4())}',
                'LATE_DATA',
                'HIGH',
                '{table_name}',
                '{alert_message}',
                {result['record_count']},
                current_timestamp(),
                NULL,
                'OPEN'
            )
        """)
        print(f"ALERT LOGGED: {alert_message}")
        return True
    else:
        print(f" No late data detected for {table_name}")
        return False

# Run detection
detect_and_log_late_data("transactions_raw")           

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM finshield_bank.bronze.pipeline_alerts
# MAGIC ORDER BY detected_at DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quarantine table for bad records
# MAGIC CREATE TABLE IF NOT EXISTS finshield_bank.bronze.quarantine_records (
# MAGIC     source_table     STRING,
# MAGIC     transaction_id   STRING,
# MAGIC     account_id       STRING,
# MAGIC     amount           STRING,
# MAGIC     currency         STRING,
# MAGIC     rejection_reason STRING,
# MAGIC     raw_data         STRING,
# MAGIC     detected_at      TIMESTAMP,
# MAGIC     status           STRING
# MAGIC )
# MAGIC USING DELTA;