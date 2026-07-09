# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

# Step 1 — Define widget FIRST
dbutils.widgets.text("tables_to_process", "all")
filter_tables = dbutils.widgets.get("tables_to_process")

# Step 2 — Define tables list
tables = [
    {"table_name": "customers_raw",          "source_folder": "customers"},
    {"table_name": "accounts_raw",           "source_folder": "accounts"},
    {"table_name": "branches_raw",           "source_folder": "branches"},
    {"table_name": "transactions_raw",       "source_folder": "transactions"},
    {"table_name": "atm_transactions_raw",   "source_folder": "atm_transactions"},
    {"table_name": "online_transactions_raw","source_folder": "online_transactions"},
    {"table_name": "card_transactions_raw",  "source_folder": "card_transactions"},
    {"table_name": "merchants_raw",          "source_folder": "merchants"},
    {"table_name": "exchange_rates_raw",     "source_folder": "exchange_rates"},
    {"table_name": "fraud_rules_raw",        "source_folder": "fraud_rules"},
    {"table_name": "blacklist_raw",          "source_folder": "blacklist"},
    {"table_name": "kyc_status_raw",         "source_folder": "kyc_status"},
    {"table_name": "wallet_transactions_raw","source_folder": "wallet_transactions"}
]

# Step 3 — Loop with filter
for table in tables:
    if filter_tables != "all" and table["table_name"] != filter_tables:
        continue

    file_location = f"abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/finshield/raw/{table['source_folder']}"
    schema_location = f"abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/finshield/checkpoints/bronze_{table['source_folder']}/schema"
    checkpoint_location = f"abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/finshield/checkpoints/bronze_{table['source_folder']}/checkpoint"
    delta_table = f"finshield_bank.bronze.{table['table_name']}"

    df = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("cloudFiles.schemaLocation", schema_location)
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
          .load(file_location))

    df = (df.withColumn("_ingested_at", current_timestamp())
           .withColumn("_source_file", col("_metadata.file_path")))

    query = (df.writeStream.format("delta")
             .option("checkpointLocation", checkpoint_location)
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .outputMode("append")
             .table(delta_table))
    query.awaitTermination()