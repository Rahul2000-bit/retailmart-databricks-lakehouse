# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

df_bronze_cust = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation","abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/checkpoints/bronze_customers/schema").load("abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/raw/customers")

df_bronze_cust = df_bronze_cust.withColumn("_ingested_at",current_timestamp()).withColumn("_source_file", col("_metadata.file_path"))

df_bronze_cust.writeStream.format("delta").option("checkpointLocation", "abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/checkpoints/bronze_customers/checkpoint").trigger(availableNow=True).outputMode("append").table("retailmart_databricks.bronze.customers_raw")



# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df_bronze_ord = spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaLocation", "abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/checkpoints/bronze_orders/schema").load("abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/raw/orders")

df_bronze_ord = df_bronze_ord.withColumn("_ingested_at", current_timestamp()).withColumn("_source_file", col("_metadata.file_path"))

df_bronze_ord.writeStream.format("delta").option("checkpointLocation", "abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/checkpoints/bronze_orders/checkpoint").trigger(availableNow=True).outputMode("append").table("retailmart_databricks.bronze.orders_raw")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df_bronze_product = spark.readStream.format("cloudFiles").option("cloudFiles.format","csv").option("cloudFiles.schemaLocation","abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/checkpoints/bronze_products/schema").load("abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/raw/products")

df_bronze_product = df_bronze_product.withColumn("_ingested_at", current_timestamp()).withColumn("_source_file", col("_metadata.file_path"))

df_bronze_product.writeStream.format("delta").option("checkpointLocation", "abfss://retailmart-data@retailmartstorage.dfs.core.windows.net/checkpoints/bronze_products/checkpoint").trigger(availableNow=True).outputMode("append").table("retailmart_databricks.bronze.products_raw")