# Databricks notebook source
from pyspark.sql.functions import regexp_replace, right, to_date, current_timestamp, col, lit
silver_cust_df = spark.read.table("retailmart_databricks.bronze.customers_raw")
silver_cust_df = silver_cust_df.dropDuplicates(["customer_id"]).withColumn("phone" , right(regexp_replace(col("phone"), "[^0-9]", ""), lit(10))).withColumn("signup_date", to_date("signup_date", "yyyy-MM-dd")).withColumn("_processed_at", current_timestamp())


silver_cust_df.write.format("delta").mode("overwrite").saveAsTable("retailmart_databricks.silver.customers_cleaned")


# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_timestamp
from pyspark.sql.types import DecimalType
silver_ord_df = spark.read.table("retailmart_databricks.bronze.orders_raw")

silver_ord_df = silver_ord_df.withColumn("total_amount", col("total_amount").cast(DecimalType(10,2))).withColumn("discount_pct", col("discount_pct").cast(DecimalType(10,2)))

silver_ord_df = silver_ord_df.filter((col("total_amount").isNotNull()) & (col("total_amount") > 0)).withColumn("discounted_amount", (col("total_amount") - (col("total_amount") * col("discount_pct") / 100)).cast(DecimalType(10,2))).withColumn("order_date", to_date("order_date", "yyyy-MM-dd")).withColumn("delivery_date", to_date("delivery_date", "yyyy-MM-dd")).withColumn("_processed_at", current_timestamp())

silver_ord_df.write.format("delta").mode("overwrite").saveAsTable("retailmart_databricks.silver.orders_enriched")



# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import DecimalType

silver_product_df = spark.read.table("retailmart_databricks.bronze.products_raw")

silver_product_df = silver_product_df.withColumn("price", col("price").cast(DecimalType(10,2))).withColumn("cost_price", col("cost_price").cast(DecimalType(10,2)))

silver_product_df = silver_product_df.dropDuplicates(["product_id"]).withColumn("profit_margin_pct", ((col("price") - col("cost_price"))/ col("price") * 100 ).cast(DecimalType(10,2))).withColumn("_processed_at", current_timestamp())

silver_product_df.write.format("delta").mode("overwrite").saveAsTable("retailmart_databricks.silver.products_cleaned")