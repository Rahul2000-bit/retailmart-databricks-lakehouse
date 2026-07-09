# Databricks notebook source
from pyspark.sql.functions import *
df_atm = (spark.read.format("delta").table("finshield_bank.bronze.atm_transactions_raw")
          .select("atm_txn_id","account_id","transaction_date","transaction_time","amount","currency","status")
          .withColumnRenamed("atm_txn_id","txn_id")
          .withColumn("transaction_channel", lit("ATM"))
          .withColumn("wallet_provider", lit(None).cast("string"))
          .withColumn("upi_vpa", lit(None).cast("string"))
         )

df_card =  (spark.read.format("delta").table("finshield_bank.bronze.card_transactions_raw")
            .select("card_txn_id","account_id","transaction_date","transaction_time","amount","currency","status")
            .withColumnRenamed("card_txn_id","txn_id")
            .withColumn("transaction_channel", lit("CARD"))
            .withColumn("wallet_provider", lit(None).cast("string"))
            .withColumn("upi_vpa", lit(None).cast("string"))
           )
df_online = (spark.read.format("delta").table("finshield_bank.bronze.online_transactions_raw")
            .select("online_txn_id","account_id","transaction_date","transaction_time","amount","currency","status")
            .withColumnRenamed("online_txn_id","txn_id")
            .withColumn("transaction_channel", lit("ONLINE"))
            .withColumn("wallet_provider", lit(None).cast("string"))
            .withColumn("upi_vpa", lit(None).cast("string"))
            )

df_wallet = (spark.read.format("delta").table("finshield_bank.bronze.wallet_transactions_raw")
            .select("wallet_txn_id","account_id","transaction_date","transaction_time","amount","currency","status","wallet_provider","upi_vpa")
            .withColumnRenamed("wallet_txn_id","txn_id")
            .withColumn("transaction_channel", lit("WALLET")) 
            )            

df = (df_atm.unionByName(df_card).unionByName(df_online).unionByName(df_wallet)
      .filter(col("status").isin("Success","Approved"))
      .withColumn("amount", when(col("amount") == "NULL", None).otherwise(col("amount")))
      .filter(col("amount").isNotNull())
      .withColumn("_processed_at", current_timestamp())
      )

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("finshield_bank.silver.unified_transactions")


# COMMAND ----------

from pyspark.sql.functions import *
df_black = (spark.read.format("delta").table("finshield_bank.bronze.blacklist_raw")
            .filter(col("entity_type") == "Merchant")
            .select("entity_type","entity_id")
           )
df_merchant =  spark.read.format("delta").table("finshield_bank.bronze.merchants_raw")
df_merchant =  (df_merchant.join(df_black, on= (df_merchant.merchant_id == df_black.entity_id), how="left")
                .withColumn("is_blacklisted", when(col("entity_id").isNull(), False).otherwise(True)).drop("entity_id","entity_type")
                .withColumn("is_high_risk", when(col("risk_level") == "High" , True).otherwise(False))
                .filter(col("is_active") == "true")
                .withColumn("_processed_at", current_timestamp())
               )
df_merchant.write.format("delta").mode("overwrite").saveAsTable("finshield_bank.silver.merchants_enriched")

# COMMAND ----------

from pyspark.sql.functions import *

df_kyc = spark.read.format("delta").table("finshield_bank.bronze.kyc_status_raw")
df_cust = spark.read.format("delta").table("finshield_bank.silver.customers_cleansed")
df_cust = (df_cust.select("customer_id", "first_name", "last_name", "kyc_status", "customer_segment")
          .withColumnRenamed("kyc_status", "customer_kyc_status"))
df_kyc = (df_kyc.join(df_cust, on = "customer_id", how="left")
          .withColumn("expiry_date", to_date(col("expiry_date"), "yyyy-MM-dd"))
          .withColumn("verification_date", to_date(col("verification_date"),"yyyy-MM-dd"))
          .withColumn("is_kyc_expired", when(col("expiry_date") < current_date(), True).otherwise(False))
          .withColumn("is_kyc_valid", when((col("customer_kyc_status") == "Verified") & (col("is_kyc_expired") == "false"), True).otherwise(False))
          .withColumn("_processed_at", current_timestamp())
          
          )
df_kyc.write.format("delta").mode("overwrite").saveAsTable("finshield_bank.silver.kyc_validated")