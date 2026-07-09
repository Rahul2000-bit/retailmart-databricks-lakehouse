# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
df_tc = spark.read.format("delta").table("finshield_bank.silver.transactions_cleansed")
df_tc = (df_tc.select("transaction_id", "account_id","merchant_id", "amount_in_inr", "transaction_date", 
         "transaction_time", "currency", "is_blacklisted_account", "is_future_dated", "status")
         .withColumnRenamed("transaction_id","txn_id")
         .withColumnRenamed("amount_in_inr", "amount")
         .withColumn("transaction_channel", lit(None))
         .withColumn("wallet_provider", lit(None).cast("string"))
         .withColumn("upi_vpa", lit(None).cast("string"))
         )


df_ut = spark.read.format("delta").table("finshield_bank.silver.unified_transactions")
df_ut= (df_ut.select("txn_id", "account_id", "amount", "transaction_date", 
        "transaction_time", "currency", "transaction_channel", "status", "wallet_provider", "upi_vpa")
        .withColumn("is_future_dated", lit(False))
        .withColumn("is_blacklisted_account", lit(False))
        .withColumn("merchant_id", lit(None))
        )

df_me = spark.read.format("delta").table("finshield_bank.silver.merchants_enriched")
df_me = df_me.select("merchant_id", "is_blacklisted","is_high_risk")      

df_all_txns = df_tc.unionByName(df_ut)
df =( df_all_txns.join(df_me, on="merchant_id", how="left")
      .withColumn("transaction_timestamp", to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")), "yyyy-MM-dd HH:mm:ss")))

velocity_window = Window.partitionBy("account_id").orderBy(col("transaction_timestamp").cast("long")).rangeBetween(-3600, 0)      


df =(df.withColumn("is_high_value_night", (col("amount") > 500000) & ((hour(col("transaction_timestamp"))>= 22) | (hour(col("transaction_timestamp")) < 5)))
      .withColumn("is_foreign_currency", col("currency") != "INR")
      .withColumn("txn_count_1hr", count("txn_id").over(velocity_window))
      .withColumn("is_velocity_breach", col("txn_count_1hr") > 3)
      .withColumn("is_high_risk_merchant", when(col("is_high_risk") == True , True).otherwise(False))

      .withColumn("fraud_flag", col("is_blacklisted_account") | col("is_future_dated") | col("is_high_value_night") | col("is_foreign_currency") | col("is_blacklisted") | col("is_velocity_breach") | col("is_high_risk_merchant"))
      .withColumn("fraud_flag", when(col("fraud_flag").isNull(), False).when(col("fraud_flag") == False, False).otherwise(True))
      .withColumn("fraud_score", when(col("is_blacklisted_account"),1).otherwise(0) + when(col("is_future_dated"),1).otherwise(0) + when(col("is_high_value_night"),1).otherwise(0) + when(col("is_foreign_currency"),1).otherwise(0) + when(col("is_blacklisted"),1).otherwise(0) + when(col("is_velocity_breach"),1).otherwise(0) + when(col("is_high_risk_merchant"),1).otherwise(0))
      .withColumn("fraud_reason", when(
        col("fraud_flag"),
        concat_ws(", ",
            when(col("is_blacklisted_account"), "Blacklisted Account"),
            when(col("is_future_dated"), "Future Dated"),
            when(col("is_high_value_night"), "High Value Night Transaction"),
            when(col("is_foreign_currency"), "Foreign Currency"),
            when(col("is_blacklisted"), "Blacklisted Merchant"),
            when(col("is_velocity_breach"), "Velocity Check Failed"),
            when(col("is_high_risk_merchant"), "High Risk Merchant")

        )
    ).otherwise("No Fraud")
                   
)
     
      )

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("finshield_bank.gold.fraud_alerts")

# COMMAND ----------

from pyspark.sql.functions import *
df_cc = (spark.read.format("delta").table("finshield_bank.silver.customers_cleansed")
         .select("customer_id", "first_name", "last_name", "city", "kyc_status", "credit_score","customer_segment")
         )
df_ac = (spark.read.format("delta").table("finshield_bank.silver.accounts_cleansed")
        .select("account_id","customer_id","account_type","account_number","is_negative_balance") 
        )

df_kv = (spark.read.format("delta").table("finshield_bank.silver.kyc_validated")
        .select("customer_id","is_kyc_expired","is_kyc_valid")
        )

df_tc = (spark.read.format("delta").table("finshield_bank.silver.transactions_cleansed")
        .select("transaction_id","account_id","amount_in_inr","is_blacklisted_account") 
        )

df_fa = (spark.read.format("delta").table("finshield_bank.gold.fraud_alerts")
        .select("txn_id","fraud_flag")
        .withColumnRenamed("txn_id","transaction_id")
        )        

df = ( df_cc.join(df_ac, on="customer_id", how="left")
            .join(df_kv, on="customer_id", how="left")
            .join(df_tc, on="account_id", how="left")
            .join(df_fa, on="transaction_id", how="left")
     )
df_txn_agg = (df.groupBy("customer_id").agg(countDistinct("account_id").alias("total_accounts"), count("transaction_id").alias          ("total_transaction"), sum("amount_in_inr").alias("total_amount_spent"), max("is_negative_balance").alias("has_negative_balance"), max("is_blacklisted_account").alias("has_blacklisted_account"), max("fraud_flag").alias("has_fraud_transaction"), max("is_kyc_expired").alias("kyc_expired"), max("is_kyc_valid").alias("kyc_valid"))
        )

df_final =  (df_txn_agg.join(df_cc, on="customer_id", how="left")
            .withColumn("risk_score", when(col("kyc_expired"), 2).otherwise(0)
                        + when(col("kyc_status") == "Pending", 2).otherwise(0)
                        + when(col("has_negative_balance"), 1).otherwise(0)
                        + when(col("has_blacklisted_account"), 2).otherwise(0)
                        + when(col("has_fraud_transaction"), 3).otherwise(0)
                        + when(col("credit_score").cast("int") < 600, 1).otherwise(0)
                       ) 
            .withColumn("risk_category", when(col("risk_score") <= 2,"Low").when((col("risk_score") > 2) & (col("risk_score") <= 4), "Medium").otherwise("High"))
            .withColumn("_processed_at", current_timestamp())
            )
df_final.write.format("delta").mode("overwrite").saveAsTable("finshield_bank.gold.customer_risk_profile")           


# COMMAND ----------

from pyspark.sql.functions import*
df_ac = (spark.read.format("delta").table("finshield_bank.silver.accounts_cleansed")
        .select("account_id","customer_id","branch_id","is_negative_balance")
        )

df_tc = (spark.read.format("delta").table("finshield_bank.silver.transactions_cleansed")
        .select("transaction_id","account_id","amount_in_inr") 
        )

df_br = (spark.read.format("delta").table("finshield_bank.bronze.branches_raw")
        .select("branch_id", "branch_name", "city", "state") 
        )
df_ff= (spark.read.format("delta").table("finshield_bank.gold.fraud_alerts")
       .select("txn_id","fraud_flag")
       .withColumnRenamed("txn_id","transaction_id")
       )

df = (df_br.join(df_ac, on="branch_id", how="left").join(df_tc, on="account_id")
          .join(df_ff,    on="transaction_id", how="left")
          
     )

df_final =      (df.groupBy("branch_id", "branch_name", "city", "state")
                   .agg(countDistinct(col("account_id")).alias("total_accounts"),
                        count("transaction_id").alias("total_transaction_volume"),
                        sum("amount_in_inr").alias("total_transaction_amount"),
                        avg("amount_in_inr").cast("Decimal(10,2)").alias("avg_transaction_amount"),
                        sum(when(col("fraud_flag") == True, 1).otherwise(0)).alias("fraud_flagged_transactions"),
                        countDistinct(when(col("is_negative_balance") == True, col("account_id"))).alias("negative_balance_accounts")    
                        )
                   .withColumn("_processed_at", current_timestamp()) 
                )
                   
df_final.write.format("delta").mode("overwrite").saveAsTable("finshield_bank.gold.branch_performance")


# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# Check if gold table already exists
table_exists = spark.catalog.tableExists("finshield_bank.gold.monthly_transaction_summary")

# gold layer aggregations 

df_tc = (spark.read.format("delta").table("finshield_bank.silver.transactions_cleansed")
        .select("transaction_id","amount_in_inr","transaction_date","status", "channel")
        .withColumnRenamed("channel","transaction_channel")
        .withColumnRenamed("amount_in_inr","amount") 
        )

df_ut = (spark.read.format("delta").table("finshield_bank.silver.unified_transactions")
        .select("txn_id","transaction_date","amount","status","transaction_channel") 
        .withColumnRenamed("txn_id","transaction_id")
        )
df_fa = (spark.read.format("delta").table("finshield_bank.gold.fraud_alerts")
        .select("txn_id","fraud_flag")
        .withColumnRenamed("txn_id","transaction_id") 
        )        

df_trans = df_tc.unionByName(df_ut).join(df_fa, on="transaction_id", how="left")
win = Window.orderBy("transaction_year", "transaction_month")
df_agg =(df_trans.groupBy(month("transaction_date").alias("transaction_month"), 
                          year("transaction_date").alias("transaction_year"))
        .agg(count("transaction_id").alias("total_transaction"),
                sum("amount").alias("total_amount"),
                sum(when(col("fraud_flag") == True, 1).otherwise(0)).alias("total_fraud_transaction"),
                sum(when((col("status") == "Success") | (col("status") == "Approved"), 1).otherwise(0)).alias("total_success_transaction"),
                sum(when(col("status") == "Failed", 1).otherwise(0)).alias("total_failed_transaction")
            )
        .withColumn("previous_total_amount", lag("total_amount",1).over(win)).withColumn("MoM_growth_pct", ((col("total_amount") - col("previous_total_amount"))/ col("previous_total_amount")* 100).cast("Decimal(10,2)")).drop("previous_total_amount")
                
           )

df_top_channel = df_trans.groupBy(month("transaction_date").alias("transaction_month"), year("transaction_date").alias("transaction_year"), "transaction_channel").agg(count("transaction_id").alias("transaction_count")) 

w = Window.partitionBy("transaction_month","transaction_year").orderBy(col("transaction_count").desc())

df_top_channel = (df_top_channel
                  .withColumn("rank", row_number().over(w))
                  .filter(col("rank") == 1)
                  .drop("rank","transaction_count")
                  .withColumnRenamed("transaction_channel","top_channel")
                  )



df_final = (df_agg.join(df_top_channel, on=["transaction_month",'transaction_year'], how="left")
            .withColumn("_processed_at", current_timestamp())
            )

if not table_exists:
        # First run — create table normally
        df_final.write.format("delta").mode("overwritre").saveAsTable("finshield_bank.gold.monthly_transaction_summary")

        print("Table created for first time")

else:
      # Subsequent runs — MERGE only affected partitions 
      target_table = DeltaTable.forName(spark, "finshield_bank.gold.monthly_transaction_summary")
      (target_table.alias("target")
       .merge(df_final.alias("source"),
              """target.transaction_year = source.transaction_year AND target.transaction_month= source. transaction_month"""
              )
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute()
       )
      
      print("Affected partitions merged successfully")

                 



# COMMAND ----------

from pyspark.sql.functions import *

df_tc = (spark.read.format("delta").table("finshield_bank.silver.transactions_cleansed")
        .select("transaction_id","account_id","transaction_date","amount_in_inr","channel", "description")
        .withColumnRenamed("amount_in_inr","amount") 
        .withColumnRenamed("channel","transaction_channel")
        )

df_ut = (spark.read.format("delta").table("finshield_bank.silver.unified_transactions")
        .select("txn_id","account_id","transaction_date","amount","transaction_channel") 
        .withColumnRenamed("txn_id","transaction_id")
        .withColumn("description", lit("None").cast("string"))
        )
df_cc = (spark.read.format("delta").table("finshield_bank.silver.customers_cleansed")
        .select("customer_id","first_name","last_name","city")
        )

df_ac = (spark.read.format("delta").table("finshield_bank.silver.accounts_cleansed")
        .select("account_id","customer_id","account_status") 
        )
     
df_kv = (spark.read.format("delta").table("finshield_bank.silver.kyc_validated")
        .select("customer_id","kyc_id","customer_kyc_status","is_kyc_valid") 
        )

df_fa = (spark.read.format("delta").table("finshield_bank.gold.fraud_alerts")
        .select("txn_id","fraud_flag")
        .withColumnRenamed("txn_id","transaction_id")
        )       

df_trans =  df_tc.unionByName(df_ut)   

df_all =  (df_trans.join(df_ac, on="account_id", how="left")
                   .join(df_cc, on="customer_id", how="left") 
                   .join(df_kv, on="customer_id", how="left")
                   .join(df_fa, on="transaction_id", how="left")
                   .withColumn("transaction_purpose_code", when(col("description").isin("Salary Credit", "Late Salary"), "SALA").when(col("description").isin("Vendor Payment", "Business Receipt", "Business", "Crypto Purchase", "Investment Return", "Property Payment", "Foreign Transfer", "International Transfer"), "BEXP").when(col("description").isin("Food", "Food Order", "Grocery", "Late Shopping"), "FOOD").when(col("description").isin("Travel Booking"), "TRVL").when(col("description").contains("Petrol"), "PETR").when(col("description").contains("Fuel"), "PETR").otherwise("OTHR"))
          )

df_all_agg = (df_all.groupBy("customer_id")
             .agg(count("transaction_id").alias("total_transactions"),
                  sum("amount").alias("total_amount"),
                  max("fraud_flag").alias("has_suspicious_activity"),
                  max("is_kyc_valid").alias("is_kyc_valid"),
                  first("customer_kyc_status").alias("kyc_status"),
                  collect_set("account_status").alias("account_status_summary"),
                  sum(when(col("amount") > 1000000, 1).otherwise(0)).alias("high_value_transactions"),
                  sum(when(col("transaction_purpose_code") == "SALA", 1).otherwise(0)).alias("salary_txn_count"),
                  sum(when(col("transaction_purpose_code") == "BEXP", 1).otherwise(0)).alias("business_txn_count"),
                  sum(when(col("transaction_purpose_code") == "FOOD", 1).otherwise(0)).alias("food_txn_count"),
                  sum(when(col("transaction_purpose_code") == "TRVL", 1).otherwise(0)).alias("travel_txn_count"),
                  sum(when(col("transaction_purpose_code") == "OTHR", 1).otherwise(0)).alias("other_txn_count")
                  )
            
             )          

df_final =  (df_cc.join(df_all_agg, on="customer_id", how="left")
            .withColumn("compliance_status", when(col("is_kyc_valid") == False, "Non-Compliant").when(col("has_suspicious_activity") == True, "Non-Compliant").when(col("high_value_transactions") > 0 , "Non-Compliant").when(col("kyc_status") == "Pending", "Non-Compliant").otherwise("Compliant"))
            .withColumn("non_compliance_reasons", concat_ws(", " , when(col("is_kyc_valid")== False,"KYC Not Valid"), when(col("has_suspicious_activity")== True, "Suspicious Activity Detected"), when(col("high_value_transactions") > 0, "High Value Transactions Found"), when(col("kyc_status") == "Pending", "KYC Pending")))
            .withColumn("_processed_at", current_timestamp())
            )  

df_final.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("finshield_bank.gold.regulatory_report")
