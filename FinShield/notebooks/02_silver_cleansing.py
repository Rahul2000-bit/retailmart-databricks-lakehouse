# Databricks notebook source
df = spark.read.format("delta").table("finshield_bank.bronze.customers_raw")
from pyspark.sql.functions import * 
from pyspark.sql.window import *
w = Window.partitionBy("customer_id").orderBy(col("_ingested_at").desc())

df = (df.withColumn("first_name", initcap(col("first_name")))
           .withColumn("last_name", initcap(col("last_name")))
           .withColumn("phone", when(col("phone") == 'NULL', None).otherwise(right(regexp_replace(col("phone"), "[^0-9]", ""),lit(10))))
           .withColumn("phone", when(length(col("phone")) < 10, None).otherwise(col("phone")))
           .withColumn("date_of_birth", to_date(regexp_replace(col("date_of_birth"), "/", "-"), "dd-MM-yyyy"))
           .withColumn("email", when(col("email").rlike("^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"),col("email")).otherwise(None))
           .withColumn("pan_card", when(col("pan_card").rlike("^[A-Z]{5}[0-9]{4}[A-Z]$"), col("pan_card")).otherwise(None))
           .withColumn("state", initcap(col("state")))
           .withColumn("row_num", row_number().over(w)).filter(col("row_num") == 1).drop("row_num")
           .withColumn("_processed_at", current_timestamp())
           
     )
df.write.format("delta").mode("overwrite").saveAsTable("finshield_bank.silver.customers_cleansed")

# COMMAND ----------

df_cust_clean = spark.read.format("delta").table("finshield_bank.silver.customers_cleansed")
df_acct_raw = spark.read.format("delta").table("finshield_bank.bronze.accounts_raw")
df = (df_acct_raw.join(df_cust_clean, on = "customer_id", how= "inner")
      .select("account_id","customer_id","account_type", "account_number","ifsc_code","branch_id","balance","currency","account_status","opening_date","last_transaction_date","overdraft_limit","interest_rate")
      .withColumn("balance", col("balance").cast("decimal(10,2)"))
      .withColumn("is_negative_balance", when(col("balance") < 0, True).otherwise(False))
      .withColumn("is_active_account", when(col("account_status") == "Active", True).otherwise(False))
      .withColumn("is_valid_ifsc", when(col("ifsc_code").rlike("^[A-Z]{4}0[A-Za-z0-9]{6}$"), True).otherwise(False))
      .withColumn("_processed_at", current_timestamp())

)
df.write.format("delta").mode("overwrite").saveAsTable("finshield_bank.silver.accounts_cleansed")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *
from delta.tables import * 

# Get last processed watermark
try:
    last_watermark = spark.sql("""
                               select max(_ingested_at) from finshield_bank.silver.transactions_cleansed
                               """).collect()[0][0]
except:
    last_watermark = None  

# Read only new records from bronze
if last_watermark:
    df = (spark.read.format("delta")
          .option("mergeSchema", "true")
          .table("finshield_bank.bronze.transactions_raw")
          .filter(col("_ingested_at") > last_watermark))      
else:
    # First run — read everything

    df = spark.read.format("delta").table("finshield_bank.bronze.transactions_raw")

# ── Separate good and bad records ──────────────────

df_bad = df.filter(
    col("amount").isNull() |
    (col("amount") == "NULL") |
    col("account_id").isNull() |
    ~col("currency").rlike("^[A-Z]{3}$")
)

df_good = df.filter(
    col("amount").isNotNull() &
    (col("amount") != "NULL") &
    col("account_id").isNotNull() &
    col("currency").rlike("^[A-Z]{3}$")
)
# ── Calculate DQ metrics ───────────────────────────
total = df.count()
bad_count = df_bad.count()
good_count = df_good.count()
valid_pct = (good_count / total * 100) if total > 0 else 100

print(f"Total: {total} | Good: {good_count} | Bad: {bad_count} | Valid%: {valid_pct:.2f}%")    

# ──  Write bad records to quarantine ─────────────────
if bad_count > 0:
    (df_bad.withColumn("source_table", lit("transactions_raw"))
           .withColumn("rejection_reason", 
               concat_ws(", ",
                   when(col("amount").isNull() | (col("amount") == "NULL"), "NULL Amount"),
                   when(col("account_id").isNull(), "Missing Account ID"),
                   when(~col("currency").rlike("^[A-Z]{3}$"), "Invalid Currency")
               ))
           .withColumn("detected_at", current_timestamp())
           .withColumn("status", lit("QUARANTINED"))
           .select("source_table","transaction_id","account_id",
                   "amount","currency","rejection_reason","detected_at","status")
           .write.format("delta").mode("append")
           .saveAsTable("finshield_bank.bronze.quarantine_records")
    )
    print(f" {bad_count} records quarantined!")

# ──  Alert if bad % exceeds threshold ────────────────
if valid_pct < 70:
    import uuid
    spark.sql(f"""
        INSERT INTO finshield_bank.bronze.pipeline_alerts VALUES (
            '{str(uuid.uuid4())}',
            'BAD_DATA_SPIKE',
            'CRITICAL',
            'transactions_raw',
            'Bad data spike detected! Only {valid_pct:.1f}% records valid. Expected >= 70%. {bad_count} records quarantined.',
            {bad_count},
            current_timestamp(),
            NULL,
            'OPEN'
        )
    """)
    print(f" CRITICAL ALERT: Bad data spike! Valid% = {valid_pct:.1f}%")   
    
# ──  Continue with ONLY good records ─────────────────
df = df_good    

win = Window.partitionBy("transaction_id").orderBy(col("transaction_date").asc(), col("transaction_time").asc())

df_black = spark.read.format("delta").table("finshield_bank.bronze.blacklist_raw")
df_black = (df_black.filter(col("entity_type") == "Account").select("entity_id")
            .drop("_ingested_at")
            )

df_rates = spark.read.format("delta").table("finshield_bank.bronze.exchange_rates_raw")
df_rates = (df_rates
    .withColumn("effective_date", to_date(col("effective_date"), "yyyy-MM-dd"))
    .withColumn("expiry_date", to_date(col("expiry_date"), "yyyy-MM-dd"))
    .drop("_ingested_at")
)

df_final = (df.withColumn("row_num", row_number().over(win)).filter(col("row_num") == 1).drop("row_num")
      .withColumn("amount", when(col("amount") == "NULL", None).otherwise(col("amount")))
      .withColumn("amount", col("amount").cast("decimal(10,2)"))
      .filter((col("amount").isNotNull())  & (col("amount") > 0))
      .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
      .withColumn("is_future_dated", when(col("transaction_date") > current_date() , True).otherwise(False))
      .withColumn("currency", upper(col("currency")))
      .join(df_black, on= df_black.entity_id == col("account_id"), how= "left")
      .withColumn("is_blacklisted_account", when(col("entity_id").isNull(), False).otherwise(True)).drop("entity_id")
      .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))
      .join(df_rates, on= (col("currency") == col("from_currency")) & (col("transaction_date").between(col("effective_date"),col("expiry_date"))), how="left")
      .withColumn("amount_in_inr", when(col("currency") == "INR", col("amount")).otherwise(col("amount") * col("exchange_rate")))
      .drop("rate_id","from_currency","to_currency","exchange_rate","effective_date","expiry_date","source","_rescued_data","_source_file")
      .withColumn("_processed_at", current_timestamp()
))

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

target_table = DeltaTable.forName(spark, "finshield_bank.silver.transactions_cleansed")
(target_table.alias("target")
 .merge(df_final.alias("source"),
        "target.transaction_id = source.transaction_id"
        )
 .whenMatchedUpdate(set={
     # Update all business columns
     "status"               : "source.status",
     "balance_after"        : "source.balance_after",
     "amount"               : "source.amount",
     "currency"             : "source.currency",
     "is_future_dated"      : "source.is_future_dated",
     "is_blacklisted_account": "source.is_blacklisted_account",
     "amount_in_inr"        : "source.amount_in_inr",
     "_processed_at"        : "source._processed_at"
     # _ingested_at NOT included → preserves original ingestion time
 })
 .whenNotMatchedInsertAll()
 .execute()
 )
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")

