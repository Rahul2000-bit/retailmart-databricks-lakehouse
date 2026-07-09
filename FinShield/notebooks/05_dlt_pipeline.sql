-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE transactions_dlt(
    CONSTRAINT valid_amount EXPECT(amount > 0) ON VIOLATION DROP ROW,
    CONSTRAINT valid_account EXPECT(account_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_date EXPECT(transaction_date IS NOT NULL) ON VIOLATION DROP ROW
)
as SELECT
transaction_id,
account_id,
transaction_type,
amount,
currency,
transaction_date,
transaction_time,
description,
merchant_id,
channel,
reference_number,
status,
balance_after,
remarks,
_rescued_data,
_ingested_at,
_source_file,
current_timestamp() As _processed_at
from STREAM(finshield_bank.bronze.transactions_raw)

-- COMMAND ----------


CREATE OR REFRESH STREAMING LIVE TABLE fraud_alerts_dlt
(
    CONSTRAINT valid_fraud_score
    EXPECT (fraud_score >= 0)
    ON VIOLATION DROP ROW
)
AS

SELECT
    *,

    -- Fraud Flag
    CASE
        WHEN currency <> 'INR'
          OR (amount > 500000
              AND (hour(transaction_time) >= 23
                   OR hour(transaction_time) < 5))
        THEN TRUE
        ELSE FALSE
    END AS fraud_flag,

    -- Fraud Reason
    concat_ws(', ',
        CASE
            WHEN currency <> 'INR'
            THEN 'Foreign Currency'
        END,
        CASE
            WHEN amount > 500000
             AND (hour(transaction_time) >= 23
                  OR hour(transaction_time) < 5)
            THEN 'High Value Night Transaction'
        END
    ) AS fraud_reason,

    -- Fraud Score
    (
        CASE
            WHEN currency <> 'INR'
            THEN 1
            ELSE 0
        END
        +
        CASE
            WHEN amount > 500000
             AND (hour(transaction_time) >= 23
                  OR hour(transaction_time) < 5)
            THEN 1
            ELSE 0
        END
    ) AS fraud_score

FROM STREAM(LIVE.transactions_dlt);