-- =============================================================================
-- SILVER: silver_cleansed_transactions
-- Source : bronze_transactions (Delta, append-only, at-least-once)
-- Grain  : one row per unique transaction_id
--
-- 1. Deduplicate replayed / redelivered events (keep first ingestion)
-- 2. Cleanse: drop null keys, negative amounts, out-of-contract labels
-- 3. Enrich: calendar attributes, amount buckets, account behaviour features
-- 4. Score: logistic risk model on the Kaggle PCA features (coefficients fitted
--    offline on a 70/30 time split; holdout ROC-AUC 0.971, PR-AUC 0.708)
-- 5. Flag: rule-based anomaly flags complementing the model
-- =============================================================================
WITH deduplicated AS (
    SELECT *
    FROM bronze_transactions
    WHERE transaction_id IS NOT NULL
      AND account_id IS NOT NULL
      AND merchant_id IS NOT NULL
      AND event_time IS NOT NULL
      AND amount IS NOT NULL AND amount >= 0
      AND is_fraud IN (0, 1)
    QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY ingested_at, kafka_offset) = 1
),

enriched AS (
    SELECT
        *,
        CAST(event_time AS DATE)                                   AS event_date,
        date_trunc('hour', event_time)                             AS event_hour,
        hour(event_time)                                           AS hour_of_day,
        CASE
            WHEN amount < 10    THEN '0-10'
            WHEN amount < 50    THEN '10-50'
            WHEN amount < 200   THEN '50-200'
            WHEN amount < 1000  THEN '200-1000'
            ELSE '1000+'
        END                                                        AS amount_bucket,
        count(*) OVER account_10min                                AS account_txn_count_10m,
        avg(amount) OVER account_all                               AS account_avg_amount,
        coalesce(stddev_samp(amount) OVER account_all, 0)          AS account_std_amount,
        -- Logistic regression logit on the strongest fraud-separating components
        -8.2705
          - 0.7211 * V14 - 0.1225 * V12 - 0.3587 * V10 + 0.0629 * V17
          + 0.5034 * V4  + 0.0864 * V11 + 0.1247 * V3  - 0.1419 * V16
          - 0.0039 * V7  + 0.0460 * ln(1 + amount)                 AS risk_logit
    FROM deduplicated
    WINDOW
        account_10min AS (
            PARTITION BY replay_cycle, account_id ORDER BY event_time
            RANGE BETWEEN INTERVAL 10 MINUTES PRECEDING AND CURRENT ROW
        ),
        account_all AS (PARTITION BY replay_cycle, account_id)
),

scored AS (
    SELECT
        *,
        round(100.0 / (1 + exp(-risk_logit)), 2)                   AS risk_score,
        amount >= 1000                                             AS high_amount_flag,
        account_txn_count_10m >= 3                                 AS velocity_flag,
        (V14 < -5 OR V12 < -5 OR V10 < -5)                         AS pca_outlier_flag,
        CASE WHEN account_std_amount > 0
             THEN (amount - account_avg_amount) / account_std_amount
             ELSE 0 END                                            AS account_amount_zscore
    FROM enriched
)

SELECT
    transaction_id,
    source_row_id,
    replay_cycle,
    event_time,
    event_date,
    event_hour,
    hour_of_day,
    account_id,
    merchant_id,
    merchant_category,
    location,
    amount,
    amount_bucket,
    is_fraud,
    account_txn_count_10m,
    round(account_amount_zscore, 3)                                AS account_amount_zscore,
    high_amount_flag,
    velocity_flag,
    pca_outlier_flag,
    account_amount_zscore > 3                                      AS amount_spike_flag,
    risk_score,
    CASE
        WHEN risk_score >= 80 THEN 'CRITICAL'
        WHEN risk_score >= 50 THEN 'HIGH'
        WHEN risk_score >= 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END                                                            AS risk_tier,
    risk_score >= 50                                               AS is_flagged,
    V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, V11, V12, V13, V14,
    V15, V16, V17, V18, V19, V20, V21, V22, V23, V24, V25, V26, V27, V28,
    ingested_at,
    now()                                                          AS silver_processed_at
FROM scored
