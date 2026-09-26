-- =============================================================================
-- GOLD: business-level fraud metrics built on silver_cleansed_transactions.
-- Each statement is materialised as its own Delta table in s3://gold/<name>.
-- =============================================================================

-- name: daily_merchant_risk_summary
SELECT
    event_date,
    merchant_id,
    merchant_category,
    count(*)                                                  AS txn_count,
    round(sum(amount), 2)                                     AS total_amount,
    round(avg(amount), 2)                                     AS avg_amount,
    count(*) FILTER (WHERE is_fraud = 1)                      AS confirmed_fraud_count,
    round(sum(CASE WHEN is_fraud = 1 THEN amount ELSE 0 END), 2) AS fraud_amount,
    round(100.0 * count(*) FILTER (WHERE is_fraud = 1) / count(*), 4) AS fraud_rate_pct,
    count(*) FILTER (WHERE is_flagged)                        AS flagged_txn_count,
    round(avg(risk_score), 3)                                 AS avg_risk_score,
    max(risk_score)                                           AS max_risk_score,
    now()                                                     AS gold_processed_at
FROM silver_cleansed_transactions
GROUP BY event_date, merchant_id, merchant_category
ORDER BY event_date, flagged_txn_count DESC, avg_risk_score DESC;

-- name: hourly_fraud_rate_metrics
SELECT
    event_hour,
    count(*)                                                  AS txn_count,
    round(sum(amount), 2)                                     AS total_amount,
    count(*) FILTER (WHERE is_fraud = 1)                      AS confirmed_fraud_count,
    round(100.0 * count(*) FILTER (WHERE is_fraud = 1) / count(*), 4) AS fraud_rate_pct,
    count(*) FILTER (WHERE is_flagged)                        AS flagged_count,
    count(*) FILTER (WHERE is_flagged AND is_fraud = 1)       AS true_positives,
    count(*) FILTER (WHERE is_flagged AND is_fraud = 0)       AS false_positives,
    count(*) FILTER (WHERE NOT is_flagged AND is_fraud = 1)   AS false_negatives,
    round(count(*) FILTER (WHERE is_flagged AND is_fraud = 1)
          / nullif(count(*) FILTER (WHERE is_flagged), 0), 4) AS precision,
    round(count(*) FILTER (WHERE is_flagged AND is_fraud = 1)
          / nullif(count(*) FILTER (WHERE is_fraud = 1), 0), 4) AS recall,
    count(*) FILTER (WHERE velocity_flag)                     AS velocity_alerts,
    count(*) FILTER (WHERE high_amount_flag)                  AS high_amount_alerts,
    round(avg(risk_score), 3)                                 AS avg_risk_score,
    now()                                                     AS gold_processed_at
FROM silver_cleansed_transactions
GROUP BY event_hour
ORDER BY event_hour;

-- name: high_risk_account_leaderboard
WITH account_stats AS (
    SELECT
        account_id,
        count(*)                                              AS txn_count,
        round(sum(amount), 2)                                 AS total_amount,
        round(avg(risk_score), 3)                             AS avg_risk_score,
        max(risk_score)                                       AS max_risk_score,
        count(*) FILTER (WHERE is_flagged)                    AS flagged_txn_count,
        count(*) FILTER (WHERE is_fraud = 1)                  AS confirmed_fraud_count,
        count(*) FILTER (WHERE velocity_flag)                 AS velocity_alerts,
        count(DISTINCT merchant_id)                           AS distinct_merchants,
        count(DISTINCT location)                              AS distinct_locations,
        max(event_time)                                       AS last_seen
    FROM silver_cleansed_transactions
    GROUP BY account_id
)
SELECT
    row_number() OVER (ORDER BY flagged_txn_count DESC, max_risk_score DESC, total_amount DESC) AS risk_rank,
    *,
    now()                                                     AS gold_processed_at
FROM account_stats
WHERE flagged_txn_count > 0 OR max_risk_score >= 20
ORDER BY risk_rank
LIMIT 100;
