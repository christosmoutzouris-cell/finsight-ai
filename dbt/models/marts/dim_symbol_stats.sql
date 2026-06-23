-- Dimension table: συνολικά στατιστικά ανά symbol
{{ config(materialized='table') }}

SELECT
    symbol,
    COUNT(*)                    AS total_days,
    ROUND(AVG(daily_return)::numeric, 4)   AS avg_daily_return,
    ROUND(MAX(daily_return)::numeric, 4)   AS best_day,
    ROUND(MIN(daily_return)::numeric, 4)   AS worst_day,
    ROUND(STDDEV(daily_return)::numeric, 4) AS volatility,
    ROUND(SUM(daily_return)::numeric, 4)   AS total_return,
    COUNT(CASE WHEN daily_return > 0 THEN 1 END) AS positive_days,
    COUNT(CASE WHEN daily_return < 0 THEN 1 END) AS negative_days,
    MAX(date)                   AS last_updated
FROM {{ ref('stg_daily_summary') }}
GROUP BY symbol