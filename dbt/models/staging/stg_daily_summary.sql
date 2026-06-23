{{ config(materialized='view') }}

SELECT
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    daily_return,
    -- Κατηγοριοποίηση ημέρας
    CASE
        WHEN daily_return > 2  THEN 'strong_up'
        WHEN daily_return > 0  THEN 'up'
        WHEN daily_return = 0  THEN 'flat'
        WHEN daily_return > -2 THEN 'down'
        ELSE 'strong_down'
    END AS day_category
FROM {{ source('public', 'daily_stock_summary') }}