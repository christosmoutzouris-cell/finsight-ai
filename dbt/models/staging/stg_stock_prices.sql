-- Staging model πάνω από το Silver layer
-- Καθαρίζει και τυποποιεί τα δεδομένα για τα marts
{{ config(materialized='view') }}

SELECT
    symbol,
    price,
    open,
    high,
    low,
    volume,
    ma5,
    ma20,
    price_change,
    pct_change,
    event_time,
    -- Προσθέτουμε date για group by
    DATE(event_time)    AS trade_date,
    -- Ώρα συναλλαγής
    EXTRACT(HOUR FROM event_time) AS trade_hour
FROM {{ source('public', 'stock_prices_silver') }}
WHERE price > 0
  AND volume > 0