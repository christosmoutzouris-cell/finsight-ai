-- Fact table: daily performance metrics ανά symbol
{{ config(materialized='table') }}

WITH daily AS (
    SELECT * FROM {{ ref('stg_daily_summary') }}
),

-- Υπολογισμός rolling metrics
performance AS (
    SELECT
        symbol,
        date,
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        day_category,

        -- Cumulative return από την αρχή
        SUM(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_return,

        -- 5-day average return
        AVG(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_return_5d,

        -- Volatility (std dev των τελευταίων 5 ημερών)
        STDDEV(daily_return) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS volatility_5d,

        -- Rank μέσα στην ημέρα (ποια μετοχή πήγε καλύτερα)
        RANK() OVER (
            PARTITION BY date
            ORDER BY daily_return DESC
        ) AS daily_rank

    FROM daily
)

SELECT * FROM performance