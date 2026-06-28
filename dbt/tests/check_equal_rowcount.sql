-- Αποτυγχάνει αν fct_stock_performance και daily_stock_summary
-- έχουν διαφορετικό αριθμό rows — σημαίνει ότι χάθηκαν δεδομένα
WITH source_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ source('public', 'daily_stock_summary') }}
),
mart_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ ref('fct_stock_performance') }}
)
SELECT
    source_count.cnt AS source_rows,
    mart_count.cnt   AS mart_rows
FROM source_count, mart_count
WHERE source_count.cnt != mart_count.cnt