-- Αποτυγχάνει αν daily_return > 50% (πιθανό data error)
SELECT symbol, date, daily_return
FROM {{ ref('fct_stock_performance') }}
WHERE ABS(daily_return) > 50