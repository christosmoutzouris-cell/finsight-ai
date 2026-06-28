-- Αποτυγχάνει αν το high < low (αδύνατο φυσικά)
SELECT symbol, date, high, low
FROM {{ ref('fct_stock_performance') }}
WHERE high < low
