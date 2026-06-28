-- Αποτυγχάνει αν υπάρχουν αρνητικές τιμές
SELECT symbol, close
FROM {{ ref('fct_stock_performance') }}
WHERE close <= 0
