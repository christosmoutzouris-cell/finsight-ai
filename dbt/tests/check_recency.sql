-- Αποτυγχάνει αν τα δεδομένα είναι παλιότερα από 7 μέρες
-- Χρήσιμο για να ξέρεις αν η pipeline σταμάτησε
SELECT
    MAX(date) AS last_date,
    CURRENT_DATE - MAX(date) AS days_since_last_update
FROM {{ ref('fct_stock_performance') }}
HAVING CURRENT_DATE - MAX(date) > 7