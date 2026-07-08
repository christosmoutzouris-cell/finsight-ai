"""
Αξιολογεί τις χθεσινές LSTM προβλέψεις
συγκρίνοντάς τες με τις πραγματικές τιμές.
"""

import logging
import psycopg2
import sys
sys.path.append("/ai/common")
from common.db_utils import get_connection, get_active_symbols

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EVALUATE] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)


def evaluate_predictions():
    """
    Βρίσκει προβλέψεις που δεν έχουν αξιολογηθεί ακόμα
    και τις συγκρίνει με τις πραγματικές τιμές.
    """
    conn   = get_connection()
    cursor = conn.cursor()

    # Βρες προβλέψεις χωρίς evaluation
    cursor.execute("""
        SELECT
            p.id,
            p.symbol,
            p.predicted_price,
            p.predicted_direction,
            p.predicted_at,
            p.current_price
        FROM price_predictions p
        WHERE p.evaluated_at IS NULL
        AND p.predicted_at < NOW() - INTERVAL '1 day'
        ORDER BY p.predicted_at DESC
    """)

    predictions = cursor.fetchall()

    if not predictions:
        log.info("Δεν υπάρχουν προβλέψεις προς αξιολόγηση")
        return

    log.info(f"Αξιολογώ {len(predictions)} προβλέψεις...")
    log.info("=" * 60)

    evaluated = 0
    correct_direction = 0

    for pred in predictions:
        pred_id, symbol, predicted_price, predicted_dir, predicted_at, prev_price = pred

        # Βρες την πραγματική τιμή την επόμενη μέρα
        cursor.execute("""
            SELECT price
            FROM stock_prices_silver
            WHERE symbol = %s
            AND event_time > %s
            AND event_time < %s + INTERVAL '2 days'
            ORDER BY event_time ASC
            LIMIT 1
        """, (symbol, predicted_at, predicted_at))

        actual = cursor.fetchone()
        if not actual:
            log.warning(f"{symbol}: δεν βρέθηκε πραγματική τιμή για {predicted_at}")
            continue

        actual_price     = float(actual[0])
        actual_direction = "UP" if actual_price > float(prev_price) else "DOWN"
        price_error      = round(abs(float(predicted_price) - actual_price), 4)
        pct_error        = round(price_error / actual_price * 100, 4)
        direction_correct = (predicted_dir == actual_direction)

        # Update prediction record
        cursor.execute("""
            UPDATE price_predictions SET
                actual_price      = %s,
                actual_direction  = %s,
                price_error       = %s,
                direction_correct = %s,
                evaluated_at      = NOW()
            WHERE id = %s
        """, (actual_price, actual_direction, price_error,
              direction_correct, pred_id))

        if direction_correct:
            correct_direction += 1

        icon = "✅" if direction_correct else "❌"
        log.info(
            f"{icon} {symbol}: "
            f"predicted=${predicted_price:.2f} {predicted_dir} | "
            f"actual=${actual_price:.2f} {actual_direction} | "
            f"error=${price_error:.2f} ({pct_error:.2f}%)"
        )
        evaluated += 1

    conn.commit()

    # Summary
    if evaluated > 0:
        accuracy = correct_direction / evaluated * 100
        log.info("\n" + "=" * 60)
        log.info(f"  Αξιολογήθηκαν: {evaluated} προβλέψεις")
        log.info(f"  Direction accuracy: {accuracy:.1f}%")
        log.info(f"  Σωστές: {correct_direction}/{evaluated}")
        log.info("=" * 60)

    cursor.close()
    conn.close()


def get_accuracy_report():
    """Συνολικό accuracy report από όλες τις αξιολογημένες προβλέψεις."""
    conn   = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            symbol,
            COUNT(*)                                    AS total_predictions,
            SUM(CASE WHEN direction_correct THEN 1 END) AS correct,
            ROUND(AVG(price_error)::numeric, 4)         AS avg_price_error,
            ROUND(
                SUM(CASE WHEN direction_correct THEN 1 END)::numeric
                / COUNT(*) * 100, 1
            )                                           AS direction_accuracy_pct
        FROM price_predictions
        WHERE evaluated_at IS NOT NULL
        GROUP BY symbol
        ORDER BY direction_accuracy_pct DESC
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        log.info("Δεν υπάρχουν αξιολογημένες προβλέψεις ακόμα")
        return

    log.info("\n" + "=" * 60)
    log.info("  LSTM ACCURACY REPORT")
    log.info("=" * 60)
    for row in rows:
        symbol, total, correct, avg_err, acc = row
        log.info(
            f"  {symbol:5s}: {acc:.1f}% accuracy "
            f"({correct}/{total}) | avg error: ${avg_err:.2f}"
        )
    log.info("=" * 60)


if __name__ == "__main__":
    evaluate_predictions()
    get_accuracy_report()