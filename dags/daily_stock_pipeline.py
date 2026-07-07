"""
DAG: daily_stock_pipeline

Τρέχει κάθε μέρα στις 16:30 ET (23:30 ώρα Ελλάδας) — μισή ώρα
μετά το κλείσιμο της Wall Street.

Τι κάνει:
1. Τραβάει daily OHLCV για κάθε μετοχή
2. Ελέγχει ότι τα δεδομένα είναι έγκυρα
3. Γράφει daily summary στη PostgreSQL
4. Εκτυπώνει report (αργότερα θα τρέχει dbt)
"""

from datetime import datetime, timedelta
import json
import psycopg2
import yfinance as yf

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# ── Default arguments ──────────────────────────────────────────────────────
# Αυτά εφαρμόζονται σε κάθε task του DAG αν δεν οριστούν αλλού
default_args = {
    "owner": "finsight",
    "retries": 2,                           # αν αποτύχει, ξαναπροσπαθεί 2 φορές
    "retry_delay": timedelta(minutes=5),    # περιμένει 5 λεπτά μεταξύ retries
    "email_on_failure": False,
}

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"]

PG_CONN = {
    "host":     "finsight-postgres",   # όνομα container στο docker network
    "port":     5432,
    "user":     "finsight",
    "password": "finsight123",
    "dbname":   "finsight_db",
}


# ── Task 1: Fetch ──────────────────────────────────────────────────────────
def fetch_daily_data(**context):
    """
    Τραβάει το daily bar (OHLCV) για κάθε μετοχή.
    
    Το **context περιέχει πληροφορίες για το τρέχον DAG run,
    π.χ. context["ds"] = η ημερομηνία εκτέλεσης (YYYY-MM-DD)
    
    Το XCom (cross-communication) επιτρέπει σε tasks να μοιράζονται δεδομένα.
    Εδώ κάνουμε push τα δεδομένα και το Task 2 τα κάνει pull.
    """
    execution_date = context["ds"]  # π.χ. "2026-06-19"
    results = []

    for symbol in SYMBOLS:
        try:
            ticker = yf.Ticker(symbol)
            # period="2d" για να έχουμε σίγουρα τη χθεσινή μέρα
            hist = ticker.history(period="2d", interval="1d")

            if hist.empty:
                print(f"WARNING: {symbol} — κενά δεδομένα")
                continue

            last = hist.iloc[-1]
            record = {
                "symbol":         symbol,
                "date":           execution_date,
                "open":           round(float(last["Open"]), 4),
                "high":           round(float(last["High"]), 4),
                "low":            round(float(last["Low"]), 4),
                "close":          round(float(last["Close"]), 4),
                "volume":         int(last["Volume"]),
                "daily_return":   round(
                    (float(last["Close"]) - float(last["Open"])) / float(last["Open"]) * 100, 4
                ),
            }
            results.append(record)
            print(f"✓ {symbol}: close=${record['close']}, return={record['daily_return']}%")

        except Exception as e:
            print(f"ERROR {symbol}: {e}")

    if not results:
        raise ValueError("Κανένα δεδομένο δεν ελήφθη — ο DAG θα αποτύχει")

    # XCom push: αποθηκεύει τα αποτελέσματα για το επόμενο task
    context["task_instance"].xcom_push(key="daily_data", value=results)
    print(f"Fetch complete: {len(results)} symbols")


# ── Task 2: Validate ───────────────────────────────────────────────────────
def validate_data(**context):
    """
    Ελέγχει ότι τα δεδομένα είναι λογικά πριν τα γράψουμε στη βάση.
    Αν κάτι δεν πάει καλά, το task αποτυγχάνει και ο DAG σταματάει.
    """
    # XCom pull: παίρνει τα δεδομένα από το Task 1
    data = context["task_instance"].xcom_pull(
        task_ids="fetch_daily_data",
        key="daily_data"
    )

    errors = []
    for record in data:
        symbol = record["symbol"]

        # Έλεγχος 1: τιμές δεν μπορεί να είναι αρνητικές
        if record["close"] <= 0:
            errors.append(f"{symbol}: αρνητική τιμή {record['close']}")

        # Έλεγχος 2: high >= low
        if record["high"] < record["low"]:
            errors.append(f"{symbol}: high < low")

        # Έλεγχος 3: daily return δεν πρέπει να είναι > 50% (πιθανό σφάλμα)
        if abs(record["daily_return"]) > 50:
            errors.append(f"{symbol}: ύποπτο daily return {record['daily_return']}%")

        # Έλεγχος 4: volume δεν μπορεί να είναι 0
        if record["volume"] == 0:
            errors.append(f"{symbol}: μηδενικό volume")

    if errors:
        raise ValueError(f"Validation απέτυχε:\n" + "\n".join(errors))

    print(f"✓ Validation OK για {len(data)} symbols")


# ── Task 3: Load ───────────────────────────────────────────────────────────
def load_to_postgres(**context):
    """
    Γράφει το daily summary στη PostgreSQL.
    Χρησιμοποιεί INSERT ... ON CONFLICT DO UPDATE (upsert):
    αν τρέξει ξανά για την ίδια μέρα, ενημερώνει αντί να διπλογράφει.
    """
    data = context["task_instance"].xcom_pull(
        task_ids="fetch_daily_data",
        key="daily_data"
    )

    conn   = psycopg2.connect(**PG_CONN)
    cursor = conn.cursor()

    # Δημιούργησε τον πίνακα αν δεν υπάρχει
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_stock_summary (
            id            BIGSERIAL PRIMARY KEY,
            symbol        VARCHAR(10)    NOT NULL,
            date          DATE           NOT NULL,
            open          NUMERIC(12,4),
            high          NUMERIC(12,4),
            low           NUMERIC(12,4),
            close         NUMERIC(12,4),
            volume        BIGINT,
            daily_return  NUMERIC(8,4),
            created_at    TIMESTAMPTZ    DEFAULT NOW(),
            UNIQUE(symbol, date)        -- αποτρέπει duplicates
        );
    """)

    upsert_sql = """
        INSERT INTO daily_stock_summary
            (symbol, date, open, high, low, close, volume, daily_return)
        VALUES
            (%(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s,
             %(close)s, %(volume)s, %(daily_return)s)
        ON CONFLICT (symbol, date)
        DO UPDATE SET
            close        = EXCLUDED.close,
            volume       = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return
    """

    cursor.executemany(upsert_sql, data)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"✓ Έγραψα {len(data)} rows στο daily_stock_summary")


# ── Task 4: Report ─────────────────────────────────────────────────────────
def generate_report(**context):
    """
    Απλό report στα logs. Αργότερα εδώ θα καλούμε dbt και AI analysis.
    """
    data = context["task_instance"].xcom_pull(
        task_ids="fetch_daily_data",
        key="daily_data"
    )

    print("\n" + "="*50)
    print(f"  DAILY REPORT — {context['ds']}")
    print("="*50)

    for r in sorted(data, key=lambda x: x["daily_return"], reverse=True):
        direction = "▲" if r["daily_return"] >= 0 else "▼"
        print(f"  {direction} {r['symbol']:5s}  close=${r['close']:>10.2f}  "
              f"return={r['daily_return']:+.2f}%  vol={r['volume']:,}")

    best  = max(data, key=lambda x: x["daily_return"])
    worst = min(data, key=lambda x: x["daily_return"])
    print(f"\n  🏆 Best:  {best['symbol']} ({best['daily_return']:+.2f}%)")
    print(f"  📉 Worst: {worst['symbol']} ({worst['daily_return']:+.2f}%)")
    print("="*50 + "\n")


# ── DAG Definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="daily_stock_pipeline",
    description="Daily OHLCV fetch, validate, load, report",
    default_args=default_args,
    # Τρέχει κάθε μέρα στις 21:30 UTC (= 23:30 Ελλάδας καλοκαίρι)
    # Cron format: λεπτά ώρα μέρα μήνας εβδομάδα
    schedule="30 21 * * 1-5",   # Δευτέρα-Παρασκευή μόνο
    start_date=datetime(2026, 6, 1),
    catchup=False,               # μην τρέξεις για παλιές ημερομηνίες
    tags=["finsight", "stocks", "daily"],
) as dag:

    t1 = PythonOperator(task_id="fetch_daily_data",  python_callable=fetch_daily_data)
    t2 = PythonOperator(task_id="validate_data",     python_callable=validate_data)
    t3 = PythonOperator(task_id="load_to_postgres",  python_callable=load_to_postgres)
    t4 = PythonOperator(task_id="generate_report",   python_callable=generate_report)

   # Όταν τελειώσει το daily pipeline, τρέχει αυτόματα το medallion
    trigger_medallion = TriggerDagRunOperator(
        task_id="trigger_medallion_pipeline",
        trigger_dag_id="spark_medallion_pipeline",  # το DAG που θα τριγκαριστεί
        wait_for_completion=False,  # δεν περιμένει να τελειώσει
        reset_dag_run=True,         # αν υπάρχει ήδη run, το επαναφέρει
    )

    t1 >> t2 >> t3 >> t4 >> trigger_medallion