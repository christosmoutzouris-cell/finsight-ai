import os
import json
import time
import logging
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2

def get_symbols_from_db() -> list[str]:
    try:
        conn = psycopg2.connect(
            host="finsight-postgres",
            port=5432,
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            dbname=os.environ["POSTGRES_DB"],
        )
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM watched_symbols WHERE is_active = TRUE ORDER BY symbol")
        symbols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return symbols
    except Exception as e:
        log.warning(f"Δεν μπόρεσα να διαβάσω symbols από DB ({e}) — χρησιμοποιώ .env")
        return os.environ["SYMBOLS"].split(",")

#alpaca recovery code 81397b4e-90e6-45ca-be99-6bb21628ba77
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC        = os.environ["KAFKA_TOPIC"]
SYMBOLS      = os.environ["SYMBOLS"].split(",")
INTERVAL     = int(os.environ["POLL_INTERVAL_SECONDS"])


def wait_for_kafka(broker: str, retries: int = 10, delay: int = 5) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            log.info(f"Συνδέθηκα στο Kafka ({broker})")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Kafka δεν είναι έτοιμο — προσπάθεια {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Αδυναμία σύνδεσης στο Kafka")


def fetch_price_history(symbol: str) -> dict | None:
    """
    Χρησιμοποιεί history() αντί για fast_info ή download().
    Δουλεύει εντός ΚΑΙ εκτός ωραρίου — επιστρέφει την τελευταία
    διαθέσιμη τιμή (π.χ. closing price της προηγούμενης μέρας αν είναι νύχτα).
    """
    try:
        ticker = yf.Ticker(symbol)
        # period="5d" = τελευταίες 5 μέρες, interval="1m" = ανά λεπτό
        # Έτσι πάντα έχουμε δεδομένα ακόμα κι αν το market είναι κλειστό
        df = ticker.history(period="5d", interval="1m")

        if df.empty:
            log.warning(f"{symbol}: κανένα ιστορικό διαθέσιμο")
            return None

        last = df.iloc[-1]

        record = {
            "symbol":     symbol,
            "price":      round(float(last["Close"]), 4),
            "open":       round(float(last["Open"]), 4),
            "high":       round(float(last["High"]), 4),
            "low":        round(float(last["Low"]), 4),
            "volume":     int(last["Volume"]),
            "market_cap": 0,
            "event_time": datetime.now(timezone.utc).isoformat(),
            # Extra field: πότε ήταν η τελευταία πραγματική τιμή
            "last_market_time": df.index[-1].isoformat(),
        }
        return record

    except Exception as e:
        log.error(f"Σφάλμα για {symbol}: {e}")
        return None


def main():
    producer = wait_for_kafka(KAFKA_BROKER)
    SYMBOLS = get_symbols_from_db()
    log.info(f"Symbols από DB: {SYMBOLS}")
    log.info(f"Ξεκινάω streaming για: {SYMBOLS} (κάθε {INTERVAL}s)")

    while True:
        records = []
        
        for symbol in SYMBOLS:
            record = fetch_price_history(symbol)
            if record:
                records.append(record)
                log.info(f"✓ {symbol}: ${record['price']} (market time: {record['last_market_time']})")
            # Μικρό delay για να αποφύγουμε rate limiting από Yahoo
            time.sleep(2)

        for record in records:
            producer.send(
                topic=TOPIC,
                key=record["symbol"].encode("utf-8"),
                value=record,
            )

        producer.flush()
        log.info(f"Έστειλα {len(records)}/{len(SYMBOLS)} records — επόμενη ανάγνωση σε {INTERVAL}s")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()