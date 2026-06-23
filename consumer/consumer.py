import os
import json
import logging
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC        = os.environ["KAFKA_TOPIC"]
PG_USER      = os.environ["POSTGRES_USER"]
PG_PASS      = os.environ["POSTGRES_PASSWORD"]
PG_DB        = os.environ["POSTGRES_DB"]
PG_HOST      = "postgres"   # όνομα service στο docker-compose
PG_PORT      = 5432


def wait_for_kafka(broker: str, topic: str, retries: int = 10, delay: int = 5) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=broker,
                # Αποσειριοποιεί το JSON bytes → Python dict αυτόματα
                value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v is not None else None,
                # group_id: αν τρέξεις πολλούς consumers, μοιράζονται τα partitions
                group_id="finsight-consumer-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            log.info(f"Συνδέθηκα στο Kafka topic: {topic}")
            return consumer
        except NoBrokersAvailable:
            log.warning(f"Kafka δεν είναι έτοιμο — προσπάθεια {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Αδυναμία σύνδεσης στο Kafka")


def get_pg_connection():
    """Επιστρέφει σύνδεση στη PostgreSQL με retry logic."""
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT,
                user=PG_USER, password=PG_PASS,
                dbname=PG_DB
            )
            log.info("Συνδέθηκα στη PostgreSQL")
            return conn
        except psycopg2.OperationalError:
            log.warning(f"PostgreSQL δεν είναι έτοιμη — προσπάθεια {attempt}/10")
            time.sleep(5)
    raise RuntimeError("Αδυναμία σύνδεσης στη PostgreSQL")


INSERT_SQL = """
    INSERT INTO stock_prices
        (symbol, price, open, high, low, volume, market_cap, event_time)
    VALUES
        (%(symbol)s, %(price)s, %(open)s, %(high)s, %(low)s,
         %(volume)s, %(market_cap)s, %(event_time)s)
"""


def main():
    consumer = wait_for_kafka(KAFKA_BROKER, TOPIC)
    conn     = get_pg_connection()
    cursor   = conn.cursor()

    log.info("Ακούω για messages...")

    # execute_batch: γράφει πολλά records μαζί — πιο αποδοτικό από INSERT ένα-ένα
    batch = []
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),

    for message in consumer:
        record = message.value
        if record is None:  
            continue
        batch.append(record)

        # Γράφουμε στη βάση κάθε 5 messages (ή μπορείς να αλλάξεις σε 1)
        if len(batch) >= 5:
            try:
                execute_batch(cursor, INSERT_SQL, batch)
                conn.commit()
                log.info(f"Έγραψα {len(batch)} records στη PostgreSQL")
                batch.clear()
            except Exception as e:
                log.error(f"Σφάλμα εγγραφής: {e}")
                conn.rollback()
                # Reconnect σε περίπτωση που έπεσε η σύνδεση
                conn = get_pg_connection()
                cursor = conn.cursor()


if __name__ == "__main__":
    main()
    
    

# cd "C:\Users\ChristosMoutzouris\OneDrive - Agile Actors\Desktop\AI Academy\finsight-ai"
# docker compose up -d    
# docker compose ps

# docker logs --tail 5 finsight-producer

# Connect to the PostgreSQL database and create the stock_prices table if it doesn't exist
#cd "C:\Users\ChristosMoutzouris\OneDrive - Agile Actors\Desktop\AI Academy\finsight-ai"
# docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, price, event_time, ingested_at FROM stock_prices ORDER BY ingested_at DESC LIMIT 10;" --for streaming data
# docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT * from daily_stock_summary ORDER BY created_at DESC LIMIT 10;" --for closing data
# docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) FROM daily_stock_summary;"
# docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) FROM stock_prices;"
#Airflow UI (http://localhost:8081)
#Καφκα http://localhost:8080     kai docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, price, ingested_at FROM stock_prices ORDER BY ingested_at DESC LIMIT 5;"
# Αν σκασει ο kafka 
#      docker exec finsight-producer pip install --upgrade yfinance
#       docker restart finsight-producer
# logs for kafka docker logs --tail 5 finsight-producer docker logs --tail 5 finsight-consumer