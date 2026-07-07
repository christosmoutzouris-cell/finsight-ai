# FinSight AI - Quick Start Guide

## 1. Start All Services

Navigate to the project directory and start all Docker containers.

```powershell
cd "C:\Users\ChristosMoutzouris\OneDrive - Agile Actors\Desktop\AI Academy\finsight-ai"
docker compose up -d
```

> **Note:** Wait approximately **2 minutes** for all services to become available.

---

## 2. Fix `yfinance`

This step is required **after every Docker restart**.

```powershell
docker exec finsight-producer pip install --upgrade yfinance -q
docker restart finsight-producer
```

---

## 3. Verify Kafka Streaming

Confirm that stock prices are being ingested into PostgreSQL.

```powershell
docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, price, ingested_at FROM stock_prices ORDER BY ingested_at DESC LIMIT 5;"
```

---

## 4. Run Spark Medallion Pipeline & dbt

### Set Spark permissions

```powershell
docker exec -u root finsight-spark-master chmod -R 777 /home/spark/.ivy2
```

### Run Bronze → Silver

```powershell
docker exec finsight-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/bronze_to_silver.py
```

### Run Silver → Gold

```powershell
docker exec finsight-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/silver_to_gold.py
```

### Run dbt Models

```powershell
docker exec finsight-dbt dbt run
```

### Run dbt Tests

```powershell
docker exec finsight-dbt dbt test
```

---

## 5. Run Sentiment Analysis

```powershell
docker compose run --rm sentiment
```

---

## 6. Verify Results

### Bronze Table

```powershell
docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) FROM stock_prices;"
```

### Silver Table

```powershell
docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) FROM stock_prices_silver;"
```

### Gold Table

```powershell
docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT * FROM gold_symbol_snapshot;"
```

### Sentiment Results

```powershell
docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, sentiment, confidence FROM sentiment_scores ORDER BY analyzed_at DESC LIMIT 10;"
```

---

# Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | `admin / admin123` |
| Spark UI | http://localhost:8082 | — |
| dbt Docs | http://localhost:8083 | — |

---

## Shutdown

Stop all services:

```powershell
docker compose down
```

Stop services while preserving volumes:

```powershell
docker compose stop
```

Restart all services:

```powershell
docker compose up -d
```