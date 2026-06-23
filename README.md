# FinSight AI — Financial Data Engineering Platform

End-to-end data engineering project με streaming, batch processing, και AI.

## Stack
- **Streaming**: Apache Kafka + Python Producer/Consumer
- **Orchestration**: Apache Airflow
- **Processing**: Apache Spark (Medallion Architecture)
- **Transformations**: dbt
- **Storage**: PostgreSQL
- **Container**: Docker Compose

## Architecture
Yahoo Finance API

↓

Kafka Producer → Kafka → Kafka Consumer

↓

PostgreSQL (Bronze)

↓

Airflow DAG (daily)

├── Spark: Bronze → Silver → Gold

└── dbt: Financial metrics

## Setup

### Prerequisites
- Docker Desktop
- Git

### Installation

1. Clone το repo:
```bash
git clone https://github.com/christosmoutzouris-cell/finsight-ai.git
cd finsight-ai
```

2. Δημιούργησε `.env` από το template:
```bash
cp .env.example .env
```

3. Εκκίνηση:
```bash
docker compose up -d
```

4. Περίμενε 2-3 λεπτά και άνοιξε:
- Kafka UI: http://localhost:8080
- Airflow: http://localhost:8081 (admin/admin123)
- Spark: http://localhost:8082

### Σημαντικό — yfinance fix
Μετά την πρώτη εκκίνηση:
```bash
docker exec finsight-producer pip install --upgrade yfinance
docker restart finsight-producer
```

### dbt setup
```bash
docker exec -u root finsight-dbt apt-get install -y git
docker exec finsight-dbt pip install "protobuf==4.25.3" -q
docker exec finsight-dbt dbt run
```

## Phases
- ✅ Phase 1: Kafka Streaming
- ✅ Phase 2: Airflow Orchestration  
- ✅ Phase 3: Spark Medallion Architecture
- ✅ Phase 4: dbt Transformations
- ⏳ Phase 5: AI/ML (Sentiment, LSTM, LLM)
- ⏳ Phase 6: FastAPI + Streamlit Dashboard



-- To clone this repo to a new machine
git clone https://github.com/christosmoutzouris-cell/finsight-ai.git
cd finsight-ai
cp .env.example .env
# Επεξεργασία .env με τα passwords σου
docker compose up -d