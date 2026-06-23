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




------------------------------------------------------------------------------------------
------------------------- prsonal notes -------------------------------

-- To clone this repo to a new machine
git clone https://github.com/christosmoutzouris-cell/finsight-ai.git
cd finsight-ai
cp .env.example .env
# Επεξεργασία .env με τα passwords σου
docker compose up -d




--setup after docker compose up -d

1. Γιατί βάλαμε τα Spark jobs στο setup script αφού υπάρχουν στο Airflow;
Το Airflow DAG τρέχει κάθε βράδυ αυτόματα — αλλά μόνο αν υπάρχουν ήδη οι πίνακες stock_prices_silver και gold_symbol_snapshot στη βάση.

Την πρώτη φορά σε νέο laptop:

Η βάση είναι άδεια
Το dbt θα αποτύχει γιατί δεν υπάρχουν ακόμα οι πίνακες Silver/Gold
Ο Airflow DAG δεν έχει τρέξει ποτέ
Οπότε το setup script τα τρέχει μια φορά για να δημιουργήσει τους πίνακες. Μετά αναλαμβάνει ο Airflow αυτόματα.

2. Πώς τα τρέχω — διπλό κλικ ή αυτόματα;
Το setup script το τρέχεις μια φορά χειροκίνητα μετά το docker compose up -d:

Windows (PowerShell):

powershell
.\setup.ps1
Mac/Linux (Terminal):

bash
chmod +x setup.sh
./setup.sh
Μετά από αυτό όλα τρέχουν αυτόματα:

Τι	Πότε	Πώς
Kafka streaming	Συνεχώς κάθε 10s	Αυτόματα με docker compose up
Airflow DAG	Κάθε βράδυ 22:00	Αυτόματα από scheduler
Spark + dbt	Μέσα στο DAG	Αυτόματα μετά το Airflow













--push changes
cd "C:\Users\ChristosMoutzouris\OneDrive - Agile Actors\Desktop\AI Academy\finsight-ai"
git init
git add .
git commit -m "Initial commit: FinSight AI Phases 1-4 + setup scripts"
git branch -M main
git remote add origin https://github.com/christosmoutzouris-cell/finsight-ai.git
git push -u origin main