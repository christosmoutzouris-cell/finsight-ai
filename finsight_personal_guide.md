# FinSight AI — Προσωπικός Οδηγός
**Data Engineer: Christos Moutzouris**
*Τελευταία ενημέρωση: Ιούλιος 2026*

---

## Τι είναι το FinSight AI

Ένα **end-to-end financial data engineering platform** που φτιάχτηκε από το μηδέν ως self-directed learning project. Καλύπτει ολόκληρο το data pipeline lifecycle: από real-time streaming δεδομένων μέχρι AI-powered insights, με orchestration, transformations, neural networks και LLM reports.

**Γιατί το έφτιαξα**: Να μάθω πρακτικά Kafka, Spark, Airflow, dbt, PyTorch και LLMs — τεχνολογίες που ζητούνται σε Data Engineer roles στην Ευρώπη.

---

## Αρχιτεκτονική Overview

```
Yahoo Finance API
      ↓
[Kafka Producer] → [Kafka] → [Kafka Consumer]
                                    ↓
                             PostgreSQL (Bronze)
                                    ↓
                    Airflow DAG (κάθε βράδυ 21:30 UTC)
                    ├── Spark: Bronze → Silver → Gold
                    ├── dbt: SQL transformations + tests
                    ├── Sentiment Analysis (rule-based)
                    ├── Neural Networks (sentiment + price direction)
                    ├── LSTM Price Prediction
                    └── LLM Reports (Ollama/Llama 3.2)
                                    ↓
                             FastAPI REST API (:8088)
```

---

## Phase 1 — Apache Kafka (Streaming)

### Τι είναι
Το **Apache Kafka** είναι ένα distributed message streaming platform. Σκέψου το σαν ταχυδρομείο υψηλής απόδοσης: ο αποστολέας (producer) βάζει μηνύματα σε ένα "γραμματοκιβώτιο" (topic), και ο παραλήπτης (consumer) τα παίρνει με τη σειρά.

### Γιατί το χρησιμοποιούμε
- **Real-time data**: τιμές μετοχών κάθε 10 δευτερόλεπτα
- **Decoupling**: ο producer δεν ξέρει τίποτα για τον consumer
- **Durability**: τα messages αποθηκεύονται — αν πέσει ο consumer, δεν χάνονται

### Τι κάνει στο FinSight
```
Producer (Python)
  → τραβάει τιμές από Yahoo Finance API (yfinance)
  → στέλνει JSON messages στο Kafka topic "stock_prices"
  → κάθε 10 δευτερόλεπτα, 6 symbols (AAPL, MSFT, GOOGL, AMZN, NVDA, TSLA)

Kafka Broker
  → αποθηκεύει τα messages σε partitions
  → διατηρεί ordering ανά symbol (key-based partitioning)

Consumer (Python)
  → διαβάζει από το topic
  → γράφει στη PostgreSQL (πίνακας stock_prices)
  → batch inserts κάθε 5 messages για απόδοση
```

### Βασικές έννοιες
- **Topic**: κανάλι μηνυμάτων (π.χ. "stock_prices")
- **Partition**: παράλληλα τμήματα ενός topic
- **Offset**: η θέση ενός message στο partition
- **Consumer Group**: ομάδα consumers που μοιράζονται partitions
- **event_time vs ingested_at**: πότε συνέβη vs πότε καταγράφηκε

### Πώς να ελέγξεις
```bash
# Kafka UI
http://localhost:8080 → Topics → stock_prices → Messages

# PostgreSQL
docker exec finsight-postgres psql -U finsight -d finsight_db \
  -c "SELECT symbol, price, ingested_at FROM stock_prices ORDER BY ingested_at DESC LIMIT 5;"
```

### Containers
- `finsight-zookeeper`: συντονίζει τον Kafka broker
- `finsight-kafka`: ο broker
- `finsight-kafka-ui`: web UI (port 8080)
- `finsight-producer`: Python script που στέλνει data
- `finsight-consumer`: Python script που λαμβάνει και αποθηκεύει

---

## Phase 2 — Apache Airflow (Orchestration)

### Τι είναι
Το **Apache Airflow** είναι ένα workflow orchestration platform. Φτιάχνεις DAGs (Directed Acyclic Graphs) — flowcharts εργασιών που εκτελούνται αυτόματα σε συγκεκριμένη ώρα.

### Γιατί το χρησιμοποιούμε
- **Scheduling**: εκτέλεση pipeline κάθε βράδυ αυτόματα
- **Dependencies**: task B τρέχει μόνο αν πετύχει το task A
- **Monitoring**: βλέπεις live αν κάθε task πέτυχε ή απέτυχε
- **Retry logic**: αν αποτύχει, ξαναπροσπαθεί αυτόματα

### Τι κάνει στο FinSight
```
DAG 1: daily_stock_pipeline (21:30 UTC, Δε-Πα)
  ├── fetch_daily_data    → τραβάει daily OHLCV από Yahoo Finance
  ├── validate_data       → ελέγχει data quality
  ├── load_to_postgres    → γράφει στο daily_stock_summary
  ├── generate_report     → εκτυπώνει daily report
  └── trigger_medallion   → ξεκινάει το DAG 2

DAG 2: spark_medallion_pipeline (triggered από DAG 1)
  ├── fix_spark_permissions   → διορθώνει permissions
  ├── cleanup_old_data        → διαγράφει παλιά δεδομένα
  ├── update_yfinance         → αναβαθμίζει το library
  ├── bronze_to_silver        → Spark transformation
  ├── silver_to_gold          → Spark aggregation
  ├── dbt_run                 → SQL models + tests
  ├── sentiment_analysis      → rule-based sentiment
  ├── nn_sentiment            → Neural Network sentiment
  ├── nn_price_direction      → Neural Network direction
  ├── llm_reports             → LLM financial reports
  ├── lstm_prediction         → LSTM price forecast
  ├── lstm_evaluate           → αξιολόγηση predictions
  └── data_quality_report     → τελικό report
```

### Βασικές έννοιες
- **DAG**: Directed Acyclic Graph — το workflow
- **Task**: ένα βήμα του DAG
- **Operator**: τύπος task (PythonOperator, BashOperator κλπ)
- **XCom**: επικοινωνία δεδομένων μεταξύ tasks
- **Schedule**: cron expression για αυτόματη εκτέλεση
- **TriggerDagRunOperator**: ένα DAG ξεκινάει άλλο DAG

### Πώς να ελέγξεις
```
http://localhost:8081 (admin/admin123)
→ DAGs → daily_stock_pipeline → Graph view
```

### Containers
- `finsight-airflow-db`: PostgreSQL για Airflow metadata
- `finsight-airflow-init`: αρχικοποίηση (τρέχει μια φορά)
- `finsight-airflow-webserver`: UI (port 8081)
- `finsight-airflow-scheduler`: εκτελεί τα DAGs

---

## Phase 3 — Apache Spark (Medallion Architecture)

### Τι είναι
Το **Apache Spark** είναι ένα distributed computing framework για επεξεργασία μεγάλου όγκου δεδομένων. Τρέχει παράλληλα σε πολλούς "workers" και είναι 100x πιο γρήγορο από MapReduce.

### Γιατί το χρησιμοποιούμε
- **Scale**: μπορεί να επεξεργαστεί terabytes δεδομένων
- **Transformations**: καθαρισμός, enrichment, aggregations
- **Medallion Architecture**: industry standard pattern

### Medallion Architecture
```
Bronze Layer (raw data)
  → stock_prices (ακριβώς όπως ήρθαν από Kafka)
  → "source of truth" — ποτέ δεν αλλάζουμε αυτά

Silver Layer (cleaned + enriched)
  → stock_prices_silver
  → καθαρά δεδομένα + MA5, MA20, pct_change, price_change
  → duplicates removed, nulls handled

Gold Layer (aggregated + business-ready)
  → gold_daily_metrics: OHLCV aggregation ανά ημέρα
  → gold_symbol_snapshot: τελευταία τιμή κάθε symbol με signals
  → έτοιμο για dashboard και AI
```

### Τι κάνει κάθε job
```python
# bronze_to_silver.py
1. Διαβάζει από stock_prices (Bronze)
2. Καθαρίζει: φιλτράρει price > 0, volume > 0
3. Αφαιρεί duplicates
4. Υπολογίζει Moving Averages (MA5, MA20) με Window Functions
5. Υπολογίζει price_change και pct_change
6. Γράφει στο stock_prices_silver (Silver)

# silver_to_gold.py
1. Διαβάζει από stock_prices_silver (Silver)
2. Aggregation ανά ημέρα: OHLCV, volatility, tick_count
3. Latest snapshot: τελευταία τιμή + MA5 signal (BULLISH/BEARISH)
4. Γράφει στο gold_daily_metrics και gold_symbol_snapshot (Gold)
```

### Βασικές έννοιες
- **SparkSession**: το entry point
- **DataFrame**: distributed table (σαν pandas αλλά distributed)
- **Transformation**: lazy operation (δεν εκτελείται αμέσως)
- **Action**: trigger execution (count, write κλπ)
- **Window Function**: υπολογισμός πάνω σε ordered partitions
- **JDBC**: σύνδεση με PostgreSQL

### Πώς να ελέγξεις
```bash
# Spark UI
http://localhost:8082

# PostgreSQL
docker exec finsight-postgres psql -U finsight -d finsight_db \
  -c "SELECT symbol, price, ma5, ma20, ma5_signal FROM gold_symbol_snapshot;"
```

### Containers
- `finsight-spark-master`: ο "διευθυντής" που κατανέμει εργασίες
- `finsight-spark-worker`: εκτελεί τις εργασίες

---

## Phase 4 — dbt (Data Transformations)

### Τι είναι
Το **dbt** (data build tool) είναι ένα SQL-first transformation framework. Γράφεις transformations σε SQL files, και το dbt τα εκτελεί με version control, tests και documentation.

### Γιατί το χρησιμοποιούμε
- **SQL με version control**: κάθε transformation σε .sql αρχείο στο git
- **Tests**: αυτόματος έλεγχος data quality
- **Documentation**: auto-generated lineage graph
- **Refs**: αναφορές μεταξύ models χωρίς hardcoded names

### Δομή
```
dbt/
├── models/
│   ├── staging/          ← Views (δεν αποθηκεύουν data)
│   │   ├── stg_stock_prices.sql      ← από Silver layer
│   │   └── stg_daily_summary.sql     ← από daily_stock_summary
│   └── marts/            ← Tables (αποθηκεύουν data)
│       ├── fct_stock_performance.sql ← cumulative return, volatility
│       └── dim_symbol_stats.sql      ← aggregate stats ανά symbol
└── tests/
    ├── check_price_positive.sql
    ├── check_high_low.sql
    └── check_daily_return.sql
```

### Tests που τρέχουν
```yaml
not_null:     κανένα null στα critical fields
unique:       κάθε symbol μια φορά στο dim_symbol_stats
accepted_values: μόνο τα 6 γνωστά symbols
expression_is_true: close > 0, volume > 0
check_recency: δεδομένα όχι παλιότερα από 7 μέρες
equal_rowcount: πηγή και mart έχουν ίδιο count
```

### ODS vs DWH (αναλογία με Eurobank)
```
ODS (Operational Data Store) = staging layer
  → raw ή ελαφρώς καθαρισμένα από source systems
  → αντιστοιχεί σε: stock_prices, daily_stock_summary

DWH (Data Warehouse) = marts layer
  → καθαρό, transformations, για reporting
  → αντιστοιχεί σε: fct_stock_performance, dim_symbol_stats

Στην Eurobank/DB2: stored procedures + ETL jobs
Στο FinSight: dbt models με SQL files + version control
```

### Πώς να ελέγξεις
```bash
# dbt docs UI
http://localhost:8083

# Tests
docker exec finsight-dbt dbt test

# Results
docker exec finsight-postgres psql -U finsight -d finsight_db \
  -c "SELECT * FROM dim_symbol_stats;"
```

---

## Phase 5a — Sentiment Analysis

### Τι είναι
**Rule-based sentiment analysis** για financial news headlines. Ταχύτατο, χωρίς training, αξιόπιστο για financial text.

### Πώς δουλεύει
```python
POSITIVE_WORDS = {"surges", "beats", "record", "growth", ...}
NEGATIVE_WORDS = {"plunges", "loss", "layoffs", "downgrade", ...}

# Για κάθε headline:
pos_score = len(words ∩ POSITIVE_WORDS)
neg_score = len(words ∩ NEGATIVE_WORDS)

if pos_score > neg_score → POSITIVE
elif neg_score > pos_score → NEGATIVE
else → NEUTRAL
```

### Πηγές headlines
1. Yahoo Finance RSS feed (real headlines)
2. Synthetic headlines ανά symbol (fallback)

### Αποθήκευση
```sql
sentiment_scores (symbol, headline, sentiment, confidence, analyzed_at)
```

---

## Phase 5b — LSTM Price Prediction

### Τι είναι
**Long Short-Term Memory** neural network — τύπος RNN που "θυμάται" long-term patterns σε time series data.

### Γιατί LSTM για τιμές;
Οι τιμές μετοχών είναι **sequential** — η τιμή αύριο εξαρτάται από τις τιμές των τελευταίων Ν ημερών. Ένα απλό NN δεν θυμάται αυτή τη σχέση. Το LSTM έχει "memory cells" που κρατούν πληροφορίες για πολλά steps.

### Αρχιτεκτονική
```
Input: τελευταίες 20 κανονικοποιημένες τιμές
  ↓
LSTM Layer 1 (hidden_dim=64)
  ↓
LSTM Layer 2
  ↓
Dropout (0.2) — αποτρέπει overfitting
  ↓
Linear Layer
  ↓
Output: 1 τιμή (η επόμενη)
```

### Normalization
Οι τιμές κανονικοποιούνται στο [0,1] με MinMaxScaler γιατί:
- AAPL ~$300 vs NVDA ~$200 → διαφορετικές κλίμακες
- Το NN μαθαίνει καλύτερα με normalized data

### Αποθήκευση
```sql
price_predictions (symbol, current_price, predicted_price,
                   predicted_change, predicted_direction,
                   actual_price, direction_correct, evaluated_at)
```

### LSTM Accuracy Evaluation
Κάθε μέρα:
1. Κάνει νέα prediction
2. Συγκρίνει χθεσινή prediction με πραγματική τιμή
3. Υπολογίζει: price_error, direction_correct

---

## Phase 5b.5 — Neural Networks

### Sentiment Neural Network
**LSTM-based classifier** για financial sentiment.

```
Αρχιτεκτονική:
Text → Tokenization → Embedding(64) → LSTM(128) → Dropout → Linear → [NEG, NEU, POS]

Training data: 60 labeled financial headlines (20 ανά κλάση)
Epochs: 30
Αποθήκευση: /app/models/sentiment_nn.pt (φορτώνεται αν υπάρχει)
```

**Διαφορά από rule-based**:
- Rule-based: κοιτάει μόνο keywords
- NN: καταλαβαίνει context ("not good" ≠ "good")

### Price Direction Neural Network
**Feedforward NN** που προβλέπει UP/DOWN.

```
Features:
  - price_change, pct_change
  - ma5/price (normalized), ma20/price (normalized)
  - volume/1M
  - (ma5-ma20)/ma20

Αρχιτεκτονική:
[6 features] → Linear(64) → ReLU → Dropout → Linear(32) → ReLU → Linear(2)

Output: [P(DOWN), P(UP)]
```

**Class Imbalance**: λόγω του ότι ο producer τρέχει 24/7 αλλά το market είναι ανοιχτό μόνο 6.5h/day, ~75% των ticks δεν έχουν αλλαγή τιμής → model bias προς DOWN. Fix: class weights στο CrossEntropyLoss.

### Αποθήκευση
```sql
sentiment_nn_scores (symbol, headline, nn_sentiment, nn_confidence, ...)
price_direction_predictions (symbol, predicted_dir, confidence, prob_up, prob_down, ...)
```

---

## Phase 5c — LLM Summaries (Ollama)

### Τι είναι
Το **Ollama** τρέχει Large Language Models τοπικά — δωρεάν, χωρίς API key, χωρίς internet. Χρησιμοποιούμε **Llama 3.2 (3B parameters)**.

### Γιατί local LLM;
- **Privacy**: financial data δεν φεύγουν από τον υπολογιστή
- **Cost**: δωρεάν vs $0.01/1K tokens (OpenAI)
- **Production pattern**: τράπεζες και fintech χρησιμοποιούν local LLM

### Πώς δουλεύει
```
1. get_symbol_data(symbol)
   → μαζεύει: τιμή, MA5/MA20, sentiment, LSTM prediction, NN direction

2. build_prompt(data)
   → φτιάχνει structured prompt με όλα τα δεδομένα
   → "You are a financial analyst. Write 3 sentences about AAPL..."

3. call_ollama(prompt)
   → HTTP POST στο http://finsight-ollama:11434/api/generate
   → temperature=0.3 (factual, όχι creative)

4. save_reports(reports)
   → γράφει στο llm_reports table
```

### Prompt Engineering
Καλές πρακτικές που χρησιμοποιούμε:
1. Ρόλος: "You are a concise financial analyst"
2. Συγκεκριμένα δεδομένα (όχι vague)
3. Explicit format: "Write exactly 3 sentences"
4. Constraints: "Max 100 words"

### Αποθήκευση
```sql
llm_reports (symbol, report, model, data_snapshot JSONB, generated_at)
```

---

## Phase 6 — FastAPI REST API

### Τι είναι
Το **FastAPI** είναι ένα modern Python web framework για REST APIs. Είναι 2-3x πιο γρήγορο από Flask και έχει automatic documentation.

### Γιατί FastAPI;
- **Auto docs**: /docs → Swagger UI, /redoc → ReDoc
- **Type safety**: Pydantic models για validation
- **Async**: υποστηρίζει async/await
- **OpenAPI**: automatic schema generation

### Endpoints
```
GET  /                    → health check
GET  /symbols             → λίστα active symbols
POST /symbols             → προσθήκη νέου symbol
DELETE /symbols/{symbol}  → απενεργοποίηση symbol
GET  /prices/{symbol}     → τελευταίες τιμές + snapshot
GET  /sentiment/{symbol}  → rule-based + NN sentiment
GET  /predictions/{symbol}→ LSTM + NN + accuracy
GET  /report/{symbol}     → LLM report
GET  /dashboard           → όλα μαζί (για frontend)
```

### Πώς να ελέγξεις με Postman
```
GET  http://localhost:8088/
GET  http://localhost:8088/symbols
GET  http://localhost:8088/dashboard
GET  http://localhost:8088/prices/AAPL
GET  http://localhost:8088/sentiment/AAPL
GET  http://localhost:8088/predictions/AAPL
GET  http://localhost:8088/report/AAPL

POST http://localhost:8088/symbols
Body: {"symbol": "META", "company": "Meta Platforms", "sector": "Technology"}

DELETE http://localhost:8088/symbols/META
```

### Swagger UI
```
http://localhost:8088/docs → interactive documentation
```

### Αρχιτεκτονική containers
```
Postman/Browser → finsight-api:8088 (FastAPI)
                        ↓ psycopg2
                 finsight-postgres:5432
```

---

## Βάση Δεδομένων — Πίνακες

```sql
-- Streaming data
stock_prices              ← Bronze: raw ticks από Kafka
stock_prices_silver       ← Silver: cleaned + MA5/MA20
gold_daily_metrics        ← Gold: daily OHLCV aggregation
gold_symbol_snapshot      ← Gold: latest price + signals

-- Daily batch
daily_stock_summary       ← daily OHLCV από Airflow

-- dbt models
stg_stock_prices          ← VIEW: staging
stg_daily_summary         ← VIEW: staging
fct_stock_performance     ← TABLE: cumulative return, volatility, rank
dim_symbol_stats          ← TABLE: aggregate stats ανά symbol

-- AI/ML
sentiment_scores          ← rule-based sentiment
sentiment_nn_scores       ← Neural Network sentiment
price_predictions         ← LSTM predictions + evaluation
price_direction_predictions ← NN direction predictions
llm_reports               ← LLM-generated reports

-- Config
watched_symbols           ← active symbols (source of truth)
```

---

## Docker Services — Πλήρης Λίστα

| Container | Port | Τι κάνει |
|---|---|---|
| finsight-zookeeper | - | Συντονίζει Kafka |
| finsight-kafka | 9092 | Message broker |
| finsight-kafka-ui | 8080 | Kafka web UI |
| finsight-postgres | 5432 | Κύρια βάση δεδομένων |
| finsight-producer | - | Στέλνει τιμές στο Kafka |
| finsight-consumer | - | Γράφει Kafka→PostgreSQL |
| finsight-airflow-db | - | Airflow metadata DB |
| finsight-airflow-init | - | Αρχικοποίηση (εφάπαξ) |
| finsight-airflow-webserver | 8081 | Airflow UI |
| finsight-airflow-scheduler | - | Εκτελεί DAGs |
| finsight-spark-master | 8082 | Spark master |
| finsight-spark-worker | - | Spark worker |
| finsight-dbt | 8083 | dbt docs server |
| finsight-ollama | 11434 | Local LLM server |
| finsight-sentiment | - | Sentiment analysis |
| finsight-lstm | - | LSTM predictions |
| finsight-neural-networks | - | NN sentiment + direction |
| finsight-llm | - | LLM reports |
| finsight-api | 8088 | FastAPI REST API |

---

## Εκκίνηση Project

### Πρώτη φορά
```bash
# Mac/Linux
./setup-env.sh        # δημιουργεί .env με password
docker compose up -d  # εκκινεί όλα τα services
./setup.sh            # first-time setup

# Windows
.\setup-env.ps1
docker compose up -d
.\setup.ps1
```

### Κανονική εκκίνηση (μετά από restart)
```bash
docker compose up -d
# Περίμενε 2 λεπτά
# Άνοιξε http://localhost:8081 → Trigger DAG αν θέλεις manual run
```

### Χρήσιμες εντολές
```bash
# Logs
docker logs --tail 20 finsight-producer
docker logs --tail 20 finsight-consumer

# Fix yfinance (αν σπάσει)
docker exec finsight-producer pip install --upgrade yfinance
docker restart finsight-producer

# Manual Spark jobs
docker exec finsight-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.3 \
  /opt/spark_jobs/bronze_to_silver.py

# dbt
docker exec finsight-dbt dbt run
docker exec finsight-dbt dbt test

# Data quality check
docker exec finsight-postgres psql -U finsight -d finsight_db \
  -c "SELECT count(*) FROM stock_prices;"
```

---

## Τεχνολογίες & Γιατί

| Τεχνολογία | Γιατί |
|---|---|
| **Apache Kafka** | Real-time streaming, decoupling producer/consumer |
| **Apache Spark** | Distributed processing, Medallion Architecture |
| **Apache Airflow** | Orchestration, scheduling, monitoring |
| **dbt** | SQL transformations με version control + tests |
| **PostgreSQL** | Αξιόπιστη relational DB για όλα τα layers |
| **PyTorch** | Neural networks (LSTM, sentiment NN) |
| **Ollama/Llama 3.2** | Local LLM, privacy, free |
| **FastAPI** | Modern REST API, auto-docs, type-safe |
| **Docker Compose** | Container orchestration σε single machine |
| **yfinance** | Yahoo Finance API (unofficial, free) |
| **scikit-learn** | Data preprocessing (MinMaxScaler, StandardScaler) |

---

## Patterns & Concepts

### Lambda Architecture
```
Speed Layer:  Kafka streaming → real-time ticks
Batch Layer:  Airflow DAG → daily processing
Serving Layer: FastAPI → unified access
```

### Medallion Architecture (Databricks pattern)
```
Bronze: raw data, never modified
Silver: cleaned + enriched
Gold:   business-ready aggregations
```

### Event Time vs Processing Time
```
event_time:   πότε συνέβη το γεγονός (Wall Street)
ingested_at:  πότε καταγράφηκε στη βάση μας
```

### Class Imbalance (ML)
Όταν μια κλάση εμφανίζεται πολύ πιο συχνά → model bias.
Fix: class weights στο loss function.

### ODS vs DWH
```
ODS: Operational Data Store → σχεδόν raw, για operations
DWH: Data Warehouse → καθαρό, για analytics/reporting
```

---

*Ο οδηγός θα ενημερωθεί μετά την προσθήκη Kubernetes.*
