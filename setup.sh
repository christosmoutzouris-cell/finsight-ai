#!/bin/bash
# FinSight AI — First-time setup script (Mac/Linux)
# Τρέξε αυτό μετά το: docker compose up -d

echo "=================================================="
echo "  FinSight AI — Setup"
echo "=================================================="

# ── Βήμα 1: Περίμενε να ξεκινήσουν τα services ───────
echo ""
echo "[1/6] Περιμένω να ξεκινήσουν τα services (60s)..."
sleep 60

# ── Βήμα 2: Fix yfinance ──────────────────────────────
echo ""
echo "[2/6] Αναβαθμίζω yfinance..."
docker exec finsight-producer pip install --upgrade yfinance -q
docker restart finsight-producer
echo "      ✓ yfinance updated"

# ── Βήμα 3: Fix dbt ───────────────────────────────────
echo ""
echo "[3/6] Ρυθμίζω dbt..."
docker exec -u root finsight-dbt apt-get install -y git -q
docker exec finsight-dbt pip install "protobuf==4.25.3" -q
echo "      ✓ dbt ready"

# ── Βήμα 4: Fix Spark permissions ─────────────────────
echo ""
echo "[4/6] Ρυθμίζω Spark..."
docker exec -u root finsight-spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u root finsight-spark-master chmod -R 777 /home/spark/.ivy2
echo "      ✓ Spark permissions OK"

# ── Βήμα 5: Τρέξε Spark jobs ──────────────────────────
echo ""
echo "[5/6] Τρέχω Spark Medallion pipeline..."
echo "      Bronze -> Silver..."
docker exec finsight-spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.3 \
    /opt/spark_jobs/bronze_to_silver.py

echo "      Silver -> Gold..."
docker exec finsight-spark-master \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.postgresql:postgresql:42.7.3 \
    /opt/spark_jobs/silver_to_gold.py
echo "      ✓ Spark jobs complete"

# ── Βήμα 6: Τρέξε dbt ────────────────────────────────
echo ""
echo "[6/6] Τρέχω dbt models..."
docker exec finsight-dbt dbt run
docker exec finsight-dbt dbt test
echo "      ✓ dbt complete"

echo ""
echo "=================================================="
echo "  Setup Complete!"
echo "  Kafka UI:  http://localhost:8080"
echo "  Airflow:   http://localhost:8081  (admin/admin123)"
echo "  Spark UI:  http://localhost:8082"
echo "  API:       http://localhost:8088"
echo "  dbt docs:  http://localhost:8083"
echo "=================================================="