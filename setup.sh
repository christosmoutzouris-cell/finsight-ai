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

# ── Βήμα 5: Τρέξε