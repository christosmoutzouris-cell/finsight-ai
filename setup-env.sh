#!/bin/bash
# FinSight AI — Environment Setup
# Τρέξε αυτό ΠΡΩΤΑ πριν το docker compose up

echo "=================================================="
echo "  FinSight AI — Environment Setup"
echo "=================================================="
echo ""

# Έλεγξε αν υπάρχει ήδη .env
if [ -f ".env" ]; then
    echo "⚠️  Το .env υπάρχει ήδη."
    read -p "Θέλεις να το αντικαταστήσεις; (y/N): " replace
    if [ "$replace" != "y" ] && [ "$replace" != "Y" ]; then
        echo "Κράτησα το υπάρχον .env"
        exit 0
    fi
fi

echo ""
echo "Παρακαλώ εισάγαγε τα credentials για τη βάση δεδομένων:"
echo "(Πάτα Enter για default τιμές)"
echo ""

# PostgreSQL credentials
read -p "PostgreSQL username [finsight]: " PG_USER
PG_USER=${PG_USER:-finsight}

while true; do
    read -s -p "PostgreSQL password: " PG_PASS
    echo ""
    if [ -z "$PG_PASS" ]; then
        echo "❌ Το password δεν μπορεί να είναι κενό"
        continue
    fi
    read -s -p "Επιβεβαίωσε password: " PG_PASS2
    echo ""
    if [ "$PG_PASS" = "$PG_PASS2" ]; then
        break
    else
        echo "❌ Τα passwords δεν ταιριάζουν. Ξαναπροσπάθησε."
    fi
done

read -p "PostgreSQL database name [finsight_db]: " PG_DB
PG_DB=${PG_DB:-finsight_db}

read -p "PostgreSQL host port [5432]: " PG_PORT
PG_PORT=${PG_PORT:-5432}

# Docker socket
if [[ "$OSTYPE" == "darwin"* ]]; then
    DOCKER_SOCK="/var/run/docker.sock"
else
    DOCKER_SOCK="/var/run/docker.sock"
fi

# Δημιούργησε το .env
cat > .env << EOF
POSTGRES_USER=${PG_USER}
POSTGRES_PASSWORD=${PG_PASS}
POSTGRES_DB=${PG_DB}
POSTGRES_PORT=${PG_PORT}

KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=stock_prices

SYMBOLS=AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA
POLL_INTERVAL_SECONDS=10

DOCKER_SOCK=${DOCKER_SOCK}
EOF

echo ""
echo "✅ Το .env δημιουργήθηκε επιτυχώς!"
echo ""
echo "Επόμενο βήμα:"
echo "  docker compose up -d"
echo "  ./setup.sh"
echo "=================================================="
