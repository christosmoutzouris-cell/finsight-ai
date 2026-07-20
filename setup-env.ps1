# FinSight AI — Environment Setup (Windows PowerShell)

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  FinSight AI — Environment Setup" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

if (Test-Path ".env") {
    $replace = Read-Host "⚠️  Το .env υπάρχει ήδη. Θέλεις να το αντικαταστήσεις; (y/N)"
    if ($replace -ne "y" -and $replace -ne "Y") {
        Write-Host "Κράτησα το υπάρχον .env"
        exit 0
    }
}

Write-Host "Παρακαλώ εισάγαγε τα credentials:" -ForegroundColor Yellow
Write-Host ""

$PG_USER = Read-Host "PostgreSQL username [finsight]"
if ([string]::IsNullOrEmpty($PG_USER)) { $PG_USER = "finsight" }

do {
    $PG_PASS  = Read-Host "PostgreSQL password" -AsSecureString
    $PG_PASS2 = Read-Host "Επιβεβαίωσε password" -AsSecureString

    $pass1 = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($PG_PASS))
    $pass2 = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($PG_PASS2))

    if ([string]::IsNullOrEmpty($pass1)) {
        Write-Host "❌ Το password δεν μπορεί να είναι κενό" -ForegroundColor Red
        $match = $false
    } elseif ($pass1 -ne $pass2) {
        Write-Host "❌ Τα passwords δεν ταιριάζουν" -ForegroundColor Red
        $match = $false
    } else {
        $match = $true
    }
} while (-not $match)

$PG_DB = Read-Host "PostgreSQL database name [finsight_db]"
if ([string]::IsNullOrEmpty($PG_DB)) { $PG_DB = "finsight_db" }

$PG_PORT = Read-Host "PostgreSQL host port [5432]"
if ([string]::IsNullOrEmpty($PG_PORT)) { $PG_PORT = "5432" }

@"
POSTGRES_USER=$PG_USER
POSTGRES_PASSWORD=$pass1
POSTGRES_DB=$PG_DB
POSTGRES_PORT=$PG_PORT

KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=stock_prices

SYMBOLS=AAPL,MSFT,GOOGL,AMZN,NVDA,TSLA
POLL_INTERVAL_SECONDS=10

DOCKER_SOCK=//./pipe/dockerDesktopLinuxEngine
"@ | Out-File -FilePath ".env" -Encoding UTF8

Write-Host ""
Write-Host "✅ Το .env δημιουργήθηκε επιτυχώς!" -ForegroundColor Green
Write-Host ""
Write-Host "Επόμενο βήμα:"
Write-Host "  docker compose up -d"
Write-Host "  .\setup.ps1"
Write-Host "==================================================" -ForegroundColor Cyan
# FinSight AI — First-time setup script (Windows PowerShell)
# Τρέξε αυτό μετά το: docker compose up -d

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  FinSight AI — Setup" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan

# ── Βήμα 1: Περίμενε να ξεκινήσουν τα services ───────
Write-Host "`n[1/6] Περιμένω να ξεκινήσουν τα services (60s)..."
Start-Sleep -Seconds 60

# ── Βήμα 2: Fix yfinance ──────────────────────────────
Write-Host "`n[2/6] Αναβαθμίζω yfinance..."
docker exec finsight-producer pip install --upgrade yfinance -q
docker restart finsight-producer
Write-Host "      ✓ yfinance updated" -ForegroundColor Green

# ── Βήμα 3: Fix dbt ───────────────────────────────────
Write-Host "`n[3/6] Ρυθμίζω dbt..."
docker exec -u root finsight-dbt apt-get install -y git -q
docker exec finsight-dbt pip install "protobuf==4.25.3" -q
Write-Host "      ✓ dbt ready" -ForegroundColor Green

# ── Βήμα 4: Fix Spark permissions ─────────────────────
Write-Host "`n[4/6] Ρυθμίζω Spark..."
docker exec -u root finsight-spark-master mkdir -p /home/spark/.ivy2/cache
docker exec -u root finsight-spark-master chmod -R 777 /home/spark/.ivy2
Write-Host "      ✓ Spark permissions OK" -ForegroundColor Green

# ── Βήμα 5: Τρέξε Spark jobs ──────────────────────────
Write-Host "`n[5/6] Τρέχω Spark Medallion pipeline..."
Write-Host "      Bronze -> Silver..."
docker exec finsight-spark-master `
    /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --packages org.postgresql:postgresql:42.7.3 `
    /opt/spark_jobs/bronze_to_silver.py

Write-Host "      Silver -> Gold..."
docker exec finsight-spark-master `
    /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --packages org.postgresql:postgresql:42.7.3 `
    /opt/spark_jobs/silver_to_gold.py
Write-Host "      ✓ Spark jobs complete" -ForegroundColor Green

# ── Βήμα 6: Τρέξε dbt ────────────────────────────────
Write-Host "`n[6/6] Τρέχω dbt models..."
docker exec finsight-dbt dbt run
docker exec finsight-dbt dbt test
Write-Host "      ✓ dbt complete" -ForegroundColor Green

Write-Host "`n==================================================" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Green
Write-Host ""
Write-Host "  Kafka UI:  http://localhost:8080"
Write-Host "  Airflow:   http://localhost:8081  (admin/admin123)"
Write-Host "  Spark UI:  http://localhost:8082"
Write-Host "==================================================" -ForegroundColor Cyan