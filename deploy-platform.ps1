$ErrorActionPreference = "Stop"

Write-Host "`nUruchamianie platformy...`n"

# 1. Docker Compose
Write-Host "[1/8] Docker Compose..."
docker compose up -d
if ($LASTEXITCODE -ne 0) { exit 1 }

# 2. PostgreSQL
Write-Host "[2/8] Czekam na PostgreSQL..."
Start-Sleep -Seconds 5
for ($i = 0; $i -lt 60; $i++) {
    $result = docker exec postgres pg_isready -U datauser 2>&1
    if ($result -match "accepting connections") {
        Write-Host "    PostgreSQL gotowy"
        break
    }
    if ($i % 5 -eq 0) { Write-Host "    Proba $($i+1)/60..." }
    Start-Sleep -Seconds 2
}
if ($i -eq 60) {
    Write-Host "    PostgreSQL nie odpowiada!" -ForegroundColor Red
    exit 1
}

# 3. Kafka
Write-Host "[3/8] Czekam na Kafka..."
Start-Sleep -Seconds 5
for ($i = 0; $i -lt 60; $i++) {
    $result = docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "    Kafka gotowy"
        break
    }
    if ($i % 5 -eq 0) { Write-Host "    Proba $($i+1)/60..." }
    Start-Sleep -Seconds 2
}

# 4. Debezium
Write-Host "[4/8] Czekam na Debezium..."
Start-Sleep -Seconds 5
for ($i = 0; $i -lt 60; $i++) {
    try {
        $response = Invoke-RestMethod -Uri "http://localhost:8083/" -TimeoutSec 2 -ErrorAction SilentlyContinue
        if ($response) {
            Write-Host "    Debezium gotowy"
            break
        }
    } catch {   }
    if ($i % 5 -eq 0) { Write-Host "    Proba $($i+1)/60..." }
    Start-Sleep -Seconds 2
}

# 5. dane do postgres
Write-Host "[5/8] Ladowanie danych do PostgreSQL..."
if (Test-Path "requirements.txt") {
    pip install -r requirements.txt --quiet 2>&1 | Out-Null
}
python python/csv_to_postgres.py
if ($LASTEXITCODE -ne 0) { exit 1 }

# 6. Debezium connector
Write-Host "[6/8] Konfiguracja Debezium..."
Start-Sleep -Seconds 5
try {
    try {
        Invoke-RestMethod -Uri "http://localhost:8083/connectors/postgres-connector" -Method Delete -ErrorAction SilentlyContinue | Out-Null
        Start-Sleep -Seconds 2
    } catch {
    }

    # connector
    $config = Get-Content "debezium/connector.json" -Raw
    Invoke-RestMethod -Uri "http://localhost:8083/connectors" -Method Post -ContentType "application/json" -Body $config -ErrorAction Stop | Out-Null
    Write-Host "    Debezium connector skonfigurowany"
} catch {
    Write-Host "    Problem z konfiguracją connectora" -ForegroundColor Yellow
}

# 7. MinIO bucket
Write-Host "[7/8] Konfiguracja MinIO bucket..."
Start-Sleep -Seconds 2
try {
    # mc.exe
    if (-not (Get-Command mc -ErrorAction SilentlyContinue)) {
        Write-Host "    Pobieranie MinIO Client..."
        Invoke-WebRequest -Uri "https://dl.min.io/client/mc/release/windows-amd64/mc.exe" -OutFile "mc.exe"
    }

    # bucket
    .\mc.exe alias set local http://localhost:9000 minio minio123 2>&1 | Out-Null
    .\mc.exe mb local/data --ignore-existing 2>&1 | Out-Null
    Write-Host "    Bucket 'data' gotowy"
} catch {
    Write-Host "    Ostrzezenie: Nie udalo sie utworzyc bucketu automatycznie"
    Write-Host "    Utworz bucket 'data' recznie w MinIO Console: http://localhost:9001"
}

# 8. Spark
Write-Host "`n[8/8] Uruchamianie Spark Streaming..."

Write-Host "    Przygotowywanie srodowiska Spark..."
docker exec -u root spark-master mkdir -p /root/.ivy2/cache 2>&1 | Out-Null

docker cp spark/streaming.py spark-master:/opt/spark/work-dir/

Write-Host "    Uruchamianie streaming job..."

# streaming job bez logów
docker exec -d -u root spark-master /opt/spark/bin/spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog /opt/spark/work-dir/streaming.py


Write-Host "    Czekam na inicjalizacje streaming job..."
Start-Sleep -Seconds 10

$jobRunning = docker exec spark-master ps aux | Select-String "streaming.py"
if ($jobRunning) {
    Write-Host "    Streaming job uruchomiony!" -ForegroundColor Green
} else {
    Write-Host "    Ostrzezenie: Nie mozna potwierdzic uruchomienia job" -ForegroundColor Yellow
    Write-Host "    Sprawdz logi: docker logs spark-master -f" -ForegroundColor Yellow
}

Write-Host "`nEndpointy:"
Write-Host "  PostgreSQL:  localhost:5432" -ForegroundColor Cyan
Write-Host "  Kafka:       localhost:9092" -ForegroundColor Cyan
Write-Host "  Debezium:    http://localhost:8083" -ForegroundColor Cyan
Write-Host "  MinIO:       http://localhost:9001 (minio/minio123)" -ForegroundColor Cyan
Write-Host "  Spark UI:    http://localhost:8080" -ForegroundColor Cyan

Write-Host "`nStreaming pipeline test:"
Write-Host '  docker exec postgres psql -U datauser -d datadb -c "INSERT INTO orders VALUES (21, 3, 7, 100);"' -ForegroundColor Yellow
Write-Host "  Sprawdz dane w MinIO Console: http://localhost:9001 -> bucket 'data' -> delta/orders/" -ForegroundColor Yellow

Write-Host "`nKomendy:"
Write-Host "  docker logs spark-master -f              # Logi Spark streaming" -ForegroundColor Gray
Write-Host "  curl http://localhost:8083/connectors    # Status Debezium connectors" -ForegroundColor Gray
Write-Host "  docker compose down -v --remove-orphans  # zamkniecie platformy" -ForegroundColor Gray

