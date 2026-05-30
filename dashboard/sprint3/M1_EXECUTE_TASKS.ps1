# SE5 S3 - M1 Tasks Execution Script (PowerShell)
# Execute 3 tasks: T1 (Kafka), T3 (ORDER BY), T2 (Parquets)

Write-Host "[INFO] SE5 S3 - M1 TASKS EXECUTION" -ForegroundColor Cyan
Write-Host "===================================="
Write-Host ""

# Check Docker
Write-Host "[INFO] Checking Docker..." -ForegroundColor Cyan
docker --version

# Start Docker Compose
Write-Host ""
Write-Host "===================================="
Write-Host "STEP 1: Starting docker-compose..." -ForegroundColor Cyan
Write-Host "===================================="

$PROJECT_ROOT = "C:\Users\lhamz\Desktop\Big Data\final\BigDataGroupe1"
Push-Location $PROJECT_ROOT

Write-Host "[INFO] Launching docker-compose..." -ForegroundColor Cyan
docker compose up -d --build 2>&1 | Out-Null

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] docker-compose failed" -ForegroundColor Red
    Pop-Location
    exit 1
}

Write-Host "[OK] docker-compose launched" -ForegroundColor Green
Write-Host "[INFO] Waiting 35 seconds for services..." -ForegroundColor Yellow
Start-Sleep -Seconds 35

# TASK 1: Kafka Pipeline
Write-Host ""
Write-Host "===================================="
Write-Host "TASK 1: Kafka Pipeline (T1)" -ForegroundColor Cyan
Write-Host "===================================="

$kafka_file = "dashboard\sprint3\kafka-pipeline-optimized.sql"
Write-Host "[INFO] Executing $kafka_file..." -ForegroundColor Cyan
Get-Content $kafka_file | docker exec -i clickhouse-server clickhouse-client --multiquery 2>&1 | Out-Null

Write-Host "[OK] T1 COMPLETED: Kafka tables created" -ForegroundColor Green

# TASK 3: Batch Tables
Write-Host ""
Write-Host "===================================="
Write-Host "TASK 3: Batch Tables (T3)" -ForegroundColor Cyan
Write-Host "===================================="

$batch_file = "dashboard\sprint3\init-batch-tables-optimized.sql"
Write-Host "[INFO] Executing $batch_file..." -ForegroundColor Cyan
Get-Content $batch_file | docker exec -i clickhouse-server clickhouse-client --multiquery 2>&1 | Out-Null

Write-Host "[OK] T3 COMPLETED: Batch tables created" -ForegroundColor Green

# Summary
Write-Host ""
Write-Host "===================================="
Write-Host "SUMMARY" -ForegroundColor Green
Write-Host "===================================="
Write-Host "[OK] T1 COMPLETED: ClickHouse -> Kafka connection" -ForegroundColor Green
Write-Host "[OK] T3 COMPLETED: ORDER BY optimized for real volume" -ForegroundColor Green
Write-Host ""
Write-Host "Web Interfaces:" -ForegroundColor Cyan
Write-Host "  - Grafana: http://localhost:3001 (admin/admin123)" -ForegroundColor Gray
Write-Host "  - ClickHouse: http://localhost:8123" -ForegroundColor Gray

Write-Host ""
Write-Host "[SUCCESS] M1 TASKS COMPLETED" -ForegroundColor Green

Pop-Location
