# ══════════════════════════════════════════════════════════════════════════
# SE5 S3 — Import Parquets SE3 dans ClickHouse  (M1 — Data Engineer)
# Prérequis :
#   1. ClickHouse container en cours (docker ps | grep clickhouse-server)
#   2. Tables créées via init-missing-tables.sql
#   3. SE3_OUTPUT_RESULTS dans le même dossier que ce script (ou ajuster $SE3_ROOT)
# Usage : .\import-se3-local.ps1
# ══════════════════════════════════════════════════════════════════════════

$ErrorActionPreference = "Stop"
$CONTAINER   = "clickhouse-server"
$DB          = "bigdata_db"
$USER_FILES  = "/var/lib/clickhouse/user_files"
$DEST_DIR    = "SE3_OUTPUT"

# Chemin vers SE3_OUTPUT_RESULTS (relatif au repo)
$SE3_ROOT    = Join-Path $PSScriptRoot "..\..\SE3_OUTPUT_RESULTS"
$SE3_ROOT    = (Resolve-Path $SE3_ROOT).Path

Write-Host "`n=== SE5 S3 — Import Parquets SE3 ===" -ForegroundColor Cyan
Write-Host "Source  : $SE3_ROOT"
Write-Host "Cible   : $CONTAINER:$USER_FILES/$DEST_DIR`n"

# ──────────────────────────────────────────────────────────────────────────
# Etape 1 — Copier les dossiers parquet vers user_files du container
# ──────────────────────────────────────────────────────────────────────────
function Copy-ToContainer($localSubPath, $label) {
    $src = Join-Path $SE3_ROOT $localSubPath
    if (-not (Test-Path $src)) {
        Write-Warning "  [SKIP] $label — dossier introuvable : $src"
        return $false
    }
    $parentDir = [System.IO.Path]::GetDirectoryName($localSubPath)
    $destParent = "$USER_FILES/$DEST_DIR/$parentDir"
    docker exec $CONTAINER mkdir -p $destParent | Out-Null
    docker cp $src "${CONTAINER}:${destParent}/"
    Write-Host "  [OK] Copie $label" -ForegroundColor Green
    return $true
}

Write-Host "--- Etape 1 : Copie des parquets ---" -ForegroundColor Yellow
Copy-ToContainer "axe1_reputation\business_with_attributes" "axe1/business_with_attributes"
Copy-ToContainer "axe1_reputation\closure_rate"             "axe1/closure_rate"
Copy-ToContainer "axe1_reputation\monthly_running_avg"      "axe1/monthly_running_avg"
Copy-ToContainer "axe2_geo\avg_by_city"                     "axe2/avg_by_city"
Copy-ToContainer "axe3_social\elite_comparison"             "axe3/elite_comparison"
Copy-ToContainer "axe3_social\seniority_analysis"           "axe3/seniority_analysis"
Copy-ToContainer "axe3_social\user_enriched"                "axe3/user_enriched"
Copy-ToContainer "axe3_social\user_review_enriched"         "axe3/user_review_enriched"
Copy-ToContainer "axe4_temporal\category_hourly"            "axe4/category_hourly"
Copy-ToContainer "axe4_temporal\heatmap_by_biz"             "axe4/heatmap_by_biz"
Copy-ToContainer "axe4_temporal\heatmap_global"             "axe4/heatmap_global"
Copy-ToContainer "axe4_temporal\monthly_trend"              "axe4/monthly_trend"
Copy-ToContainer "axe4_temporal\stars_by_hour"              "axe4/stars_by_hour"
Copy-ToContainer "axe4_temporal\stars_by_dow"               "axe4/stars_by_dow"

# ──────────────────────────────────────────────────────────────────────────
# Helper — exécute une requête ClickHouse dans le container
# ──────────────────────────────────────────────────────────────────────────
function Run-CH($sql, $label) {
    Write-Host "  [IMPORT] $label ..." -NoNewline
    $result = docker exec $CONTAINER clickhouse-client `
        --database $DB `
        --query $sql 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host " ERREUR" -ForegroundColor Red
        Write-Warning $result
    } else {
        Write-Host " OK" -ForegroundColor Green
    }
}

# ──────────────────────────────────────────────────────────────────────────
# Etape 2 — Vider puis importer chaque table
# ──────────────────────────────────────────────────────────────────────────
Write-Host "`n--- Etape 2 : Import SQL ---" -ForegroundColor Yellow

$BASE = "$DEST_DIR"

# AXE 1
Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe1_business_attributes" "TRUNCATE axe1_business_attributes"
Run-CH "INSERT INTO $DB.axe1_business_attributes SELECT business_id, name, city, state, postal_code, stars, review_count, is_open, latitude, longitude, categories, categories_list, score_class, wifi, noise_level, price_range, alcohol, toUInt8(accepts_credit_cards), toUInt8(good_for_kids), toUInt8(has_tv), toUInt8(outdoor_seating), toUInt8(accepts_reservations), toUInt8(bike_parking), toUInt8(has_delivery), toUInt8(has_takeout), weekly_open_minutes FROM file('$BASE/axe1_reputation/business_with_attributes/*.parquet', Parquet)" "axe1_business_attributes (150K)"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe1_closure_rate" "TRUNCATE axe1_closure_rate"
Run-CH "INSERT INTO $DB.axe1_closure_rate SELECT stars_bucket, total, closed, closure_rate_pct FROM file('$BASE/axe1_reputation/closure_rate/*.parquet', Parquet)" "axe1_closure_rate (4 lignes)"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe1_monthly_running_avg" "TRUNCATE axe1_monthly_running_avg"
Run-CH "INSERT INTO $DB.axe1_monthly_running_avg SELECT business_id, yr, mth, sum_stars, count_rev, avg_stars_month, cumul_sum, cumul_count, running_avg FROM file('$BASE/axe1_reputation/monthly_running_avg/*.parquet', Parquet)" "axe1_monthly_running_avg (3.4M)"

# AXE 2
Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe2_avg_by_city" "TRUNCATE axe2_avg_by_city"
Run-CH "INSERT INTO $DB.axe2_avg_by_city SELECT city, state, nb_businesses, avg_stars, stddev_stars, total_reviews FROM file('$BASE/axe2_geo/avg_by_city/*.parquet', Parquet)" "axe2_avg_by_city (495 villes)"

# AXE 3
Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe3_elite_comparison" "TRUNCATE axe3_elite_comparison"
Run-CH "INSERT INTO $DB.axe3_elite_comparison SELECT toUInt8(is_elite), nb_reviews, avg_stars_given, avg_text_length, avg_useful_received, avg_funny_received, avg_cool_received FROM file('$BASE/axe3_social/elite_comparison/*.parquet', Parquet)" "axe3_elite_comparison"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe3_seniority_analysis" "TRUNCATE axe3_seniority_analysis"
Run-CH "INSERT INTO $DB.axe3_seniority_analysis SELECT seniority_bucket, nb_reviews, avg_stars, stddev_stars FROM file('$BASE/axe3_social/seniority_analysis/*.parquet', Parquet)" "axe3_seniority_analysis"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe3_user_enriched" "TRUNCATE axe3_user_enriched"
Run-CH "INSERT INTO $DB.axe3_user_enriched SELECT user_id, fans, average_stars, account_age_years, friends_count, toUInt8(is_elite), total_compliments, pagerank FROM file('$BASE/axe3_social/user_enriched/*.parquet', Parquet)" "axe3_user_enriched"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe3_user_review_enriched" "TRUNCATE axe3_user_review_enriched"
Run-CH "INSERT INTO $DB.axe3_user_review_enriched SELECT user_id, business_id, stars, text_length, useful, toUInt8(is_elite), fans, pagerank, account_age_years FROM file('$BASE/axe3_social/user_review_enriched/*.parquet', Parquet)" "axe3_user_review_enriched"

# AXE 4
Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe4_category_hourly" "TRUNCATE axe4_category_hourly"
Run-CH "INSERT INTO $DB.axe4_category_hourly SELECT type, hour, total_checkins FROM file('$BASE/axe4_temporal/category_hourly/*.parquet', Parquet)" "axe4_category_hourly"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe4_heatmap_by_biz" "TRUNCATE axe4_heatmap_by_biz"
Run-CH "INSERT INTO $DB.axe4_heatmap_by_biz SELECT business_id, day_of_week, hour, checkin_count FROM file('$BASE/axe4_temporal/heatmap_by_biz/*.parquet', Parquet)" "axe4_heatmap_by_biz"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe4_heatmap_global" "TRUNCATE axe4_heatmap_global"
Run-CH "INSERT INTO $DB.axe4_heatmap_global SELECT day_of_week, hour, total_checkins FROM file('$BASE/axe4_temporal/heatmap_global/*.parquet', Parquet)" "axe4_heatmap_global"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe4_monthly_trend" "TRUNCATE axe4_monthly_trend"
Run-CH "INSERT INTO $DB.axe4_monthly_trend SELECT year, month, nb_reviews, avg_stars FROM file('$BASE/axe4_temporal/monthly_trend/*.parquet', Parquet)" "axe4_monthly_trend"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe4_stars_by_hour" "TRUNCATE axe4_stars_by_hour"
Run-CH "INSERT INTO $DB.axe4_stars_by_hour SELECT hour, nb_reviews, avg_stars FROM file('$BASE/axe4_temporal/stars_by_hour/*.parquet', Parquet)" "axe4_stars_by_hour"

Run-CH "TRUNCATE TABLE IF EXISTS $DB.axe4_stars_by_dow" "TRUNCATE axe4_stars_by_dow"
Run-CH "INSERT INTO $DB.axe4_stars_by_dow SELECT day_of_week, nb_reviews, avg_stars FROM file('$BASE/axe4_temporal/stars_by_dow/*.parquet', Parquet)" "axe4_stars_by_dow"

# ──────────────────────────────────────────────────────────────────────────
# Etape 3 — Vérification rapide
# ──────────────────────────────────────────────────────────────────────────
Write-Host "`n--- Etape 3 : Verification ---" -ForegroundColor Yellow

$verifySQL = @"
SELECT 'axe1_business_attributes'  AS tbl, count() AS n FROM bigdata_db.axe1_business_attributes
UNION ALL SELECT 'axe1_closure_rate',        count() FROM bigdata_db.axe1_closure_rate
UNION ALL SELECT 'axe1_monthly_running_avg', count() FROM bigdata_db.axe1_monthly_running_avg
UNION ALL SELECT 'axe2_avg_by_city',         count() FROM bigdata_db.axe2_avg_by_city
UNION ALL SELECT 'axe3_elite_comparison',    count() FROM bigdata_db.axe3_elite_comparison
UNION ALL SELECT 'axe3_seniority_analysis',  count() FROM bigdata_db.axe3_seniority_analysis
UNION ALL SELECT 'axe3_user_enriched',       count() FROM bigdata_db.axe3_user_enriched
UNION ALL SELECT 'axe4_stars_by_hour',       count() FROM bigdata_db.axe4_stars_by_hour
UNION ALL SELECT 'axe4_stars_by_dow',        count() FROM bigdata_db.axe4_stars_by_dow
UNION ALL SELECT 'axe4_monthly_trend',       count() FROM bigdata_db.axe4_monthly_trend
FORMAT Pretty
"@

docker exec $CONTAINER clickhouse-client --database $DB --multiquery --query $verifySQL

Write-Host "`n=== Import termine ! ===" -ForegroundColor Cyan
Write-Host "Ouvrez Grafana sur http://localhost:3001 pour voir les donnees." -ForegroundColor Green
