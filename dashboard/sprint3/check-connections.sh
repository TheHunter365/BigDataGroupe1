#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# SE5 S3 — Vérification des connexions avant branchement réel
# M1 : Data Engineer
#
# Vérifie :
#   1. ClickHouse accessible + toutes les tables existent
#   2. Topics Kafka SE2 présents (topic_business, topic_reviews)
#   3. Topic Flink SE4 présent (yelp-negative-reviews)
#   4. Comptage des lignes dans les tables live + batch
#
# Usage :
#   bash check-connections.sh
#   KAFKA_BROKER=localhost:9092 bash check-connections.sh
# ══════════════════════════════════════════════════════════════

set -uo pipefail

CH_CONTAINER="${CH_CONTAINER:-clickhouse-server}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka-broker}"

PASS=0
FAIL=0

# ── Helpers ───────────────────────────────────────────────────
ok()   { echo "  ✓ $1"; PASS=$((PASS + 1)); }
fail() { echo "  ✗ $1 [ÉCHEC]"; FAIL=$((FAIL + 1)); }

check() {
    local label="$1"; shift
    if "$@" &>/dev/null; then ok "$label"; else fail "$label"; fi
}

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   SE5 S3 — Vérification des connexions                      ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ─────────────────────────────────────────────────────────────
# 1. CLICKHOUSE
# ─────────────────────────────────────────────────────────────
echo "1. ClickHouse ($CH_CONTAINER)"

check "ClickHouse répond" \
    docker exec "$CH_CONTAINER" clickhouse-client --query "SELECT 1"

check "Base bigdata_db existe" \
    docker exec "$CH_CONTAINER" clickhouse-client \
        --query "SELECT 1 FROM system.databases WHERE name = 'bigdata_db'"

echo ""
echo "   Tables streaming (M1-1 / M1-2) :"
for table in businesses_live reviews_live negative_alerts_live; do
    check "Table $table" \
        docker exec "$CH_CONTAINER" clickhouse-client \
            --query "SELECT 1 FROM system.tables WHERE database='bigdata_db' AND name='$table'"
done

echo ""
echo "   Tables batch (M1-3) :"
for table in business_enriched \
             axe1_score_distribution axe1_monthly_running_avg \
             axe2_avg_by_city axe2_businesses_by_state \
             axe3_top_categories axe3_segmentation \
             axe4_checkins_heatmap; do
    check "Table $table" \
        docker exec "$CH_CONTAINER" clickhouse-client \
            --query "SELECT 1 FROM system.tables WHERE database='bigdata_db' AND name='$table'"
done

echo ""
echo "   Pipelines Kafka (Materialized Views) :"
for mv in mv_kafka_to_businesses mv_kafka_to_reviews mv_kafka_to_negative_alerts; do
    check "MV $mv" \
        docker exec "$CH_CONTAINER" clickhouse-client \
            --query "SELECT 1 FROM system.tables WHERE database='bigdata_db' AND name='$mv'"
done

# ─────────────────────────────────────────────────────────────
# 2. KAFKA
# ─────────────────────────────────────────────────────────────
echo ""
echo "2. Kafka ($KAFKA_CONTAINER)"

check "Kafka broker accessible" \
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics.sh --bootstrap-server localhost:29092 --list

echo ""
echo "   Topics SE2 (requis pour M1-1) :"
for topic in topic_business topic_reviews; do
    check "Topic $topic présent" \
        docker exec "$KAFKA_CONTAINER" \
            kafka-topics.sh --bootstrap-server localhost:29092 \
            --describe --topic "$topic"
done

echo ""
echo "   Topic SE4 (requis pour M1-2) :"
check "Topic yelp-negative-reviews présent" \
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics.sh --bootstrap-server localhost:29092 \
        --describe --topic "yelp-negative-reviews"

echo ""
echo "   Lag des consumer groups ClickHouse :"
for group in clickhouse_se5_business clickhouse_se5_reviews clickhouse_se5_alerts; do
    LAG=$(docker exec "$KAFKA_CONTAINER" \
        kafka-consumer-groups.sh --bootstrap-server localhost:29092 \
        --describe --group "$group" 2>/dev/null \
        | awk 'NR>1 && $6~/^[0-9]+$/ {sum+=$6} END {print sum+0}')
    echo "    $group lag = $LAG messages"
done

# ─────────────────────────────────────────────────────────────
# 3. COMPTAGE DES DONNÉES
# ─────────────────────────────────────────────────────────────
echo ""
echo "3. Données en base"
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT
    name  AS table,
    formatReadableQuantity(total_rows) AS lignes
FROM system.tables
WHERE database = 'bigdata_db'
  AND name IN (
      'businesses_live', 'reviews_live', 'negative_alerts_live',
      'business_enriched', 'axe1_score_distribution',
      'axe2_avg_by_city', 'axe3_top_categories'
  )
ORDER BY name
FORMAT PrettyCompact
" 2>/dev/null || echo "  (ClickHouse inaccessible)"

# ─────────────────────────────────────────────────────────────
# 4. RÉSUMÉ
# ─────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  ✓ $PASS vérification(s) réussie(s)"
if [ "$FAIL" -gt 0 ]; then
    echo "  ✗ $FAIL vérification(s) échouée(s)"
    echo ""
    echo "  Actions à faire :"
    echo "    - ClickHouse manquant  → bash setup-s2.sh"
    echo "    - Tables batch manquantes → docker exec -i clickhouse-server clickhouse-client"
    echo "        --multiquery < dashboard/S2/M1/init-batch-tables.sql"
    echo "    - Topics Kafka manquants → attendre SE2 sur bigdata-net"
    echo "    - Topic yelp-negative-reviews manquant → attendre SE4"
    exit 1
else
    echo ""
    echo "  Tout est prêt. Lancer import-parquets.sh quand SE3 livre."
fi
