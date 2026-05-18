#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# SE5 S2 — Script de setup du pipeline Kafka → ClickHouse
# M1 : Data Engineer
#
# Ce script :
#   1. Démarre tous les services Docker (Kafka, ClickHouse, Grafana)
#   2. Attend que Kafka soit prêt
#   3. Crée les topics SE2 (topic_business + topic_reviews)
#   4. Attend que ClickHouse soit prêt
#   5. Applique kafka-pipeline.sql (2 pipelines complets)
#
# Usage : bash setup-s2.sh
# ══════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔══════════════════════════════════════╗"
echo "║   SE5 S2 — Setup Pipeline Kafka      ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ── Étape 1 : Démarrage des services ──────────────────────────
echo "[1/4] Démarrage des services Docker..."
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d
echo "      Services lancés."
echo ""

# ── Étape 2 : Attente Kafka ────────────────────────────────────
echo "[2/4] Attente du broker Kafka (peut prendre ~30s)..."
RETRIES=20
until docker exec kafka-broker kafka-topics.sh \
        --bootstrap-server localhost:29092 --list &>/dev/null; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -le 0 ]; then
        echo "      [ERREUR] Kafka n'a pas démarré. Vérifiez : docker logs kafka-broker"
        exit 1
    fi
    echo "      Kafka pas encore prêt, nouvelle tentative dans 5s... ($RETRIES restantes)"
    sleep 5
done
echo "      Kafka prêt."
echo ""

# ── Étape 3 : Création des topics SE2 ─────────────────────────
echo "[3/4] Création des topics SE2..."

docker exec kafka-broker kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --create --if-not-exists \
    --topic topic_business \
    --partitions 3 \
    --replication-factor 1
echo "      ✓ topic_business créé (3 partitions)"

docker exec kafka-broker kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --create --if-not-exists \
    --topic topic_reviews \
    --partitions 3 \
    --replication-factor 1
echo "      ✓ topic_reviews créé (3 partitions)"

docker exec kafka-broker kafka-topics.sh \
    --bootstrap-server localhost:29092 \
    --create --if-not-exists \
    --topic yelp-negative-reviews \
    --partitions 1 \
    --replication-factor 1
echo "      ✓ yelp-negative-reviews créé (1 partition — output Flink SE4)"
echo ""

# ── Étape 4 : Application du pipeline ClickHouse ───────────────
echo "[4/4] Application du pipeline ClickHouse..."
RETRIES=15
until docker exec clickhouse-server \
        clickhouse-client --query "SELECT 1" &>/dev/null; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -le 0 ]; then
        echo "      [ERREUR] ClickHouse n'a pas démarré. Vérifiez : docker logs clickhouse-server"
        exit 1
    fi
    echo "      ClickHouse pas encore prêt, nouvelle tentative dans 3s..."
    sleep 3
done

docker exec -i clickhouse-server \
    clickhouse-client --multiquery \
    < "$SCRIPT_DIR/kafka-pipeline.sql"
echo "      ✓ Pipeline topic_business       appliqué (Kafka Engine + MV + MergeTree)"
echo "      ✓ Pipeline topic_reviews        appliqué (Kafka Engine + MV + MergeTree)"
echo "      ✓ Pipeline yelp-negative-reviews appliqué (Kafka Engine + MV + MergeTree)"
echo ""

# ── Résumé ─────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Setup S2 terminé avec succès !                            ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  Lancer le simulateur SE2 (mock) :                         ║"
echo "║    pip install -r requirements.txt                          ║"
echo "║    python3 mock-producer.py                                 ║"
echo "║    python3 mock-producer.py --businesses 500 --reviews 200  ║"
echo "║                                                              ║"
echo "║  Ou utiliser le vrai producer SE2 :                        ║"
echo "║    cd /home/haytam/Desktop/M2/Big\ Data/SE2                ║"
echo "║    python3 producer_mock.py                                 ║"
echo "║                                                              ║"
echo "║  Vérifier l'ingestion dans ClickHouse :                     ║"
echo "║    docker exec -it clickhouse-server clickhouse-client      ║"
echo "║    > SELECT count() FROM bigdata_db.businesses_live;        ║"
echo "║    > SELECT count() FROM bigdata_db.reviews_live;           ║"
echo "║                                                              ║"
echo "║  Grafana : http://localhost:3001  (admin / admin123)        ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
