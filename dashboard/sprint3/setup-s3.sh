#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# SE5 S3 — Script de setup des données réelles
#
# Ce script (à lancer après setup-s2.sh) :
#   1. Crée les tables batch (business_enriched, axe1_*, axe2_*,
#      axe3_*, axe4_*) dans ClickHouse via init-batch-tables.sql
#   2. Vérifie les connexions SE2 / SE3 / SE4
#
# Prérequis : bash dashboard/sprint2/setup-s2.sh déjà exécuté
#
# Usage : bash dashboard/sprint3/setup-s3.sh
# ══════════════════════════════════════════════════════════════

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔══════════════════════════════════════╗"
echo "║   SE5 S3 — Setup Données Réelles     ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ── Étape 1 : ClickHouse accessible ? ─────────────────────────
echo "[1/3] Vérification ClickHouse..."
RETRIES=10
until docker exec clickhouse-server \
        clickhouse-client --query "SELECT 1" &>/dev/null; do
    RETRIES=$((RETRIES - 1))
    if [ "$RETRIES" -le 0 ]; then
        echo "      [ERREUR] ClickHouse inaccessible."
        echo "      Lancer d'abord : bash dashboard/sprint2/setup-s2.sh"
        exit 1
    fi
    echo "      ClickHouse pas encore prêt ($RETRIES tentatives)..."
    sleep 3
done
echo "      ClickHouse prêt."
echo ""

# ── Étape 2 : Création des tables batch ───────────────────────
echo "[2/3] Création des tables batch S3..."
docker exec -i clickhouse-server \
    clickhouse-client --multiquery \
    < "$SCRIPT_DIR/init-batch-tables.sql"
echo "      ✓ business_enriched           (import SE3 : clean_S2.parquet)"
echo "      ✓ axe1_score_distribution     (import SE3 : agg_score_distribution.parquet)"
echo "      ✓ axe1_monthly_running_avg    (calculé depuis reviews_live après M1-1)"
echo "      ✓ axe2_avg_by_city            (import SE3 : agg_avg_stars_city.parquet)"
echo "      ✓ axe2_businesses_by_state    (import SE3 : agg_businesses_state.parquet)"
echo "      ✓ axe3_top_categories         (import SE3 : agg_top_categories.parquet)"
echo "      ✓ axe3_segmentation           (import SE3 : agg_segmentation.parquet)"
echo "      ✓ axe4_checkins_heatmap       (import checkins si disponibles)"
echo ""

# ── Étape 3 : Vérification des connexions ─────────────────────
echo "[3/3] Vérification des connexions SE2 / SE3 / SE4..."
bash "$SCRIPT_DIR/check-connections.sh" || true
echo ""

# ── Résumé ─────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Setup S3 terminé !                                        ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  Étapes suivantes (dépendent des autres sous-équipes) :     ║"
echo "║                                                              ║"
echo "║  [M1-1] Quand SE2 lance son broker sur bigdata-net :        ║"
echo "║    → Les Materialized Views alimentent automatiquement      ║"
echo "║      businesses_live et reviews_live                        ║"
echo "║    → Vérifier : bash check-connections.sh                   ║"
echo "║                                                              ║"
echo "║  [M1-2] Quand SE4 lance son job Flink sur topic_reviews :   ║"
echo "║    → negative_alerts_live se remplit automatiquement        ║"
echo "║                                                              ║"
echo "║  [M1-3] Quand SE3 livre les Parquets :                      ║"
echo "║    bash import-parquets.sh /chemin/vers/parquets/SE3        ║"
echo "║                                                              ║"
echo "║  Grafana : http://localhost:3001  (admin / admin123)        ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
