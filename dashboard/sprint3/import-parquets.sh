#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
# SE5 S3 — Import des Parquets SE3 → ClickHouse
# M1 : Data Engineer
#
# Prérequis :
#   - docker exec clickhouse-server accessible
#   - init-batch-tables.sql déjà exécuté (tables créées)
#
# Usage :
#   bash import-parquets.sh /chemin/vers/dossier/parquets
#
# Exemple :
#   bash import-parquets.sh /home/user/se3_output
#   bash import-parquets.sh ./data/parquets
#
# Fichiers attendus dans le dossier :
#   clean_S2.parquet              → business_enriched
#   agg_avg_stars_city.parquet    → axe2_avg_by_city
#   agg_businesses_state.parquet  → axe2_businesses_by_state
#   agg_top_categories.parquet    → axe3_top_categories
#   agg_score_distribution.parquet→ axe1_score_distribution
#   agg_segmentation.parquet      → axe3_segmentation
# ══════════════════════════════════════════════════════════════

set -euo pipefail

PARQUET_DIR="${1:-}"
CH_CONTAINER="${CH_CONTAINER:-clickhouse-server}"

# ── Validation arguments ───────────────────────────────────────
if [ -z "$PARQUET_DIR" ]; then
    echo "[ERREUR] Chemin du dossier Parquet manquant."
    echo "Usage : bash $0 /chemin/vers/parquets"
    exit 1
fi

if [ ! -d "$PARQUET_DIR" ]; then
    echo "[ERREUR] Dossier introuvable : $PARQUET_DIR"
    exit 1
fi

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   SE5 S3 — Import Parquets SE3 → ClickHouse                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
echo "  Source    : $PARQUET_DIR"
echo "  Container : $CH_CONTAINER"
echo ""

ERRORS=0

# ── Fonction d'import ─────────────────────────────────────────
import_parquet() {
    local filename="$1"
    local table="$2"
    local file="$PARQUET_DIR/$filename"

    if [ ! -f "$file" ]; then
        echo "  [SKIP] $filename — fichier introuvable, passé"
        ERRORS=$((ERRORS + 1))
        return
    fi

    local size
    size=$(du -sh "$file" | cut -f1)
    echo "  ► $filename ($size) → bigdata_db.$table ..."

    docker exec -i "$CH_CONTAINER" \
        clickhouse-client \
        --query "INSERT INTO bigdata_db.$table FORMAT Parquet" \
        < "$file"

    local count
    count=$(docker exec "$CH_CONTAINER" \
        clickhouse-client --query "SELECT count() FROM bigdata_db.$table")

    echo "    ✓ $table : $count lignes"
}

# ── Imports dans l'ordre de dépendance Grafana ────────────────
echo "[1/6] Axe 0 — Businesses enrichis"
import_parquet "clean_S2.parquet" "business_enriched"
echo ""

echo "[2/6] Axe 1 — Réputation"
import_parquet "agg_score_distribution.parquet" "axe1_score_distribution"
echo ""

echo "[3/6] Axe 2 — Géographie (villes)"
import_parquet "agg_avg_stars_city.parquet" "axe2_avg_by_city"
echo ""

echo "[4/6] Axe 2 — Géographie (états)"
import_parquet "agg_businesses_state.parquet" "axe2_businesses_by_state"
echo ""

echo "[5/6] Axe 3 — Catégories"
import_parquet "agg_top_categories.parquet" "axe3_top_categories"
echo ""

echo "[6/6] Axe 3 — Segmentation"
import_parquet "agg_segmentation.parquet" "axe3_segmentation"
echo ""

# ── Calcul de axe1_monthly_running_avg depuis reviews_live ────
echo "[+] Calcul axe1_monthly_running_avg depuis reviews_live ..."
REVIEW_COUNT=$(docker exec "$CH_CONTAINER" \
    clickhouse-client --query "SELECT count() FROM bigdata_db.reviews_live" 2>/dev/null || echo "0")

if [ "$REVIEW_COUNT" -gt 0 ]; then
    docker exec "$CH_CONTAINER" clickhouse-client --multiquery << 'SQL'
INSERT INTO bigdata_db.axe1_monthly_running_avg
SELECT
    business_id,
    toYear(date)  AS yr,
    toMonth(date) AS mth,
    round(avg(stars), 3) AS avg_stars,
    round(avg(avg(stars)) OVER (
        PARTITION BY business_id
        ORDER BY toYear(date), toMonth(date)
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 3) AS running_avg
FROM bigdata_db.reviews_live
GROUP BY business_id, yr, mth
ORDER BY business_id, yr, mth;
SQL
    COUNT=$(docker exec "$CH_CONTAINER" \
        clickhouse-client --query "SELECT count() FROM bigdata_db.axe1_monthly_running_avg")
    echo "    ✓ axe1_monthly_running_avg : $COUNT lignes"
else
    echo "    [SKIP] reviews_live vide — à relancer après M1-1"
fi
echo ""

# ── Résumé final ──────────────────────────────────────────────
echo "══════════════════════════════════════════════════════════════"
echo "Résumé des tables batch :"
docker exec "$CH_CONTAINER" clickhouse-client --query "
SELECT
    name  AS table,
    formatReadableQuantity(total_rows) AS lignes
FROM system.tables
WHERE database = 'bigdata_db'
  AND name IN (
      'business_enriched',
      'axe1_score_distribution', 'axe1_monthly_running_avg',
      'axe2_avg_by_city', 'axe2_businesses_by_state',
      'axe3_top_categories', 'axe3_segmentation',
      'axe4_checkins_heatmap'
  )
ORDER BY name
FORMAT PrettyCompact
" 2>/dev/null

echo ""
if [ "$ERRORS" -eq 0 ]; then
    echo "Import terminé. Grafana peut être rechargé."
else
    echo "$ERRORS fichier(s) manquant(s) — vérifier le dossier source."
fi
