-- ══════════════════════════════════════════════════════════════
-- SE5 S3 — Import Complet Parquets SE3 dans ClickHouse (M1 T2)
-- Data Engineer — Import Batch Layer
--
-- Prérequis :
--   1. init-batch-tables-optimized.sql exécuté (tables créées)
--   2. Parquets SE3 copiés dans /tmp/SE3_OUTPUT_RESULTS/
--   3. ClickHouse accès au filesystem local
--
-- Exécution :
--   docker exec -i clickhouse-server clickhouse-client --multiquery \
--     < dashboard/sprint3/import-se3-complete.sql
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PHASE 1 : VÉRIFICATION PRÉALABLE
-- ╚══════════════════════════════════════════════════════════════╝

-- Vérifier que les tables batch existent
SELECT
    'Tables batch' AS section,
    name AS table_name,
    engine
FROM system.tables
WHERE database = 'bigdata_db'
  AND name IN (
    'business_enriched',
    'axe1_monthly_running_avg',
    'axe2_avg_by_city',
    'axe4_checkins_heatmap'
  )
FORMAT PrettyCompact;


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PHASE 2 : IMPORT PARQUETS SE3 (4 AXES)
-- ╚══════════════════════════════════════════════════════════════╝

-- ══════════════════════════════════════════════════════════════
-- AXE 1 — RÉPUTATION (business_with_attributes + monthly_running_avg)
-- ══════════════════════════════════════════════════════════════

-- Import : business_enriched (150K businesses)
-- Source : SE3_OUTPUT_RESULTS/axe1_reputation/business_with_attributes/
-- Volume : ~150K rows | ~50 MB
INSERT INTO business_enriched
SELECT * FROM file(
    '/tmp/SE3_OUTPUT_RESULTS/axe1_reputation/business_with_attributes/*.parquet',
    'Parquet'
)
SETTINGS
    input_format_parquet_use_native_reader = 1;

-- Vérification import Axe 1
SELECT
    'Axe 1 - Réputation' AS section,
    COUNT(*) AS rows_imported,
    formatReadableQuantity(sum(stars)) AS stars_sum
FROM business_enriched;


-- ══════════════════════════════════════════════════════════════
-- AXE 2 — GÉOGRAPHIE (avg_by_city)
-- ══════════════════════════════════════════════════════════════

-- Import : axe2_avg_by_city (495 villes)
-- Source : SE3_OUTPUT_RESULTS/axe2_geo/avg_by_city/
-- Volume : <1 KB | ~495 rows
INSERT INTO axe2_avg_by_city
SELECT * FROM file(
    '/tmp/SE3_OUTPUT_RESULTS/axe2_geo/avg_by_city/*.parquet',
    'Parquet'
);

-- Vérification import Axe 2
SELECT
    'Axe 2 - Géographie' AS section,
    COUNT(*) AS cities,
    formatReadableQuantity(sum(nb_businesses)) AS total_businesses,
    formatReadable(avg(avg_stars)) AS avg_stars_mean
FROM axe2_avg_by_city;


-- ══════════════════════════════════════════════════════════════
-- AXE 3 — SOCIAL / CATÉGORIES (si SE3 les a produits)
-- ══════════════════════════════════════════════════════════════

-- Optionnel : Si SE3 a produit category_analysis
-- INSERT INTO axe3_top_categories
-- SELECT * FROM file(
--     '/tmp/SE3_OUTPUT_RESULTS/axe3_social/category_analysis/*.parquet',
--     'Parquet'
-- );

SELECT 'Axe 3 - Social' AS section, 'À configurer selon SE3' AS status;


-- ══════════════════════════════════════════════════════════════
-- AXE 4 — TEMPOREL / CHECKINS (heatmap_by_biz)
-- ══════════════════════════════════════════════════════════════

-- Import : axe4_checkins_heatmap (patterns 7j × 24h)
-- Source : SE3_OUTPUT_RESULTS/axe4_temporal/heatmap_by_biz/
-- Volume : Variable (~1KB à 10KB selon données)
INSERT INTO axe4_checkins_heatmap
SELECT * FROM file(
    '/tmp/SE3_OUTPUT_RESULTS/axe4_temporal/heatmap_by_biz/*.parquet',
    'Parquet'
);

-- Vérification import Axe 4
SELECT
    'Axe 4 - Temporel' AS section,
    COUNT(DISTINCT business_id) AS unique_businesses,
    COUNT() AS total_heatmap_entries,
    formatReadableQuantity(sum(nb_checkins)) AS total_checkins
FROM axe4_checkins_heatmap;


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PHASE 3 : VÉRIFICATION INTÉGRITÉ & STATISTIQUES
-- ╚══════════════════════════════════════════════════════════════╝

-- Résumé complet des imports
SELECT
    'business_enriched' AS table_name,
    COUNT(*) AS row_count,
    formatReadableQuantity(sum(bytes)) AS total_size,
    formatReadable(max(stars)) AS max_stars,
    formatReadable(min(stars)) AS min_stars
FROM (
    SELECT bytes, stars FROM system.parts
    WHERE table = 'business_enriched' AND database = 'bigdata_db'
)
UNION ALL
SELECT
    'axe2_avg_by_city',
    COUNT(*),
    formatReadableQuantity(sum(bytes)),
    formatReadable(max(avg_stars)),
    formatReadable(min(avg_stars))
FROM (
    SELECT bytes, avg_stars FROM system.parts
    WHERE table = 'axe2_avg_by_city' AND database = 'bigdata_db'
)
UNION ALL
SELECT
    'axe4_checkins_heatmap',
    COUNT(*),
    formatReadableQuantity(sum(bytes)),
    '--',
    '--'
FROM (
    SELECT bytes FROM system.parts
    WHERE table = 'axe4_checkins_heatmap' AND database = 'bigdata_db'
)
FORMAT PrettyCompact;


-- ══════════════════════════════════════════════════════════════
-- PHASE 4 : REQUÊTES DE VALIDATION
-- ══════════════════════════════════════════════════════════════

-- Validation 1 : Distribution des notes (business_enriched)
SELECT
    'Validation 1' AS check_name,
    'Score distribution' AS description,
    count() AS count,
    score_class
FROM business_enriched
GROUP BY score_class
FORMAT PrettyCompact;

-- Validation 2 : Top 10 villes (axe2_avg_by_city)
SELECT
    'Validation 2' AS check_name,
    'Top 10 cities by avg_stars' AS description,
    city,
    state,
    formatReadable(avg_stars) AS avg_stars,
    nb_businesses
FROM axe2_avg_by_city
ORDER BY avg_stars DESC
LIMIT 10
FORMAT PrettyCompact;

-- Validation 3 : Patterns temporels (axe4_checkins_heatmap)
SELECT
    'Validation 3' AS check_name,
    'Peak hours heatmap' AS description,
    hour,
    day_of_week,
    count(DISTINCT business_id) AS num_businesses,
    formatReadableQuantity(sum(nb_checkins)) AS total_checkins
FROM axe4_checkins_heatmap
GROUP BY hour, day_of_week
ORDER BY sum(nb_checkins) DESC
LIMIT 10
FORMAT PrettyCompact;


-- ══════════════════════════════════════════════════════════════
-- PHASE 5 : SUMMARY
-- ══════════════════════════════════════════════════════════════

SELECT
    '✓ IMPORT COMPLET' AS status,
    (SELECT COUNT(*) FROM business_enriched) AS businesses_imported,
    (SELECT COUNT(*) FROM axe2_avg_by_city) AS cities_imported,
    (SELECT COUNT(*) FROM axe4_checkins_heatmap) AS heatmap_entries_imported,
    now() AS completion_time
FORMAT PrettyCompact;

-- Final : Afficher l'espace disque utilisé
SELECT
    table AS table_name,
    formatReadableQuantity(sum(bytes)) AS total_size,
    count() AS num_parts
FROM system.parts
WHERE database = 'bigdata_db'
  AND table IN ('business_enriched', 'axe2_avg_by_city', 'axe4_checkins_heatmap')
GROUP BY table
ORDER BY sum(bytes) DESC
FORMAT PrettyCompact;
