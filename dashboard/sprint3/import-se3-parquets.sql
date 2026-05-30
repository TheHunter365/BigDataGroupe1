-- ══════════════════════════════════════════════════════════════
-- SE5 S3 — Import Parquets SE3 dans ClickHouse
-- M1 : Data Engineer
--
-- Ce script charge tous les fichiers Parquet générés par SE3
-- dans les tables batch ClickHouse (init-batch-tables.sql)
--
-- Prérequis :
--   1. init-batch-tables.sql exécuté (tables créées)
--   2. Fichiers Parquet de SE3 accessibles (chemin local ou HDFS)
--   3. ClickHouse configuré avec accès fichier
--
-- Exécution :
--   docker exec -i clickhouse-server clickhouse-client --multiquery \
--     < dashboard/sprint3/import-se3-parquets.sql
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;

-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 1 — RÉPUTATION (business_with_attributes + monthly_running_avg)
-- ╚══════════════════════════════════════════════════════════════╝

-- business_enriched : données de base business enrichies (150K businesses)
-- Source Parquet : SE3_OUTPUT_RESULTS/axe1_reputation/business_with_attributes/
-- Structure attendue : business_id, name, city, state, stars, score_class, region, is_restaurant, nb_categories
INSERT INTO business_enriched
SELECT
    business_id,
    name,
    city,
    state,
    stars,
    score_class,
    region,
    is_restaurant,
    nb_categories
FROM format(
    'Parquet',
    'INSERT INTO business_enriched (business_id, name, city, state, stars, score_class, region, is_restaurant, nb_categories)
    SELECT * FROM business_enriched'
)
SETTINGS
    format = 'Parquet',
    input_format_parquet_use_native_reader = 1;


-- ── Alternativement, si les Parquets sont accessibles via HTTP ──────────────
-- INSERT INTO business_enriched
-- SELECT * FROM url(
--   'file:///data/SE3_OUTPUT_RESULTS/axe1_reputation/business_with_attributes/*.parquet',
--   'Parquet'
-- );


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 2 — GÉOGRAPHIE
-- ╚══════════════════════════════════════════════════════════════╝

-- avg_by_city : agrégation par ville
-- Source Parquet : SE3_OUTPUT_RESULTS/axe2_geo/avg_by_city/
INSERT INTO axe2_avg_by_city
SELECT * FROM url(
    'file:///SE3/SE3_OUTPUT_RESULTS/axe2_geo/avg_by_city/*.parquet',
    'Parquet'
);


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 3 — SOCIAL / CATÉGORIES
-- ╚══════════════════════════════════════════════════════════════╝

-- top_categories : catégories les plus populaires
-- Source Parquet : SE3_OUTPUT_RESULTS/axe3_social/ (si SE3 l'a produit)
-- INSERT INTO axe3_top_categories
-- SELECT * FROM url(
--     'file:///SE3/SE3_OUTPUT_RESULTS/axe3_social/category_analysis/*.parquet',
--     'Parquet'
-- );


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 4 — TEMPOREL / CHECKINS
-- ╚══════════════════════════════════════════════════════════════╝

-- checkins_heatmap : patterns 7j × 24h
-- Source Parquet : SE3_OUTPUT_RESULTS/axe4_temporal/heatmap_by_biz/
-- INSERT INTO axe4_checkins_heatmap
-- SELECT * FROM url(
--     'file:///SE3/SE3_OUTPUT_RESULTS/axe4_temporal/heatmap_by_biz/*.parquet',
--     'Parquet'
-- );


-- ══════════════════════════════════════════════════════════════
-- ÉTAPE 2 : VÉRIFICATION DES IMPORTS
-- ══════════════════════════════════════════════════════════════

SHOW TABLES FROM bigdata_db;

SELECT
    'business_enriched' AS table_name,
    count() AS row_count,
    formatReadableQuantity(sum(bytes)) AS size_bytes
FROM system.parts
WHERE table = 'business_enriched' AND database = 'bigdata_db'
GROUP BY table;

SELECT
    'axe2_avg_by_city' AS table_name,
    count() AS row_count
FROM axe2_avg_by_city;
