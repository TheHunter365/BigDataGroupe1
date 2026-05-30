-- ══════════════════════════════════════════════════════════════
-- SE5 S3 — Tables Batch (OPTIMISÉ pour données réelles)
-- M1 : Data Engineer
--
-- Optimisations S3 :
--   • Tous ORDER BY révisés pour volumétrie 7M reviews + 150K businesses
--   • ReplacingMergeTree pour dédup Parquets
--   • Partitioning temporel sur séries
--   • Compression Snappy (ClickHouse default)
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 0 — business_enriched (Parquet SE3)                    ║
-- ║  Structure: 150K businesses Yelp + attributs                 ║
-- ╚══════════════════════════════════════════════════════════════╝

CREATE TABLE IF NOT EXISTS business_enriched
(
    business_id   String,
    name          String,
    city          LowCardinality(String),
    state         LowCardinality(String),
    stars         Float32,
    score_class   LowCardinality(String),
    region        LowCardinality(String),
    is_restaurant UInt8,
    nb_categories UInt16
)
ENGINE = ReplacingMergeTree()
-- ✨ OPTIMISATION S3 : ORDER BY pour requêtes Grafana géo
--    state → région (< 50 values, très compressible)
--    city → filtres ville (~ 500 values)
--    stars → agrégations (~ 20 buckets)
--    business_id → primaire (dédup avec ReplacingMergeTree)
ORDER BY (state, city, stars, business_id)
SETTINGS
    index_granularity = 8192;  -- bon pour 150K rows


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 1 — RÉPUTATION (Parquets SE3)                          ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Distribution scores par région
CREATE TABLE IF NOT EXISTS axe1_score_distribution
(
    region        LowCardinality(String),
    score_class   LowCardinality(String),
    nb_businesses UInt32,
    pct           Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (region, score_class)
SETTINGS
    index_granularity = 128;  -- petit tableau (< 1000 rows)


-- Moyenne mobile mensuelle par business
-- ✨ OPTIMISATION S3 : (yr, mth, business_id) pour séries temporelles Grafana
--    yr, mth en PREMIER : Grafana affiche trends mensuels d'abord
--    business_id : agrégation par business
CREATE TABLE IF NOT EXISTS axe1_monthly_running_avg
(
    business_id String,
    yr          UInt16,
    mth         UInt8,
    avg_stars   Float64,
    running_avg Float64
)
ENGINE = MergeTree()
PARTITION BY yr
ORDER BY (yr, mth, business_id)
SETTINGS
    index_granularity = 8192;  -- 3.4M rows → besoin index bon


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 2 — GÉOGRAPHIE (Parquets SE3)                          ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Moyenne par ville
CREATE TABLE IF NOT EXISTS axe2_avg_by_city
(
    city          LowCardinality(String),
    state         LowCardinality(String),
    avg_stars     Float64,
    nb_businesses UInt32
)
ENGINE = ReplacingMergeTree()
ORDER BY (state, city)
SETTINGS
    index_granularity = 128;  -- petit (~ 500 rows)


-- Distribution par état
CREATE TABLE IF NOT EXISTS axe2_businesses_by_state
(
    state         LowCardinality(String),
    region        LowCardinality(String),
    nb_businesses UInt32,
    avg_stars     Float64,
    total_reviews UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (region, state)
SETTINGS
    index_granularity = 128;  -- très petit (50 rows)


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 3 — SOCIAL / CATÉGORIES (Parquets SE3)                 ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Top catégories
CREATE TABLE IF NOT EXISTS axe3_top_categories
(
    category       String,
    nb_occurrences UInt32,
    avg_stars      Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY category
SETTINGS
    index_granularity = 128;  -- < 1000 rows


-- Segmentation par type business
CREATE TABLE IF NOT EXISTS axe3_segmentation
(
    is_restaurant UInt8,
    is_healthcare UInt8,
    nb_businesses UInt32,
    avg_stars     Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (is_restaurant, is_healthcare)
SETTINGS
    index_granularity = 128;  -- 8 rows max


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 4 — TEMPOREL / CHECKINS (Parquets SE3)                 ║
-- ║  Heatmaps: 7 jours × 24 heures par business                 ║
-- ╚══════════════════════════════════════════════════════════════╝

-- ✨ OPTIMISATION S3 : ORDER BY (business_id, day_of_week, hour)
--    → Heatmap affichée pour UN business à la fois
--    → day_of_week + hour groupés pour visualisation rapide
--    → Perfect pour WHERE business_id = ? GROUP BY day_of_week, hour
CREATE TABLE IF NOT EXISTS axe4_checkins_heatmap
(
    business_id String,
    day_of_week UInt8,    -- 1=Lun, 7=Dim
    hour        UInt8,    -- 0-23
    nb_checkins UInt32
)
ENGINE = MergeTree()
ORDER BY (business_id, day_of_week, hour)
SETTINGS
    index_granularity = 8192;  -- variable selon SE3


-- ══════════════════════════════════════════════════════════════
-- STATISTIQUES ET VÉRIFICATION
-- ══════════════════════════════════════════════════════════════

-- Voir toutes les tables créées
SHOW TABLES FROM bigdata_db;

-- Vérifier ORDER BY optimisés
SELECT
    name AS table_name,
    engine,
    formatReadableQuantity(total_bytes) AS total_size,
    total_rows
FROM system.tables
WHERE database = 'bigdata_db'
ORDER BY total_bytes DESC
FORMAT PrettyCompact;

-- Vérifier index granularité
SELECT
    table AS table_name,
    count() AS num_parts,
    formatReadableQuantity(sum(bytes)) AS total_bytes,
    formatReadableQuantity(sum(marks_bytes)) AS index_bytes
FROM system.parts
WHERE database = 'bigdata_db'
GROUP BY table
ORDER BY total_bytes DESC
FORMAT PrettyCompact;
