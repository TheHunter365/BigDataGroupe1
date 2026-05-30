-- ══════════════════════════════════════════════════════════════
-- SE5 S3 — Tables Batch · Import Parquets SE3
-- M1 : Data Engineer
--
-- Tables créées ici (structure prête → insert quand SE3 livre) :
--   • business_enriched          ← clean_S2.parquet (150K businesses)
--   • axe1_score_distribution    ← agg_score_distribution.parquet
--   • axe1_monthly_running_avg   ← calculé depuis reviews_live
--   • axe2_avg_by_city           ← agg_avg_stars_city.parquet
--   • axe2_businesses_by_state   ← agg_businesses_state.parquet
--   • axe3_top_categories        ← agg_top_categories.parquet
--   • axe3_segmentation          ← agg_segmentation.parquet
--   • axe4_checkins_heatmap      ← données checkins (si disponibles)
--
-- Exécution :
--   docker exec -i clickhouse-server clickhouse-client --multiquery \
--     < dashboard/S2/M1/init-batch-tables.sql
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 0 — business_enriched                                  ║
-- ║  Source : clean_S2.parquet (SE3 · 150K businesses Yelp)     ║
-- ╚══════════════════════════════════════════════════════════════╝
CREATE TABLE IF NOT EXISTS business_enriched
(
    business_id   String,
    name          String,
    city          LowCardinality(String),
    state         LowCardinality(String),
    stars         Float32,
    score_class   LowCardinality(String),  -- ex: "Excellent","Bon","Moyen","Faible"
    region        LowCardinality(String),  -- ex: "West","East","South","Midwest"
    is_restaurant UInt8,                   -- 1 = restaurant, 0 = autre
    nb_categories UInt16
)
ENGINE = ReplacingMergeTree()
-- Filtres Grafana : par région/ville/note — tri optimisé pour les panels géo
ORDER BY (region, city, stars, business_id);


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 1 — Réputation                                         ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Distribution des scores par région
-- Source : agg_score_distribution.parquet (SE3)
CREATE TABLE IF NOT EXISTS axe1_score_distribution
(
    region        LowCardinality(String),
    score_class   LowCardinality(String),
    nb_businesses UInt32,
    pct           Float64    -- pourcentage dans la région
)
ENGINE = ReplacingMergeTree()
ORDER BY (region, score_class);

-- Moyenne mobile mensuelle des notes par business
-- Alimentée depuis reviews_live après import SE2 (M1-1)
-- ORDER BY optimisé M1-4 : (business_id, yr, mth) pour les jointures temporelles
CREATE TABLE IF NOT EXISTS axe1_monthly_running_avg
(
    business_id String,
    yr          UInt16,
    mth         UInt8,
    avg_stars   Float64,
    running_avg Float64    -- moyenne glissante sur les 3 derniers mois
)
ENGINE = MergeTree()
ORDER BY (business_id, yr, mth);


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 2 — Géographie                                         ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Note moyenne agrégée par ville
-- Source : agg_avg_stars_city.parquet (SE3)
CREATE TABLE IF NOT EXISTS axe2_avg_by_city
(
    city          LowCardinality(String),
    state         LowCardinality(String),
    avg_stars     Float64,
    nb_businesses UInt32
)
ENGINE = ReplacingMergeTree()
ORDER BY (state, city);

-- Distribution géo par état
-- Source : agg_businesses_state.parquet (SE3)
CREATE TABLE IF NOT EXISTS axe2_businesses_by_state
(
    state         LowCardinality(String),
    region        LowCardinality(String),
    nb_businesses UInt32,
    avg_stars     Float64,
    total_reviews UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (region, state);


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 3 — Social / Catégories                                ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Top catégories par popularité
-- Source : agg_top_categories.parquet (SE3)
CREATE TABLE IF NOT EXISTS axe3_top_categories
(
    category       String,
    nb_occurrences UInt32,
    avg_stars      Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY category;

-- Segmentation restaurants vs santé vs autres
-- Source : agg_segmentation.parquet (SE3)
CREATE TABLE IF NOT EXISTS axe3_segmentation
(
    is_restaurant UInt8,
    is_healthcare UInt8,
    nb_businesses UInt32,
    avg_stars     Float64
)
ENGINE = ReplacingMergeTree()
ORDER BY (is_restaurant, is_healthcare);


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  AXE 4 — Temporel / Checkins                                ║
-- ║  ORDER BY optimisé M1-4 : (business_id, day_of_week, hour)  ║
-- ╚══════════════════════════════════════════════════════════════╝

-- Heatmap des checkins 7 jours × 24 heures
-- Source : données checkins Yelp (si SE3 les livre)
CREATE TABLE IF NOT EXISTS axe4_checkins_heatmap
(
    business_id String,
    day_of_week UInt8,    -- 1=Lundi ... 7=Dimanche
    hour        UInt8,    -- 0-23
    nb_checkins UInt32
)
ENGINE = MergeTree()
ORDER BY (business_id, day_of_week, hour);


-- ══════════════════════════════════════════════════════════════
-- VÉRIFICATION
-- ══════════════════════════════════════════════════════════════
SELECT
    name AS table,
    engine,
    formatReadableQuantity(total_rows) AS lignes
FROM system.tables
WHERE database = 'bigdata_db'
ORDER BY name
FORMAT PrettyCompact;
