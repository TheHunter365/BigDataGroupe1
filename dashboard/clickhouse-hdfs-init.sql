-- ================================================================
-- INIT ClickHouse — Création des tables HDFS
-- ================================================================
-- Ce script crée des tables virtuelles ClickHouse qui lisent
-- directement depuis les fichiers Parquet stockés dans HDFS
-- ================================================================

-- ================================================================
-- AXE 1 — RÉPUTATION
-- ================================================================

-- Table: business_with_attributes (Parquet de 13.5 MB)
-- Contient: 150,346 businesses enrichis avec 40+ attributs
CREATE TABLE IF NOT EXISTS axe1_business_with_attributes (
    business_id String,
    name String,
    city String,
    state String,
    postal_code String,
    latitude Float64,
    longitude Float64,
    stars Float32,
    review_count Int32,
    is_open Int8
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe1_reputation/business_with_attributes/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Businesses enrichis avec attributs (Parquet depuis HDFS)';

-- Table: monthly_running_avg (Parquet de 29.4 MB)
-- Contient: 3,404,085 rows — Moyennes mobiles mensuelles par business
CREATE TABLE IF NOT EXISTS axe1_monthly_running_avg (
    business_id String,
    review_date Date,
    running_avg_stars Float32,
    month_num Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe1_reputation/monthly_running_avg/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Moyennes mobiles mensuelles des stars (Parquet depuis HDFS)';

-- Table: closure_rate (Parquet de 1.3 KB)
-- Contient: 4 rows — Taux de fermeture par bande de notation
CREATE TABLE IF NOT EXISTS axe1_closure_rate (
    stars_band String,
    closure_percentage Float32,
    closed_count Int32,
    total_count Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe1_reputation/closure_rate/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Taux de fermeture par bande de notation (Parquet depuis HDFS)';

-- ================================================================
-- AXE 2 — GÉOGRAPHIE
-- ================================================================

-- Table: avg_by_city (Parquet de 12.1 KB)
-- Contient: 495 villes analysées
CREATE TABLE IF NOT EXISTS axe2_avg_by_city (
    city String,
    avg_stars Float32,
    total_reviews Int64,
    avg_review_count Float32,
    business_count Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe2_geo/avg_by_city/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Moyennes par ville (Parquet depuis HDFS)';

-- Table: business_with_competitors (Parquet)
-- Contient: 150,346 businesses — Densité concurrence (rayon 500m)
CREATE TABLE IF NOT EXISTS axe2_business_with_competitors (
    business_id String,
    name String,
    latitude Float64,
    longitude Float64,
    competitor_count Int32,
    density_index Float32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe2_geo/business_with_competitors/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Businesses avec analyse de concurrence locale (Parquet depuis HDFS)';

-- ================================================================
-- AXE 3 — RÉSEAU SOCIAL & INFLUENCE
-- ================================================================

-- Table: user_enriched (Parquet de 2.2 KB)
-- Contient: Scores PageRank des utilisateurs
CREATE TABLE IF NOT EXISTS axe3_user_enriched (
    user_id String,
    page_rank_score Float32,
    review_count Int32,
    useful_count Int64,
    average_rating Float32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/user_enriched/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Utilisateurs enrichis avec PageRank (Parquet depuis HDFS)';

-- Table: user_review_enriched (Parquet de 2.4 KB)
-- Contient: Métriques d'influence des reviews
CREATE TABLE IF NOT EXISTS axe3_user_review_enriched (
    review_id String,
    user_id String,
    business_id String,
    page_rank_score Float32,
    useful Int32,
    cool Int32,
    funny Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/user_review_enriched/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Reviews enrichies avec scores d\'influence (Parquet depuis HDFS)';

-- Table: elite_comparison (Parquet de 2.1 KB)
-- Contient: Comparaison elite vs. regular users
CREATE TABLE IF NOT EXISTS axe3_elite_comparison (
    user_type String,
    avg_stars Float32,
    avg_review_count Float32,
    avg_useful_count Float32,
    user_count Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/elite_comparison/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Comparaison utilisateurs élites vs. réguliers (Parquet depuis HDFS)';

-- Table: seniority_analysis (Parquet de 1.2 KB)
-- Contient: Analyse par quintiles de tenure
CREATE TABLE IF NOT EXISTS axe3_seniority_analysis (
    tenure_quintile String,
    avg_review_count Float32,
    avg_useful_count Float32,
    user_count Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/seniority_analysis/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Analyse tenure utilisateurs par quintiles (Parquet depuis HDFS)';

-- ================================================================
-- AXE 4 — TEMPORALITÉ & SAISONNALITÉ
-- ================================================================

-- Table: stars_by_hour (Parquet de 1.0 KB)
-- Contient: Patterns 24 heures
CREATE TABLE IF NOT EXISTS axe4_stars_by_hour (
    hour_of_day Int32,
    avg_stars Float32,
    review_count Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/stars_by_hour/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Ratings par heure de la journée (Parquet depuis HDFS)';

-- Table: stars_by_dow (Parquet de 1.0 KB)
-- Contient: Patterns 7 jours
CREATE TABLE IF NOT EXISTS axe4_stars_by_dow (
    day_of_week Int32,
    day_name String,
    avg_stars Float32,
    review_count Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/stars_by_dow/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Ratings par jour de la semaine (Parquet depuis HDFS)';

-- Table: monthly_trend (Parquet de 1.2 KB)
-- Contient: Trends mensuels
CREATE TABLE IF NOT EXISTS axe4_monthly_trend (
    month_name String,
    avg_stars Float32,
    review_count Int64,
    volatility Float32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/monthly_trend/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Tendances mensuelles (Parquet depuis HDFS)';

-- Table: heatmap_global (Parquet de 1.0 KB)
-- Contient: Matrice 24×7 globale
CREATE TABLE IF NOT EXISTS axe4_heatmap_global (
    hour_of_day Int32,
    day_of_week Int32,
    avg_stars Float32,
    review_count Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/heatmap_global/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Heatmap globale hour×day (Parquet depuis HDFS)';

-- Table: heatmap_by_biz (Parquet de 1.3 KB)
-- Contient: Patterns par business
CREATE TABLE IF NOT EXISTS axe4_heatmap_by_biz (
    business_id String,
    hour_of_day Int32,
    day_of_week Int32,
    avg_stars Float32,
    review_count Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/heatmap_by_biz/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Heatmaps par business (Parquet depuis HDFS)';

-- Table: category_hourly (Parquet de 0.5 KB)
-- Contient: Trends par catégorie
CREATE TABLE IF NOT EXISTS axe4_category_hourly (
    category String,
    hour_of_day Int32,
    avg_stars Float32,
    review_count Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/category_hourly/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Trends catégories par heure (Parquet depuis HDFS)';

-- ================================================================
-- DATASET ENRICHI POUR SE5
-- ================================================================

-- Table: business_enriched (Parquet de 6.9 KB)
-- Fusion finale de tous les axes
CREATE TABLE IF NOT EXISTS business_enriched_final (
    business_id String,
    name String,
    city String,
    stars Float32,
    review_count Int32,
    is_open Int8,
    competitor_count Int32,
    page_rank_score Float32,
    latitude Float64,
    longitude Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/business_enriched/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Dataset enrichi final — fusion tous les axes (Parquet depuis HDFS)';

-- ================================================================
-- VUES UTILES
-- ================================================================

-- Vue: Top 10 villes par rating
CREATE VIEW IF NOT EXISTS top_cities_by_rating AS
SELECT city, avg_stars, business_count
FROM axe2_avg_by_city
ORDER BY avg_stars DESC
LIMIT 10;

-- Vue: Distribution temporelle global
CREATE VIEW IF NOT EXISTS temporal_distribution AS
SELECT
    hour_of_day,
    day_of_week,
    avg_stars,
    review_count
FROM axe4_heatmap_global
ORDER BY day_of_week, hour_of_day;

-- Vue: Utilisateurs influents (Top PageRank)
CREATE VIEW IF NOT EXISTS top_influential_users AS
SELECT user_id, page_rank_score, review_count, useful_count
FROM axe3_user_enriched
ORDER BY page_rank_score DESC
LIMIT 20;

-- ================================================================
-- INFORMATION
-- ================================================================
--
-- Pour utiliser ces tables:
--
-- 1. Requête simple:
--    SELECT COUNT(*) FROM axe1_business_with_attributes;
--
-- 2. Analyse temporelle:
--    SELECT hour_of_day, avg_stars FROM axe4_stars_by_hour ORDER BY hour_of_day;
--
-- 3. Analyse géographique:
--    SELECT city, avg_stars FROM axe2_avg_by_city ORDER BY avg_stars DESC LIMIT 10;
--
-- 4. Influence utilisateurs:
--    SELECT user_id, page_rank_score FROM axe3_user_enriched ORDER BY page_rank_score DESC LIMIT 20;
--
-- ================================================================
