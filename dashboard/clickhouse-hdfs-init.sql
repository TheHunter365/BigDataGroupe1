-- ================================================================
-- SE5 — ClickHouse HDFS Engine Tables
-- Tables virtuelles lisant les Parquet HDFS produits par spark_etl.py v3
-- Noms de colonnes alignés avec les sorties réelles du ETL
-- ================================================================

CREATE DATABASE IF NOT EXISTS bigdata_db;

-- ================================================================
-- AXE 1 — RÉPUTATION
-- ================================================================

-- 150 346 businesses enrichis avec 25+ attributs
-- Colonnes issues de axe1_biz_attrs.select(...) dans spark_etl.py
CREATE TABLE IF NOT EXISTS bigdata_db.axe1_business_with_attributes_hdfs
(
    business_id          String,
    name                 String,
    city                 String,
    state                String,
    postal_code          String,
    stars                Float64,
    review_count         Int32,
    is_open              Int32,
    latitude             Float64,
    longitude            Float64,
    categories           String,
    categories_list      Array(String),
    score_class          String,
    wifi                 String,
    noise_level          String,
    price_range          String,
    alcohol              String,
    accepts_credit_cards UInt8,
    good_for_kids        UInt8,
    has_tv               UInt8,
    outdoor_seating      UInt8,
    accepts_reservations UInt8,
    bike_parking         UInt8,
    has_delivery         UInt8,
    has_takeout          UInt8,
    weekly_open_minutes  Int32
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe1_reputation/business_with_attributes/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Businesses enrichis avec attributs (Parquet HDFS)';

-- 3 404 085 lignes — running average mensuelle par business
-- Colonnes : business_id, yr, mth, sum_stars, count_rev, avg_stars_month, cumul_sum, cumul_count, running_avg
CREATE TABLE IF NOT EXISTS bigdata_db.axe1_monthly_running_avg_hdfs
(
    business_id     String,
    yr              Int32,
    mth             Int32,
    sum_stars       Float64,
    count_rev       Int64,
    avg_stars_month Float64,
    cumul_sum       Float64,
    cumul_count     Int64,
    running_avg     Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe1_reputation/monthly_running_avg/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Moyennes mobiles mensuelles (Parquet HDFS)';

-- 4 lignes — taux de fermeture par tranche de note
-- Colonnes : stars_bucket, total, closed, closure_rate_pct
CREATE TABLE IF NOT EXISTS bigdata_db.axe1_closure_rate_hdfs
(
    stars_bucket     String,
    total            Int64,
    closed           Int64,
    closure_rate_pct Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe1_reputation/closure_rate/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Taux de fermeture par tranche de note (Parquet HDFS)';

-- ================================================================
-- AXE 2 — GÉOGRAPHIE
-- ================================================================

-- 495 villes — Colonnes : city, state, nb_businesses, avg_stars, stddev_stars, total_reviews
CREATE TABLE IF NOT EXISTS bigdata_db.axe2_avg_by_city_hdfs
(
    city          String,
    state         String,
    nb_businesses Int64,
    avg_stars     Float64,
    stddev_stars  Float64,
    total_reviews Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe2_geo/avg_by_city/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Moyennes par ville (Parquet HDFS)';

-- 150 346 businesses avec densité de concurrence (rayon 500m)
-- Colonnes : toutes celles de axe1_biz_attrs + nb_competitors_500m
CREATE TABLE IF NOT EXISTS bigdata_db.axe2_business_with_competitors_hdfs
(
    business_id          String,
    name                 String,
    city                 String,
    state                String,
    postal_code          String,
    stars                Float64,
    review_count         Int32,
    is_open              Int32,
    latitude             Float64,
    longitude            Float64,
    categories           String,
    wifi                 String,
    price_range          String,
    has_delivery         UInt8,
    has_takeout          UInt8,
    nb_competitors_500m  Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe2_geo/business_with_competitors/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Businesses avec analyse concurrence 500m (Parquet HDFS)';

-- Dataset enrichi final (même données, chemin alternatif)
CREATE TABLE IF NOT EXISTS bigdata_db.business_enriched_hdfs
(
    business_id          String,
    name                 String,
    city                 String,
    state                String,
    postal_code          String,
    stars                Float64,
    review_count         Int32,
    is_open              Int32,
    latitude             Float64,
    longitude            Float64,
    categories           String,
    wifi                 String,
    price_range          String,
    has_delivery         UInt8,
    has_takeout          UInt8,
    nb_competitors_500m  Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/business_enriched/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Dataset enrichi final tous axes (Parquet HDFS)';

-- ================================================================
-- AXE 3 — RÉSEAU SOCIAL & INFLUENCE
-- ================================================================

-- Colonnes : user_id, fans, average_stars, account_age_years, friends_count, is_elite, total_compliments, pagerank
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_user_enriched_hdfs
(
    user_id           String,
    fans              Int32,
    average_stars     Float64,
    account_age_years Float64,
    friends_count     Int32,
    is_elite          UInt8,
    total_compliments Int32,
    pagerank          Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/user_enriched/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Utilisateurs enrichis avec PageRank (Parquet HDFS)';

-- Colonnes : user_id, business_id, stars, text_length, useful, is_elite, fans, pagerank, account_age_years
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_user_review_enriched_hdfs
(
    user_id           String,
    business_id       String,
    stars             Int64,
    text_length       Int32,
    useful            Int64,
    is_elite          UInt8,
    fans              Int32,
    pagerank          Float64,
    account_age_years Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/user_review_enriched/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Reviews enrichies avec scores influence (Parquet HDFS)';

-- Colonnes : is_elite, nb_reviews, avg_stars_given, avg_text_length, avg_useful_received, avg_funny_received, avg_cool_received
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_elite_comparison_hdfs
(
    is_elite             UInt8,
    nb_reviews           Int64,
    avg_stars_given      Float64,
    avg_text_length      Float64,
    avg_useful_received  Float64,
    avg_funny_received   Float64,
    avg_cool_received    Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/elite_comparison/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Comparaison elite vs standard (Parquet HDFS)';

-- Colonnes : seniority_bucket, nb_reviews, avg_stars, stddev_stars
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_seniority_analysis_hdfs
(
    seniority_bucket String,
    nb_reviews       Int64,
    avg_stars        Float64,
    stddev_stars     Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe3_social/seniority_analysis/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Analyse anciennete utilisateurs (Parquet HDFS)';

-- ================================================================
-- AXE 4 — TEMPORALITÉ & SAISONNALITÉ
-- ================================================================

-- Colonnes : hour, nb_reviews, avg_stars
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_stars_by_hour_hdfs
(
    hour       Int32,
    nb_reviews Int64,
    avg_stars  Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/stars_by_hour/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Note par heure de la journee (Parquet HDFS)';

-- Colonnes : day_of_week, nb_reviews, avg_stars
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_stars_by_dow_hdfs
(
    day_of_week Int32,
    nb_reviews  Int64,
    avg_stars   Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/stars_by_dow/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Note par jour de la semaine (Parquet HDFS)';

-- Colonnes : year, month, nb_reviews, avg_stars
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_monthly_trend_hdfs
(
    year       Int32,
    month      Int32,
    nb_reviews Int64,
    avg_stars  Float64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/monthly_trend/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Tendances mensuelles (Parquet HDFS)';

-- Colonnes : day_of_week, hour, total_checkins
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_heatmap_global_hdfs
(
    day_of_week    Int32,
    hour           Int32,
    total_checkins Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/heatmap_global/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Heatmap globale jour x heure (Parquet HDFS)';

-- Colonnes : business_id, day_of_week, hour, checkin_count
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_heatmap_by_biz_hdfs
(
    business_id   String,
    day_of_week   Int32,
    hour          Int32,
    checkin_count Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/heatmap_by_biz/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Heatmap par business (Parquet HDFS)';

-- Colonnes : type, hour, total_checkins
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_category_hourly_hdfs
(
    type           String,
    hour           Int32,
    total_checkins Int64
)
ENGINE = HDFS(
    'hdfs://namenode:9000/data/processed/axe4_temporal/category_hourly/part-*.snappy.parquet',
    'Parquet'
)
COMMENT 'Checkins par categorie et heure (Parquet HDFS)';

-- ================================================================
-- VUES UTILES
-- ================================================================

CREATE VIEW IF NOT EXISTS bigdata_db.top_cities_by_rating AS
SELECT city, state, avg_stars, nb_businesses
FROM bigdata_db.axe2_avg_by_city_hdfs
ORDER BY avg_stars DESC
LIMIT 10;

CREATE VIEW IF NOT EXISTS bigdata_db.top_influential_users AS
SELECT user_id, pagerank, fans, total_compliments
FROM bigdata_db.axe3_user_enriched_hdfs
ORDER BY pagerank DESC
LIMIT 20;

CREATE VIEW IF NOT EXISTS bigdata_db.temporal_heatmap AS
SELECT day_of_week, hour, total_checkins
FROM bigdata_db.axe4_heatmap_global_hdfs
ORDER BY day_of_week, hour;
