-- ══════════════════════════════════════════════════════════════════════════
-- SE5 S3 — DDL Tables Batch  (M1 — ClickHouse Data Engineer)
-- Tables alimentées par import Parquet SE3 (via import-se3-local.ps1)
-- Schemas validés pyarrow contre SE3_OUTPUT_RESULTS réels
-- Base : bigdata_db
-- ══════════════════════════════════════════════════════════════════════════

USE bigdata_db;

-- ──────────────────────────────────────────────────────────────
-- AXE 1 — RÉPUTATION  (données réelles : 150K businesses, 3.4M rows)
-- ──────────────────────────────────────────────────────────────

-- 150 346 businesses avec coordonnées GPS + attributs WiFi/livraison/etc.
-- SOURCE : axe1_reputation/business_with_attributes/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe1_business_attributes
(
    business_id          String,
    name                 String,
    city                 String,
    state                String,
    postal_code          String,
    stars                Float64,
    review_count         Int64,
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
ENGINE = MergeTree()
ORDER BY (state, city, business_id)
SETTINGS index_granularity = 8192;

-- 4 classes de notes → taux de fermeture
-- SOURCE : axe1_reputation/closure_rate/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe1_closure_rate
(
    stars_bucket     String,
    total            Int64,
    closed           Int64,
    closure_rate_pct Float64
)
ENGINE = MergeTree()
ORDER BY stars_bucket;

-- 3 404 085 lignes — running avg par business par mois
-- SOURCE : axe1_reputation/monthly_running_avg/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe1_monthly_running_avg
(
    business_id      String,
    yr               Int32,
    mth              Int32,
    sum_stars        Float64,
    count_rev        Int64,
    avg_stars_month  Float64,
    cumul_sum        Float64,
    cumul_count      Int64,
    running_avg      Float64
)
ENGINE = MergeTree()
ORDER BY (business_id, yr, mth)
SETTINGS index_granularity = 8192;

-- ──────────────────────────────────────────────────────────────
-- AXE 2 — GÉOGRAPHIE  (données réelles : 495 villes)
-- ──────────────────────────────────────────────────────────────

-- 495 villes avec stats agrégées
-- SOURCE : axe2_geo/avg_by_city/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe2_avg_by_city
(
    city          String,
    state         String,
    nb_businesses Int64,
    avg_stars     Float64,
    stddev_stars  Float64,
    total_reviews Int64
)
ENGINE = MergeTree()
ORDER BY (state, city);

-- ──────────────────────────────────────────────────────────────
-- AXE 3 — SOCIAL  (données partielles — 1 ligne mock chacune)
-- ──────────────────────────────────────────────────────────────

-- SOURCE : axe3_social/elite_comparison/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_elite_comparison
(
    is_elite             UInt8,
    nb_reviews           Int64,
    avg_stars_given      Float64,
    avg_text_length      Float64,
    avg_useful_received  Float64,
    avg_funny_received   Float64,
    avg_cool_received    Float64
)
ENGINE = MergeTree()
ORDER BY is_elite;

-- SOURCE : axe3_social/seniority_analysis/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_seniority_analysis
(
    seniority_bucket String,
    nb_reviews       Int64,
    avg_stars        Float64,
    stddev_stars     Float64
)
ENGINE = MergeTree()
ORDER BY seniority_bucket;

-- SOURCE : axe3_social/user_enriched/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_user_enriched
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
ENGINE = MergeTree()
ORDER BY user_id;

-- SOURCE : axe3_social/user_review_enriched/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe3_user_review_enriched
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
ENGINE = MergeTree()
ORDER BY (user_id, business_id);

-- ──────────────────────────────────────────────────────────────
-- AXE 4 — TEMPOREL  (données partielles — quelques lignes mock)
-- ──────────────────────────────────────────────────────────────

-- SOURCE : axe4_temporal/category_hourly/*.parquet  (0 lignes réelles)
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_category_hourly
(
    type           String,
    hour           Int32,
    total_checkins Int64
)
ENGINE = MergeTree()
ORDER BY (type, hour);

-- SOURCE : axe4_temporal/heatmap_by_biz/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_heatmap_by_biz
(
    business_id   String,
    day_of_week   Int32,
    hour          Int32,
    checkin_count Int64
)
ENGINE = MergeTree()
ORDER BY (business_id, day_of_week, hour);

-- SOURCE : axe4_temporal/heatmap_global/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_heatmap_global
(
    day_of_week    Int32,
    hour           Int32,
    total_checkins Int64
)
ENGINE = MergeTree()
ORDER BY (day_of_week, hour);

-- SOURCE : axe4_temporal/monthly_trend/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_monthly_trend
(
    year       Int32,
    month      Int32,
    nb_reviews Int64,
    avg_stars  Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

-- SOURCE : axe4_temporal/stars_by_hour/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_stars_by_hour
(
    hour       Int32,
    nb_reviews Int64,
    avg_stars  Float64
)
ENGINE = MergeTree()
ORDER BY hour;

-- SOURCE : axe4_temporal/stars_by_dow/*.parquet
CREATE TABLE IF NOT EXISTS bigdata_db.axe4_stars_by_dow
(
    day_of_week Int32,
    nb_reviews  Int64,
    avg_stars   Float64
)
ENGINE = MergeTree()
ORDER BY day_of_week;
