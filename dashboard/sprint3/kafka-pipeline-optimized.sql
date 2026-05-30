-- ══════════════════════════════════════════════════════════════
-- SE5 S3 — Pipeline Kafka → ClickHouse (OPTIMISÉ pour données réelles)
-- M1 : Data Engineer
--
-- Optimisations S3 par rapport à S2 :
--   • reviews_live  : ORDER BY (date, business_id, stars) — date en premier pour Grafana
--   • businesses_live : ORDER BY (state, city, stars, business_id) — agrégations géo
--   • alertes        : Partitionnement et TTL ajoutés
--
-- Prérequis :
--   Kafka broker sur kafka:29092
--   Topics : 'topic_business', 'topic_reviews', 'yelp-negative-reviews'
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PIPELINE 1 : topic_business (SE2 réel)                     ║
-- ╚══════════════════════════════════════════════════════════════╝

-- ── Étape 1 : Kafka Engine (ingestion brute businesses) ────────
CREATE TABLE IF NOT EXISTS kafka_raw_business
(
    business_id   String,
    name          String,
    address       String,
    city          String,
    state         String,
    postal_code   String,
    latitude      Float64,
    longitude     Float64,
    stars         Float32,
    review_count  Int32,
    is_open       Int8,
    categories    String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'topic_business',
    kafka_group_name           = 'clickhouse_se5_business',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_max_block_size       = 5000,
    kafka_skip_broken_messages = 50;


-- ── Étape 2 : MergeTree destination businesses (OPTIMISÉ S3) ───
-- ✨ OPTIMISATION : ORDER BY (state, city, stars, business_id)
--    → state/city pour filtres Grafana géo (moins de 100K combinaisons)
--    → stars pour agrégations (20 buckets)
--    → business_id pour dédupliquage
CREATE TABLE IF NOT EXISTS businesses_live
(
    business_id   String,
    name          String,
    address       String,
    city          LowCardinality(String),
    state         LowCardinality(String),
    postal_code   String,
    latitude      Float64,
    longitude     Float64,
    stars         Float32,
    review_count  Int32,
    is_open       UInt8,
    categories    String,
    ingested_at   DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (state, city, stars, business_id)
SETTINGS
    index_granularity = 8192;  -- optimal pour 150K businesses


-- ── Étape 3 : Materialized View businesses ─────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_businesses
TO businesses_live
AS
SELECT
    business_id,
    trim(name)                              AS name,
    trim(address)                           AS address,
    trim(city)                              AS city,
    upper(trim(state))                      AS state,
    trim(postal_code)                       AS postal_code,
    latitude,
    longitude,
    if(stars < 0 OR stars > 5, 0, stars)   AS stars,
    if(review_count < 0, 0, review_count)  AS review_count,
    if(is_open = 1, 1, 0)                  AS is_open,
    trim(categories)                        AS categories
FROM kafka_raw_business
WHERE business_id != ''
  AND name        != '';


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PIPELINE 2 : topic_reviews (SE2 réel)                      ║
-- ╚══════════════════════════════════════════════════════════════╝

-- ── Étape 1 : Kafka Engine (ingestion brute reviews) ───────────
CREATE TABLE IF NOT EXISTS kafka_raw_reviews
(
    review_id   String,
    user_id     String,
    business_id String,
    stars       Float32,
    useful      Int32,
    funny       Int32,
    cool        Int32,
    text        String,
    date        String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'topic_reviews',
    kafka_group_name           = 'clickhouse_se5_reviews',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_max_block_size       = 1000,
    kafka_skip_broken_messages = 10;


-- ── Étape 2 : MergeTree destination reviews (OPTIMISÉ S3) ──────
-- ✨ OPTIMISATION : ORDER BY (date, business_id, stars)
--    → date en PREMIER : Grafana filtre par plage dates (le plus sélectif ~7M lignes)
--    → business_id : jointures rapides avec businesses_live
--    → stars : agrégations (COUNT, AVG, GROUP BY stars)
-- ✨ PARTITIONING : toYear(date) pour éviter TOO_MANY_PARTS
--    (~9 ans Yelp → 9 partitions, pas 100+)
CREATE TABLE IF NOT EXISTS reviews_live
(
    review_id   String,
    user_id     String,
    business_id String,
    stars       Float32,
    useful      UInt32,
    funny       UInt32,
    cool        UInt32,
    text        String,
    date        DateTime,
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYear(date)
ORDER BY (date, business_id, stars)
SETTINGS
    index_granularity = 8192,  -- optimal pour ~7M rows
    ttl_only_drop_parts = 1;   -- performance TTL (optionnel)


-- ── Étape 3 : Materialized View reviews ────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_reviews
TO reviews_live
AS
SELECT
    review_id,
    user_id,
    business_id,
    if(stars < 1 OR stars > 5, 0, stars)   AS stars,
    toUInt32(if(useful < 0, 0, useful))     AS useful,
    toUInt32(if(funny  < 0, 0, funny))      AS funny,
    toUInt32(if(cool   < 0, 0, cool))       AS cool,
    text,
    parseDateTimeBestEffort(date)           AS date
FROM kafka_raw_reviews
WHERE review_id   != ''
  AND business_id != ''
  AND kafka_raw_reviews.date != '';


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PIPELINE 3 : yelp-negative-reviews (SE4 Flink - Alertes)   ║
-- ╚══════════════════════════════════════════════════════════════╝

-- ── Étape 1 : Kafka Engine (alertes Flink SE4) ────────────────
CREATE TABLE IF NOT EXISTS kafka_raw_negative_alerts
(
    business_id String,
    stars       Float32,
    alert       String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:29092',
    kafka_topic_list           = 'yelp-negative-reviews',
    kafka_group_name           = 'clickhouse_se5_alerts',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 1,
    kafka_max_block_size       = 500,
    kafka_skip_broken_messages = 10;


-- ── Étape 2 : MergeTree destination alertes (S3 OPTIMISÉ) ──────
-- ✨ OPTIMISATION : Partition mensuelle + TTL 6 mois (données temps-réel)
-- ✨ ORDER BY : (received_at, business_id)
--    → reçue_at : requêtes récentes (alertes vivantes)
--    → business_id : jointure rapide avec businesses_live
CREATE TABLE IF NOT EXISTS negative_alerts_live
(
    business_id  String,
    stars        Float32,
    alert        String,
    received_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, business_id)
TTL received_at + INTERVAL 6 MONTH DELETE
SETTINGS
    index_granularity = 8192;


-- ── Étape 3 : Materialized View alertes ────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_negative_alerts
TO negative_alerts_live
AS
SELECT
    business_id,
    if(stars < 1 OR stars > 5, 0, stars)  AS stars,
    alert
FROM kafka_raw_negative_alerts
WHERE business_id != ''
  AND alert       != '';


-- ══════════════════════════════════════════════════════════════
-- VÉRIFICATION
-- ══════════════════════════════════════════════════════════════

-- Afficher tous les objets créés
SHOW TABLES FROM bigdata_db;

-- Afficher les ORDER BY actuels
SELECT
    table_name,
    engine,
    formatReadableQuantity(total_bytes) AS size
FROM system.tables
WHERE database = 'bigdata_db'
ORDER BY total_bytes DESC
FORMAT PrettyCompact;
