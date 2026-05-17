-- ══════════════════════════════════════════════════════════════
-- SE5 S2 — Pipeline Kafka → ClickHouse  (M1 Work)
-- Auteur : Membre 1 (Data Engineer)
--
-- Topics SE2 consommés :
--   • topic_business  → 141 035 messages (batch, données Yelp businesses)
--   • topic_reviews   → messages streaming (données Yelp reviews)
--
-- Architecture par topic (3 objets chacun) :
--   1. kafka_raw_*    : Kafka Engine  → lecture brute du topic
--   2. *_live         : MergeTree     → table destination optimisée
--   3. mv_kafka_to_*  : Materialized View → nettoyage + routage
--
-- Prérequis : Kafka broker sur kafka:29092 (réseau Docker bigdata-net)
--             Topics 'topic_business' et 'topic_reviews' existants
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PIPELINE 1 : topic_business                                ║
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
    categories    String    -- ex: "Restaurants, Food, Italian"
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


-- ── Étape 2 : MergeTree destination businesses ─────────────────
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
ORDER BY (city, stars, business_id);    -- optimisé : filtres Grafana par ville + note
-- NB : pas de PRIMARY KEY séparée — ClickHouse impose qu'elle soit un préfixe
--      de l'ORDER BY ; l'ORDER BY tient donc lieu de clé primaire.


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
    if(stars < 0 OR stars > 5, 0, stars)   AS stars,   -- sanitize hors [0,5]
    if(review_count < 0, 0, review_count)  AS review_count,
    if(is_open = 1, 1, 0)                  AS is_open,
    trim(categories)                        AS categories
FROM kafka_raw_business
WHERE business_id != ''
  AND name        != '';


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PIPELINE 2 : topic_reviews                                 ║
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
    date        String      -- format : "YYYY-MM-DD HH:MM:SS"
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


-- ── Étape 2 : MergeTree destination reviews ────────────────────
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
-- Partitionnement ANNUEL : le dataset Yelp couvre ~9 ans (2015-2023).
-- toYYYYMM générerait +100 partitions par bloc d'insertion → erreur
-- TOO_MANY_PARTS (limite max_partitions_per_insert_block = 100).
PARTITION BY toYear(date)
ORDER BY (business_id, date, stars);    -- optimisé : jointures + time range Grafana
-- NB : pas de TTL sur 'date' — le dataset Yelp est historique (avis de 2015 à
--      2023). Un TTL « date + INTERVAL N YEAR » supprimerait la quasi-totalité
--      des avis dès leur insertion.


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
  -- colonne qualifiée : évite que l'analyseur ClickHouse confonde
  -- 'date' avec l'alias DateTime du SELECT (erreur CANNOT_PARSE_DATETIME).
  AND kafka_raw_reviews.date != '';


-- ╔══════════════════════════════════════════════════════════════╗
-- ║  PIPELINE 3 : yelp-negative-reviews  (output Flink SE4)     ║
-- ║  Alertes temps réel : avis négatifs détectés par Flink       ║
-- ╚══════════════════════════════════════════════════════════════╝

-- ── Étape 1 : Kafka Engine (alertes brutes depuis Flink SE4) ───
CREATE TABLE IF NOT EXISTS kafka_raw_negative_alerts
(
    business_id String,
    stars       Float32,
    alert       String    -- ex: "Avis Négatif Détecté"
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


-- ── Étape 2 : MergeTree destination alertes ────────────────────
CREATE TABLE IF NOT EXISTS negative_alerts_live
(
    business_id  String,
    stars        Float32,
    alert        String,
    received_at  DateTime DEFAULT now()    -- horodatage d'arrivée dans ClickHouse
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, business_id)
TTL received_at + INTERVAL 6 MONTH;


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
SHOW TABLES FROM bigdata_db;
