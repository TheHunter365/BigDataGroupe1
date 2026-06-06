-- ══════════════════════════════════════════════════════════════════════════
-- SE5 — ClickHouse Init Script complet
-- Schémas alignés avec spark_etl.py v3 · Mock data pour tests
-- ══════════════════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS bigdata_db;

-- ══════════════════════════════════════════════════════════════════════════
-- TABLES BATCH (importées depuis Parquet SE3)
-- Schémas validés contre spark_etl.py v3
-- ══════════════════════════════════════════════════════════════════════════

-- AXE 1 — RÉPUTATION ──────────────────────────────────────────────────────

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

CREATE TABLE IF NOT EXISTS bigdata_db.axe1_closure_rate
(
    stars_bucket     String,
    total            Int64,
    closed           Int64,
    closure_rate_pct Float64
)
ENGINE = MergeTree()
ORDER BY stars_bucket;

CREATE TABLE IF NOT EXISTS bigdata_db.axe1_monthly_running_avg
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
ENGINE = MergeTree()
ORDER BY (business_id, yr, mth)
SETTINGS index_granularity = 8192;

-- AXE 2 — GÉOGRAPHIE ──────────────────────────────────────────────────────

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

-- business_enriched : axe1_biz_attrs + nb_competitors_500m (Axe 2)
CREATE TABLE IF NOT EXISTS bigdata_db.business_enriched
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
    weekly_open_minutes  Int32,
    nb_competitors_500m  Int64
)
ENGINE = MergeTree()
ORDER BY (state, city, business_id);

-- AXE 3 — RÉSEAU SOCIAL ───────────────────────────────────────────────────

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

CREATE TABLE IF NOT EXISTS bigdata_db.axe3_seniority_analysis
(
    seniority_bucket String,
    nb_reviews       Int64,
    avg_stars        Float64,
    stddev_stars     Float64
)
ENGINE = MergeTree()
ORDER BY seniority_bucket;

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

-- AXE 4 — TEMPORALITÉ ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS bigdata_db.axe4_category_hourly
(
    type           String,
    hour           Int32,
    total_checkins Int64
)
ENGINE = MergeTree()
ORDER BY (type, hour);

CREATE TABLE IF NOT EXISTS bigdata_db.axe4_heatmap_by_biz
(
    business_id   String,
    day_of_week   Int32,
    hour          Int32,
    checkin_count Int64
)
ENGINE = MergeTree()
ORDER BY (business_id, day_of_week, hour);

CREATE TABLE IF NOT EXISTS bigdata_db.axe4_heatmap_global
(
    day_of_week    Int32,
    hour           Int32,
    total_checkins Int64
)
ENGINE = MergeTree()
ORDER BY (day_of_week, hour);

CREATE TABLE IF NOT EXISTS bigdata_db.axe4_monthly_trend
(
    year       Int32,
    month      Int32,
    nb_reviews Int64,
    avg_stars  Float64
)
ENGINE = MergeTree()
ORDER BY (year, month);

CREATE TABLE IF NOT EXISTS bigdata_db.axe4_stars_by_hour
(
    hour       Int32,
    nb_reviews Int64,
    avg_stars  Float64
)
ENGINE = MergeTree()
ORDER BY hour;

CREATE TABLE IF NOT EXISTS bigdata_db.axe4_stars_by_dow
(
    day_of_week Int32,
    nb_reviews  Int64,
    avg_stars   Float64
)
ENGINE = MergeTree()
ORDER BY day_of_week;

-- ══════════════════════════════════════════════════════════════════════════
-- TABLES STREAMING (live depuis Kafka/Flink)
-- ══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bigdata_db.businesses_live
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
    is_open       UInt8,
    categories    String,
    ingested_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (state, city, business_id);

CREATE TABLE IF NOT EXISTS bigdata_db.reviews_live
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
ORDER BY (business_id, date);

CREATE TABLE IF NOT EXISTS bigdata_db.negative_alerts_live
(
    business_id  String,
    stars        Float32,
    alert        String,
    received_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(received_at)
ORDER BY (received_at, business_id)
TTL received_at + INTERVAL 6 MONTH;

-- Table Kafka Engine : consomme le topic yelp-negative-reviews produit par Flink
-- Format des messages : {"business_id":"...","stars":1.0,"date":"...","alert":"..."}
CREATE TABLE IF NOT EXISTS bigdata_db.negative_alerts_kafka_engine
(
    business_id String,
    stars       Float32,
    date        String,
    alert       String
)
ENGINE = Kafka(
    'kafka:29092',
    'yelp-negative-reviews',
    'clickhouse-consumer-group',
    'JSONEachRow'
);

-- Materialized View : copie automatiquement chaque message vers negative_alerts_live
CREATE MATERIALIZED VIEW IF NOT EXISTS bigdata_db.negative_alerts_mv
TO bigdata_db.negative_alerts_live
AS
SELECT
    business_id,
    stars,
    alert,
    now() AS received_at
FROM bigdata_db.negative_alerts_kafka_engine;

-- ══════════════════════════════════════════════════════════════════════════
-- TABLES LEGACY (compatibilité code existant)
-- ══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS bigdata_db.reviews
(
    review_id   String,
    user_id     String,
    business_id String,
    stars       Float32,
    useful      UInt32,
    funny       UInt32,
    cool        UInt32,
    text        String,
    date        DateTime
)
ENGINE = MergeTree()
ORDER BY (date, business_id);

CREATE TABLE IF NOT EXISTS bigdata_db.daily_stats
(
    date            Date,
    total_reviews   UInt64,
    avg_stars       Float64,
    total_useful    UInt64,
    total_funny     UInt64,
    total_cool      UInt64,
    positive_count  UInt64,
    neutral_count   UInt64,
    negative_count  UInt64
)
ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE IF NOT EXISTS bigdata_db.business_stats
(
    business_id     String,
    total_reviews   UInt64,
    avg_stars       Float64,
    total_useful    UInt64,
    positive_count  UInt64,
    negative_count  UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY business_id;

-- ══════════════════════════════════════════════════════════════════════════
-- MOCK DATA — LIVE TABLES
-- ══════════════════════════════════════════════════════════════════════════

INSERT INTO bigdata_db.businesses_live
    (business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, categories)
VALUES
    ('restau_A', 'Restaurant Alpha',  '120 Broadway',     'New York', 'NY', '10001', 40.71280, -74.00600, 3.6, 24, 1, 'Restaurants, French'),
    ('restau_B', 'Bistrot Beta',      '45 5th Avenue',    'New York', 'NY', '10003', 40.71500, -74.00800, 3.5, 23, 1, 'Restaurants, American'),
    ('cafe_C',   'Cafe Charlie',      '88 Park Ave',      'New York', 'NY', '10016', 40.71600, -73.99900, 3.8, 22, 1, 'Coffee & Tea, Cafes'),
    ('bar_D',    'Bar Delta',         '210 West St',      'New York', 'NY', '10013', 40.71000, -74.00500, 3.7, 22, 0, 'Bars, Nightlife'),
    ('diner_E',  'Diner Echo',        '33 Fulton St',     'New York', 'NY', '10038', 40.71400, -74.00300, 3.5, 22, 1, 'Diners, Breakfast & Brunch'),
    ('bistro_F', 'Bistro Foxtrot',    '7 Greenwich Ave',  'New York', 'NY', '10014', 40.71200, -74.00700, 3.6, 21, 1, 'Restaurants, Italian');

-- ══════════════════════════════════════════════════════════════════════════
-- MOCK DATA — REVIEWS (7 jours · 140 avis · 6 businesses)
-- ══════════════════════════════════════════════════════════════════════════

INSERT INTO bigdata_db.reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
    ('rev_0001', 'usr_001', 'restau_A', 5.0, 2, 0, 1, 'Amazing food and incredible service. One of the best places I have visited!', '2026-04-28 08:15:00'),
    ('rev_0002', 'usr_002', 'restau_B', 4.0, 1, 0, 0, 'Really good place. Food was fresh and the service attentive.', '2026-04-28 09:00:00'),
    ('rev_0003', 'usr_003', 'cafe_C',   5.0, 3, 1, 2, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-04-28 09:30:00'),
    ('rev_0004', 'usr_004', 'bar_D',    3.0, 0, 0, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-04-28 10:00:00'),
    ('rev_0005', 'usr_005', 'diner_E',  5.0, 1, 0, 1, 'Fantastic experience! Will definitely come back soon.', '2026-04-28 10:30:00'),
    ('rev_0006', 'usr_006', 'bistro_F', 2.0, 0, 0, 0, 'Service was slow and the food arrived lukewarm.', '2026-04-28 11:00:00'),
    ('rev_0007', 'usr_007', 'restau_A', 4.0, 2, 0, 1, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-04-28 11:30:00'),
    ('rev_0008', 'usr_008', 'restau_B', 1.0, 1, 2, 0, 'Terrible experience. Will not be returning.', '2026-04-28 12:00:00'),
    ('rev_0009', 'usr_009', 'cafe_C',   4.0, 0, 0, 0, 'Very good experience overall. Would come back again.', '2026-04-28 12:30:00'),
    ('rev_0010', 'usr_010', 'bar_D',    5.0, 4, 1, 2, 'Best bar I have tried in a long time. Highly recommend!', '2026-04-28 13:00:00'),
    ('rev_0011', 'usr_011', 'diner_E',  3.0, 0, 1, 0, 'Food was okay. Service was average.', '2026-04-28 13:30:00'),
    ('rev_0012', 'usr_012', 'bistro_F', 5.0, 2, 0, 1, 'Outstanding quality and friendly staff. A must-visit place.', '2026-04-28 14:00:00'),
    ('rev_0013', 'usr_013', 'restau_A', 2.0, 1, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-04-28 14:30:00'),
    ('rev_0014', 'usr_014', 'restau_B', 5.0, 3, 0, 2, 'Loved every bite. The staff made us feel very welcome.', '2026-04-28 15:00:00'),
    ('rev_0015', 'usr_015', 'cafe_C',   3.0, 0, 0, 0, 'It was fine. Expected a bit more for the price.', '2026-04-28 15:30:00'),
    ('rev_0016', 'usr_016', 'bar_D',    4.0, 1, 1, 0, 'Great drinks and nice ambiance. A bit pricey but worth it.', '2026-04-28 16:00:00'),
    ('rev_0017', 'usr_017', 'diner_E',  1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-04-28 16:30:00'),
    ('rev_0018', 'usr_018', 'bistro_F', 4.0, 0, 0, 1, 'Solid choice. The menu has something for everyone.', '2026-04-28 17:00:00'),
    ('rev_0019', 'usr_019', 'restau_A', 3.0, 1, 0, 0, 'A standard experience. Nothing stood out particularly.', '2026-04-28 17:30:00'),
    ('rev_0020', 'usr_020', 'restau_B', 2.0, 0, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-04-28 18:00:00'),
    ('rev_0021', 'usr_021', 'cafe_C',   5.0, 1, 0, 1, 'Amazing food and incredible service. One of the best places I have visited!', '2026-04-29 08:00:00'),
    ('rev_0022', 'usr_022', 'bar_D',    5.0, 2, 1, 0, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-04-29 08:30:00'),
    ('rev_0023', 'usr_023', 'diner_E',  4.0, 0, 0, 1, 'Really good place. Food was fresh and the service attentive.', '2026-04-29 09:00:00'),
    ('rev_0024', 'usr_024', 'bistro_F', 3.0, 1, 0, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-04-29 09:30:00'),
    ('rev_0025', 'usr_025', 'restau_A', 5.0, 3, 0, 2, 'Fantastic experience! Will definitely come back soon.', '2026-04-29 10:00:00'),
    ('rev_0026', 'usr_026', 'restau_B', 4.0, 1, 0, 0, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-04-29 10:30:00'),
    ('rev_0027', 'usr_027', 'cafe_C',   2.0, 0, 1, 0, 'Service was slow and the food arrived lukewarm.', '2026-04-29 11:00:00'),
    ('rev_0028', 'usr_028', 'bar_D',    5.0, 2, 0, 1, 'Best bar I have tried in a long time. Highly recommend!', '2026-04-29 11:30:00'),
    ('rev_0029', 'usr_029', 'diner_E',  1.0, 1, 2, 0, 'Terrible experience. Will not be returning.', '2026-04-29 12:00:00'),
    ('rev_0030', 'usr_030', 'bistro_F', 4.0, 0, 0, 0, 'Very good experience overall. Would come back again.', '2026-04-29 12:30:00'),
    ('rev_0031', 'usr_031', 'restau_A', 3.0, 0, 0, 0, 'Food was okay. Service was average.', '2026-04-29 13:00:00'),
    ('rev_0032', 'usr_032', 'restau_B', 5.0, 4, 1, 3, 'Outstanding quality and friendly staff. A must-visit place.', '2026-04-29 13:30:00'),
    ('rev_0033', 'usr_033', 'cafe_C',   4.0, 1, 0, 1, 'Great ambiance and lovely food. A bit pricey but worth it.', '2026-04-29 14:00:00'),
    ('rev_0034', 'usr_034', 'bar_D',    2.0, 0, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-04-29 14:30:00'),
    ('rev_0035', 'usr_035', 'diner_E',  5.0, 2, 0, 1, 'Loved every bite. The staff made us feel very welcome.', '2026-04-29 15:00:00'),
    ('rev_0036', 'usr_036', 'bistro_F', 3.0, 0, 1, 0, 'It was fine. Expected a bit more for the price.', '2026-04-29 15:30:00'),
    ('rev_0037', 'usr_037', 'restau_A', 4.0, 1, 0, 0, 'Solid choice. The menu has something for everyone.', '2026-04-29 16:00:00'),
    ('rev_0038', 'usr_038', 'restau_B', 1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-04-29 16:30:00'),
    ('rev_0039', 'usr_039', 'cafe_C',   5.0, 3, 0, 2, 'Amazing food and incredible service. One of the best places I have visited!', '2026-04-29 17:00:00'),
    ('rev_0040', 'usr_040', 'bar_D',    3.0, 0, 0, 0, 'A standard experience. Nothing stood out particularly.', '2026-04-29 17:30:00'),
    ('rev_0041', 'usr_001', 'diner_E',  4.0, 1, 0, 1, 'Really good place. Food was fresh and the service attentive.', '2026-04-30 08:00:00'),
    ('rev_0042', 'usr_002', 'bistro_F', 5.0, 2, 1, 1, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-04-30 08:30:00'),
    ('rev_0043', 'usr_003', 'restau_A', 3.0, 0, 0, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-04-30 09:00:00'),
    ('rev_0044', 'usr_004', 'restau_B', 5.0, 3, 0, 2, 'Fantastic experience! Will definitely come back soon.', '2026-04-30 09:30:00'),
    ('rev_0045', 'usr_005', 'cafe_C',   2.0, 1, 0, 0, 'Service was slow and the food arrived lukewarm.', '2026-04-30 10:00:00'),
    ('rev_0046', 'usr_006', 'bar_D',    5.0, 4, 1, 3, 'Best place I have tried in a long time. Highly recommend!', '2026-04-30 10:30:00'),
    ('rev_0047', 'usr_007', 'diner_E',  4.0, 0, 0, 1, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-04-30 11:00:00'),
    ('rev_0048', 'usr_008', 'bistro_F', 1.0, 1, 3, 0, 'Terrible experience. Will not be returning.', '2026-04-30 11:30:00'),
    ('rev_0049', 'usr_009', 'restau_A', 5.0, 2, 0, 1, 'Outstanding quality and friendly staff. A must-visit place.', '2026-04-30 12:00:00'),
    ('rev_0050', 'usr_010', 'restau_B', 3.0, 0, 1, 0, 'Food was okay. Service was average.', '2026-04-30 12:30:00'),
    ('rev_0051', 'usr_011', 'cafe_C',   4.0, 1, 0, 0, 'Very good experience overall. Would come back again.', '2026-04-30 13:00:00'),
    ('rev_0052', 'usr_012', 'bar_D',    5.0, 3, 0, 2, 'Loved every bite. The staff made us feel very welcome.', '2026-04-30 13:30:00'),
    ('rev_0053', 'usr_013', 'diner_E',  2.0, 0, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-04-30 14:00:00'),
    ('rev_0054', 'usr_014', 'bistro_F', 4.0, 1, 1, 1, 'Great food and nice ambiance. A bit pricey but worth it.', '2026-04-30 14:30:00'),
    ('rev_0055', 'usr_015', 'restau_A', 3.0, 0, 0, 0, 'It was fine. Expected a bit more for the price.', '2026-04-30 15:00:00'),
    ('rev_0056', 'usr_016', 'restau_B', 5.0, 2, 0, 1, 'Amazing food and incredible service. One of the best places I have visited!', '2026-04-30 15:30:00'),
    ('rev_0057', 'usr_017', 'cafe_C',   4.0, 0, 0, 0, 'Solid choice. The menu has something for everyone.', '2026-04-30 16:00:00'),
    ('rev_0058', 'usr_018', 'bar_D',    2.0, 1, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-04-30 16:30:00'),
    ('rev_0059', 'usr_019', 'diner_E',  5.0, 4, 1, 2, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-04-30 17:00:00'),
    ('rev_0060', 'usr_020', 'bistro_F', 1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-04-30 17:30:00'),
    ('rev_0061', 'usr_021', 'restau_A', 5.0, 1, 0, 1, 'Best restaurant I have tried in a long time. Highly recommend!', '2026-05-01 08:00:00'),
    ('rev_0062', 'usr_022', 'restau_B', 4.0, 2, 0, 1, 'Really good place. Food was fresh and the service attentive.', '2026-05-01 08:30:00'),
    ('rev_0063', 'usr_023', 'cafe_C',   3.0, 0, 1, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-05-01 09:00:00'),
    ('rev_0064', 'usr_024', 'bar_D',    5.0, 3, 0, 2, 'Fantastic experience! Will definitely come back soon.', '2026-05-01 09:30:00'),
    ('rev_0065', 'usr_025', 'diner_E',  2.0, 0, 0, 0, 'Service was slow and the food arrived lukewarm.', '2026-05-01 10:00:00'),
    ('rev_0066', 'usr_026', 'bistro_F', 5.0, 4, 1, 3, 'Outstanding quality and friendly staff. A must-visit place.', '2026-05-01 10:30:00'),
    ('rev_0067', 'usr_027', 'restau_A', 4.0, 1, 0, 0, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-05-01 11:00:00'),
    ('rev_0068', 'usr_028', 'restau_B', 1.0, 1, 2, 0, 'Terrible experience. Will not be returning.', '2026-05-01 11:30:00'),
    ('rev_0069', 'usr_029', 'cafe_C',   5.0, 2, 0, 1, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-05-01 12:00:00'),
    ('rev_0070', 'usr_030', 'bar_D',    3.0, 0, 0, 0, 'Food was okay. Service was average.', '2026-05-01 12:30:00'),
    ('rev_0071', 'usr_031', 'diner_E',  4.0, 0, 1, 1, 'Very good experience overall. Would come back again.', '2026-05-01 13:00:00'),
    ('rev_0072', 'usr_032', 'bistro_F', 5.0, 3, 0, 2, 'Loved every bite. The staff made us feel very welcome.', '2026-05-01 13:30:00'),
    ('rev_0073', 'usr_033', 'restau_A', 2.0, 1, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-05-01 14:00:00'),
    ('rev_0074', 'usr_034', 'restau_B', 5.0, 2, 1, 1, 'Amazing food and incredible service. One of the best places I have visited!', '2026-05-01 14:30:00'),
    ('rev_0075', 'usr_035', 'cafe_C',   3.0, 0, 0, 0, 'It was fine. Expected a bit more for the price.', '2026-05-01 15:00:00'),
    ('rev_0076', 'usr_036', 'bar_D',    4.0, 1, 0, 0, 'Great drinks and nice ambiance. A bit pricey but worth it.', '2026-05-01 15:30:00'),
    ('rev_0077', 'usr_037', 'diner_E',  1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-05-01 16:00:00'),
    ('rev_0078', 'usr_038', 'bistro_F', 4.0, 0, 0, 1, 'Solid choice. The menu has something for everyone.', '2026-05-01 16:30:00'),
    ('rev_0079', 'usr_039', 'restau_A', 5.0, 3, 0, 2, 'Fantastic experience! Will definitely come back soon.', '2026-05-01 17:00:00'),
    ('rev_0080', 'usr_040', 'restau_B', 3.0, 0, 1, 0, 'A standard experience. Nothing stood out particularly.', '2026-05-01 17:30:00'),
    ('rev_0081', 'usr_041', 'cafe_C',   5.0, 2, 0, 1, 'Best cafe I have tried in a long time. Highly recommend!', '2026-05-02 08:00:00'),
    ('rev_0082', 'usr_042', 'bar_D',    3.0, 0, 0, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-05-02 08:30:00'),
    ('rev_0083', 'usr_043', 'diner_E',  4.0, 1, 0, 1, 'Really good place. Food was fresh and the service attentive.', '2026-05-02 09:00:00'),
    ('rev_0084', 'usr_044', 'bistro_F', 5.0, 3, 1, 2, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-05-02 09:30:00'),
    ('rev_0085', 'usr_045', 'restau_A', 2.0, 0, 0, 0, 'Service was slow and the food arrived lukewarm.', '2026-05-02 10:00:00'),
    ('rev_0086', 'usr_046', 'restau_B', 5.0, 4, 0, 3, 'Fantastic experience! Will definitely come back soon.', '2026-05-02 10:30:00'),
    ('rev_0087', 'usr_047', 'cafe_C',   4.0, 1, 1, 0, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-05-02 11:00:00'),
    ('rev_0088', 'usr_048', 'bar_D',    1.0, 1, 2, 0, 'Terrible experience. Will not be returning.', '2026-05-02 11:30:00'),
    ('rev_0089', 'usr_049', 'diner_E',  5.0, 2, 0, 1, 'Outstanding quality and friendly staff. A must-visit place.', '2026-05-02 12:00:00'),
    ('rev_0090', 'usr_050', 'bistro_F', 3.0, 0, 1, 0, 'Food was okay. Service was average.', '2026-05-02 12:30:00'),
    ('rev_0091', 'usr_001', 'restau_A', 5.0, 2, 0, 1, 'Loved every bite. The staff made us feel very welcome.', '2026-05-02 13:00:00'),
    ('rev_0092', 'usr_002', 'restau_B', 4.0, 0, 0, 0, 'Very good experience overall. Would come back again.', '2026-05-02 13:30:00'),
    ('rev_0093', 'usr_003', 'cafe_C',   3.0, 1, 0, 0, 'It was fine. Expected a bit more for the price.', '2026-05-02 14:00:00'),
    ('rev_0094', 'usr_004', 'bar_D',    5.0, 3, 1, 2, 'Amazing food and incredible service. One of the best places I have visited!', '2026-05-02 14:30:00'),
    ('rev_0095', 'usr_005', 'diner_E',  2.0, 1, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-05-02 15:00:00'),
    ('rev_0096', 'usr_006', 'bistro_F', 4.0, 0, 0, 1, 'Great food and nice ambiance. A bit pricey but worth it.', '2026-05-02 15:30:00'),
    ('rev_0097', 'usr_007', 'restau_A', 1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-05-02 16:00:00'),
    ('rev_0098', 'usr_008', 'restau_B', 5.0, 1, 0, 1, 'Best restaurant I have tried in a long time. Highly recommend!', '2026-05-02 16:30:00'),
    ('rev_0099', 'usr_009', 'cafe_C',   4.0, 1, 0, 0, 'Solid choice. The menu has something for everyone.', '2026-05-02 17:00:00'),
    ('rev_0100', 'usr_010', 'bar_D',    2.0, 0, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-05-02 17:30:00'),
    ('rev_0101', 'usr_011', 'diner_E',  5.0, 2, 0, 1, 'Fantastic experience! Will definitely come back soon.', '2026-05-03 08:00:00'),
    ('rev_0102', 'usr_012', 'bistro_F', 4.0, 1, 1, 0, 'Really good place. Food was fresh and the service attentive.', '2026-05-03 08:30:00'),
    ('rev_0103', 'usr_013', 'restau_A', 3.0, 0, 0, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-05-03 09:00:00'),
    ('rev_0104', 'usr_014', 'restau_B', 5.0, 3, 0, 2, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-05-03 09:30:00'),
    ('rev_0105', 'usr_015', 'cafe_C',   2.0, 1, 0, 0, 'Service was slow and the food arrived lukewarm.', '2026-05-03 10:00:00'),
    ('rev_0106', 'usr_016', 'bar_D',    5.0, 4, 2, 3, 'Outstanding quality and friendly staff. A must-visit place.', '2026-05-03 10:30:00'),
    ('rev_0107', 'usr_017', 'diner_E',  4.0, 1, 0, 1, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-05-03 11:00:00'),
    ('rev_0108', 'usr_018', 'bistro_F', 1.0, 1, 3, 0, 'Terrible experience. Will not be returning.', '2026-05-03 11:30:00'),
    ('rev_0109', 'usr_019', 'restau_A', 5.0, 2, 0, 1, 'Loved every bite. The staff made us feel very welcome.', '2026-05-03 12:00:00'),
    ('rev_0110', 'usr_020', 'restau_B', 3.0, 0, 1, 0, 'Food was okay. Service was average.', '2026-05-03 12:30:00'),
    ('rev_0111', 'usr_021', 'cafe_C',   5.0, 3, 0, 2, 'Amazing food and incredible service. One of the best places I have visited!', '2026-05-03 13:00:00'),
    ('rev_0112', 'usr_022', 'bar_D',    4.0, 0, 0, 0, 'Very good experience overall. Would come back again.', '2026-05-03 13:30:00'),
    ('rev_0113', 'usr_023', 'diner_E',  2.0, 0, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-05-03 14:00:00'),
    ('rev_0114', 'usr_024', 'bistro_F', 5.0, 2, 1, 1, 'Best bistro I have tried in a long time. Highly recommend!', '2026-05-03 14:30:00'),
    ('rev_0115', 'usr_025', 'restau_A', 4.0, 1, 0, 0, 'Great food and nice ambiance. A bit pricey but worth it.', '2026-05-03 15:00:00'),
    ('rev_0116', 'usr_026', 'restau_B', 3.0, 0, 0, 0, 'It was fine. Expected a bit more for the price.', '2026-05-03 15:30:00'),
    ('rev_0117', 'usr_027', 'cafe_C',   5.0, 4, 1, 3, 'Fantastic experience! Will definitely come back soon.', '2026-05-03 16:00:00'),
    ('rev_0118', 'usr_028', 'bar_D',    1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-05-03 16:30:00'),
    ('rev_0119', 'usr_029', 'diner_E',  4.0, 0, 0, 1, 'Solid choice. The menu has something for everyone.', '2026-05-03 17:00:00'),
    ('rev_0120', 'usr_030', 'bistro_F', 2.0, 1, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-05-03 17:30:00'),
    ('rev_0121', 'usr_031', 'restau_A', 5.0, 3, 0, 2, 'Best restaurant I have tried in a long time. Highly recommend!', '2026-05-04 08:00:00'),
    ('rev_0122', 'usr_032', 'restau_B', 4.0, 1, 0, 1, 'Really good place. Food was fresh and the service attentive.', '2026-05-04 08:30:00'),
    ('rev_0123', 'usr_033', 'cafe_C',   5.0, 2, 1, 1, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-05-04 09:00:00'),
    ('rev_0124', 'usr_034', 'bar_D',    3.0, 0, 0, 0, 'Decent place, nothing extraordinary but not bad either.', '2026-05-04 09:30:00'),
    ('rev_0125', 'usr_035', 'diner_E',  4.0, 1, 0, 0, 'Enjoyed our visit. Minor wait but totally worth it.', '2026-05-04 10:00:00'),
    ('rev_0126', 'usr_036', 'bistro_F', 5.0, 4, 1, 3, 'Outstanding quality and friendly staff. A must-visit place.', '2026-05-04 10:30:00'),
    ('rev_0127', 'usr_037', 'restau_A', 2.0, 0, 0, 0, 'Service was slow and the food arrived lukewarm.', '2026-05-04 11:00:00'),
    ('rev_0128', 'usr_038', 'restau_B', 1.0, 1, 2, 0, 'Terrible experience. Will not be returning.', '2026-05-04 11:30:00'),
    ('rev_0129', 'usr_039', 'cafe_C',   5.0, 2, 0, 1, 'Fantastic experience! Will definitely come back soon.', '2026-05-04 12:00:00'),
    ('rev_0130', 'usr_040', 'bar_D',    4.0, 0, 1, 0, 'Very good experience overall. Would come back again.', '2026-05-04 12:30:00'),
    ('rev_0131', 'usr_041', 'diner_E',  3.0, 1, 0, 0, 'Food was okay. Service was average.', '2026-05-04 13:00:00'),
    ('rev_0132', 'usr_042', 'bistro_F', 5.0, 3, 0, 2, 'Loved every bite. The staff made us feel very welcome.', '2026-05-04 13:30:00'),
    ('rev_0133', 'usr_043', 'restau_A', 4.0, 1, 0, 1, 'Great food and nice ambiance. A bit pricey but worth it.', '2026-05-04 14:00:00'),
    ('rev_0134', 'usr_044', 'restau_B', 2.0, 0, 0, 0, 'Not impressed. Expected better quality for this price.', '2026-05-04 14:30:00'),
    ('rev_0135', 'usr_045', 'cafe_C',   5.0, 4, 1, 3, 'Amazing food and incredible service. One of the best places I have visited!', '2026-05-04 15:00:00'),
    ('rev_0136', 'usr_046', 'bar_D',    3.0, 0, 1, 0, 'It was fine. Expected a bit more for the price.', '2026-05-04 15:30:00'),
    ('rev_0137', 'usr_047', 'diner_E',  4.0, 1, 0, 0, 'Solid choice. The menu has something for everyone.', '2026-05-04 16:00:00'),
    ('rev_0138', 'usr_048', 'bistro_F', 1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-05-04 16:30:00'),
    ('rev_0139', 'usr_049', 'restau_A', 5.0, 2, 0, 1, 'Absolutely loved it. The atmosphere was perfect and the food divine.', '2026-05-04 17:00:00'),
    ('rev_0140', 'usr_050', 'restau_B', 3.0, 0, 0, 0, 'A standard experience. Nothing stood out particularly.', '2026-05-04 17:30:00');

-- Copier les reviews vers reviews_live (même données pour les panels live)
INSERT INTO bigdata_db.reviews_live
    (review_id, user_id, business_id, stars, useful, funny, cool, text, date)
SELECT review_id, user_id, business_id, stars, useful, funny, cool, text, date
FROM bigdata_db.reviews;

-- ══════════════════════════════════════════════════════════════════════════
-- MOCK DATA — ALERTES NÉGATIVES
-- ══════════════════════════════════════════════════════════════════════════

INSERT INTO bigdata_db.negative_alerts_live (business_id, stars, alert, received_at) VALUES
    ('restau_B', 1.0, 'Avis Negatif Detecte', '2026-04-28 12:00:00'),
    ('bistro_F', 2.0, 'Avis Negatif Detecte', '2026-04-28 11:00:00'),
    ('diner_E',  1.0, 'Avis Negatif Detecte', '2026-04-28 16:30:00'),
    ('bistro_F', 1.0, 'Avis Negatif Detecte', '2026-04-30 11:30:00'),
    ('restau_B', 1.0, 'Avis Negatif Detecte', '2026-04-29 16:30:00'),
    ('diner_E',  1.0, 'Avis Negatif Detecte', '2026-04-29 12:00:00'),
    ('bar_D',    1.0, 'Avis Negatif Detecte', '2026-05-02 11:30:00'),
    ('restau_A', 1.0, 'Avis Negatif Detecte', '2026-05-02 16:00:00'),
    ('bistro_F', 1.0, 'Avis Negatif Detecte', '2026-05-03 11:30:00'),
    ('restau_B', 1.0, 'Avis Negatif Detecte', '2026-05-04 11:30:00');

-- ══════════════════════════════════════════════════════════════════════════
-- MOCK DATA — TABLES BATCH (valeurs agrégées approximatives)
-- Remplacées par les vraies données Parquet lors du déploiement SE3
-- ══════════════════════════════════════════════════════════════════════════

INSERT INTO bigdata_db.axe1_business_attributes
    (business_id, name, city, state, postal_code, stars, review_count, is_open,
     latitude, longitude, categories, categories_list, score_class, wifi,
     noise_level, price_range, alcohol, accepts_credit_cards, good_for_kids,
     has_tv, outdoor_seating, accepts_reservations, bike_parking, has_delivery,
     has_takeout, weekly_open_minutes)
VALUES
    ('restau_A', 'Restaurant Alpha', 'New York', 'NY', '10001', 3.6, 24, 1, 40.71280, -74.00600, 'Restaurants, French',     ['Restaurants','French'],            'Moyen', 'free',    'average',  'euro2',   'full_bar',     1, 1, 1, 1, 1, 0, 0, 1, 3900),
    ('restau_B', 'Bistrot Beta',     'New York', 'NY', '10003', 3.5, 23, 1, 40.71500, -74.00800, 'Restaurants, American',   ['Restaurants','American'],          'Moyen', 'no',      'loud',     'euro2',   'beer_and_wine',1, 1, 1, 0, 1, 1, 1, 1, 4200),
    ('cafe_C',   'Cafe Charlie',     'New York', 'NY', '10016', 3.8, 22, 1, 40.71600, -73.99900, 'Coffee & Tea, Cafes',     ['Coffee & Tea','Cafes'],            'Moyen', 'free',    'quiet',    'euro1',   'none',         1, 1, 0, 1, 0, 1, 0, 0, 3360),
    ('bar_D',    'Bar Delta',        'New York', 'NY', '10013', 3.7, 22, 0, 40.71000, -74.00500, 'Bars, Nightlife',         ['Bars','Nightlife'],                'Moyen', 'paid',    'loud',     'euro2',   'full_bar',     1, 0, 1, 0, 0, 0, 0, 0, 4200),
    ('diner_E',  'Diner Echo',       'New York', 'NY', '10038', 3.5, 22, 1, 40.71400, -74.00300, 'Diners, Breakfast',       ['Diners','Breakfast & Brunch'],     'Moyen', 'no',      'average',  'euro1',   'none',         1, 1, 1, 0, 0, 0, 1, 1, 4620),
    ('bistro_F', 'Bistro Foxtrot',   'New York', 'NY', '10014', 3.6, 21, 1, 40.71200, -74.00700, 'Restaurants, Italian',   ['Restaurants','Italian'],           'Moyen', 'unknown', 'average',  'euro3',   'full_bar',     1, 0, 0, 1, 1, 0, 0, 0, 3780);

INSERT INTO bigdata_db.axe1_closure_rate (stars_bucket, total, closed, closure_rate_pct) VALUES
    ('1.0-1.5',  420,   336,  80.0),
    ('2.0-2.5', 1850,  1110,  60.0),
    ('3.0-3.5', 8200,  2460,  30.0),
    ('4.0-5.0', 139876, 5875,  4.2);

INSERT INTO bigdata_db.axe1_monthly_running_avg
    (business_id, yr, mth, sum_stars, count_rev, avg_stars_month, cumul_sum, cumul_count, running_avg)
VALUES
    ('restau_A', 2026, 4,  37.0, 10, 3.70,  37.0, 10, 3.70),
    ('restau_A', 2026, 5,  51.0, 14, 3.64,  88.0, 24, 3.67),
    ('restau_B', 2026, 4,  30.0, 10, 3.00,  30.0, 10, 3.00),
    ('restau_B', 2026, 5,  45.0, 13, 3.46,  75.0, 23, 3.26),
    ('cafe_C',   2026, 4,  40.0, 10, 4.00,  40.0, 10, 4.00),
    ('cafe_C',   2026, 5,  44.0, 12, 3.67,  84.0, 22, 3.82),
    ('bar_D',    2026, 4,  43.0, 10, 4.30,  43.0, 10, 4.30),
    ('bar_D',    2026, 5,  38.0, 12, 3.17,  81.0, 22, 3.68),
    ('diner_E',  2026, 4,  36.0, 10, 3.60,  36.0, 10, 3.60),
    ('diner_E',  2026, 5,  41.0, 12, 3.42,  77.0, 22, 3.50),
    ('bistro_F', 2026, 4,  37.0, 10, 3.70,  37.0, 10, 3.70),
    ('bistro_F', 2026, 5,  42.0, 11, 3.82,  79.0, 21, 3.76);

INSERT INTO bigdata_db.axe2_avg_by_city (city, state, nb_businesses, avg_stars, stddev_stars, total_reviews) VALUES
    ('New York',       'NY', 6,    3.62, 0.11,  134),
    ('Las Vegas',      'NV', 1250, 3.71, 0.92, 185000),
    ('Philadelphia',   'PA', 850,  3.65, 0.88, 112000),
    ('Tucson',         'AZ', 640,  3.68, 0.90,  87000),
    ('Tampa',          'FL', 720,  3.74, 0.86,  98000),
    ('Indianapolis',   'IN', 530,  3.62, 0.91,  72000),
    ('Nashville',      'TN', 490,  3.78, 0.84,  68000),
    ('New Orleans',    'LA', 410,  3.82, 0.89,  58000),
    ('Charlotte',      'NC', 380,  3.69, 0.87,  51000),
    ('Scottsdale',     'AZ', 360,  3.85, 0.81,  49000),
    ('Edmonton',       'AB', 320,  3.58, 0.94,  43000),
    ('Calgary',        'AB', 295,  3.61, 0.93,  40000),
    ('Montreal',       'QC', 280,  3.66, 0.92,  38000),
    ('Pittsburgh',     'PA', 265,  3.63, 0.90,  36000),
    ('Cleveland',      'OH', 240,  3.55, 0.96,  32000),
    ('Boise',          'ID', 220,  3.77, 0.85,  29000),
    ('Mesa',           'AZ', 210,  3.72, 0.88,  28000),
    ('Henderson',      'NV', 195,  3.69, 0.89,  26000),
    ('Reno',           'NV', 185,  3.64, 0.91,  24000),
    ('Santa Barbara',  'CA', 175,  3.88, 0.80,  23000);

INSERT INTO bigdata_db.business_enriched
    (business_id, name, city, state, postal_code, stars, review_count, is_open,
     latitude, longitude, categories, wifi, noise_level, price_range, alcohol,
     accepts_credit_cards, good_for_kids, has_tv, outdoor_seating, accepts_reservations,
     bike_parking, has_delivery, has_takeout, weekly_open_minutes, nb_competitors_500m)
VALUES
    ('restau_A', 'Restaurant Alpha', 'New York', 'NY', '10001', 3.6, 24, 1, 40.71280, -74.00600, 'Restaurants, French',   'free',    'average', 'euro2', 'full_bar',     1,1,1,1,1,0,0,1,3900, 3),
    ('restau_B', 'Bistrot Beta',     'New York', 'NY', '10003', 3.5, 23, 1, 40.71500, -74.00800, 'Restaurants, American', 'no',      'loud',    'euro2', 'beer_and_wine',1,1,1,0,1,1,1,1,4200, 4),
    ('cafe_C',   'Cafe Charlie',     'New York', 'NY', '10016', 3.8, 22, 1, 40.71600, -73.99900, 'Coffee & Tea, Cafes',   'free',    'quiet',   'euro1', 'none',         1,1,0,1,0,1,0,0,3360, 2),
    ('bar_D',    'Bar Delta',        'New York', 'NY', '10013', 3.7, 22, 0, 40.71000, -74.00500, 'Bars, Nightlife',       'paid',    'loud',    'euro2', 'full_bar',     1,0,1,0,0,0,0,0,4200, 5),
    ('diner_E',  'Diner Echo',       'New York', 'NY', '10038', 3.5, 22, 1, 40.71400, -74.00300, 'Diners, Breakfast',     'no',      'average', 'euro1', 'none',         1,1,1,0,0,0,1,1,4620, 1),
    ('bistro_F', 'Bistro Foxtrot',   'New York', 'NY', '10014', 3.6, 21, 1, 40.71200, -74.00700, 'Restaurants, Italian',  'unknown', 'average', 'euro3', 'full_bar',     1,0,0,1,1,0,0,0,3780, 3);

INSERT INTO bigdata_db.axe3_elite_comparison
    (is_elite, nb_reviews, avg_stars_given, avg_text_length, avg_useful_received, avg_funny_received, avg_cool_received)
VALUES
    (0,  5250000, 3.72, 312.0, 0.38, 0.12, 0.21),
    (1,   842000, 3.61, 542.0, 1.24, 0.35, 0.68);

INSERT INTO bigdata_db.axe3_seniority_analysis (seniority_bucket, nb_reviews, avg_stars, stddev_stars) VALUES
    ('0-2 ans',   1420000, 3.85, 1.18),
    ('2-5 ans',   2180000, 3.74, 1.12),
    ('5-10 ans',  1950000, 3.63, 1.08),
    ('10+ ans',    542000, 3.58, 1.04);

INSERT INTO bigdata_db.axe3_user_enriched
    (user_id, fans, average_stars, account_age_years, friends_count, is_elite, total_compliments, pagerank)
VALUES
    ('usr_001', 42,  3.8, 12.5,  385, 1, 210,  0.000842),
    ('usr_002', 18,  4.1,  9.2,  124, 1,  98,  0.000614),
    ('usr_003',  7,  3.5,  6.8,   56, 0,  34,  0.000321),
    ('usr_004',  2,  3.9,  4.1,   18, 0,  12,  0.000198),
    ('usr_005', 85,  3.6, 14.0,  892, 1, 445,  0.001253),
    ('usr_006',  0,  4.2,  1.5,    3, 0,   2,  0.000087),
    ('usr_007', 31,  3.7,  8.0,  210, 0, 125,  0.000487),
    ('usr_008', 12,  3.3,  5.5,   67, 0,  48,  0.000276),
    ('usr_009', 156, 3.9, 16.2, 1240, 1, 832,  0.002187),
    ('usr_010',  0,  4.0,  2.3,    7, 0,   5,  0.000112);

INSERT INTO bigdata_db.axe3_user_review_enriched
    (user_id, business_id, stars, text_length, useful, is_elite, fans, pagerank, account_age_years)
VALUES
    ('usr_001', 'restau_A', 5, 312, 2, 1, 42,  0.000842, 12.5),
    ('usr_002', 'restau_B', 4, 287, 1, 1, 18,  0.000614,  9.2),
    ('usr_003', 'cafe_C',   5, 198, 3, 0,  7,  0.000321,  6.8),
    ('usr_004', 'bar_D',    3, 145, 0, 0,  2,  0.000198,  4.1),
    ('usr_005', 'diner_E',  5, 421, 1, 1, 85,  0.001253, 14.0),
    ('usr_006', 'bistro_F', 2,  89, 0, 0,  0,  0.000087,  1.5),
    ('usr_007', 'restau_A', 4, 265, 2, 0, 31,  0.000487,  8.0),
    ('usr_008', 'restau_B', 1, 178, 1, 0, 12,  0.000276,  5.5),
    ('usr_009', 'cafe_C',   4, 198, 0, 1, 156, 0.002187, 16.2),
    ('usr_010', 'bar_D',    5, 312, 4, 0,  0,  0.000112,  2.3);

INSERT INTO bigdata_db.axe4_monthly_trend (year, month, nb_reviews, avg_stars) VALUES
    (2016,  1,  98000, 3.72), (2016,  2,  94000, 3.70), (2016,  3, 108000, 3.68),
    (2016,  4, 112000, 3.74), (2016,  5, 125000, 3.71), (2016,  6, 138000, 3.69),
    (2016,  7, 145000, 3.73), (2016,  8, 148000, 3.75), (2016,  9, 132000, 3.70),
    (2016, 10, 128000, 3.68), (2016, 11, 118000, 3.66), (2016, 12, 122000, 3.65),
    (2017,  1, 102000, 3.71), (2017,  2,  98000, 3.69), (2017,  3, 115000, 3.67),
    (2017,  4, 118000, 3.73), (2017,  5, 132000, 3.70), (2017,  6, 145000, 3.68),
    (2017,  7, 152000, 3.72), (2017,  8, 155000, 3.74), (2017,  9, 140000, 3.69),
    (2017, 10, 135000, 3.67), (2017, 11, 124000, 3.65), (2017, 12, 128000, 3.64),
    (2018,  1, 108000, 3.70), (2018,  2, 104000, 3.68), (2018,  3, 121000, 3.66),
    (2018,  6, 152000, 3.68), (2018,  7, 160000, 3.71), (2018, 12, 136000, 3.63),
    (2019,  6, 162000, 3.70), (2019, 12, 145000, 3.62),
    (2020,  1, 115000, 3.68), (2020,  3,  82000, 3.55), (2020,  6,  68000, 3.52),
    (2020, 12,  92000, 3.60),
    (2021,  3, 105000, 3.64), (2021,  6, 148000, 3.69), (2021, 12, 138000, 3.65),
    (2022,  6, 155000, 3.72), (2022, 12, 142000, 3.67),
    (2023,  6, 158000, 3.73), (2023, 12, 146000, 3.68),
    (2024,  6, 162000, 3.74), (2024, 12, 150000, 3.70),
    (2026,  4,      3, 3.65), (2026,  5,      7, 3.64);

INSERT INTO bigdata_db.axe4_stars_by_hour (hour, nb_reviews, avg_stars) VALUES
    (0,   85000, 3.62), (1,   52000, 3.58), (2,   38000, 3.55), (3,   28000, 3.53),
    (4,   22000, 3.57), (5,   35000, 3.65), (6,   78000, 3.71), (7,  145000, 3.74),
    (8,  218000, 3.76), (9,  285000, 3.73), (10, 312000, 3.70), (11, 385000, 3.68),
    (12, 425000, 3.65), (13, 418000, 3.66), (14, 395000, 3.68), (15, 378000, 3.70),
    (16, 362000, 3.71), (17, 398000, 3.68), (18, 452000, 3.64), (19, 468000, 3.62),
    (20, 445000, 3.61), (21, 398000, 3.63), (22, 285000, 3.65), (23, 168000, 3.64);

INSERT INTO bigdata_db.axe4_stars_by_dow (day_of_week, nb_reviews, avg_stars) VALUES
    (1, 852000, 3.68),
    (2, 892000, 3.70),
    (3, 878000, 3.69),
    (4, 895000, 3.68),
    (5, 932000, 3.65),
    (6, 985000, 3.62),
    (7, 958000, 3.67);

INSERT INTO bigdata_db.axe4_heatmap_global (day_of_week, hour, total_checkins) VALUES
    (1,8,1200),(1,9,2100),(1,10,3200),(1,11,4100),(1,12,5800),(1,13,5200),(1,14,4100),(1,15,3800),(1,16,4200),(1,17,4800),(1,18,5500),(1,19,5200),(1,20,4100),(1,21,2800),
    (2,8,1400),(2,9,2400),(2,10,3600),(2,11,4800),(2,12,6200),(2,13,5800),(2,14,4600),(2,15,4100),(2,16,4600),(2,17,5200),(2,18,6100),(2,19,5800),(2,20,4500),(2,21,3100),
    (3,8,1350),(3,9,2350),(3,10,3500),(3,11,4600),(3,12,6100),(3,13,5600),(3,14,4500),(3,15,4000),(3,16,4500),(3,17,5100),(3,18,6000),(3,19,5700),(3,20,4400),(3,21,3000),
    (4,8,1420),(4,9,2420),(4,10,3620),(4,11,4820),(4,12,6320),(4,13,5920),(4,14,4720),(4,15,4220),(4,16,4720),(4,17,5420),(4,18,6220),(4,19,5920),(4,20,4620),(4,21,3220),
    (5,8,1500),(5,9,2600),(5,10,3800),(5,11,5200),(5,12,6800),(5,13,6400),(5,14,5200),(5,15,4600),(5,16,5200),(5,17,6200),(5,18,7800),(5,19,8200),(5,20,7100),(5,21,5200),
    (6,8,2800),(6,9,4200),(6,10,5600),(6,11,7200),(6,12,8400),(6,13,8200),(6,14,7200),(6,15,6800),(6,16,7200),(6,17,8200),(6,18,9800),(6,19,10200),(6,20,9400),(6,21,7200),
    (7,8,3200),(7,9,4800),(7,10,6200),(7,11,8200),(7,12,9400),(7,13,8800),(7,14,7800),(7,15,7200),(7,16,7600),(7,17,8200),(7,18,8800),(7,19,8200),(7,20,7400),(7,21,5800);

INSERT INTO bigdata_db.axe4_category_hourly (type, hour, total_checkins) VALUES
    ('Pizzeria', 10, 1200), ('Pizzeria', 11, 2800), ('Pizzeria', 12, 4200),
    ('Pizzeria', 13, 3800), ('Pizzeria', 14, 3200), ('Pizzeria', 15, 2800),
    ('Pizzeria', 16, 3200), ('Pizzeria', 17, 4800), ('Pizzeria', 18, 7200),
    ('Pizzeria', 19, 8400), ('Pizzeria', 20, 7800), ('Pizzeria', 21, 5200),
    ('Gastronomique', 11, 800), ('Gastronomique', 12, 2400), ('Gastronomique', 13, 3800),
    ('Gastronomique', 14, 3200), ('Gastronomique', 15, 2600), ('Gastronomique', 16, 2200),
    ('Gastronomique', 17, 2800), ('Gastronomique', 18, 4200), ('Gastronomique', 19, 5800),
    ('Gastronomique', 20, 6200), ('Gastronomique', 21, 4800), ('Gastronomique', 22, 2400);

-- ══════════════════════════════════════════════════════════════════════════
-- AGRÉGATIONS LEGACY
-- ══════════════════════════════════════════════════════════════════════════

INSERT INTO bigdata_db.daily_stats
SELECT
    toDate(date)        AS date,
    count()             AS total_reviews,
    avg(stars)          AS avg_stars,
    sum(useful)         AS total_useful,
    sum(funny)          AS total_funny,
    sum(cool)           AS total_cool,
    countIf(stars >= 4) AS positive_count,
    countIf(stars = 3)  AS neutral_count,
    countIf(stars <= 2) AS negative_count
FROM bigdata_db.reviews
GROUP BY date;

INSERT INTO bigdata_db.business_stats
SELECT
    business_id,
    count()             AS total_reviews,
    avg(stars)          AS avg_stars,
    sum(useful)         AS total_useful,
    countIf(stars >= 4) AS positive_count,
    countIf(stars <= 2) AS negative_count
FROM bigdata_db.reviews
GROUP BY business_id;
