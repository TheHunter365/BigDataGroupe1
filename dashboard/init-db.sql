-- ══════════════════════════════════════════════════════════════
-- SE5 — BigData Project · ClickHouse Init Script
-- Aligné avec le dataset Yelp (format reviews + businesses)
-- ══════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS bigdata_db;

USE bigdata_db;

-- ── Table principale : avis Yelp (même format que ingestion/streaming) ───
CREATE TABLE IF NOT EXISTS reviews (
    review_id   String,
    user_id     String,
    business_id String,
    stars       Float32,
    useful      UInt32,
    funny       UInt32,
    cool        UInt32,
    text        String,
    date        DateTime
) ENGINE = MergeTree()
ORDER BY (date, business_id);

-- ── Table analytique : statistiques quotidiennes ─────────────
CREATE TABLE IF NOT EXISTS daily_stats (
    date            Date,
    total_reviews   UInt64,
    avg_stars       Float64,
    total_useful    UInt64,
    total_funny     UInt64,
    total_cool      UInt64,
    positive_count  UInt64,
    neutral_count   UInt64,
    negative_count  UInt64
) ENGINE = MergeTree()
ORDER BY date;

-- ── Table analytique : statistiques par business ─────────────
CREATE TABLE IF NOT EXISTS business_stats (
    business_id     String,
    total_reviews   UInt64,
    avg_stars       Float64,
    total_useful    UInt64,
    positive_count  UInt64,
    negative_count  UInt64
) ENGINE = ReplacingMergeTree()
ORDER BY business_id;

-- ══════════════════════════════════════════════════════════════
-- DONNÉES MOCK — 7 jours · 20 avis/jour · 140 avis au total
-- Businesses : restau_A, restau_B, cafe_C, bar_D, diner_E, bistro_F
-- (mêmes IDs que le Kafka Producer de SE4)
-- ══════════════════════════════════════════════════════════════

-- Jour 1 : 28 avril 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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
    ('rev_0020', 'usr_020', 'restau_B', 2.0, 0, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-04-28 18:00:00');

-- Jour 2 : 29 avril 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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
    ('rev_0040', 'usr_040', 'bar_D',    3.0, 0, 0, 0, 'A standard experience. Nothing stood out particularly.', '2026-04-29 17:30:00');

-- Jour 3 : 30 avril 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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
    ('rev_0060', 'usr_020', 'bistro_F', 1.0, 2, 1, 0, 'Very disappointing. Cold food and rude staff.', '2026-04-30 17:30:00');

-- Jour 4 : 1er mai 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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
    ('rev_0080', 'usr_040', 'restau_B', 3.0, 0, 1, 0, 'A standard experience. Nothing stood out particularly.', '2026-05-01 17:30:00');

-- Jour 5 : 2 mai 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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
    ('rev_0100', 'usr_010', 'bar_D',    2.0, 0, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-05-02 17:30:00');

-- Jour 6 : 3 mai 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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
    ('rev_0120', 'usr_030', 'bistro_F', 2.0, 1, 0, 0, 'Disappointing visit. Several items on the menu were unavailable.', '2026-05-03 17:30:00');

-- Jour 7 : 4 mai 2026
INSERT INTO reviews (review_id, user_id, business_id, stars, useful, funny, cool, text, date) VALUES
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

-- ══════════════════════════════════════════════════════════════
-- AGRÉGATION : Remplissage de daily_stats
-- ══════════════════════════════════════════════════════════════
INSERT INTO daily_stats
SELECT
    toDate(date)          AS date,
    count()               AS total_reviews,
    avg(stars)            AS avg_stars,
    sum(useful)           AS total_useful,
    sum(funny)            AS total_funny,
    sum(cool)             AS total_cool,
    countIf(stars >= 4)   AS positive_count,
    countIf(stars = 3)    AS neutral_count,
    countIf(stars <= 2)   AS negative_count
FROM reviews
GROUP BY date;

-- ══════════════════════════════════════════════════════════════
-- AGRÉGATION : Remplissage de business_stats
-- ══════════════════════════════════════════════════════════════
INSERT INTO business_stats
SELECT
    business_id,
    count()               AS total_reviews,
    avg(stars)            AS avg_stars,
    sum(useful)           AS total_useful,
    countIf(stars >= 4)   AS positive_count,
    countIf(stars <= 2)   AS negative_count
FROM reviews
GROUP BY business_id;
