-- ══════════════════════════════════════════════════════════════
-- SE5 — BigData Project · ClickHouse Init Script
-- Crée la base, les tables mock et insère des données simulées
-- ══════════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS bigdata_db;

USE bigdata_db;

-- ── Table principale : événements mock ───────────────────────
CREATE TABLE IF NOT EXISTS mock_events (
    timestamp DateTime64(3, 'UTC'),
    device_id String,
    event_type Enum8('click' = 1, 'view' = 2, 'purchase' = 3),
    value Float64,
    country String
) ENGINE = MergeTree()
ORDER BY (timestamp, device_id);

-- ── Table analytique : métriques agrégées par heure ──────────
CREATE TABLE IF NOT EXISTS hourly_metrics (
    hour DateTime,
    country String,
    event_type String,
    total_events UInt64,
    total_value Float64,
    unique_devices UInt32
) ENGINE = SummingMergeTree()
ORDER BY (hour, country, event_type);

-- ── Table KPI : résumé quotidien ─────────────────────────────
CREATE TABLE IF NOT EXISTS daily_kpi (
    date Date,
    total_revenue Float64,
    total_clicks UInt64,
    total_views UInt64,
    total_purchases UInt64,
    conversion_rate Float64,
    avg_order_value Float64,
    active_devices UInt32,
    top_country String
) ENGINE = MergeTree()
ORDER BY date;

-- ══════════════════════════════════════════════════════════════
-- INSERTION DE DONNÉES MOCK (7 jours de simulation)
-- ══════════════════════════════════════════════════════════════

-- Jour 1 : 28 avril 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-04-28 08:00:00.000', 'dev_001', 'view', 0.0, 'MA'),
    ('2026-04-28 08:05:00.000', 'dev_001', 'click', 5.2, 'MA'),
    ('2026-04-28 08:15:00.000', 'dev_001', 'purchase', 149.99, 'MA'),
    ('2026-04-28 09:00:00.000', 'dev_002', 'view', 0.0, 'FR'),
    ('2026-04-28 09:10:00.000', 'dev_002', 'click', 3.1, 'FR'),
    ('2026-04-28 10:00:00.000', 'dev_003', 'view', 0.0, 'US'),
    ('2026-04-28 10:30:00.000', 'dev_003', 'click', 7.8, 'US'),
    ('2026-04-28 10:45:00.000', 'dev_003', 'purchase', 89.99, 'US'),
    ('2026-04-28 11:00:00.000', 'dev_004', 'view', 0.0, 'ES'),
    ('2026-04-28 11:20:00.000', 'dev_005', 'view', 0.0, 'MA'),
    ('2026-04-28 12:00:00.000', 'dev_005', 'click', 12.5, 'MA'),
    ('2026-04-28 12:30:00.000', 'dev_005', 'purchase', 249.99, 'MA'),
    ('2026-04-28 14:00:00.000', 'dev_006', 'view', 0.0, 'DE'),
    ('2026-04-28 14:15:00.000', 'dev_006', 'click', 2.0, 'DE'),
    ('2026-04-28 15:00:00.000', 'dev_007', 'view', 0.0, 'FR'),
    ('2026-04-28 15:30:00.000', 'dev_007', 'purchase', 59.99, 'FR'),
    ('2026-04-28 16:00:00.000', 'dev_008', 'view', 0.0, 'MA'),
    ('2026-04-28 16:45:00.000', 'dev_008', 'click', 8.3, 'MA'),
    ('2026-04-28 17:00:00.000', 'dev_009', 'view', 0.0, 'US'),
    ('2026-04-28 17:30:00.000', 'dev_010', 'view', 0.0, 'ES');

-- Jour 2 : 29 avril 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-04-29 07:30:00.000', 'dev_011', 'view', 0.0, 'MA'),
    ('2026-04-29 07:45:00.000', 'dev_011', 'click', 6.7, 'MA'),
    ('2026-04-29 08:00:00.000', 'dev_011', 'purchase', 199.99, 'MA'),
    ('2026-04-29 08:30:00.000', 'dev_012', 'view', 0.0, 'FR'),
    ('2026-04-29 09:00:00.000', 'dev_013', 'view', 0.0, 'US'),
    ('2026-04-29 09:15:00.000', 'dev_013', 'click', 4.5, 'US'),
    ('2026-04-29 09:30:00.000', 'dev_013', 'purchase', 329.99, 'US'),
    ('2026-04-29 10:00:00.000', 'dev_014', 'view', 0.0, 'DE'),
    ('2026-04-29 10:30:00.000', 'dev_014', 'click', 9.1, 'DE'),
    ('2026-04-29 11:00:00.000', 'dev_015', 'view', 0.0, 'ES'),
    ('2026-04-29 11:30:00.000', 'dev_015', 'click', 3.3, 'ES'),
    ('2026-04-29 11:45:00.000', 'dev_015', 'purchase', 79.99, 'ES'),
    ('2026-04-29 12:00:00.000', 'dev_016', 'view', 0.0, 'MA'),
    ('2026-04-29 12:30:00.000', 'dev_016', 'click', 11.2, 'MA'),
    ('2026-04-29 13:00:00.000', 'dev_017', 'view', 0.0, 'FR'),
    ('2026-04-29 13:30:00.000', 'dev_017', 'purchase', 119.99, 'FR'),
    ('2026-04-29 14:00:00.000', 'dev_018', 'view', 0.0, 'US'),
    ('2026-04-29 14:30:00.000', 'dev_018', 'click', 15.0, 'US'),
    ('2026-04-29 15:00:00.000', 'dev_019', 'view', 0.0, 'MA'),
    ('2026-04-29 15:30:00.000', 'dev_020', 'view', 0.0, 'DE'),
    ('2026-04-29 16:00:00.000', 'dev_020', 'click', 7.7, 'DE'),
    ('2026-04-29 16:30:00.000', 'dev_020', 'purchase', 449.99, 'DE');

-- Jour 3 : 30 avril 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-04-30 06:00:00.000', 'dev_021', 'view', 0.0, 'MA'),
    ('2026-04-30 06:15:00.000', 'dev_021', 'click', 4.0, 'MA'),
    ('2026-04-30 07:00:00.000', 'dev_022', 'view', 0.0, 'FR'),
    ('2026-04-30 07:30:00.000', 'dev_022', 'click', 8.5, 'FR'),
    ('2026-04-30 07:45:00.000', 'dev_022', 'purchase', 179.99, 'FR'),
    ('2026-04-30 08:00:00.000', 'dev_023', 'view', 0.0, 'US'),
    ('2026-04-30 08:30:00.000', 'dev_023', 'click', 6.2, 'US'),
    ('2026-04-30 09:00:00.000', 'dev_024', 'view', 0.0, 'ES'),
    ('2026-04-30 09:30:00.000', 'dev_024', 'click', 3.8, 'ES'),
    ('2026-04-30 09:45:00.000', 'dev_024', 'purchase', 99.99, 'ES'),
    ('2026-04-30 10:00:00.000', 'dev_025', 'view', 0.0, 'MA'),
    ('2026-04-30 10:30:00.000', 'dev_025', 'click', 10.0, 'MA'),
    ('2026-04-30 10:45:00.000', 'dev_025', 'purchase', 299.99, 'MA'),
    ('2026-04-30 11:00:00.000', 'dev_026', 'view', 0.0, 'DE'),
    ('2026-04-30 11:30:00.000', 'dev_027', 'view', 0.0, 'FR'),
    ('2026-04-30 12:00:00.000', 'dev_027', 'click', 5.5, 'FR'),
    ('2026-04-30 13:00:00.000', 'dev_028', 'view', 0.0, 'US'),
    ('2026-04-30 13:30:00.000', 'dev_028', 'purchase', 69.99, 'US'),
    ('2026-04-30 14:00:00.000', 'dev_029', 'view', 0.0, 'MA'),
    ('2026-04-30 14:30:00.000', 'dev_030', 'view', 0.0, 'ES');

-- Jour 4 : 1er mai 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-05-01 08:00:00.000', 'dev_031', 'view', 0.0, 'MA'),
    ('2026-05-01 08:10:00.000', 'dev_031', 'click', 14.3, 'MA'),
    ('2026-05-01 08:30:00.000', 'dev_031', 'purchase', 399.99, 'MA'),
    ('2026-05-01 09:00:00.000', 'dev_032', 'view', 0.0, 'FR'),
    ('2026-05-01 09:15:00.000', 'dev_032', 'click', 2.1, 'FR'),
    ('2026-05-01 10:00:00.000', 'dev_033', 'view', 0.0, 'US'),
    ('2026-05-01 10:30:00.000', 'dev_033', 'click', 11.8, 'US'),
    ('2026-05-01 10:45:00.000', 'dev_033', 'purchase', 549.99, 'US'),
    ('2026-05-01 11:00:00.000', 'dev_034', 'view', 0.0, 'DE'),
    ('2026-05-01 11:30:00.000', 'dev_034', 'click', 6.0, 'DE'),
    ('2026-05-01 11:45:00.000', 'dev_034', 'purchase', 129.99, 'DE'),
    ('2026-05-01 12:00:00.000', 'dev_035', 'view', 0.0, 'ES'),
    ('2026-05-01 12:30:00.000', 'dev_035', 'click', 8.9, 'ES'),
    ('2026-05-01 13:00:00.000', 'dev_036', 'view', 0.0, 'MA'),
    ('2026-05-01 13:30:00.000', 'dev_036', 'purchase', 189.99, 'MA'),
    ('2026-05-01 14:00:00.000', 'dev_037', 'view', 0.0, 'FR'),
    ('2026-05-01 14:30:00.000', 'dev_037', 'click', 7.4, 'FR'),
    ('2026-05-01 14:45:00.000', 'dev_037', 'purchase', 89.99, 'FR'),
    ('2026-05-01 15:00:00.000', 'dev_038', 'view', 0.0, 'US'),
    ('2026-05-01 15:30:00.000', 'dev_039', 'view', 0.0, 'MA'),
    ('2026-05-01 16:00:00.000', 'dev_040', 'view', 0.0, 'DE'),
    ('2026-05-01 16:30:00.000', 'dev_040', 'click', 3.6, 'DE');

-- Jour 5 : 2 mai 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-05-02 07:00:00.000', 'dev_041', 'view', 0.0, 'MA'),
    ('2026-05-02 07:15:00.000', 'dev_041', 'click', 9.9, 'MA'),
    ('2026-05-02 07:30:00.000', 'dev_041', 'purchase', 259.99, 'MA'),
    ('2026-05-02 08:00:00.000', 'dev_042', 'view', 0.0, 'FR'),
    ('2026-05-02 08:30:00.000', 'dev_042', 'click', 5.0, 'FR'),
    ('2026-05-02 09:00:00.000', 'dev_043', 'view', 0.0, 'US'),
    ('2026-05-02 09:15:00.000', 'dev_043', 'click', 13.2, 'US'),
    ('2026-05-02 09:30:00.000', 'dev_043', 'purchase', 199.99, 'US'),
    ('2026-05-02 10:00:00.000', 'dev_044', 'view', 0.0, 'ES'),
    ('2026-05-02 10:30:00.000', 'dev_044', 'click', 4.7, 'ES'),
    ('2026-05-02 10:45:00.000', 'dev_044', 'purchase', 149.99, 'ES'),
    ('2026-05-02 11:00:00.000', 'dev_045', 'view', 0.0, 'DE'),
    ('2026-05-02 11:30:00.000', 'dev_045', 'click', 7.1, 'DE'),
    ('2026-05-02 12:00:00.000', 'dev_046', 'view', 0.0, 'MA'),
    ('2026-05-02 12:30:00.000', 'dev_046', 'purchase', 319.99, 'MA'),
    ('2026-05-02 13:00:00.000', 'dev_047', 'view', 0.0, 'FR'),
    ('2026-05-02 13:30:00.000', 'dev_047', 'click', 6.3, 'FR'),
    ('2026-05-02 13:45:00.000', 'dev_047', 'purchase', 79.99, 'FR'),
    ('2026-05-02 14:00:00.000', 'dev_048', 'view', 0.0, 'US'),
    ('2026-05-02 14:30:00.000', 'dev_049', 'view', 0.0, 'MA'),
    ('2026-05-02 15:00:00.000', 'dev_050', 'view', 0.0, 'ES');

-- Jour 6 : 3 mai 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-05-03 08:00:00.000', 'dev_001', 'view', 0.0, 'MA'),
    ('2026-05-03 08:10:00.000', 'dev_001', 'click', 11.0, 'MA'),
    ('2026-05-03 08:30:00.000', 'dev_001', 'purchase', 179.99, 'MA'),
    ('2026-05-03 09:00:00.000', 'dev_002', 'view', 0.0, 'FR'),
    ('2026-05-03 09:30:00.000', 'dev_002', 'click', 4.2, 'FR'),
    ('2026-05-03 09:45:00.000', 'dev_002', 'purchase', 99.99, 'FR'),
    ('2026-05-03 10:00:00.000', 'dev_003', 'view', 0.0, 'US'),
    ('2026-05-03 10:30:00.000', 'dev_003', 'click', 8.8, 'US'),
    ('2026-05-03 10:45:00.000', 'dev_003', 'purchase', 229.99, 'US'),
    ('2026-05-03 11:00:00.000', 'dev_004', 'view', 0.0, 'ES'),
    ('2026-05-03 11:30:00.000', 'dev_005', 'view', 0.0, 'MA'),
    ('2026-05-03 11:45:00.000', 'dev_005', 'click', 15.5, 'MA'),
    ('2026-05-03 12:00:00.000', 'dev_005', 'purchase', 499.99, 'MA'),
    ('2026-05-03 13:00:00.000', 'dev_006', 'view', 0.0, 'DE'),
    ('2026-05-03 13:30:00.000', 'dev_006', 'click', 3.0, 'DE'),
    ('2026-05-03 14:00:00.000', 'dev_007', 'view', 0.0, 'FR'),
    ('2026-05-03 14:30:00.000', 'dev_008', 'view', 0.0, 'MA'),
    ('2026-05-03 15:00:00.000', 'dev_009', 'view', 0.0, 'US'),
    ('2026-05-03 15:30:00.000', 'dev_010', 'view', 0.0, 'ES'),
    ('2026-05-03 16:00:00.000', 'dev_010', 'click', 6.6, 'ES');

-- Jour 7 : 4 mai 2026
INSERT INTO mock_events (timestamp, device_id, event_type, value, country) VALUES
    ('2026-05-04 07:00:00.000', 'dev_011', 'view', 0.0, 'MA'),
    ('2026-05-04 07:15:00.000', 'dev_011', 'click', 10.5, 'MA'),
    ('2026-05-04 07:30:00.000', 'dev_011', 'purchase', 349.99, 'MA'),
    ('2026-05-04 08:00:00.000', 'dev_012', 'view', 0.0, 'FR'),
    ('2026-05-04 08:30:00.000', 'dev_012', 'click', 7.3, 'FR'),
    ('2026-05-04 08:45:00.000', 'dev_012', 'purchase', 159.99, 'FR'),
    ('2026-05-04 09:00:00.000', 'dev_013', 'view', 0.0, 'US'),
    ('2026-05-04 09:30:00.000', 'dev_014', 'view', 0.0, 'DE'),
    ('2026-05-04 09:45:00.000', 'dev_014', 'click', 12.0, 'DE'),
    ('2026-05-04 09:50:00.000', 'dev_014', 'purchase', 289.99, 'DE'),
    ('2026-05-04 10:00:00.000', 'dev_015', 'view', 0.0, 'ES'),
    ('2026-05-04 10:30:00.000', 'dev_015', 'click', 5.5, 'ES'),
    ('2026-05-04 11:00:00.000', 'dev_016', 'view', 0.0, 'MA'),
    ('2026-05-04 11:30:00.000', 'dev_016', 'click', 9.0, 'MA'),
    ('2026-05-04 11:45:00.000', 'dev_016', 'purchase', 199.99, 'MA'),
    ('2026-05-04 12:00:00.000', 'dev_017', 'view', 0.0, 'FR'),
    ('2026-05-04 12:30:00.000', 'dev_018', 'view', 0.0, 'US'),
    ('2026-05-04 13:00:00.000', 'dev_019', 'view', 0.0, 'MA'),
    ('2026-05-04 13:30:00.000', 'dev_020', 'view', 0.0, 'DE');

-- ══════════════════════════════════════════════════════════════
-- AGRÉGATION : Remplissage de la table hourly_metrics
-- ══════════════════════════════════════════════════════════════
INSERT INTO hourly_metrics
SELECT
    toStartOfHour(timestamp) AS hour,
    country,
    CAST(event_type AS String) AS event_type,
    count() AS total_events,
    sum(value) AS total_value,
    uniqExact(device_id) AS unique_devices
FROM mock_events
GROUP BY hour, country, event_type;

-- ══════════════════════════════════════════════════════════════
-- AGRÉGATION : Remplissage de la table daily_kpi
-- ══════════════════════════════════════════════════════════════
INSERT INTO daily_kpi
SELECT
    date,
    total_revenue,
    total_clicks,
    total_views,
    total_purchases,
    if(total_views > 0, total_purchases / total_views * 100, 0) AS conversion_rate,
    if(total_purchases > 0, total_revenue / total_purchases, 0) AS avg_order_value,
    active_devices,
    'MA' AS top_country
FROM (
    SELECT
        toDate(timestamp) AS date,
        sumIf(value, event_type = 'purchase') AS total_revenue,
        countIf(event_type = 'click') AS total_clicks,
        countIf(event_type = 'view') AS total_views,
        countIf(event_type = 'purchase') AS total_purchases,
        uniqExact(device_id) AS active_devices
    FROM mock_events
    GROUP BY date
);
