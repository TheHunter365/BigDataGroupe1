-- ══════════════════════════════════════════════════════════════
-- SE5 S2 — Requêtes analytiques Grafana  (M2 — Data Analyst)
-- Source : tables alimentées par le pipeline Kafka de M1
--          (businesses_live · reviews_live · negative_alerts_live)
-- Toutes les requêtes ciblent la base bigdata_db.
-- ══════════════════════════════════════════════════════════════

USE bigdata_db;

-- ──────────────────────────────────────────────────────────────
-- KPIs CLÉS (panels « stat »)
-- ──────────────────────────────────────────────────────────────

-- KPI 1 — Total Businesses
SELECT count() AS "Businesses" FROM businesses_live;

-- KPI 2 — Total Avis
SELECT count() AS "Avis" FROM reviews_live;

-- KPI 3 — Note moyenne des businesses
SELECT round(avg(stars), 2) AS "Note" FROM businesses_live;

-- KPI 4 — Note moyenne des avis
SELECT round(avg(stars), 2) AS "Note" FROM reviews_live;

-- KPI 5 — Taux d'ouverture (% businesses ouverts)
SELECT round(100 * countIf(is_open = 1) / count(), 1) AS "Ouverts" FROM businesses_live;

-- KPI 6 — Nombre d'alertes négatives (SE4)
SELECT count() AS "Alertes" FROM negative_alerts_live;


-- ──────────────────────────────────────────────────────────────
-- TENDANCES TEMPORELLES (panels « time series »)
-- $__timeFilter() = macro Grafana liée au sélecteur de temps
-- ──────────────────────────────────────────────────────────────

-- Panel 7 — Volume d'avis par mois
SELECT toDateTime(toStartOfMonth(date)) AS time,
       count()                          AS "Nombre d'avis"
FROM reviews_live
WHERE $__timeFilter(date)
GROUP BY time
ORDER BY time;

-- Panel 8 — Note moyenne par mois
SELECT toDateTime(toStartOfMonth(date)) AS time,
       round(avg(stars), 2)             AS "Note moyenne"
FROM reviews_live
WHERE $__timeFilter(date)
GROUP BY time
ORDER BY time;


-- ──────────────────────────────────────────────────────────────
-- ANALYSE DES NOTES
-- ──────────────────────────────────────────────────────────────

-- Panel 9 — Répartition des notes (pie / donut)
SELECT concat(toString(toUInt8(stars)), ' etoile(s)') AS note,
       count()                                        AS total
FROM reviews_live
GROUP BY note
ORDER BY note;

-- Panel 10 — Note moyenne par ville (bar chart, top 15)
SELECT city                  AS "Ville",
       round(avg(stars), 2)  AS "Note moyenne"
FROM businesses_live
GROUP BY city
ORDER BY "Note moyenne" DESC
LIMIT 15;

-- Panel 11 — Businesses ouverts vs fermés (pie)
SELECT if(is_open = 1, 'Ouvert', 'Ferme') AS statut,
       count()                            AS total
FROM businesses_live
GROUP BY statut;


-- ──────────────────────────────────────────────────────────────
-- CLASSEMENTS & GÉOGRAPHIE
-- ──────────────────────────────────────────────────────────────

-- Panel 12 — Top 10 businesses par nombre d'avis
SELECT name         AS "Business",
       review_count AS "Nombre d'avis"
FROM businesses_live
ORDER BY review_count DESC
LIMIT 10;

-- Panel 13 — Top 10 villes par nombre de businesses
SELECT city    AS "Ville",
       count() AS "Businesses"
FROM businesses_live
GROUP BY city
ORDER BY "Businesses" DESC
LIMIT 10;


-- ──────────────────────────────────────────────────────────────
-- DÉTAILS & ALERTES (panels « table »)
-- ──────────────────────────────────────────────────────────────

-- Panel 14 — Derniers avis reçus
SELECT date                   AS "Date avis",
       business_id            AS "Business",
       stars                  AS "Note",
       useful                 AS "Utile",
       funny                  AS "Drole",
       cool                   AS "Cool",
       substring(text, 1, 70) AS "Extrait"
FROM reviews_live
ORDER BY ingested_at DESC
LIMIT 50;

-- Panel 15 — Alertes négatives temps réel (SE4)
SELECT received_at AS "Recu le",
       business_id AS "Business",
       stars       AS "Note",
       alert       AS "Alerte"
FROM negative_alerts_live
ORDER BY received_at DESC
LIMIT 50;


-- ──────────────────────────────────────────────────────────────
-- VÉRIFICATION RAPIDE — les données arrivent-elles ?
-- ──────────────────────────────────────────────────────────────
SELECT 'businesses_live'      AS table, count() AS lignes FROM businesses_live
UNION ALL
SELECT 'reviews_live'         AS table, count() AS lignes FROM reviews_live
UNION ALL
SELECT 'negative_alerts_live' AS table, count() AS lignes FROM negative_alerts_live;
