# SE5 — Guide M2 (Data Analyst) : Semaine 2

Ce fichier contient tout ce dont M2 a besoin pour créer les panels Grafana en S2.

---

## 1. Démarrage de l'environnement

```bash
# Étape 1 — Lancer le setup complet (Kafka + ClickHouse + Grafana)
bash setup-s2.sh

# Étape 2 — Installer les dépendances Python
pip install -r requirements.txt

# Étape 3 — Lancer le générateur de données mock (simule SE2 + SE4)
python3 mock-producer.py

# Options utiles du mock producer :
python3 mock-producer.py --businesses 500 --reviews 300   # plus de données
python3 mock-producer.py --rate 5                         # 5 reviews/seconde
python3 mock-producer.py --only reviews                   # reviews seulement

# Étape 4 — Ouvrir Grafana
# URL      : http://localhost:3001
# Login    : admin
# Password : admin123
```

---

## 2. Tables disponibles dans ClickHouse (`bigdata_db`)

### `businesses_live` — Businesses Yelp en streaming (SE2)

| Colonne | Type | Description |
|---|---|---|
| `business_id` | String | Identifiant unique |
| `name` | String | Nom du business |
| `city` | String | Ville |
| `state` | String | État / Province |
| `stars` | Float32 | Note (0.0 – 5.0) |
| `review_count` | Int32 | Nombre d'avis |
| `is_open` | UInt8 | 1 = ouvert, 0 = fermé |
| `categories` | String | Ex: "Restaurants, Food" |
| `ingested_at` | DateTime | Horodatage d'ingestion |

---

### `reviews_live` — Reviews Yelp en streaming (SE2)

| Colonne | Type | Description |
|---|---|---|
| `review_id` | String | Identifiant unique |
| `user_id` | String | Utilisateur |
| `business_id` | String | Référence au business |
| `stars` | Float32 | Note (1.0 – 5.0) |
| `useful` | UInt32 | Votes "utile" |
| `funny` | UInt32 | Votes "drôle" |
| `cool` | UInt32 | Votes "cool" |
| `text` | String | Texte de l'avis |
| `date` | DateTime | Date de l'avis |
| `ingested_at` | DateTime | Horodatage d'ingestion |

---

### `negative_alerts_live` — Alertes Flink (SE4)

| Colonne | Type | Description |
|---|---|---|
| `business_id` | String | Business concerné |
| `stars` | Float32 | Note négative détectée |
| `alert` | String | Message d'alerte |
| `received_at` | DateTime | Horodatage de réception |

---

### `mock_events` — Données mock statiques (S1, déjà remplies)

| Colonne | Type | Description |
|---|---|---|
| `timestamp` | DateTime64 | Horodatage de l'événement |
| `device_id` | String | Appareil |
| `event_type` | String | `view`, `click`, `purchase` |
| `value` | Float64 | Valeur (0 si view) |
| `country` | String | Pays (MA, FR, US…) |

---

### `hourly_metrics` — Métriques agrégées par heure (S1)

| Colonne | Type | Description |
|---|---|---|
| `hour` | DateTime | Heure arrondie |
| `country` | String | Pays |
| `event_type` | String | Type d'événement |
| `total_events` | UInt64 | Nombre d'événements |
| `total_value` | Float64 | Somme des valeurs |
| `unique_devices` | UInt32 | Appareils uniques |

---

### `daily_kpi` — KPI quotidiens (S1)

| Colonne | Type | Description |
|---|---|---|
| `date` | Date | Jour |
| `total_revenue` | Float64 | Chiffre d'affaires |
| `total_clicks` | UInt64 | Clics totaux |
| `total_views` | UInt64 | Vues totales |
| `total_purchases` | UInt64 | Achats totaux |
| `conversion_rate` | Float64 | Taux de conversion (%) |
| `avg_order_value` | Float64 | Panier moyen |
| `active_devices` | UInt32 | Appareils actifs |

---

## 3. Requêtes SQL suggérées pour Grafana

### Panel : Note moyenne par ville (Bar chart)
```sql
SELECT city, round(avg(stars), 2) AS avg_stars, count() AS nb_businesses
FROM businesses_live
GROUP BY city
ORDER BY avg_stars DESC
LIMIT 15
```

### Panel : Evolution des reviews dans le temps (Time Series)
```sql
SELECT
    toStartOfHour(date) AS time,
    count()             AS nb_reviews,
    round(avg(stars), 2) AS avg_stars
FROM reviews_live
WHERE $__timeFilter(date)
GROUP BY time
ORDER BY time
```

### Panel : Répartition des notes (Pie chart)
```sql
SELECT
    toString(toInt32(stars)) AS note,
    count() AS total
FROM reviews_live
GROUP BY note
ORDER BY note
```

### Panel : Alertes négatives en temps réel (Table)
```sql
SELECT received_at, business_id, stars, alert
FROM negative_alerts_live
ORDER BY received_at DESC
LIMIT 50
```

### Panel : KPI quotidiens - Chiffre d'affaires (Time Series — données S1)
```sql
SELECT date, total_revenue, avg_order_value, conversion_rate
FROM daily_kpi
ORDER BY date
```

### Panel : Flux d'événements mock (Time Series — données S1)
```sql
SELECT
    toStartOfMinute(timestamp) AS time,
    countIf(event_type = 'purchase') AS purchases,
    countIf(event_type = 'click')    AS clicks,
    countIf(event_type = 'view')     AS views
FROM mock_events
WHERE $__timeFilter(timestamp)
GROUP BY time
ORDER BY time
```

### Panel : Top businesses par nombre d'avis (Bar chart)
```sql
SELECT name, city, review_count, stars
FROM businesses_live
ORDER BY review_count DESC
LIMIT 10
```

---

## 4. Vérifier que les données arrivent

```sql
-- Dans ClickHouse (docker exec -it clickhouse-server clickhouse-client)
SELECT count() FROM businesses_live;
SELECT count() FROM reviews_live;
SELECT count() FROM negative_alerts_live;
SELECT count() FROM mock_events;
```

---

## 5. Structure des fichiers fournis

```
S2_M1/
├── docker-compose.yml       → Infrastructure complète (Kafka + ClickHouse + Grafana)
├── setup-s2.sh              → Script de démarrage automatisé
├── kafka-pipeline.sql       → Référence des tables ClickHouse (schemas complets)
├── mock-producer.py         → Simulateur de données ingestion/streaming
├── requirements.txt         → Dépendances Python
├── init-db.sql              → Données mock statiques S1 (auto-chargées)
├── provisioning/            → Configuration automatique Grafana (datasource ClickHouse)
├── dashboards/              → Dashboard JSON de base S1
└── GUIDE_M2.md              → Ce fichier
```
