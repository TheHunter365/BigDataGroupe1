# SE5 — Semaine 2 : Datastore & Visualisation temps réel

Module **SE5** du projet Big Data (Groupe 1, SNIA S2). La Semaine 2 fait évoluer
l'environnement statique de la Semaine 1 vers un **pipeline d'ingestion temps réel**
des données Yelp (mock) jusqu'à un dashboard analytique.

## Architecture

```
mock-producer.py  -->  Kafka (KRaft)  -->  ClickHouse  -->  Grafana
   (producteur)        (bus de msg)     (Kafka Engine +    (15 panels)
                                         vues matérialisées)
```

## Structure du dossier

| Dossier | Rôle | Auteur |
|---------|------|--------|
| `M1/` | Environnement d'ingestion : `docker-compose.yml`, `kafka-pipeline.sql`, `mock-producer.py`, provisioning Grafana, dashboards provisionnés | M1 — Data Engineer |
| `M2/` | Volet analyse : `queries.sql` (requêtes des KPIs) et documentation | M2 — Data Analyst |

Le dashboard `M1/dashboards/se5-yelp-dashboard.json` (15 panels — KPIs, séries
temporelles, répartitions, classements, tables) est le livrable de conception **M2** ;
il est provisionné automatiquement par la stack M1.

## Lancer l'environnement

Pré-requis : Docker et Docker Compose.

```bash
cd M1
docker-compose up -d
```

Services exposés :

| Service | URL / port |
|---------|-----------|
| Grafana | http://localhost:3001 |
| ClickHouse | http://localhost:8123 |
| Kafka | localhost:9092 |

Une fois la stack démarrée, lancer le producteur pour alimenter le pipeline :

```bash
pip install -r M1/requirements.txt
python M1/mock-producer.py
```

Le dashboard est ensuite consultable dans Grafana (dashboard *SE5 — Yelp*).

## Livrables Semaine 2

- Pipeline Kafka → ClickHouse opérationnel (Kafka Table Engine + vues matérialisées).
- Dashboard Grafana 15 panels alimenté en temps réel.
- `M2/queries.sql` : requêtes SQL documentées des indicateurs.
