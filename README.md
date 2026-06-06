# BigDataGroupe1 — Pipeline Lambda Architecture · Yelp Dataset

Projet Big Data — Architecture Lambda complète avec ingestion Kafka, traitement batch Spark, streaming Flink et visualisation Grafana/ClickHouse.

## Prérequis

- Docker Desktop (>= 24.0) avec au moins **8 Go de RAM alloués**
- Docker Compose v2
- Git

## Lancement en moins de 10 minutes

### 1. Cloner le dépôt

```bash
git clone <url-du-repo>
cd BigDataGroupe1
```

### 2. Lancer tous les services

```bash
docker compose up -d
```

Cette commande démarre les 17 services : HDFS (NameNode + DataNodes), YARN, Kafka, Flink, Spark, ClickHouse et Grafana.

### 3. Attendre que les services soient prêts (~3-4 min)

```bash
docker compose ps
```

Vérifier que `namenode`, `kafka`, `clickhouse` et `grafana` sont `healthy`.

### 4. Vérifier le pipeline

| Service | URL | Identifiants |
|---|---|---|
| Grafana Dashboard | http://localhost:3001 | admin / admin123 |
| Kafka UI | http://localhost:8080 | — |
| HDFS NameNode UI | http://localhost:9870 | — |
| Spark History | http://localhost:18080 | — |
| YARN ResourceManager | http://localhost:8088 | — |

Le dashboard Grafana est accessible directement sur http://localhost:3001

## Structure du projet

```
BigDataGroupe1/
├── docker-compose.yml        # Orchestration complète (17 services)
├── hadoop.env                # Configuration Hadoop/YARN
├── data/sample/              # Échantillon Yelp (0.15% — ~219 businesses)
│   ├── yelp_academic_dataset_business.json
│   ├── yelp_academic_dataset_review.json
│   ├── yelp_academic_dataset_user.json
│   ├── yelp_academic_dataset_tip.json
│   └── yelp_academic_dataset_checkin.json
├── ingestion/                # SE2 — Producteur Kafka (Python)
│   ├── kafka-producer.py
│   ├── Dockerfile
│   └── README.md
├── etl/                      # SE3 — Job Spark ETL (batch)
│   ├── scripts/spark_etl.py
│   └── README.md
├── streaming/                # SE4 — Pipeline Flink temps réel
│   ├── flink.py
│   ├── Dockerfile
│   └── README.md
├── dashboard/                # SE5 — ClickHouse + Grafana
│   ├── docker-compose.yml
│   ├── init-db.sql
│   ├── clickhouse-hdfs-init.sql
│   ├── dashboards/
│   └── provisioning/
└── docs/                     # Schémas d'architecture + rapports
```

## Architecture Lambda

```
Yelp Dataset (data/sample/)
        │
        ▼
  Kafka Producer (Python)
        │
        ├──────────────────────────────────────┐
        ▼                                      ▼
  Apache HDFS                         Apache Flink
  (Data Lake)                         (Streaming)
        │                                      │
        ▼                                      ▼
  Apache Spark                        ClickHouse
  (Batch ETL)                         (negative_alerts_live)
        │                                      │
        ▼                                      ▼
  ClickHouse ◄──────────────────────── Grafana Dashboard
  (axe1..4 tables)
```

**Couche Batch** : HDFS → Spark ETL (4 axes analytiques + PageRank) → ClickHouse → Grafana

**Couche Streaming** : Kafka → Flink (analyse de sentiment) → ClickHouse → Grafana

## Données

Le dossier `data/sample/` contient un échantillon reproductible du dataset Yelp académique :
- **219 businesses** (0.15% du dataset complet)
- **15 450 reviews** associées
- **14 610 utilisateurs** associés
- Généré avec `sample_dataset.py` — `SAMPLE_RATE=0.0015`, `random.seed(42)`

## Axes analytiques (Spark ETL)

| Axe | Description |
|---|---|
| Axe 1 — Réputation | Attributs businesses, running average, taux de fermeture |
| Axe 2 — Géographie | Concurrence 500m (Haversine), disparités par ville/état |
| Axe 3 — Réseau Social | PageRank utilisateurs, comparaison Elite vs Standard |
| Axe 4 — Temporalité | Heatmap checkins, tendances mensuelles, patterns horaires |
