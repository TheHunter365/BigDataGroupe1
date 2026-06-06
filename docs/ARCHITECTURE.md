# Architecture du Système — BigDataGroupe1

## Vue d'ensemble : Lambda Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SOURCES DE DONNÉES                              │
│                    data/sample/ (Yelp Dataset)                          │
│          219 businesses · 15 450 reviews · 14 610 utilisateurs          │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    COUCHE INGESTION (SE2)                               │
│                  Python Kafka Producer                                  │
│     Batch: 131 072 bytes · linger: 20ms · compression: lz4             │
│                   Topic: topic_reviews                                  │
└──────────────┬──────────────────────────────────────┬───────────────────┘
               │                                      │
               ▼                                      ▼
┌──────────────────────────┐          ┌───────────────────────────────────┐
│   COUCHE BATCH (SE3)     │          │     COUCHE STREAMING (SE4)        │
│                          │          │                                   │
│   Apache HDFS 3.3        │          │   Apache Flink 1.20 (PyFlink)    │
│   (NameNode + 2 DataNode)│          │   Analyse de sentiment (règles)   │
│   YARN ResourceManager   │          │   Fenêtres 10s · checkpoint 5s    │
│         │                │          │   Filtre: avis négatifs (≤2★)    │
│         ▼                │          │         │                         │
│   Apache Spark 3.2       │          │         ▼                         │
│   (ETL + PageRank)       │          │   Topic: yelp-negative-reviews    │
│   4 axes analytiques     │          └───────────────┬───────────────────┘
│   Parquet → HDFS         │                          │
└──────────────┬───────────┘                          │
               │                                      │
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  COUCHE SERVING — Apache ClickHouse                     │
│                                                                         │
│  Tables Batch (MergeTree):        Tables Streaming:                     │
│  · axe1_business_attributes       · negative_alerts_live (Kafka Engine) │
│  · axe1_monthly_running_avg       · reviews_live                        │
│  · axe1_closure_rate              · businesses_live                     │
│  · axe2_avg_by_city                                                     │
│  · business_enriched              HDFS Engine Tables (_hdfs):           │
│  · axe3_user_enriched             · Lecture directe Parquet HDFS        │
│  · axe3_user_review_enriched                                            │
│  · axe4_stars_by_hour/dow/trend                                         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  COUCHE VISUALISATION — Grafana 13                      │
│                  http://localhost:3001 (admin/admin123)                 │
│                                                                         │
│  Dashboard SE5 — 32 panels:                                             │
│  · KPIs Live (Kafka → ClickHouse temps réel)                           │
│  · Carte géographique des businesses (geomap)                           │
│  · Analyse réputation, géographie, réseau social, temporalité          │
│  · Alertes négatives Flink en temps réel                               │
└─────────────────────────────────────────────────────────────────────────┘
```

## Stack Technologique

| Technologie | Version | Rôle |
|---|---|---|
| Apache Hadoop (HDFS + YARN) | 3.3 | Data Lake distribué |
| Apache Kafka (KRaft) | 3.x | Bus de messagerie temps réel |
| Apache Spark | 3.2 | ETL batch + PageRank |
| Apache Flink (PyFlink) | 1.20 | Streaming temps réel |
| Apache ClickHouse | 26.5 | Base OLAP serving layer |
| Grafana | 13.0 | Visualisation BI |
| Docker Compose | v2 | Orchestration (17 services) |

## Services Docker

| Service | Port | Rôle |
|---|---|---|
| namenode | 9870, 9000 | HDFS NameNode |
| datanode1, datanode2 | — | HDFS DataNodes |
| resourcemanager | 8088 | YARN ResourceManager |
| nodemanager1, nodemanager2 | — | YARN NodeManagers |
| historyserver | 19888 | Hadoop History Server |
| kafka | 9092 | Kafka Broker (KRaft) |
| kafka-ui | 8080 | Interface Kafka |
| python-producer | — | Producteur Kafka (SE2) |
| spark-submit-job | — | Job ETL Spark (SE3) |
| spark-history | 18080 | Spark History Server |
| jobmanager | 8081 | Flink Job Manager |
| taskmanager | — | Flink Task Manager |
| python-flink | — | Job PyFlink (SE4) |
| clickhouse | 8123, 9000 | Base de données OLAP |
| grafana | 3001 | Dashboard BI (SE5) |

## Flux de données détaillé

```
data/sample/*.json
    └──> kafka-producer.py ──> Kafka (topic_reviews)
              │
              ├──> [HDFS] hdfs-init ──> /data/raw/
              │         └──> spark-submit-job ──> /data/processed/ (Parquet)
              │                   └──> ClickHouse (axe1..4 MergeTree)
              │
              └──> [Flink] python-flink
                        └──> Kafka (yelp-negative-reviews)
                                  └──> ClickHouse Kafka Engine
                                            └──> negative_alerts_live (MV)
```
