# BigDataGroupe1 — Projet Big Data (synthèse)

Ce dépôt contient plusieurs sous-projets et environnements pour le projet 2 Big Data (Yelp mock data).

Structure générale

- `ingestion/` — Scripts d'ingestion (mock + Kafka). Voir [ingestion/README.md](ingestion/README.md) et [ingestion/README-kafka.md](ingestion/README-kafka.md).
- `etl/` — Jobs de transformation batch (Pandas). Voir [etl/README.md](etl/README.md).
- `streaming/` — Pipeline temps réel (Flink + Kafka). Voir [streaming/README.md](streaming/README.md).
- `dashboard/` — Configs BI / notebooks. Voir [dashboard/README.md](dashboard/README.md).
- `data/sample/` — Données mock (CSV/JSON).
- `docs/` — Schémas d’architecture et rapports (incl. documentation d’infrastructure).

- `docker-compose.yml` (racine) — orchestration globale (optionnelle selon configuration locale).

But

But : fournir un environnement reproductible pour tester des pipelines d'ingestion (mock), des traitements batch locaux et une infrastructure distribuée (Hadoop / Flink + Kafka) à des fins pédagogiques.

Prérequis globaux

- Docker & docker-compose installés pour lancer les environnements d’ingestion et streaming.
- Python 3.8+ pour exécuter les scripts locaux d’ingestion et d’ETL.
