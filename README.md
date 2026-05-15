# BigDataGroupe1 — Projet Big Data (synthèse)

Ce dépôt contient plusieurs sous-projets et environnements pour un projet pédagogique Big Data (Yelp mock data).

Structure générale

- `SE1-Infrastructure/` — Déploiement Docker Compose d'un cluster Hadoop/YARN (HDFS, ResourceManager, DataNodes). Voir [SE1-Infrastructure/README.md](SE1-Infrastructure/README.md).
- `SE2/` — Simulation d'ingestion : scripts et fichiers mock pour produire des données Yelp (producteur offline). Voir [SE2/README.md](SE2/README.md).
- `SE3-etl-batch/` — Sprint 1 : scripts de nettoyage local (Pandas) et jeux de données mock. Voir [SE3-etl-batch/README.md](SE3-etl-batch/README.md).
- `SE4/` — Intégration Flink + Kafka ; contient un `Dockerfile` et un `docker-compose.yml` pour lancer Kafka, Flink et des conteneurs producteurs/consommateurs. Voir [SE4/README.md](SE4/README.md).

- `docker-compose.yml` (racine) — orchestration globale (optionnelle selon configuration locale).

But

But : fournir un environnement reproductible pour tester des pipelines d'ingestion (mock), des traitements batch locaux et une infrastructure distribuée (Hadoop / Flink + Kafka) à des fins pédagogiques.

Prérequis globaux

- Docker & docker-compose installés pour lancer les environnements `SE1` et `SE4`.
- Python 3.8+ pour exécuter les scripts locaux dans `SE2` et `SE3`