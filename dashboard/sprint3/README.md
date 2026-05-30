# SE5 — Sprint 3 : Connexion aux données réelles

Le Sprint 3 branche ClickHouse et Grafana sur les **données réelles** produites
par SE2, SE3 et SE4, en remplacement des données mock du Sprint 2.

## Architecture S3

```
SE2 (Kafka réel)  ──►  ClickHouse (Kafka Engine)  ──►  Grafana
  topic_business           businesses_live
  topic_reviews            reviews_live

SE4 (Flink)       ──►  ClickHouse                 ──►  Grafana
  yelp-negative-reviews    negative_alerts_live

SE3 (Parquets)    ──►  ClickHouse (INSERT batch)  ──►  Grafana
  clean_S2.parquet         business_enriched
  agg_*.parquet            axe1_*, axe2_*, axe3_*
```

## Fichiers

| Fichier | Rôle | Quand l'utiliser |
|---|---|---|
| `setup-s3.sh` | Crée les tables batch + vérifie les connexions | Une fois, au démarrage S3 |
| `init-batch-tables.sql` | DDL des 8 tables batch ClickHouse | Appelé par `setup-s3.sh` |
| `import-parquets.sh` | Importe les Parquets SE3 dans ClickHouse | Quand SE3 livre |
| `check-connections.sh` | Vérifie Kafka SE2/SE4 + tables ClickHouse | À tout moment |

## Lancer le Sprint 3

Pré-requis : Sprint 2 déjà démarré (`bash dashboard/sprint2/setup-s2.sh`).

```bash
# 1. Créer les tables batch et vérifier les connexions
bash dashboard/sprint3/setup-s3.sh

# 2. Quand SE3 livre les fichiers Parquet
bash dashboard/sprint3/import-parquets.sh /chemin/vers/parquets/SE3

# 3. Vérifier l'état à tout moment
bash dashboard/sprint3/check-connections.sh
```

## Dépendances

| Tâche | Dépend de | Action SE5 quand livré |
|---|---|---|
| businesses + reviews live | SE2 broker sur `bigdata-net` | Automatique (MV active) |
| alertes Flink | SE4 job Flink sur `topic_reviews` | Automatique (MV active) |
| import batch | SE3 Parquets sur dataset réel | `bash import-parquets.sh` |

## Livrables Sprint 3

- Tables batch créées et prêtes à recevoir les données réelles.
- Script d'import Parquets SE3 prêt à exécuter en une commande.
- Script de validation des connexions SE2/SE3/SE4.
- Pipeline end-to-end opérationnel dès que SE2/SE3/SE4 livrent.
