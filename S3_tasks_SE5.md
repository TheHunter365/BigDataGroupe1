# SE5 — Tâches S3 : Connexion au Vrai Cluster & Flux Réels

**Objectif S3** : Brancher ClickHouse et Grafana sur les données réelles produites par les autres équipes.
**Livrable S3** : Dashboard connecté aux données réelles — pipeline end-to-end opérationnel.

---

## M1 — Data Engineer (ClickHouse)

### Tâche 1 : Brancher ClickHouse sur les topics réels de Flink (SE4)

| | |
|---|---|
| **Action** | Mettre à jour `dashboard/S2/M1/kafka-pipeline.sql` pour remplacer les topics mock par les vrais topics produits par SE4 (`yelp-negative-reviews`, métriques live) |
| **Attend SE4 ?** | **OUI** — SE4 doit avoir ses jobs Flink opérationnels et ses topics Kafka finaux configurés avant de pouvoir faire ce branchement |
| **Attend SE2 ?** | **OUI** — le broker Kafka de SE2 doit être accessible sur le vrai cluster |

### Tâche 2 : Importer les Parquets réels de SE3 dans ClickHouse

| | |
|---|---|
| **Action** | Charger les fichiers Parquet produits par SE3 dans les tables batch ClickHouse (`axe1_monthly_running_avg`, `axe2_avg_by_city`, `axe3_user_enriched`, `axe4_checkins_heatmap`, etc.) via `clickhouse-client` |
| **Commande type** | `clickhouse-client --query "INSERT INTO bigdata_db.axe1_monthly_running_avg FORMAT Parquet" < axe1_monthly_running_avg.parquet` |
| **Attend SE3 ?** | **OUI** — SE3 doit avoir produit et livré les fichiers Parquet finaux (au minimum les Parquets Axe 1 et Axe 2) pour débloquer M1 sur cette tâche |

### Tâche 3 : Optimiser les clés de tri ORDER BY

| | |
|---|---|
| **Action** | Revoir et ajuster les clauses `ORDER BY` dans les DDL ClickHouse (`init-db.sql`) pour accélérer les requêtes Grafana sur la volumétrie réelle (~7M reviews au lieu de 140 lignes mock) |
| **Attend une autre SE ?** | **NON** — peut être fait immédiatement, sans attendre personne |

---

## M2 — Data Analyst (Grafana)

### Tâche 1 : Ajouter des variables interactives

| | |
|---|---|
| **Action** | Ajouter des listes déroulantes dans les dashboards Grafana pour filtrer dynamiquement par catégorie, ville ou état (modifier `dashboard/S2/M1/dashboards/se5-yelp-dashboard.json`) |
| **Attend une autre SE ?** | **NON** — peut être développé et testé sur les données mock existantes, puis branché sur les données réelles une fois M1 a terminé l'import |

### Tâche 2 : Configurer les alertes Grafana

| | |
|---|---|
| **Action** | Créer des règles d'alerte sur seuil critique (ex : note moyenne d'un business passe sous 2.0, pic d'avis négatifs) avec envoi de notification |
| **Attend une autre SE ?** | **PARTIELLEMENT** — la structure des alertes peut être configurée maintenant, mais les seuils réels ne peuvent être validés qu'une fois M1 a importé les vraies données (volumes réels différents du mock) |

### Tâche 3 : Ajuster l'échelle des graphiques pour la volumétrie réelle

| | |
|---|---|
| **Action** | Adapter les axes, limites et performances de rendu des panels existants pour supporter ~7M reviews au lieu de 140 lignes mock |
| **Attend M1 ?** | **OUI** — M1 doit d'abord importer les vraies données (Tâche 2) pour que M2 puisse tester et ajuster l'échelle réelle |

---

## Résumé des dépendances

```
SE2 (Kafka broker) ──────────────────────────────► M1 Tâche 1
SE4 (Flink topics finaux) ───────────────────────► M1 Tâche 1
SE3 (Parquets finaux) ───────────────────────────► M1 Tâche 2
                                                         │
M1 Tâche 2 (données réelles importées) ─────────────────► M2 Tâche 3
M1 Tâche 2 (données réelles importées) ─────────────────► M2 Tâche 2 (validation seuils)

M1 Tâche 3   ─── indépendant ──► peut démarrer maintenant
M2 Tâche 1   ─── indépendant ──► peut démarrer maintenant
```

## Ce qui peut démarrer immédiatement (sans attendre)

- **M1** : Optimisation des `ORDER BY` (Tâche 3)
- **M2** : Variables interactives Grafana (Tâche 1) — développement sur mock, branchement sur réel plus tard
- **M2** : Structure des alertes Grafana (Tâche 2) — configuration en attente de validation sur données réelles
