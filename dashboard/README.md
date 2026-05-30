# SE5 — Dashboard · Vue d'ensemble

**Sous-Équipe 5 — Visualisation & Dashboard** · Groupe 1 · SNIA S2 · FSR Rabat
Technologies : ClickHouse · Grafana · Kafka · Python

## Structure

```
dashboard/
├── sprint1/    Setup statique ClickHouse + Grafana (sans Kafka)
├── sprint2/    Pipeline Kafka → ClickHouse → Grafana sur données mock
└── sprint3/    Connexion aux données réelles SE2 / SE3 / SE4
```

## Progression par sprint

| Sprint | Livrable SE5 | Statut |
|---|---|---|
| S1 | Dashboard esquissé sur données mock statiques | ✅ |
| S2 | Dashboard MOCK terminé — pipeline Kafka opérationnel | ✅ |
| S3 | Tables batch créées, scripts prêts — en attente SE2/SE3/SE4 | 🔄 |
| S4 | Dashboard final + insights documentés | ⬜ |

## Lancer l'environnement

### Sprint 1 (statique — autonome)

```bash
cd dashboard/sprint1
docker-compose up -d
# Grafana : http://localhost:3001 (admin / admin123)
```

### Sprint 2 (mock — autonome)

```bash
cd dashboard/sprint2
bash setup-s2.sh
pip install -r requirements.txt
python3 mock-producer.py
# Grafana : http://localhost:3001  (admin / admin123)
```

### Sprint 3 (données réelles — après setup-s2.sh)

```bash
# Créer les tables batch
bash dashboard/sprint3/setup-s3.sh

# Quand SE3 livre les Parquets
bash dashboard/sprint3/import-parquets.sh /chemin/parquets

# Vérifier les connexions SE2 / SE4
bash dashboard/sprint3/check-connections.sh
```

## Architecture globale (S3)

```
             mock-producer.py (S2)
                    │
SE2 (Kafka) ───────┼──► ClickHouse (Kafka Engine + MV) ──► Grafana
SE4 (Flink) ───────┘         │
SE3 (Parquets) ──────────────┘
```

## Documentation par sprint

- [sprint1/README.md](sprint1/README.md) — Setup statique (ClickHouse + Grafana)
- [sprint2/README.md](sprint2/README.md) — Pipeline Kafka temps réel + Dashboard 15 panels
- [sprint3/README.md](sprint3/README.md) — Connexion données réelles + scripts d'import
- [sprint2/GUIDE_M2.md](sprint2/GUIDE_M2.md) — Guide Data Analyst (description détaillée des 15 panels)

## Prérequis globaux

- Docker et Docker Compose
- Python 3.8+ (pour mock-producer.py et import-parquets.sh)
- bash ou Git Bash (Windows)

## Dépendances entre sprints

```
Sprint 1 (S1) ──► Sprint 2 (S2) ──► Sprint 3 (S3) ──► Sprint 4 (S4)
(statique)      (mock temps réel)  (données réelles)  (final)
```

- **S1 → S2** : S2 est un upgrade de S1 avec Kafka. Peut coexister.
- **S2 → S3** : S3 ajoute les connexions réelles. Nécessite que S2 ait démarré (`setup-s2.sh`).
- **S3 → S4** : S4 finalise le dashboard et ajoute les insights métier.

## Principales tâches par sprint

### Sprint 1 (M1 + M2)
- ✅ Setup ClickHouse + Grafana
- ✅ Dashboard avec données statiques (6 KPIs)

### Sprint 2 (M1 + M2)
- ✅ Pipeline Kafka → ClickHouse
- ✅ Dashboard complet (15 panels)
- ✅ Requêtes SQL documentées

### Sprint 3 (M1 + M2)
- [ ] Connexion Kafka réel (SE2)
- [ ] Alertes Flink (SE4)
- [ ] Import Parquets SE3
- [ ] Variables interactives Grafana
- [ ] Alertes Grafana

### Sprint 4 (M2)
- [ ] Peaufinage UX dashboard
- [ ] Documentation insights métier
- [ ] Vidéo de démo
- [ ] Résumés techniques

## Fichiers clés

| Fichier | Sprint | Rôle |
|---|---|---|
| `init-db.sql` | S1 | DDL tables + données statiques |
| `docker-compose.yml` | S1, S2 | Orchestration services |
| `setup-s2.sh` | S2 | Démarrage complet S2 (services + topics + pipelines) |
| `kafka-pipeline.sql` | S2 | Pipelines Kafka Engine + Materialized Views |
| `mock-producer.py` | S2 | Générateur données mock |
| `se5-yelp-dashboard.json` | S2, S3 | Dashboard 15 panels |
| `queries.sql` | S2 | Requêtes SQL documentées |
| `setup-s3.sh` | S3 | Création tables batch S3 |
| `import-parquets.sh` | S3 | Import Parquets SE3 |
| `check-connections.sh` | S3 | Validation connexions SE2/SE3/SE4 |

## Pour plus d'informations

Voir les README individuels de chaque sprint pour les détails d'installation, d'utilisation et de dépannage.
