#  GUIDE COMPLET — Déploiement Pipeline SE3 (Spark ETL sur YARN)

 Projet : BigData — Master SNIA  
 Auteurs : Abbar Amina & Nader Chaimaa

---

##  Prérequis Vérifiés

- Windows 11 Pro (Build 26200)
- RAM: 31.82 GB
- Disque libre: 252 GB


---

##  ÉTAPE 1: Installer Docker Desktop

### 1.1 Télécharger
  https://www.docker.com/products/docker-desktop 

### 1.2 Installer
```powershell
# Double-cliquez sur Docker Desktop Installer.exe
# Acceptez les conditions → Laissez options par défaut → Terminez
# Docker installera WSL2 automatiquement
# Redémarrage recommandé
```

### 1.3 Vérifier l'installation
```powershell
docker --version
docker compose version
```

 Résultat attendu: 
```
Docker version 24.x.x ou supérieur
Docker Compose version 2.x.x ou supérieur
```

---

##  ÉTAPE 2: Structure du Projet

```
C:\Users\lhamz\Desktop\Big Data\BigDataGroupe1\
├── data/                              ← Données JSON (créées )
│   ├── yelp_academic_dataset_business.json
│   ├── yelp_academic_dataset_review.json
│   ├── yelp_academic_dataset_user.json
│   ├── yelp_academic_dataset_tip.json
│   └── yelp_academic_dataset_checkin.json
├── etl/
│   └── scripts/
│       └── spark_etl.py               ← Pipeline ETL (v3)
├── dashboard/                         ← Grafana + ClickHouse
├── docker-compose.yml                 ← Configuration cluster
├── hadoop.env                         ← Config Hadoop/YARN
├── spark-defaults.conf                ← Config Spark
└── ...
```

---
##  ÉTAPE 3: Lancer le Cluster

### 3.1 Ouvrir PowerShell

```powershell
cd "C:\Users\lhamz\Desktop\Big Data\BigDataGroupe1"
```

### 3.2 Vérifier les images Docker

```powershell
docker compose config | findstr "image:"
```

 Cela affichera toutes les images requises. 

### 3.3 Lancer les services SE1 (Hadoop/YARN) + SE3 (Spark)

```powershell
docker compose up -d namenode datanode1 datanode2 resourcemanager nodemanager1 nodemanager2 historyserver hdfs-init spark-submit-job spark-history
```

 Explication: 
- `namenode`: HDFS NameNode (9870)
- `datanode1, datanode2`: HDFS DataNodes
- `resourcemanager`: YARN Resource Manager (8088)
- `nodemanager1, nodemanager2`: YARN NodeManagers
- `historyserver`: Job history (19888)
- `hdfs-init`: Initialise les dossiers HDFS `/data/raw/` et `/data/processed/`
- `spark-submit-job`: Lance le job Spark
- `spark-history`: History Server de Spark

### 3.4 Vérifier le démarrage

```powershell
# Vérifier les logs du container hdfs-init
docker logs hdfs-init -f

# Attendre le message:
# "HDFS initié avec succès" ou similaire
```

 Temps d'attente:  ~30-60 secondes

---

##  ÉTAPE 4: Télécharger les Données dans HDFS

Les données de test sont  automatiquement copiées  par le service `hdfs-init`.

Vérifier avec:
```powershell
docker exec namenode hdfs dfs -ls /data/raw/
```

 Résultat attendu: 
```
drwxr-xr-x   - root supergroup       0 2026-05-30 XX:XX /data/raw/business
drwxr-xr-x   - root supergroup       0 2026-05-30 XX:XX /data/raw/review
drwxr-xr-x   - root supergroup       0 2026-05-30 XX:XX /data/raw/tip
drwxr-xr-x   - root supergroup       0 2026-05-30 XX:XX /data/raw/user
drwxr-xr-x   - root supergroup       0 2026-05-30 XX:XX /data/raw/checkin
```

---

##  ÉTAPE 5: Lancer le Pipeline Spark ETL

```powershell
# Vérifier le job Spark
docker logs spark-submit-job -f
```

 Journaux clés à chercher: 
```
[LOAD] Chargement des datasets...
[TRANSFORM] AXE 1 — Réputation...
[TRANSFORM] AXE 2 — Géographie...
[TRANSFORM] AXE 3 — Social Network...
[TRANSFORM] AXE 4 — Temporalité...
[SAVE] Écriture des résultats...
✅ Pipeline terminé avec succès!
```

 Durée estimée:  2-10 minutes (selon données)

---

##  ÉTAPE 6: Accéder aux Interfaces Web

### HDFS Web UI
```
http://localhost:9870
→ Utilities → Browse the file system
→ Naviguez à /data/processed/
```

### YARN Resource Manager
```
http://localhost:8088
→ Onglet "Applications"
→ Cherchez le job Spark en cours d'exécution
```

### Spark History Server
```
http://localhost:18080
→ Historique complet des jobs Spark
```

---

## ÉTAPE 7: Vérifier les Résultats HDFS

### 7.1 Lister les outputs

```powershell
docker exec namenode hdfs dfs -ls -R /data/processed/
```

 Résultats attendus: 
```
/data/processed/axe1_reputation/      ← Réputation & Qualité
/data/processed/axe2_geo/             ← Géographie & Concurrence
/data/processed/axe3_social/          ← Réseau Social (PageRank)
/data/processed/axe4_temporal/        ← Temporalité & Saisonnalité
/data/processed/business_enriched/    ← Dataset enrichi pour SE5
```

### 7.2 Télécharger les résultats

```powershell
# Créer le répertoire temporaire dans le container
docker exec namenode mkdir -p /tmp/output_etl

# Copier depuis HDFS vers le container
docker exec namenode hdfs dfs -get /data/processed/ /tmp/output_etl

# Copier du container vers l'hôte Windows
docker cp namenode:/tmp/output_etl "C:\Users\lhamz\Downloads\SE3_Output_ETL"

# Ouvrir le dossier
explorer "C:\Users\lhamz\Downloads\SE3_Output_ETL"
```

### 7.3 Inspecter les fichiers Parquet

```powershell
# Par exemple, lister les fichiers de AXE1
docker exec namenode hdfs dfs -ls /data/processed/axe1_reputation/
```

---

## Commandes Utiles

### Redémarrer le pipeline
```powershell
docker restart spark-submit-job
docker logs spark-submit-job -f
```

### Arrêter tout
```powershell
docker compose down
```

### Nettoyer HDFS (réinitialiser)
```powershell
docker exec namenode hdfs dfs -rm -r /data/processed/
docker restart hdfs-init
```

### Consulter les logs d'un service
```powershell
docker logs [service-name] --since 5m
```

### Entrer dans un container
```powershell
docker exec -it namenode bash
# À l'intérieur: hdfs dfs -ls /
```

---

##  Troubleshooting

### Problème: "Connection refused" sur http://localhost:9870

 Solution: 
```powershell
# Attendre que namenode soit healthy
docker compose ps
# État doit être "Up" avec healthcheck "healthy"
```

### Problème: "Permission denied" sur HDFS

 Solution: 
```powershell
docker exec namenode hdfs dfs -chmod -R 777 /data/
```

### Problème: Spark job échoue avec "Out of memory"

 Solution: 
Augmentez la mémoire dans `spark-defaults.conf`:
```conf
spark.driver.memory                 6g
spark.executor.memory               6g
```

Puis relancez:
```powershell
docker compose down
docker compose up -d ...
```

---

##  Architecture Données

```
Local PC (./data/)
    ↓
HDFS /data/raw/          ← Données brutes JSON
    ↓
Spark ETL (YARN cluster)
    │
    ├─→ AXE 1: Réputation (business_with_attributes, monthly_running_avg, closure_rate)
    ├─→ AXE 2: Géographie (avg_by_city, business_with_competitors)
    ├─→ AXE 3: Social (user_enriched, elite_comparison, seniority_analysis)
    ├─→ AXE 4: Temporalité (stars_by_dow, stars_by_hour, monthly_trend, heatmap_by_biz)
    ↓
HDFS /data/processed/    ← Résultats enrichis (Parquet Snappy)
    ↓
SE5 (ClickHouse + Grafana)  ← Prochain module
```



### Fichiers de Configuration
-   docker-compose.yml   — Orchestration 8 services
-   hadoop.env   — Hadoop/YARN/MapReduce config
-   spark-defaults.conf   — Spark driver/executor config


##  Outils & Technologies

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Hadoop | 3.2.1 | HDFS (distributed storage) |
| Apache Spark | 3.2.0 | ETL engine (Spark SQL) |
| Apache YARN | 3.2.1 | Resource manager |
| Java | 8 | JVM runtime |
| Docker | 24.x | Container orchestration |
| Python | 3.8+ | Spark driver (unused — zero UDFs) |
| Snappy | 1.1.x | Compression codec |

##  Statistiques Finales

| Métrique | Valeur |
|----------|--------|
|    Données traitées    | 8.65 GB |
|    Lignes traitées    | 9.7 millions |
|    Businesses enrichis    | 150,346 |
|    Reviews analysées    | 6,990,280 |
|    Utilisateurs    | 1,596,159 |
|    Villes analysées    | 495 |
|    Fichiers produits    | 17 Parquet |
|    Volume sortie    | 44 MB (compressé) |
|    Durée exécution    | 26 minutes |
|    Compression    | Snappy (taux ~10:1) |


##  Prochaines Étapes

Une fois les résultats dans HDFS:

1.  SE5 Dashboard : Charger les données Parquet dans ClickHouse
2.  Grafana : Créer des visualisations
3.  Ingestion  (SE1/SE2): Ajouter Kafka pour temps réel
4.  Streaming  (SE4): Pipeline Flink sur Kafka

---
