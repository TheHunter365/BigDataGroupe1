# 📊 Guide ClickHouse + HDFS — SE5 Dashboard

**Date**: 2026-05-30  
**Objectif**: Connecter ClickHouse à HDFS pour lire les données Parquet de SE3

---

## 🎯 Qu'est-ce qu'on vient de configurer?

Vous avez maintenant **ClickHouse connecté à HDFS**. Cela signifie:

✅ ClickHouse peut **lire directement** les fichiers Parquet depuis HDFS  
✅ Pas besoin de copier les données  
✅ Requêtes SQL sur données distribuées  
✅ Prêt pour Grafana dashboards  

---

## 📝 Fichiers Créés

### 1. **docker-compose.yml** (Modifié)
```yaml
services:
  clickhouse:
    ...
    env_file:
      - ./hadoop.env          ← Charge les configs Hadoop
    depends_on:
      namenode:
        condition: service_healthy
```

**Effet**: ClickHouse attend que HDFS soit prêt avant de démarrer

### 2. **hadoop.env** (Enrichi)
```bash
HADOOP_USER_NAME=hdfs
HADOOP_ROOT_LOGGER=INFO,console
HDFS_NAMENODE=namenode
HDFS_PORT=9000
```

**Effet**: Variables d'authentification HDFS injectées dans ClickHouse

### 3. **clickhouse-hdfs-config.xml** (Nouveau)
Configuration XML pour la connexion HDFS native de ClickHouse

### 4. **clickhouse-hdfs-init.sql** (Nouveau)
13 tables virtuelles qui mappent les fichiers Parquet depuis HDFS

---

## 🚀 Comment Utiliser

### Étape 1: Démarrer le cluster avec ClickHouse

```powershell
cd C:\Users\lhamz\Desktop\Big Data\BigDataGroupe1

# Lancer TOUT (HDFS + Spark + ClickHouse + Grafana)
docker compose up -d

# Attendre ~2 min que tout soit prêt
docker compose ps
```

**Vérifier que ClickHouse est "healthy"**:
```powershell
docker logs clickhouse-server | grep "Ready for connections"
```

### Étape 2: Connexion à ClickHouse

#### Option A: Ligne de commande
```powershell
docker exec -it clickhouse-server clickhouse-client
```

#### Option B: Interface Web
```
http://localhost:8123/play
```

#### Option C: Python (optionnel)
```python
from clickhouse_driver import Client

client = Client('localhost', port=9000)
result = client.execute('SELECT COUNT(*) FROM axe1_business_with_attributes')
print(result)
```

---

## 📊 Exemples de Requêtes

### Requête 1: Compter les businesses
```sql
SELECT COUNT(*) as total_businesses
FROM axe1_business_with_attributes;
```

**Résultat attendu**: 150,346

### Requête 2: Top 10 villes par rating
```sql
SELECT 
    city,
    avg_stars,
    business_count
FROM axe2_avg_by_city
ORDER BY avg_stars DESC
LIMIT 10;
```

### Requête 3: Pattern temporel (heure de la journée)
```sql
SELECT 
    hour_of_day,
    avg_stars,
    review_count
FROM axe4_stars_by_hour
ORDER BY hour_of_day;
```

**Insight**: À quelle heure du jour les avis sont-ils meilleurs?

### Requête 4: Utilisateurs influents (PageRank)
```sql
SELECT 
    user_id,
    page_rank_score,
    review_count,
    useful_count
FROM axe3_user_enriched
ORDER BY page_rank_score DESC
LIMIT 20;
```

### Requête 5: Heatmap jour × heure
```sql
SELECT 
    day_of_week,
    hour_of_day,
    avg_stars
FROM axe4_heatmap_global
ORDER BY day_of_week, hour_of_day;
```

### Requête 6: Analyse densité concurrence
```sql
SELECT 
    COUNT(*) as high_competition,
    AVG(competitor_count) as avg_competitors
FROM axe2_business_with_competitors
WHERE competitor_count > 10;
```

---

## 🔧 Troubleshooting

### Problème: "Table engine HDFS is not available"

**Solution**: S'assurer que ClickHouse est démarré APRÈS HDFS

```powershell
# Redémarrer ClickHouse
docker restart clickhouse-server

# Vérifier les logs
docker logs clickhouse-server
```

### Problème: "Cannot connect to NameNode at namenode:9000"

**Solution**: Vérifier que HDFS est prêt

```powershell
# Vérifier HDFS
docker exec namenode hdfs dfs -ls /data/processed/

# Si OK, redémarrer ClickHouse
docker restart clickhouse-server
```

### Problème: "Permission denied"

**Solution**: Les fichiers Parquet doivent être lisibles

```powershell
# Donner les permissions dans HDFS
docker exec namenode hdfs dfs -chmod -R 777 /data/processed/
```

---

## 📈 Performance

### Taille des données
| Table | Lignes | Taille |
|-------|--------|--------|
| axe1_business_with_attributes | 150,346 | 13.5 MB |
| axe1_monthly_running_avg | 3,404,085 | 29.4 MB |
| axe2_avg_by_city | 495 | 12.1 KB |
| axe3_user_enriched | ~ | 2.2 KB |
| axe4_heatmap_global | 168 | 1.0 KB |
| **TOTAL** | **9.7M** | **~44 MB** |

### Speed Tips

1. **Utiliser des LIMIT** sur les requêtes exploratoires:
   ```sql
   SELECT * FROM axe1_business_with_attributes LIMIT 10;
   ```

2. **Filter au niveau HDFS** plutôt qu'en post-traitement:
   ```sql
   -- ✅ Bon: Filter dans la requête
   SELECT * FROM axe1_business_with_attributes WHERE stars > 4;
   
   -- ❌ Mauvais: Lire tous les fichiers puis filter
   SELECT * FROM axe1_business_with_attributes;
   ```

3. **Paralléliser les lectures** (automatique avec HDFS engine)

---

## 🎨 Préparer pour Grafana

Une fois ClickHouse fonctionnel, Grafana peut:

1. **Se connecter** à ClickHouse (datasource HTTP)
2. **Créer des panels** avec les 13 tables
3. **Afficher**:
   - 📊 Heatmaps temporelles
   - 🗺️ Cartes géographiques
   - 📈 Trends mensuels
   - 👥 Influence utilisateurs

### Configuration Grafana → ClickHouse

```json
{
  "type": "clickhouse",
  "url": "http://clickhouse-server:8123",
  "database": "bigdata_db"
}
```

---

## 📚 Référence Complète

### Tables Disponibles

**AXE 1 — Réputation**
- `axe1_business_with_attributes` — 150K businesses + 40 attributs
- `axe1_monthly_running_avg` — 3.4M moyennes mobiles
- `axe1_closure_rate` — 4 taux de fermeture

**AXE 2 — Géographie**
- `axe2_avg_by_city` — 495 villes
- `axe2_business_with_competitors` — Densité 500m

**AXE 3 — Social Network**
- `axe3_user_enriched` — PageRank utilisateurs
- `axe3_user_review_enriched` — Influence reviews
- `axe3_elite_comparison` — Elite vs. regular
- `axe3_seniority_analysis` — Tenure quintiles

**AXE 4 — Temporalité**
- `axe4_stars_by_hour` — 24 heures
- `axe4_stars_by_dow` — 7 jours
- `axe4_monthly_trend` — 12 mois
- `axe4_heatmap_global` — 24×7 matrice
- `axe4_heatmap_by_biz` — Patterns business
- `axe4_category_hourly` — Trends catégories

**Final**
- `business_enriched_final` — Fusion tous les axes

---

## ✨ Recap

```
SE3 (Spark ETL) → Parquet dans HDFS
              ↓
        [Configuration]
        ✅ docker-compose.yml (env_file)
        ✅ hadoop.env (variables Hadoop)
        ✅ clickhouse-hdfs-config.xml
              ↓
SE5 (ClickHouse) ← Lire directement HDFS
              ↓
    [Grafana Dashboards]
    📊 Visualisations interactives
```

---

## 🎯 Prochaines Étapes

1. ✅ **Vérifier les tables**: `SHOW TABLES;`
2. ✅ **Tester une requête**: `SELECT COUNT(*) FROM axe1_business_with_attributes;`
3. ✅ **Connecter Grafana**: Configuration datasource ClickHouse
4. ✅ **Créer dashboards**: Panels avec heatmaps, trends, etc.

---

**Pipeline SE3 → SE5 : Complètement Configuré!** 🎉
