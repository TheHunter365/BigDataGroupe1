# SE3 — Sprint 2 : ETL Complet MOCK

## Description

Ce sprint réalise l'ETL complet sur le dataset **Yelp MOCK** en utilisant **PySpark + Spark SQL**.  
Il prend les données nettoyées produites par Sprint 1 (`clean_S1.csv`) et les enrichit  
avec 8 nouvelles colonnes métier, plus 5 agrégations analytiques exportées en **format Parquet**.

---

## Structure du projet

```
S2/
├── data/
│   ├── clean_S1.csv                      # Entrée : sortie nettoyée 
│   ├── clean_S2.parquet/                 # Dataset principal enrichi 
│   ├── agg_avg_stars_city.parquet/       # AGG1 : moyenne stars par 
│   ├── agg_businesses_state.parquet/     # AGG2 : distribution par 
│   ├── agg_top_categories.parquet/       # AGG3 : top 30 catégories
│   ├── agg_score_distribution.parquet/   # AGG4 : score_class par 
│   └── agg_segmentation.parquet/        # AGG5 : segmentation métier
├── sprint2_etl.py                        # Script principal PySpark
└── README.md                             # Ce fichier
```

---

## Prérequis

### Logiciels requis

| Logiciel | Version | Lien |
|---|---|---|
| Python | 3.x | https://www.python.org |
| Java | 17 | https://adoptium.net |
| PySpark | 3.x | via pip |
| winutils (Windows) | hadoop-3.3.5 | https://github.com/cdarlint/winutils |

### Installation

```bash
pip install pyspark
```

### Configuration Windows (obligatoire)

```powershell
# Définir JAVA_HOME
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.11.9-hotspot"
$env:PATH = "$env:PATH;C:\Program Files\Eclipse Adoptium\jdk-17.0.11.9-hotspot\bin"

# Définir HADOOP_HOME (winutils.exe requis pour écriture Parquet)
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:PATH;C:\hadoop\bin"
```

---

## Lancer le script

```bash
# Se placer dans le dossier S2
cd S2

# Vérifier que clean_S1.csv est dans data/
ls data/clean_S1.csv

# Lancer le pipeline ETL
python sprint2_etl.py
```

### Sortie attendue en console

```
[S2] Lignes en entrée : 54
[S2] Top 10 villes par note moyenne : ...
[S2] Distribution par état : ...
[S2] Top 30 catégories : ...
[S2] Distribution score_class par région : ...
[S2] Segmentation métier : ...
[S2] Dataset enrichi exporté → data/clean_S2.parquet
[S2] Tous les agrégats exportés en Parquet.
```

---

## Ce que fait le script

### Groupe 1 — Formatage et Standardisation (4 transformations)

| # | Transformation | Colonne | Justification |
|---|---|---|---|
| T1 | `int` → `boolean` | `is_open` | Sémantiquement correct, filtrage plus rapide |
| T2 | Remplacer `&` par `and` | `categories` | Évite conflits parsing Spark SQL |
| T3 | Arrondir à 1 décimale | `stars` | Cohérence entre sources |
| T4 | Nettoyer espaces autour virgules | `categories` | Prévention pour Sprint 3 |

### Groupe 2 — Enrichissement Métier (8 nouvelles colonnes)

| Colonne | Type | Description |
|---|---|---|
| `categories_list` | Array | Liste des catégories — ex: `["Restaurants", "Fast Food"]` |
| `nb_categories` | Integer | Nombre de catégories du commerce |
| `score_class` | String | `Faible` (< 2) / `Moyen` (2-4) / `Bon` (≥ 4) |
| `region` | String | Zone géographique USA/Canada selon l'état |
| `is_restaurant` | Boolean | `true` si "Restaurants" dans les catégories |
| `is_healthcare` | Boolean | `true` si "Health and Medical" dans les catégories |
| `name_length` | Integer | Longueur du nom — détection d'anomalies |
| `state_upper` | String | Code état en majuscules — évite faux doublons |

### Groupe 3 — Agrégations Spark SQL (5 analyses)

| Fichier Parquet | Description | KPI |
|---|---|---|
| `agg_avg_stars_city` | Moyenne stars par ville | Quelle ville a les meilleurs commerces ? |
| `agg_businesses_state` | Distribution par état + région | Où sont concentrés les commerces ? |
| `agg_top_categories` | Top 30 catégories | Quelles catégories dominent ? |
| `agg_score_distribution` | % Bon/Moyen/Faible par région | Quelle région a la meilleure qualité ? |
| `agg_segmentation` | Restaurant vs Healthcare vs autres | Quel segment est le mieux noté ? |

### Groupe 4 — Export Parquet

Tous les résultats exportés en **Parquet** (format columnar compressé) :
- 5x moins lourd que CSV
- 10x plus rapide à lire par Spark
- Compatible HDFS, Druid et Superset (SE5)

---


## Technologies

| Technologie | Version | Rôle |
|---|---|---|
| Python | 3.14 | Langage principal |
| PySpark | 3.x | Traitement distribué |
| Spark SQL | 3.x | Agrégations analytiques |
| Java | 17 (Temurin) | Runtime Spark |
| Parquet + Snappy | — | Format de sortie colonnaire compressé |
| winutils | hadoop-3.3.5 | Compatibilité Windows |

---

## Auteurs

- **Abbar Amina** — Master SNIA, Université Mohammed V Rabat  
- **Nader Chaimaa** — Master SNIA, Université Mohammed V Rabat  

Encadré par **Pr. EL ASRI Mohamed Youssef**
