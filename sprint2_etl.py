"""
Sprint 2 — ETL Complet MOCK
Sous-Équipe 3 (SE3) — Pipeline de Traitement Yelp Academic Business
Technologies : PySpark + Spark SQL
Entrée  : clean_S1.csv  (produit par Sprint 1)
Sorties : clean_S2.parquet + agrégats analytiques (parquet)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, FloatType

# ─────────────────────────────────────────────
# 0. Initialisation de la session Spark
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("SE3_Sprint2_ETL_MOCK")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────
# 1. Lecture du fichier nettoyé Sprint 1
# ─────────────────────────────────────────────
INPUT_PATH  = "data/clean_S1.csv"
OUTPUT_PATH = "data/clean_S2.parquet"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_PATH)
)

print(f"[S2] Lignes en entrée : {df.count()}")
df.printSchema()
# Fix city normalization — initcap + trim
df = df.withColumn("city", 
    F.initcap(F.trim(F.col("city"))))

# Fix NULL cities
df = df.withColumn("city",
    F.when(F.col("city").isNull(), "Unknown")
    .when(F.col("city") == "", "Unknown")
    .otherwise(F.col("city")))
# ─────────────────────────────────────────────────────────────────────────────
# GROUPE 1 — Formatage et Standardisation
# ─────────────────────────────────────────────────────────────────────────────

# Transformation 1 : int → bool (is_open)
# Justification : sémantiquement correct ; optimise le filtrage booléen dans Spark
df = df.withColumn("is_open", F.col("is_open").cast(BooleanType()))

# Transformation 2 : Remplacer & par "and" dans categories
# Justification : évite les conflits de parsing dans Spark SQL et Superset/Druid
df = df.withColumn(
    "categories",
    F.regexp_replace(F.col("categories"), "&", "and")
)

# Transformation 3 : Arrondir stars à 1 décimale
# Justification : garantir la lisibilité et la cohérence entre sources
df = df.withColumn("stars", F.round(F.col("stars").cast(FloatType()), 1))

# Transformation 4 : Supprimer espaces superflus dans categories
# Justification : mesure préventive pour les données réelles S3
df = df.withColumn(
    "categories",
    F.regexp_replace(F.col("categories"), r"\s*,\s*", ", ")
)

# ─────────────────────────────────────────────────────────────────────────────
# GROUPE 2 — Enrichissement Métier (Nouvelles Colonnes)
# ─────────────────────────────────────────────────────────────────────────────

# Colonne 5 : categories_list — split(',') sur categories
# Justification : permet filtrage par catégorie et explode() pour analyses
df = df.withColumn(
    "categories_list",
    F.split(F.col("categories"), ",\\s*")
)

# Colonne 6 : nb_categories — len(categories_list)
# Justification : feature de diversité commerciale
df = df.withColumn("nb_categories", F.size(F.col("categories_list")))

# Colonne 7 : score_class — classification des notes
# Justification : classe lisible pour filtrage et visualisation
df = df.withColumn(
    "score_class",
    F.when(F.col("stars") < 2, "Faible")
     .when((F.col("stars") >= 2) & (F.col("stars") < 4), "Moyen")
     .otherwise("Bon")
)

# Colonne 8 : region — mapping state → zone géographique (USA / Canada)
# Justification : analyses macro par grande région géographique
state_to_region = {
    # USA — Régions Census Bureau
    "AZ": "South-West", "CA": "West", "CO": "Mountain", "HI": "West",
    "ID": "Mountain", "MT": "Mountain", "NV": "South-West", "NM": "South-West",
    "OR": "West", "UT": "Mountain", "WA": "West", "WY": "Mountain",
    "AL": "South", "AR": "South", "FL": "South", "GA": "South",
    "KY": "South", "LA": "South", "MS": "South", "NC": "South",
    "SC": "South", "TN": "South", "TX": "South", "VA": "South",
    "WV": "South", "IL": "Midwest", "IN": "Midwest", "IA": "Midwest",
    "KS": "Midwest", "MI": "Midwest", "MN": "Midwest", "MO": "Midwest",
    "NE": "Midwest", "ND": "Midwest", "OH": "Midwest", "SD": "Midwest",
    "WI": "Midwest", "CT": "North-East", "ME": "North-East", "MA": "North-East",
    "NH": "North-East", "NJ": "North-East", "NY": "North-East", "PA": "North-East",
    "RI": "North-East", "VT": "North-East", "DE": "North-East", "MD": "North-East",
    "DC": "North-East", "AK": "West",
    # Canada
    "AB": "Canada-West", "BC": "Canada-West", "MB": "Canada-Central",
    "NB": "Canada-East",  "NL": "Canada-East", "NS": "Canada-East",
    "NT": "Canada-North", "NU": "Canada-North", "ON": "Canada-Central",
    "PE": "Canada-East",  "QC": "Canada-East", "SK": "Canada-Central",
    "YT": "Canada-North",
}

# Création de la mapping via une expression SQL CASE WHEN générée dynamiquement
mapping_expr = F.when(F.lit(False), F.lit("Unknown"))
for state_code, region_name in state_to_region.items():
    mapping_expr = mapping_expr.when(
        F.upper(F.col("state")) == state_code, region_name
    )
mapping_expr = mapping_expr.otherwise("Other")

df = df.withColumn("region", mapping_expr)

# Colonne 9 : is_restaurant
# Justification : segmentation rapide restauration vs autres commerces
df = df.withColumn(
    "is_restaurant",
    F.array_contains(F.col("categories_list"), "Restaurants")
)

# Colonne 10 : is_healthcare
# Justification : segment médical pour analyses santé spécifiques
df = df.withColumn(
    "is_healthcare",
    F.array_contains(F.col("categories_list"), "Health and Medical")
)

# Colonne 11 : name_length
# Justification : détection d'anomalies (noms trop courts ou très longs)
df = df.withColumn("name_length", F.length(F.col("name")))

# Colonne 12 : state_upper — homogénéisation des codes état
# Justification : éviter les faux doublons lors des jointures
df = df.withColumn("state_upper", F.upper(F.col("state")))

# ─────────────────────────────────────────────────────────────────────────────
# GROUPE 3 — Agrégations Analytiques via Spark SQL
# ─────────────────────────────────────────────────────────────────────────────

# Enregistrement de la vue temporaire pour les requêtes SQL
df.createOrReplaceTempView("yelp_clean")

# ── Agrégation 13 : Moyenne stars par ville ──────────────────────────────────
# KPI : quelle ville a les meilleurs commerces ?
avg_stars_per_city = spark.sql("""
    SELECT
        city,
        state_upper                        AS state,
        ROUND(AVG(stars), 2)               AS avg_stars,
        COUNT(business_id)                 AS nb_businesses
    FROM yelp_clean
    GROUP BY city, state_upper
    ORDER BY avg_stars DESC
""")

print("\n[S2] Top 10 villes par note moyenne :")
avg_stars_per_city.show(10, truncate=False)

# ── Agrégation 14 : Nombre de commerces par état ─────────────────────────────
# Distribution géographique des commerces
businesses_per_state = spark.sql("""
    SELECT
        state_upper                        AS state,
        region,
        COUNT(business_id)                 AS nb_businesses,
        ROUND(AVG(stars), 2)               AS avg_stars,
        SUM(review_count)                  AS total_reviews
    FROM yelp_clean
    GROUP BY state_upper, region
    ORDER BY nb_businesses DESC
""")

print("\n[S2] Distribution par état :")
businesses_per_state.show(20, truncate=False)

# ── Agrégation 15 : Top catégories ───────────────────────────────────────────
# Catégories les plus représentées (explode nécessaire)
top_categories = spark.sql("""
    SELECT
        TRIM(category)                     AS category,
        COUNT(*)                           AS nb_occurrences,
        ROUND(AVG(stars), 2)              AS avg_stars
    FROM (
        SELECT EXPLODE(categories_list) AS category, stars
        FROM yelp_clean
    )
    WHERE TRIM(category) != '' AND TRIM(category) != 'Unknown'
    GROUP BY TRIM(category)
    ORDER BY nb_occurrences DESC
    LIMIT 30
""")

print("\n[S2] Top 30 catégories :")
top_categories.show(30, truncate=False)

# ── Agrégation Bonus : Score class par région ────────────────────────────────
score_distribution = spark.sql("""
    SELECT
        region,
        score_class,
        COUNT(*) AS nb_businesses,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY region), 1) AS pct
    FROM yelp_clean
    GROUP BY region, score_class
    ORDER BY region, score_class
""")

print("\n[S2] Distribution score_class par région :")
score_distribution.show(40, truncate=False)

# ── Agrégation Bonus : Segmentation restaurant vs autres ─────────────────────
restaurant_segmentation = spark.sql("""
    SELECT
        is_restaurant,
        is_healthcare,
        COUNT(*)               AS nb_businesses,
        ROUND(AVG(stars), 2)   AS avg_stars,
        SUM(review_count)      AS total_reviews
    FROM yelp_clean
    GROUP BY is_restaurant, is_healthcare
    ORDER BY nb_businesses DESC
""")

print("\n[S2] Segmentation métier :")
restaurant_segmentation.show()

# ─────────────────────────────────────────────────────────────────────────────
# GROUPE 4 — Export des résultats
# ─────────────────────────────────────────────────────────────────────────────

# Dataset principal enrichi → Parquet (format colonne, compressé)
df_export = df.drop("categories_list")  # les listes ne sont pas supportées en CSV
df_export.write.mode("overwrite").parquet(OUTPUT_PATH)
print(f"\n[S2] Dataset enrichi exporté → {OUTPUT_PATH}")

# Agrégats → Parquet
avg_stars_per_city.write.mode("overwrite").parquet("data/agg_avg_stars_city.parquet")
businesses_per_state.write.mode("overwrite").parquet("data/agg_businesses_state.parquet")
top_categories.write.mode("overwrite").parquet("data/agg_top_categories.parquet")
score_distribution.write.mode("overwrite").parquet("data/agg_score_distribution.parquet")
restaurant_segmentation.write.mode("overwrite").parquet("data/agg_segmentation.parquet")

print("[S2] Tous les agrégats exportés en Parquet.")

# ─────────────────────────────────────────────────────────────────────────────
# Rapport de qualité final
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("[S2] RAPPORT DE QUALITÉ — DATASET ENRICHI")
print("=" * 60)
print(f"  Lignes totales      : {df.count()}")
print(f"  Colonnes totales    : {len(df.columns)}")
print(f"  Nouvelles colonnes  : score_class, region, is_restaurant,")
print(f"                        is_healthcare, name_length, state_upper,")
print(f"                        nb_categories, categories_list")
print("=" * 60)

df.select(
    "business_id", "name", "city", "state_upper", "stars",
    "score_class", "region", "nb_categories", "is_restaurant"
).show(10, truncate=True)

spark.stop()
