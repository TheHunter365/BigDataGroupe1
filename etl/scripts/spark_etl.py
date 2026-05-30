# ============================================================
#  SE3 — Pipeline ETL COMPLET CORRIGE v3
#  Compatible Python 3.5+ / Spark 3.x / YARN
# ============================================================

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from functools import reduce
from operator import add

# ============================================================
# CONFIGURATION
# ============================================================
PATH_BUSINESS = "hdfs://namenode:9000/data/raw/business/yelp_academic_dataset_business.json"
PATH_REVIEW   = "hdfs://namenode:9000/data/raw/review/yelp_academic_dataset_review.json"
PATH_USER     = "hdfs://namenode:9000/data/raw/user/yelp_academic_dataset_user.json"
PATH_TIP      = "hdfs://namenode:9000/data/raw/tip/yelp_academic_dataset_tip.json"
PATH_CHECKIN  = "hdfs://namenode:9000/data/raw/checkin/yelp_academic_dataset_checkin.json"
OUTPUT_BASE   = "hdfs://namenode:9000/data/processed"   

EARTH_RADIUS_M = 6371000.0
PAGERANK_ITER  = 10
DAMPING_FACTOR = 0.85
GEO_BUCKET = 0.009

# Use getOrCreate() to natively ingest cluster flags passed by spark-submit
spark = SparkSession.builder \
    .appName("SE3_ETL_COMPLET_NO_UDF_v3") \
    .getOrCreate()

spark.sparkContext.setCheckpointDir("hdfs://namenode:9000/spark/checkpoints")
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("  SE3 ETL COMPLET v3 — ZERO UDF — Spark SQL natif")
print("="*60 + "\n")


# ============================================================
# ETAPE 1 — CHARGEMENT
# ============================================================
print("[LOAD] Chargement des datasets...")
df_biz = spark.read.json(PATH_BUSINESS)
df_rev = spark.read.json(PATH_REVIEW)
df_usr = spark.read.json(PATH_USER)
df_chk = spark.read.json(PATH_CHECKIN)
df_tip = spark.read.json(PATH_TIP)

print("[LOAD] business  : {} lignes".format(df_biz.count()))
print("[LOAD] review    : {} lignes".format(df_rev.count()))
print("[LOAD] user      : {} lignes".format(df_usr.count()))
print("[LOAD] checkin   : {} lignes".format(df_chk.count()))
print("[LOAD] tip       : {} lignes".format(df_tip.count()))

print("\n[INFO] Schema attributes :")
df_biz.select("attributes").printSchema()


# ============================================================
# ETAPE 2 — NETTOYAGE
# ============================================================

print("\n[CLEAN] Nettoyage business...")
for col_name in ["postal_code", "attributes", "hours"]:
    if col_name not in df_biz.columns:
        df_biz = df_biz.withColumn(col_name, F.lit(None))

df_biz = df_biz \
    .filter(F.col("business_id").isNotNull()) \
    .dropDuplicates(["business_id"]) \
    .filter(F.col("stars").between(1.0, 5.0)) \
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull()) \
    .filter(F.col("city").isNotNull() & (F.trim(F.col("city")) != "")) \
    .withColumn("city",   F.initcap(F.trim(F.col("city")))) \
    .withColumn("state",  F.upper(F.trim(F.col("state")))) \
    .withColumn("stars",  F.round(F.col("stars").cast(DoubleType()), 1)) \
    .withColumn("categories",
        F.when(F.col("categories").isNull() | (F.trim(F.col("categories")) == ""),
               F.lit("Unknown"))
         .otherwise(F.trim(F.col("categories")))) \
    .withColumn("is_open", F.col("is_open").cast(IntegerType())) \
    .withColumn("categories_list", F.split(F.col("categories"), ", ")) \
    .withColumn("score_class",
        F.when(F.col("stars") < 2, "Faible")
         .when(F.col("stars") < 4, "Moyen")
         .otherwise("Bon"))

print("[CLEAN] business cleaned : {} lignes".format(df_biz.count()))

print("[CLEAN] Nettoyage review...")
df_rev = df_rev \
    .filter(F.col("review_id").isNotNull()) \
    .filter(F.col("business_id").isNotNull()) \
    .filter(F.col("user_id").isNotNull()) \
    .filter(F.col("stars").between(1, 5)) \
    .dropDuplicates(["review_id"]) \
    .withColumn("date", F.to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
print("[CLEAN] review cleaned : {} lignes".format(df_rev.count()))

print("[CLEAN] Nettoyage user...")
for col_name in ["fans", "average_stars", "elite"]:
    if col_name not in df_usr.columns:
        df_usr = df_usr.withColumn(col_name, F.lit(None))

compliment_cols = [c for c in df_usr.columns if c.startswith("compliment_")]
if compliment_cols:
    df_usr = df_usr.withColumn(
        "total_compliments",
        sum(F.coalesce(F.col(c), F.lit(0)) for c in compliment_cols)
    )
else:
    df_usr = df_usr.withColumn("total_compliments", F.lit(0))

df_usr = df_usr \
    .filter(F.col("user_id").isNotNull()) \
    .dropDuplicates(["user_id"]) \
    .withColumn("yelping_since", F.to_timestamp("yelping_since", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("friends_list",
        F.when(
            F.col("friends").isNull() | (F.col("friends") == "None"),
            F.array()
        ).otherwise(F.split(F.col("friends"), ", "))) \
    .withColumn("friends_count", F.size(F.col("friends_list"))) \
    .withColumn("account_age_years",
        F.round(F.datediff(F.current_date(), F.col("yelping_since")) / 365.0, 1)) \
    .withColumn("is_elite",
        F.when(
            F.col("elite").isNull() |
            (F.trim(F.col("elite")) == "") |
            (F.trim(F.col("elite")) == "None"),
            F.lit(False)
        ).otherwise(F.lit(True))) \
    .withColumn("fans", F.coalesce(F.col("fans").cast(IntegerType()), F.lit(0))) \
    .withColumn("average_stars", F.coalesce(F.col("average_stars").cast(DoubleType()), F.lit(0.0)))

print("[CLEAN] user cleaned : {} lignes".format(df_usr.count()))


# ============================================================
# ETAPE 3 — AXE 1 : REPUTATION
# ============================================================
print("\n" + "="*60)
print("[AXE1] Calcul Reputation...")
print("="*60)

print("[AXE1] A1.1 — Running average mensuelle...")
monthly = df_rev \
    .withColumn("yr",  F.year("date")) \
    .withColumn("mth", F.month("date")) \
    .groupBy("business_id", "yr", "mth") \
    .agg(
        F.sum("stars").alias("sum_stars"),
        F.count("review_id").alias("count_rev"),
        F.round(F.avg("stars"), 2).alias("avg_stars_month")
    )

w_cumul = Window \
    .partitionBy("business_id") \
    .orderBy("yr", "mth") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

axe1_monthly = monthly \
    .withColumn("cumul_sum",   F.sum("sum_stars").over(w_cumul)) \
    .withColumn("cumul_count", F.sum("count_rev").over(w_cumul)) \
    .withColumn("running_avg", F.round(F.col("cumul_sum") / F.col("cumul_count"), 2))

print("[AXE1] monthly_running_avg : {} lignes".format(axe1_monthly.count()))

print("[AXE1] A1.2+A1.3 — Aplatissement attributs + heures ouverture...")

# Récupération dynamique des sous-champs réels pour éviter l'AnalysisException
try:
    attr_fields = [f.name for f in df_biz.schema["attributes"].dataType.fields]
except Exception:
    attr_fields = []

def get_attr(field):
    """Lit un champ du struct de manière sûre en vérifiant sa présence réelle dans le schéma."""
    if field in attr_fields:
        return F.when(F.col("attributes").isNotNull(), F.col("attributes." + field)).otherwise(F.lit(None).cast("string"))
    else:
        return F.lit(None).cast("string")

axe1_biz_attrs = (df_biz
    .withColumn("wifi",
        F.when(get_attr("WiFi").isNull(),                    "unknown")
         .when(get_attr("WiFi").contains("free"),             "free")
         .when(get_attr("WiFi").contains("paid"),             "paid")
         .when(get_attr("WiFi").contains("no"),               "no")
         .otherwise("unknown"))
    .withColumn("noise_level",
        F.when(get_attr("NoiseLevel").isNull(),               "unknown")
         .when(get_attr("NoiseLevel").contains("very_loud"),  "very_loud")
         .when(get_attr("NoiseLevel").contains("loud"),       "loud")
         .when(get_attr("NoiseLevel").contains("average"),    "average")
         .when(get_attr("NoiseLevel").contains("quiet"),      "quiet")
         .otherwise("unknown"))
    .withColumn("price_range",
        F.when(get_attr("RestaurantsPriceRange2").isNull(),   "unknown")
         .when(get_attr("RestaurantsPriceRange2") == "1",     "euro1")
         .when(get_attr("RestaurantsPriceRange2") == "2",     "euro2")
         .when(get_attr("RestaurantsPriceRange2") == "3",     "euro3")
         .when(get_attr("RestaurantsPriceRange2") == "4",     "euro4")
         .otherwise("unknown"))
    .withColumn("alcohol",
        F.when(get_attr("Alcohol").isNull(),                  "unknown")
         .when(get_attr("Alcohol").contains("full_bar"),      "full_bar")
         .when(get_attr("Alcohol").contains("beer"),          "beer_and_wine")
         .when(get_attr("Alcohol").contains("none"),          "none")
         .otherwise("unknown"))
    .withColumn("accepts_credit_cards",
        F.when(get_attr("BusinessAcceptsCreditCards") == "True", True)
         .otherwise(False))
    .withColumn("good_for_kids",
        F.when(get_attr("GoodForKids") == "True", True)
         .otherwise(False))
    .withColumn("has_tv",
        F.when(get_attr("HasTV") == "True", True)
         .otherwise(False))
    .withColumn("outdoor_seating",
        F.when(get_attr("OutdoorSeating") == "True", True)
         .otherwise(False))
    .withColumn("accepts_reservations",
        F.when(get_attr("RestaurantsReservations") == "True", True)
         .otherwise(False))
    .withColumn("bike_parking",
        F.when(get_attr("BikeParking") == "True", True)
         .otherwise(False))
    .withColumn("has_delivery",
        F.when(get_attr("RestaurantsDelivery") == "True", True)
         .otherwise(False))
    .withColumn("has_takeout",
        F.when(get_attr("RestaurantsTakeOut") == "True", True)
         .otherwise(False))
    .withColumn("weekly_open_minutes",
    reduce(
        add,
        [
            F.when(
                F.col("hours." + day).isNotNull(),
                F.greatest(
                    F.lit(0),
                    F.regexp_extract(
                        F.split(F.col("hours." + day), "-").getItem(1),
                        r"(\d+):(\d+)", 1
                    ).cast(IntegerType()) * 60
                    +
                    F.regexp_extract(
                        F.split(F.col("hours." + day), "-").getItem(1),
                        r"(\d+):(\d+)", 2
                    ).cast(IntegerType())
                    -
                    F.regexp_extract(
                        F.split(F.col("hours." + day), "-").getItem(0),
                        r"(\d+):(\d+)", 1
                    ).cast(IntegerType()) * 60
                    -
                    F.regexp_extract(
                        F.split(F.col("hours." + day), "-").getItem(0),
                        r"(\d+):(\d+)", 2
                    ).cast(IntegerType())
                )
            ).otherwise(F.lit(0))
            for day in [
                "Monday", "Tuesday", "Wednesday",
                "Thursday", "Friday", "Saturday", "Sunday"
            ]
        ]
    ))
    .select(
        "business_id", "name", "city", "state", "postal_code",
        "stars", "review_count", "is_open", "latitude", "longitude",
        "categories", "categories_list", "score_class",
        "wifi", "noise_level", "price_range", "alcohol",
        "accepts_credit_cards", "good_for_kids", "has_tv",
        "outdoor_seating", "accepts_reservations", "bike_parking",
        "has_delivery", "has_takeout",   
        "weekly_open_minutes"
    ))

print("[AXE1] business_with_attributes : {} lignes".format(axe1_biz_attrs.count()))

print("[AXE1] A1.4 — Taux de fermeture...")
axe1_closure = df_biz \
    .withColumn("stars_bucket",
        F.when(F.col("stars") < 2.0, "1.0-1.5")
         .when(F.col("stars") < 3.0, "2.0-2.5")
         .when(F.col("stars") < 4.0, "3.0-3.5")
         .otherwise("4.0-5.0")) \
    .groupBy("stars_bucket") \
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_open") == 0, 1).otherwise(0)).alias("closed"),
        F.round(
            F.sum(F.when(F.col("is_open") == 0, 1).otherwise(0)) * 100.0 / F.count("*"), 1
        ).alias("closure_rate_pct")
    )

print("[AXE1] closure_rate : {} lignes".format(axe1_closure.count()))

print("[AXE1] Export...")
axe1_monthly.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe1_reputation/monthly_running_avg".format(OUTPUT_BASE))
axe1_biz_attrs.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe1_reputation/business_with_attributes".format(OUTPUT_BASE))
axe1_closure.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe1_reputation/closure_rate".format(OUTPUT_BASE))
print("[AXE1] Export terminé")


# ============================================================
# ETAPE 4 — AXE 2 : GEOGRAPHIE
# ============================================================
print("\n" + "="*60)
print("[AXE2] Calcul Géographie — Haversine SQL natif...")
print("="*60)

print("[AXE2] A2.2 — Moyenne par ville...")
axe2_city_avg = df_biz \
    .groupBy("city", "state") \
    .agg(
        F.count("*").alias("nb_businesses"),
        F.round(F.avg("stars"), 2).alias("avg_stars"),
        F.round(F.stddev("stars"), 2).alias("stddev_stars"),
        F.sum("review_count").alias("total_reviews")
    ) \
    .filter(F.col("nb_businesses") >= 10)

print("[AXE2] avg_by_city : {} lignes".format(axe2_city_avg.count()))

print("[AXE2] A2.1 — Densité concurrence — bucket {:.3f}° + Haversine...".format(GEO_BUCKET))

biz_geo = df_biz.select("business_id", "latitude", "longitude", "categories_list") \
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull()) \
    .withColumn("lat_bucket", F.round(F.col("latitude")  / F.lit(GEO_BUCKET), 0) * F.lit(GEO_BUCKET)) \
    .withColumn("lon_bucket", F.round(F.col("longitude") / F.lit(GEO_BUCKET), 0) * F.lit(GEO_BUCKET))

left_geo  = biz_geo.alias("a")
right_geo = biz_geo.alias("b")

PI = F.lit(3.14159265358979)
R  = F.lit(EARTH_RADIUS_M)
lat1 = F.col("a.latitude")  * PI / F.lit(180.0)
lat2 = F.col("b.latitude")  * PI / F.lit(180.0)
lon1 = F.col("a.longitude") * PI / F.lit(180.0)
lon2 = F.col("b.longitude") * PI / F.lit(180.0)
dlat = lat2 - lat1
dlon = lon2 - lon1
a_hav = (F.pow(F.sin(dlat / F.lit(2.0)), 2)
         + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / F.lit(2.0)), 2))
a_hav_safe  = F.greatest(F.lit(0.0), F.least(F.lit(1.0), a_hav))
distance_m  = F.lit(2.0) * R * F.asin(F.sqrt(a_hav_safe))

pairs_geo = left_geo.join(right_geo,
    (F.col("a.business_id") != F.col("b.business_id")) &
    (F.abs(F.col("a.lat_bucket") - F.col("b.lat_bucket")) <= F.lit(GEO_BUCKET)) &
    (F.abs(F.col("a.lon_bucket") - F.col("b.lon_bucket")) <= F.lit(GEO_BUCKET)),
    "inner"
)

competitors = pairs_geo \
    .withColumn("distance_m", distance_m) \
    .filter(F.col("distance_m") <= 500) \
    .filter(F.size(F.array_intersect(F.col("a.categories_list"), F.col("b.categories_list"))) > 0) \
    .groupBy(F.col("a.business_id").alias("business_id")) \
    .agg(F.count("*").alias("nb_competitors_500m"))

business_enriched = axe1_biz_attrs \
    .join(competitors, "business_id", "left") \
    .na.fill(0, ["nb_competitors_500m"])

print("[AXE2] business_with_competitors : {} lignes".format(business_enriched.count()))

print("[AXE2] Export...")
axe2_city_avg.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe2_geo/avg_by_city".format(OUTPUT_BASE))
business_enriched.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe2_geo/business_with_competitors".format(OUTPUT_BASE))
business_enriched.coalesce(1).write.mode("overwrite") \
    .parquet("{}/business_enriched".format(OUTPUT_BASE))
print("[AXE2] Export terminé")


# ============================================================
# ETAPE 5 — AXE 3 : RESEAU SOCIAL
# ============================================================
print("\n" + "="*60)
print("[AXE3] Calcul Réseau Social — PageRank itératif optimisé...")
print("="*60)

edges = df_usr \
    .filter(F.col("friends").isNotNull()) \
    .filter(F.trim(F.col("friends")) != "None") \
    .filter(F.trim(F.col("friends")) != "") \
    .select(
        F.col("user_id").alias("src"),
        F.explode(F.split(F.col("friends"), ", ")).alias("dst")
    ) \
    .filter(F.col("dst") != "") \
    .persist()   

out_degree = edges \
    .groupBy("src") \
    .agg(F.count("*").alias("out_degree")) \
    .persist()   

total_users = df_usr.select("user_id").distinct().count()
init_rank   = 1.0 / float(total_users)
dangling_contrib = (1.0 - DAMPING_FACTOR) / float(total_users)

pr = df_usr \
    .select(F.col("user_id").alias("id")) \
    .withColumn("pagerank", F.lit(init_rank))

print("[AXE3] {} utilisateurs — {} itérations...".format(total_users, PAGERANK_ITER))

for i in range(PAGERANK_ITER):
    contributions = edges \
        .join(
            pr.withColumnRenamed("id", "src").withColumnRenamed("pagerank", "pr_src"),
            "src"
        ) \
        .join(out_degree, "src") \
        .withColumn("contrib", F.col("pr_src") / F.col("out_degree").cast(DoubleType())) \
        .groupBy(F.col("dst").alias("id")) \
        .agg(F.sum("contrib").alias("incoming"))

    pr = pr \
        .join(contributions, "id", "left") \
        .withColumn("pagerank",
            F.lit(dangling_contrib) +
            F.lit(DAMPING_FACTOR) * F.coalesce(F.col("incoming"), F.lit(0.0))
        ) \
        .select("id", "pagerank")

    if (i + 1) % 3 == 0:
        pr = pr.checkpoint()

    print("[AXE3] Itération {}/{} terminée".format(i + 1, PAGERANK_ITER))

edges.unpersist()
out_degree.unpersist()

user_pagerank = pr \
    .withColumnRenamed("id", "user_id") \
    .withColumn("pagerank", F.round(F.col("pagerank"), 6))

user_enriched = df_usr \
    .join(user_pagerank, "user_id", "left") \
    .na.fill(0.0, ["pagerank"]) \
    .select(
        "user_id", "fans", "average_stars", "account_age_years",
        "friends_count", "is_elite", "total_compliments", "pagerank"
    )

print("[AXE3] user_enriched : {} lignes".format(user_enriched.count()))

axe3_user_review = df_rev \
    .join(
        user_enriched.select(
            "user_id", "is_elite", "fans", "pagerank",
            "average_stars", "account_age_years",
            "friends_count", "total_compliments"
        ),
        "user_id", "inner"
    ) \
    .withColumn("text_length", F.length(F.col("text")))

print("[AXE3] user_review_enriched : {} lignes".format(axe3_user_review.count()))

axe3_elite_comparison = axe3_user_review.groupBy("is_elite").agg(
    F.count("*").alias("nb_reviews"),
    F.round(F.avg("stars"),       2).alias("avg_stars_given"),
    F.round(F.avg("text_length"), 0).alias("avg_text_length"),
    F.round(F.avg("useful"),      2).alias("avg_useful_received"),
    F.round(F.avg("funny"),       2).alias("avg_funny_received"),
    F.round(F.avg("cool"),        2).alias("avg_cool_received")
)

axe3_seniority = axe3_user_review \
    .withColumn("seniority_bucket",
        F.when(F.col("account_age_years") < 2,  "0-2 ans")
         .when(F.col("account_age_years") < 5,  "2-5 ans")
         .when(F.col("account_age_years") < 10, "5-10 ans")
         .otherwise("10+ ans")) \
    .groupBy("seniority_bucket") \
    .agg(
        F.count("*").alias("nb_reviews"),
        F.round(F.avg("stars"),       2).alias("avg_stars"),
        F.round(F.stddev("stars"),    2).alias("stddev_stars")
    )

print("[AXE3] elite_comparison   : {} lignes".format(axe3_elite_comparison.count()))
print("[AXE3] seniority_analysis : {} lignes".format(axe3_seniority.count()))

print("[AXE3] Export...")
user_enriched.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe3_social/user_enriched".format(OUTPUT_BASE))
axe3_elite_comparison.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe3_social/elite_comparison".format(OUTPUT_BASE))
axe3_seniority.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe3_social/seniority_analysis".format(OUTPUT_BASE))
axe3_user_review.select(
    "user_id", "business_id", "stars", "text_length",
    "useful", "is_elite", "fans", "pagerank", "account_age_years"
).coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe3_social/user_review_enriched".format(OUTPUT_BASE))
print("[AXE3] Export terminé")


# ============================================================
# ETAPE 6 — AXE 4 : TEMPORALITE
# ============================================================
print("\n" + "="*60)
print("[AXE4] Calcul Temporalité...")
print("="*60)

print("[AXE4] A4.1 — Explosion checkins...")
df_chk_exploded = df_chk \
    .withColumn("timestamp_str", F.explode(F.split(F.col("date"), ", "))) \
    .withColumn("timestamp", F.to_timestamp(F.trim(F.col("timestamp_str")), "yyyy-MM-dd HH:mm:ss")) \
    .filter(F.col("timestamp").isNotNull()) \
    .withColumn("year",        F.year("timestamp")) \
    .withColumn("month",       F.month("timestamp")) \
    .withColumn("day_of_week", F.dayofweek("timestamp")) \
    .withColumn("hour",        F.hour("timestamp")) \
    .drop("date", "timestamp_str")

print("[AXE4] checkins explodés : {} lignes".format(df_chk_exploded.count()))

print("[AXE4] A4.2 — Heatmap affluence (by_biz + globale)...")
axe4_heatmap_by_biz = df_chk_exploded \
    .groupBy("business_id", "day_of_week", "hour") \
    .agg(F.count("*").alias("checkin_count"))

axe4_heatmap_global = df_chk_exploded \
    .groupBy("day_of_week", "hour") \
    .agg(F.count("*").alias("total_checkins"))

df_rev_temp = df_rev \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("hour",  F.hour("date")) \
    .withColumn("month", F.month("date")) \
    .withColumn("year",  F.year("date"))

axe4_dow = df_rev_temp \
    .groupBy("day_of_week") \
    .agg(
        F.count("*").alias("nb_reviews"),
        F.round(F.avg("stars"), 3).alias("avg_stars")
    ) \
    .orderBy("day_of_week")

axe4_hour = df_rev_temp \
    .groupBy("hour") \
    .agg(
        F.count("*").alias("nb_reviews"),
        F.round(F.avg("stars"), 3).alias("avg_stars")
    ) \
    .orderBy("hour")

axe4_monthly = df_rev_temp \
    .groupBy("year", "month") \
    .agg(
        F.count("*").alias("nb_reviews"),
        F.round(F.avg("stars"), 2).alias("avg_stars")
    ) \
    .orderBy("year", "month")

chk_with_cat = df_chk_exploded.join(
    df_biz.select("business_id", "categories"), "business_id", "inner"
)

axe4_cat_hourly = chk_with_cat \
    .withColumn("type",
        F.when(F.col("categories").contains("Pizza"),        "Pizzeria")
         .when(F.col("categories").contains("Fine Dining"),  "Gastronomique")
         .otherwise(None)) \
    .filter(F.col("type").isNotNull()) \
    .groupBy("type", "hour") \
    .agg(F.count("*").alias("total_checkins"))

print("[AXE4] heatmap_by_biz    : {} lignes".format(axe4_heatmap_by_biz.count()))
print("[AXE4] heatmap_global    : {} lignes".format(axe4_heatmap_global.count()))
print("[AXE4] dow               : {} lignes".format(axe4_dow.count()))
print("[AXE4] hour              : {} lignes".format(axe4_hour.count()))
print("[AXE4] monthly           : {} lignes".format(axe4_monthly.count()))
print("[AXE4] cat_hourly        : {} lignes".format(axe4_cat_hourly.count()))

print("[AXE4] Export...")
axe4_heatmap_by_biz.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe4_temporal/heatmap_by_biz".format(OUTPUT_BASE))
axe4_heatmap_global.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe4_temporal/heatmap_global".format(OUTPUT_BASE))    
axe4_dow.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe4_temporal/stars_by_dow".format(OUTPUT_BASE))
axe4_hour.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe4_temporal/stars_by_hour".format(OUTPUT_BASE))
axe4_monthly.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe4_temporal/monthly_trend".format(OUTPUT_BASE))
axe4_cat_hourly.coalesce(1).write.mode("overwrite") \
    .parquet("{}/axe4_temporal/category_hourly".format(OUTPUT_BASE))
print("[AXE4] Export terminé")


# ============================================================
# FIN
# ============================================================
print("\n" + "="*60)
print("  SE3 ETL COMPLET v3 — TOUS LES LIVRABLES PRODUITS")
print("="*60)
print("  Outputs -> {}".format(OUTPUT_BASE))
print("  axe1/ : monthly_running_avg, business_with_attributes, closure_rate")
print("  axe2/ : avg_by_city, business_with_competitors")
print("  axe3/ : user_enriched, elite_comparison, seniority_analysis, user_review_enriched")
print("  axe4/ : heatmap_by_biz, heatmap_global, stars_by_dow,")
print("          stars_by_hour, monthly_trend, category_hourly")
print("  business_enriched/")
print("="*60 + "\n")

spark.stop()