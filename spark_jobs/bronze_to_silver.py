"""
Spark Job: Bronze → Silver

Διαβάζει raw stock_prices από PostgreSQL (Bronze),
κάνει cleaning + enrichment, και γράφει Silver layer.

Τι κάνει:
- Αφαιρεί duplicates
- Φιλτράρει invalid τιμές
- Προσθέτει moving averages (MA5, MA20)
- Προσθέτει price_change και pct_change
- Γράφει στον πίνακα stock_prices_silver
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Spark Session ──────────────────────────────────────────────────────────
# Το SparkSession είναι το entry point — χωρίς αυτό δεν γίνεται τίποτα
# .master("spark://spark-master:7077") = συνδέεται στον cluster μας
spark = SparkSession.builder \
    .appName("FinSight Bronze to Silver") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── PostgreSQL connection ──────────────────────────────────────────────────
PG_URL = "jdbc:postgresql://finsight-postgres:5432/finsight_db"
PG_PROPS = {
    "user":     "finsight",
    "password": "finsight123",
    "driver":   "org.postgresql.Driver",
}

print("=" * 50)
print("  Bronze → Silver transformation")
print("=" * 50)

# ── Step 1: Διάβασε Bronze ─────────────────────────────────────────────────
# Το Spark διαβάζει όλο τον πίνακα ως DataFrame
# DataFrame = σαν pandas DataFrame αλλά distributed
print("\n[1/5] Διαβάζω Bronze layer...")
bronze_df = spark.read.jdbc(url=PG_URL, table="stock_prices", properties=PG_PROPS)
total_bronze = bronze_df.count()
print(f"      {total_bronze} rows στο Bronze")

# ── Step 2: Cleaning ───────────────────────────────────────────────────────
print("\n[2/5] Cleaning...")

silver_df = bronze_df \
    .filter(F.col("price") > 0) \
    .filter(F.col("volume") > 0) \
    .filter(F.col("price").isNotNull()) \
    .dropDuplicates(["symbol", "event_time"])

print(f"      {silver_df.count()} rows μετά το cleaning "
      f"(αφαιρέθηκαν {total_bronze - silver_df.count()})")

# ── Step 3: Moving Averages ────────────────────────────────────────────────
# Window function: υπολογίζει για κάθε row λαμβάνοντας υπόψη
# τα προηγούμενα N rows του ίδιου symbol, ταξινομημένα χρονολογικά
print("\n[3/5] Υπολογίζω moving averages...")

window_5  = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-4, 0)
window_20 = Window.partitionBy("symbol").orderBy("event_time").rowsBetween(-19, 0)
window_lag = Window.partitionBy("symbol").orderBy("event_time")

silver_df = silver_df \
    .withColumn("ma5",  F.round(F.avg("price").over(window_5),  4)) \
    .withColumn("ma20", F.round(F.avg("price").over(window_20), 4)) \
    .withColumn("prev_price", F.lag("price", 1).over(window_lag)) \
    .withColumn("price_change", F.round(F.col("price") - F.col("prev_price"), 4)) \
    .withColumn("pct_change",
        F.round((F.col("price") - F.col("prev_price")) / F.col("prev_price") * 100, 4)
    ) \
    .drop("prev_price")

# ── Step 4: Προσθήκη metadata ──────────────────────────────────────────────
print("\n[4/5] Προσθήκη metadata...")

silver_df = silver_df \
    .withColumn("processed_at", F.current_timestamp()) \
    .withColumn("layer", F.lit("silver"))

# ── Step 5: Γράψε Silver ───────────────────────────────────────────────────
print("\n[5/5] Γράφω Silver layer στη PostgreSQL...")

silver_df.write \
    .option("truncate", "true") \
    .jdbc(
        url=PG_URL,
        table="stock_prices_silver",
        mode="overwrite",    # overwrite: αντικαθιστά κάθε φορά
        properties=PG_PROPS
    )

final_count = silver_df.count()
print(f"\n✓ Silver layer: {final_count} rows")
print("\nSample data:")
silver_df.select("symbol", "price", "ma5", "ma20", "pct_change", "event_time") \
         .orderBy("symbol", "event_time") \
         .show(10, truncate=False)

spark.stop()
print("=" * 50)
print("  Bronze → Silver COMPLETE")
print("=" * 50)



# RUN SPARK JOB 
# docker exec finsight-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/bronze_to_silver.py