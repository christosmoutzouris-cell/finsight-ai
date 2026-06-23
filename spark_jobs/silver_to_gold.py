"""
Spark Job: Silver → Gold

Διαβάζει Silver layer και δημιουργεί aggregated metrics
έτοιμα για dashboard και AI analysis.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("FinSight Silver to Gold") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PG_URL = "jdbc:postgresql://finsight-postgres:5432/finsight_db"
PG_PROPS = {
    "user":     "finsight",
    "password": "finsight123",
    "driver":   "org.postgresql.Driver",
}

print("=" * 50)
print("  Silver → Gold transformation")
print("=" * 50)

# ── Διάβασε Silver ─────────────────────────────────────────────────────────
silver_df = spark.read.jdbc(url=PG_URL, table="stock_prices_silver", properties=PG_PROPS)
print(f"\nΔιάβασα {silver_df.count()} rows από Silver")

# ── Gold Table 1: Daily metrics per symbol ─────────────────────────────────
# Aggregation ανά symbol ανά ημέρα
print("\nΔημιουργώ gold_daily_metrics...")

gold_daily = silver_df \
    .withColumn("date", F.to_date("event_time")) \
    .groupBy("symbol", "date") \
    .agg(
        F.first("price").alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.last("price").alias("close"),
        F.sum("volume").alias("total_volume"),
        F.count("*").alias("tick_count"),
        F.avg("pct_change").alias("avg_pct_change"),
        F.stddev("price").alias("price_volatility"),
        F.last("ma5").alias("ma5"),
        F.last("ma20").alias("ma20"),
    ) \
    .withColumn("avg_pct_change",  F.round("avg_pct_change",  4)) \
    .withColumn("price_volatility", F.round("price_volatility", 4)) \
    .withColumn("processed_at", F.current_timestamp())

gold_daily.write \
    .option("truncate", "true") \
    .jdbc(
        url=PG_URL, table="gold_daily_metrics",
        mode="overwrite", properties=PG_PROPS
    )
print(f"✓ gold_daily_metrics: {gold_daily.count()} rows")

# ── Gold Table 2: Symbol comparison (latest snapshot) ─────────────────────
print("\nΔημιουργώ gold_symbol_snapshot...")

window_latest = Window.partitionBy("symbol").orderBy(F.desc("event_time"))

gold_snapshot = silver_df \
    .withColumn("rank", F.rank().over(window_latest)) \
    .filter(F.col("rank") == 1) \
    .select(
        "symbol", "price", "ma5", "ma20",
        "pct_change", "volume", "event_time"
    ) \
    .withColumn("ma5_signal",
        F.when(F.col("price") > F.col("ma5"), "BULLISH")
         .otherwise("BEARISH")
    ) \
    .withColumn("processed_at", F.current_timestamp())

gold_snapshot.write \
    .option("truncate", "true") \
    .jdbc(
        url=PG_URL, table="gold_symbol_snapshot",
        mode="overwrite", properties=PG_PROPS
    )
print(f"✓ gold_symbol_snapshot: {gold_snapshot.count()} rows")

print("\nLatest snapshot:")
gold_snapshot.select("symbol", "price", "ma5", "ma20", "ma5_signal", "pct_change") \
             .show(truncate=False)

spark.stop()
print("=" * 50)
print("  Silver → Gold COMPLETE")
print("=" * 50)



# spark jobs http://localhost:8082

# RUN SPARK JOB docker exec finsight-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark_jobs/silver_to_gold.py

# CHECKS 
#docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) as bronze FROM stock_prices; "
#docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT count(*) as silver FROM stock_prices_silver;"
#docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, price, ma5, ma20, pct_change FROM stock_prices_silver ORDER BY event_time DESC LIMIT 5;"
#docker exec finsight-postgres psql -U finsight -d finsight_db -c "SELECT symbol, price, ma5_signal, pct_change FROM gold_symbol_snapshot;"