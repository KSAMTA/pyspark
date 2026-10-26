import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("OptimizeSlowJob") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

trips = spark.read.parquet("yellow_tripdata_2024-01.parquet")
zones = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)

results = {}

# --- 1. BASELINE: no broadcast hint, AQE off, no repartitioning ---
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force NO broadcast, forces sort-merge join

start = time.time()
baseline = trips.join(zones, trips.PULocationID == zones.LocationID, "inner") \
    .groupBy("Zone") \
    .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
baseline_count = baseline.count()
results["baseline (no optimization)"] = time.time() - start
print("Baseline done:", results["baseline (no optimization)"], "seconds |", baseline_count, "rows")

print("\n=== Baseline plan ===")
baseline.explain()

# --- 2. BROADCAST JOIN ---
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # restore default (10MB)

start = time.time()
broadcast_version = trips.join(F.broadcast(zones), trips.PULocationID == zones.LocationID, "inner") \
    .groupBy("Zone") \
    .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
broadcast_count = broadcast_version.count()
results["broadcast join"] = time.time() - start
print("\nBroadcast join done:", results["broadcast join"], "seconds |", broadcast_count, "rows")

print("\n=== Broadcast join plan ===")
broadcast_version.explain()

# --- 3. REPARTITIONING (on top of broadcast) ---
start = time.time()
trips_repart = trips.repartition(8, "PULocationID")
repartitioned = trips_repart.join(F.broadcast(zones), trips_repart.PULocationID == zones.LocationID, "inner") \
    .groupBy("Zone") \
    .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
repart_count = repartitioned.count()
results["repartition + broadcast"] = time.time() - start
print("\nRepartition + broadcast done:", results["repartition + broadcast"], "seconds |", repart_count, "rows")

# --- 4. AQE ENABLED (on top of everything) ---
spark.conf.set("spark.sql.adaptive.enabled", "true")

start = time.time()
aqe_version = trips.join(F.broadcast(zones), trips.PULocationID == zones.LocationID, "inner") \
    .groupBy("Zone") \
    .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare"))
aqe_count = aqe_version.count()
results["AQE enabled + broadcast"] = time.time() - start
print("\nAQE version done:", results["AQE enabled + broadcast"], "seconds |", aqe_count, "rows")

# --- Summary ---
print("\n=== SUMMARY ===")
for k, v in results.items():
    print(f"{k:35s}: {v:.3f} seconds")

input("\nPress Enter to check Spark UI, then stop...")
spark.stop()