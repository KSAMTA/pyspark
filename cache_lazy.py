import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("LazyEvalAndCaching") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

trips = spark.read.parquet("yellow_tripdata_2024-01.parquet")

# --- 1. Build a transformation chain (nothing runs yet) ---
chain = trips \
    .filter(F.col("fare_amount") > 10) \
    .select("PULocationID", "fare_amount", "trip_distance", "tip_amount") \
    .withColumn("tip_pct", F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2))

print("Chain built — no data has been processed yet (lazy evaluation).")
chain.explain()  # shows the plan, still doesn't execute

# --- 2. Run WITHOUT caching, twice, time it ---
start = time.time()
print("Row count (run 1, no cache):", chain.count())
print("Time:", time.time() - start, "seconds")

start = time.time()
print("Row count (run 2, no cache):", chain.count())
print("Time:", time.time() - start, "seconds")

# --- 3. Cache it, then run twice, time it ---
chain_cached = chain.cache()

start = time.time()
print("Row count (run 1, WITH cache):", chain_cached.count())  # this triggers caching
print("Time:", time.time() - start, "seconds")

start = time.time()
print("Row count (run 2, WITH cache):", chain_cached.count())  # should be much faster
print("Time:", time.time() - start, "seconds")

input("Press Enter to check Spark UI (Storage + Jobs tabs), then stop...")

chain_cached.unpersist()
spark.stop()