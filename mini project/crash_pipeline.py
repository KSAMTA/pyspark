import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("NYCCrashPipeline") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# IMPORTANT: disable strict ANSI casting so invalid numeric strings
# (e.g. "Unspecified" landing in a numeric column due to messy CSV data)
# become NULL instead of crashing the job.
spark.conf.set("spark.sql.ansi.enabled", "false")

phase_times = {}

# ============ 1. INGEST ============
t0 = time.time()
crashes = spark.read.csv(
    "nyc_collisions.csv",
    header=True,
    inferSchema=True,
    multiLine=True,   # handles rows with embedded newlines/commas in quoted fields
    escape='"'
)

raw_count = crashes.count()
phase_times["ingest"] = time.time() - t0
print(f"[INGEST] Loaded {raw_count:,} raw crash records in {phase_times['ingest']:.2f}s")
crashes.printSchema()

# ============ 2. CLEAN ============
t0 = time.time()

# Standardize column names (original has spaces)
for col in crashes.columns:
    crashes = crashes.withColumnRenamed(col, col.strip().replace(" ", "_").upper())

# Safe numeric casts: .cast("int") now returns NULL instead of erroring
# thanks to ansi.enabled=false above, then we coalesce NULL -> 0
cleaned = crashes \
    .filter(F.col("CRASH_DATE").isNotNull()) \
    .filter(F.col("BOROUGH").isNotNull()) \
    .withColumn("NUMBER_OF_PERSONS_INJURED",
                F.coalesce(F.col("NUMBER_OF_PERSONS_INJURED").cast("int"), F.lit(0))) \
    .withColumn("NUMBER_OF_PERSONS_KILLED",
                F.coalesce(F.col("NUMBER_OF_PERSONS_KILLED").cast("int"), F.lit(0))) \
    .dropDuplicates(["COLLISION_ID"])

cleaned_count = cleaned.count()
removed = raw_count - cleaned_count
phase_times["clean"] = time.time() - t0
print(f"[CLEAN] Kept {cleaned_count:,} rows, removed {removed:,} "
      f"({removed/raw_count*100:.2f}%) in {phase_times['clean']:.2f}s")

# ============ 3. TRANSFORM ============
t0 = time.time()

transformed = cleaned \
    .withColumn("crash_date_parsed", F.to_date("CRASH_DATE", "MM/dd/yyyy")) \
    .withColumn("crash_year", F.year("crash_date_parsed")) \
    .withColumn("crash_month", F.month("crash_date_parsed")) \
    .withColumn("crash_dayofweek", F.date_format("crash_date_parsed", "EEEE")) \
    .withColumn("crash_hour", F.hour(F.col("CRASH_TIME"))) \
    .withColumn("total_injured", F.col("NUMBER_OF_PERSONS_INJURED").cast("int")) \
    .withColumn("total_killed", F.col("NUMBER_OF_PERSONS_KILLED").cast("int")) \
    .withColumn("is_fatal", F.when(F.col("total_killed") > 0, True).otherwise(False)) \
    .withColumn("BOROUGH", F.initcap(F.col("BOROUGH")))

# Filter out rows where date/hour parsing failed
transformed = transformed.filter(
    F.col("crash_date_parsed").isNotNull() & F.col("crash_hour").isNotNull()
)

transformed_count = transformed.count()
phase_times["transform"] = time.time() - t0
print(f"[TRANSFORM] {transformed_count:,} rows after transform in {phase_times['transform']:.2f}s")

transformed.cache()

# ============ 4. AGGREGATE ============
t0 = time.time()

print("\n--- Crashes & injuries by borough ---")
by_borough = transformed.groupBy("BOROUGH") \
    .agg(F.count("*").alias("crash_count"),
         F.sum("total_injured").alias("total_injured"),
         F.sum("total_killed").alias("total_killed")) \
    .orderBy(F.desc("crash_count"))
by_borough.show(truncate=False)

print("--- Crashes by hour of day ---")
by_hour = transformed.groupBy("crash_hour") \
    .agg(F.count("*").alias("crash_count"),
         F.sum("total_injured").alias("total_injured")) \
    .orderBy("crash_hour")
by_hour.show(24)

print("--- Crashes by day of week ---")
by_day = transformed.groupBy("crash_dayofweek") \
    .agg(F.count("*").alias("crash_count")) \
    .orderBy(F.desc("crash_count"))
by_day.show()

print("--- Top contributing factors ---")
top_factors = transformed.filter(F.col("CONTRIBUTING_FACTOR_VEHICLE_1").isNotNull()) \
    .filter(F.col("CONTRIBUTING_FACTOR_VEHICLE_1") != "Unspecified") \
    .groupBy("CONTRIBUTING_FACTOR_VEHICLE_1") \
    .agg(F.count("*").alias("crash_count")) \
    .orderBy(F.desc("crash_count"))
top_factors.show(10, truncate=False)

print("--- Deadliest crash per borough (window function) ---")
w = Window.partitionBy("BOROUGH").orderBy(F.desc("total_killed"), F.desc("total_injured"))
deadliest = transformed.withColumn("rank", F.rank().over(w)) \
    .filter(F.col("rank") == 1) \
    .select("BOROUGH", "CRASH_DATE", "total_killed", "total_injured")
deadliest.show(truncate=False)

phase_times["aggregate"] = time.time() - t0
print(f"[AGGREGATE] Done in {phase_times['aggregate']:.2f}s")

# ============ 5. OUTPUT ============
t0 = time.time()

by_borough.coalesce(1).write.mode("overwrite").option("header", True).csv("output/crashes_by_borough")
by_hour.coalesce(1).write.mode("overwrite").option("header", True).csv("output/crashes_by_hour")
by_day.coalesce(1).write.mode("overwrite").option("header", True).csv("output/crashes_by_day")
top_factors.coalesce(1).write.mode("overwrite").option("header", True).csv("output/top_factors")

phase_times["output"] = time.time() - t0
print(f"[OUTPUT] Results written to ./output/ in {phase_times['output']:.2f}s")

# ============ SUMMARY ============
print("\n=== PIPELINE SUMMARY ===")
print(f"Raw rows:       {raw_count:,}")
print(f"Cleaned rows:   {cleaned_count:,}  ({removed/raw_count*100:.2f}% removed)")
print(f"Transformed:    {transformed_count:,}")
print("\nPhase timings:")
for phase, t in phase_times.items():
    print(f"  {phase:12s}: {t:.2f}s")
print(f"  {'TOTAL':12s}: {sum(phase_times.values()):.2f}s")

transformed.unpersist()
spark.stop()