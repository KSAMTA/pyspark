from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYCTaxiAnalysis") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

trips = spark.read.parquet("yellow_tripdata_2024-01.parquet")
zones = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)

trips.printSchema()
trips.show(5)
zones.show(5)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Aggregation: trip count & avg fare by pickup zone
trips.groupBy("PULocationID") \
    .agg(F.count("*").alias("trip_count"), F.avg("fare_amount").alias("avg_fare")) \
    .orderBy(F.desc("trip_count")) \
    .show(10)

# Join with zone names
joined = trips.join(zones, trips.PULocationID == zones.LocationID, "left")
joined.select("PULocationID", "Zone", "fare_amount").show(5)

# Window function: top 3 fares per pickup zone
w = Window.partitionBy("PULocationID").orderBy(F.desc("fare_amount"))
trips.withColumn("rank", F.rank().over(w)) \
    .filter(F.col("rank") <= 3) \
    .show(20)

# Spark SQL syntax
trips.createOrReplaceTempView("trips")
spark.sql("""
    SELECT PULocationID, COUNT(*) AS trip_count
    FROM trips
    GROUP BY PULocationID
    ORDER BY trip_count DESC
    LIMIT 10
""").show()

# --- Execution plan analysis ---
print("=== Plan for groupBy aggregation ===")
trips.groupBy("PULocationID").count().explain(True)

print("=== Plan for join ===")
joined.explain(True)


add: input("Press Enter to stop Spark...")
spark.stop()