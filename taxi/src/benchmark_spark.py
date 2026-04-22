from pyspark.sql import SparkSession
import time

file = "yellow_tripdata_2023-01.parquet"

spark = SparkSession.builder \
    .appName("benchmark") \
    .master("local[*]") \
    .getOrCreate()

t0 = time.time()
df = spark.read.parquet(file)
t1 = time.time()

print("=== Benchmark Spark ===")
print(f"Tempo de carregamento: {t1 - t0:.3f} s")

df.selectExpr("avg(fare_amount)").show()

spark.stop()

