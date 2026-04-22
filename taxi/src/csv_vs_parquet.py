from pyspark.sql import SparkSession
import time
import os

# --- Création de la session Spark ---
spark = SparkSession.builder.appName("CSVvsParquet").getOrCreate()

# --- Chemins relatifs (dans le même dossier que le script) ---
csv_path = "yellow_tripdata_2023-01.csv"
parquet_path = "yellow_tripdata_2023-01.parquet"

def mesurer_temps(fonction):
    debut = time.time()
    resultat = fonction()
    fin = time.time()
    return resultat, fin - debut

print("\n=== Comparaison CSV vs Parquet ===")

# --- Lecture CSV ---
df_csv, t_csv_lecture = mesurer_temps(
    lambda: spark.read.csv(csv_path, header=True, inferSchema=True)
)
_, t_csv_projection = mesurer_temps(
    lambda: df_csv.select("tpep_pickup_datetime", "trip_distance", "total_amount").count()
)

# --- Lecture Parquet ---
df_parquet, t_parquet_lecture = mesurer_temps(
    lambda: spark.read.parquet(parquet_path)
)
_, t_parquet_projection = mesurer_temps(
    lambda: df_parquet.select("tpep_pickup_datetime", "trip_distance", "total_amount").count()
)

print(f"Temps lecture CSV : {t_csv_lecture:.3f} s")
print(f"Temps lecture Parquet : {t_parquet_lecture:.3f} s")
print(f"Temps projection CSV : {t_csv_projection:.3f} s")
print(f"Temps projection Parquet : {t_parquet_projection:.3f} s\n")

spark.stop()
