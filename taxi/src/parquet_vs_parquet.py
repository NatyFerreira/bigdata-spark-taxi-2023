from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("ParquetVsPartitioned")
    .getOrCreate()
)

def mesurer_temps(operation, description):
    debut = time.time()
    resultat = operation()
    fin = time.time()
    duree = fin - debut
    print(f"{description} : {duree:.3f} secondes")
    return resultat, duree

# -------------------------------------------------------------------
# Lecture du Parquet (flat) depuis l’URL
# -------------------------------------------------------------------
parquet_flat = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

df_flat, t_flat_read = mesurer_temps(
    lambda: spark.read.parquet(parquet_flat),
    "Lecture Parquet (flat)"
)

# -------------------------------------------------------------------
# Création d’une version partitionnée EN MÉMOIRE
# -------------------------------------------------------------------
df_part, t_part_read = mesurer_temps(
    lambda: df_flat.repartition("passenger_count"),
    "Repartition (in-memory) sur passenger_count"
)

# -------------------------------------------------------------------
# Projection
# -------------------------------------------------------------------
colonnes = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance"]

_, t_flat_proj = mesurer_temps(
    lambda: df_flat.select(*colonnes).limit(100000).collect(),
    "Projection Parquet (flat)"
)

_, t_part_proj = mesurer_temps(
    lambda: df_part.select(*colonnes).limit(100000).collect(),
    "Projection Parquet (partitionné)"
)

# -------------------------------------------------------------------
# Agrégation
# -------------------------------------------------------------------
_, t_flat_agg = mesurer_temps(
    lambda: df_flat.groupBy("passenger_count").count().collect(),
    "Agrégation Parquet (flat)"
)

_, t_part_agg = mesurer_temps(
    lambda: df_part.groupBy("passenger_count").count().collect(),
    "Agrégation Parquet (partitionné)"
)

# -------------------------------------------------------------------
# Résumé
# -------------------------------------------------------------------
print("\n=== Résumé ===")
print(f"Lecture flat          : {t_flat_read:.3f} s")
print(f"Repartition (in-mem)  : {t_part_read:.3f} s")
print(f"Projection flat       : {t_flat_proj:.3f} s")
print(f"Projection partitionné: {t_part_proj:.3f} s")
print(f"Agrégation flat       : {t_flat_agg:.3f} s")
print(f"Agrégation partitionné: {t_part_agg:.3f} s")

spark.stop()
