"""
===========================================================
VISUALISATION SPARK - Iteration 5
===========================================================

Ce script realise plusieurs visualisations classiques du
pipeline Big Data : histogrammes, repartitions horaires,
moyennes par categorie, etc. Les donnees proviennent du
dataset nettoye (Parquet).
===========================================================
"""

# ----------------------------------------------------------
# 0) Importations
# ----------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# ----------------------------------------------------------
# 1) SparkSession
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("visualisation_taxi") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------
# 2) Chargement du dataset nettoye
# ----------------------------------------------------------
df = spark.read.parquet("yellow_tripdata_2023-01-clean.parquet")

print("=== Apercu du dataset nettoye ===")
df.show(5)

# ----------------------------------------------------------
# 3) Histogramme du montant des courses
# ----------------------------------------------------------
pdf = df.select("fare_amount").sample(fraction=0.01).toPandas()

plt.figure(figsize=(8,5))
sns.histplot(pdf["fare_amount"], bins=50, kde=True, color="steelblue")
plt.title("Distribution du montant des courses (échantillon 1%)")
plt.xlabel("Montant ($)")
plt.ylabel("Fréquence")
plt.tight_layout()
plt.savefig("hist_fare_amount.png")
plt.close()

print("Histogramme sauvegarde : hist_fare_amount.png")

# ----------------------------------------------------------
# 4) Nombre de courses par heure
# ----------------------------------------------------------
df_hour = df.withColumn("hour", hour(col("tpep_pickup_datetime")))
df_hour_count = df_hour.groupBy("hour").count().orderBy("hour")

pdf_hour = df_hour_count.toPandas()

plt.figure(figsize=(8,5))
sns.barplot(data=pdf_hour, x="hour", y="count", color="darkorange")
plt.title("Nombre de courses par heure")
plt.xlabel("Heure")
plt.ylabel("Nombre de courses")
plt.tight_layout()
plt.savefig("courses_par_heure.png")
plt.close()

print("Graphique sauvegarde : courses_par_heure.png")

# ----------------------------------------------------------
# 5) Distance moyenne par zone de depart
# ----------------------------------------------------------
df_dist = df.groupBy("PULocationID").avg("trip_distance").orderBy("PULocationID")
pdf_dist = df_dist.toPandas()

plt.figure(figsize=(10,5))
sns.lineplot(data=pdf_dist, x="PULocationID", y="avg(trip_distance)", color="green")
plt.title("Distance moyenne par zone de depart")
plt.xlabel("Zone de depart (ID)")
plt.ylabel("Distance moyenne (miles)")
plt.tight_layout()
plt.savefig("distance_par_zone.png")
plt.close()

print("Graphique sauvegarde : distance_par_zone.png")

# ----------------------------------------------------------
# 6) Fin du script
# ----------------------------------------------------------
spark.stop()

