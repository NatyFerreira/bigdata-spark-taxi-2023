"""
===========================================================
ANALYSE SPARK - Statistiques et agregations
===========================================================

Ce script charge le dataset nettoye, calcule plusieurs
statistiques descriptives et produit des aggregations utiles
pour l'analyse exploratoire. Chaque section est commentee
pour faciliter la comprehension et l'integration dans le
rapport final.
===========================================================
"""

# ----------------------------------------------------------
# 0) Importations
# ----------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")  # Supprime les warnings Python

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg, count
from rich.console import Console
from rich.table import Table

console = Console()

# ----------------------------------------------------------
# 1) Creation de la SparkSession
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("analyse_taxi") \
    .master("local[*]") \
    .getOrCreate()

# Supprime les logs Spark
spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------
# 2) Chargement du dataset nettoye
# ----------------------------------------------------------
df = spark.read.parquet("yellow_tripdata_2023-01-clean.parquet")

print("=== Apercu du dataset nettoye ===")
df.show(5)

# ----------------------------------------------------------
# 3) Statistiques descriptives globales
# ----------------------------------------------------------
print("=== Statistiques descriptives ===")
df.describe().show()

# ----------------------------------------------------------
# 4) Moyenne des montants par type de paiement
# ----------------------------------------------------------
print("=== Montant moyen par type de paiement ===")
df_payment = df.groupBy("payment_type") \
  .agg(avg("fare_amount").alias("moyenne_fare")) \
  .orderBy("payment_type")

# --- Tabela bonita com rich ---
table = Table(title="Montant moyen par type de paiement")
table.add_column("Type de paiement", style="cyan")
table.add_column("Montant moyen ($)", style="green")

for row in df_payment.collect():
    table.add_row(str(row["payment_type"]), f"{row['moyenne_fare']:.2f}")

console.print(table)

# ----------------------------------------------------------
# 5) Nombre de courses par heure de la journee
# ----------------------------------------------------------
print("=== Nombre de courses par heure ===")
df_hours = df.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
  .groupBy("hour") \
  .agg(count("*").alias("nb_courses")) \
  .orderBy("hour")

# --- Tabela bonita ---
table = Table(title="Nombre de courses par heure")
table.add_column("Heure", style="cyan")
table.add_column("Nombre de courses", style="green")

for row in df_hours.collect():
    table.add_row(str(row["hour"]), str(row["nb_courses"]))

console.print(table)

# ----------------------------------------------------------
# 6) Distance moyenne par zone de depart
# ----------------------------------------------------------
print("=== Distance moyenne par zone de depart ===")
df_dist = df.groupBy("PULocationID") \
  .agg(avg("trip_distance").alias("distance_moyenne")) \
  .orderBy("PULocationID")

# --- Tabela bonita ---
table = Table(title="Distance moyenne par zone de depart")
table.add_column("Zone", style="cyan")
table.add_column("Distance moyenne (miles)", style="green")

for row in df_dist.collect():
    table.add_row(str(row["PULocationID"]), f"{row['distance_moyenne']:.2f}")

console.print(table)

# ----------------------------------------------------------
# 7) Fin du script
# ----------------------------------------------------------
spark.stop()

