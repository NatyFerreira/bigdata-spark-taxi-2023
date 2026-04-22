"""
===========================================================
NETTOYAGE SPARK - Pipeline de nettoyage des donnees
===========================================================

Adaptation du pipeline du professeur pour les donnees 2023,
qui sont fournies uniquement au format Parquet (et non CSV).
Ce script charge le dataset brut, applique un nettoyage
minimal (valeurs nulles + valeurs incoherentes) et sauvegarde
un dataset propre au format Parquet.
===========================================================
"""

# ----------------------------------------------------------
# 0) Importations
# ----------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ----------------------------------------------------------
# 1) Creation de la SparkSession
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("nettoyage_taxi") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------
# 2) Chargement du dataset brut (PARQUET)
# ----------------------------------------------------------
df = spark.read.parquet("yellow_tripdata_2023-01.parquet")

print("=== Apercu du dataset brut ===")
df.show(5)

# ----------------------------------------------------------
# 3) Nettoyage : suppression des valeurs nulles
# ----------------------------------------------------------
df_clean = df.dropna()

# ----------------------------------------------------------
# 4) Nettoyage : suppression des valeurs incoherentes
# ----------------------------------------------------------
df_clean = df_clean.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("passenger_count") > 0)
)

# 4.1) Tableau riche
from rich.console import Console
from rich.table import Table

console = Console()

nb_brut = df.count()

nb_clean = df_clean.count()
pct = (nb_clean / nb_brut) * 100

table = Table(title="Résumé du pipeline de nettoyage")
table.add_column("Métrique", style="cyan")
table.add_column("Valeur", style="green")

table.add_row("Lignes brutes", str(nb_brut))
table.add_row("Lignes après nettoyage", str(nb_clean))
table.add_row("Pourcentage conservé", f"{pct:.2f}%")

console.print(table)


# ----------------------------------------------------------
# 5) Sauvegarde du dataset nettoye
# ----------------------------------------------------------
df_clean.write.mode("overwrite").parquet("yellow_tripdata_2023-01-clean.parquet")

print("=== Nettoyage termine ===")
print("Nombre de lignes conservees :", df_clean.count())

# ----------------------------------------------------------
# 6) Fin du script
# ----------------------------------------------------------
spark.stop()

