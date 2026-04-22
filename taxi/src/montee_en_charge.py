from pyspark.sql import SparkSession
import time

# --- Création de la session Spark ---
spark = SparkSession.builder.appName("MonteeEnCharge").getOrCreate()

print("\n=== Montée en charge ===")

def job():
    df = spark.range(50000000)
    return df.count()

temps = []

# --- Exécutions successives ---
for i in range(4):
    debut = time.time()
    resultat = job()
    fin = time.time()
    duree = fin - debut
    temps.append(duree)
    print(f"Exécution {i+1} : {duree:.3f} s")

print("\nTemps mesurés :", temps)
print("Analysez ces valeurs selon le nombre de workers disponibles.\n")

spark.stop()
