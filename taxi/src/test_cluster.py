from pyspark.sql import SparkSession

# --- Création de la session Spark ---
spark = SparkSession.builder.appName("TestCluster").getOrCreate()

print("\n=== Test du Cluster ===")

# --- Petit job simple ---
df = spark.range(1000000)
count = df.count()

print("Résultat du job :", count)
print("Le cluster fonctionne correctement.\n")

spark.stop()
