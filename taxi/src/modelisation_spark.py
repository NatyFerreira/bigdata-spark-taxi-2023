"""
===========================================================
MODELISATION SPARK - Regression lineaire (MLlib)
===========================================================

Ce script entraine un modele de regression lineaire pour
predire le montant des courses de taxi. Toutes les etapes
sont commentees pour faciliter l'analyse et l'integration
dans le rapport final.
===========================================================
"""

# ----------------------------------------------------------
# 0) Importations
# ----------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# ----------------------------------------------------------
# 1) Creation de la SparkSession
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("modelisation_taxi") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ----------------------------------------------------------
# 2) Chargement du dataset nettoye
# ----------------------------------------------------------
df = spark.read.parquet("yellow_tripdata_2023-01-clean.parquet")

# Ajout de la colonne "hour"
df = df.withColumn("hour", hour(col("tpep_pickup_datetime")))

print("=== Apercu du dataset ===")
df.show(5)

# ----------------------------------------------------------
# 3) Selection des variables explicatives
# ----------------------------------------------------------
features = [
    "trip_distance",
    "passenger_count",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "hour"
]

assembler = VectorAssembler(
    inputCols=features,
    outputCol="features"
)

df_ml = assembler.transform(df).select("features", "fare_amount")

# ----------------------------------------------------------
# 4) Separation train/test
# ----------------------------------------------------------
train, test = df_ml.randomSplit([0.8, 0.2], seed=42)

# ----------------------------------------------------------
# 5) Entrainement du modele
# ----------------------------------------------------------
lr = LinearRegression(
    featuresCol="features",
    labelCol="fare_amount"
)

model = lr.fit(train)

print("=== Coefficients du modele ===")
print(model.coefficients)
print("Intercept :", model.intercept)

# ----------------------------------------------------------
# 6) Predictions
# ----------------------------------------------------------
predictions = model.transform(test)

# ----------------------------------------------------------
# 7) Evaluation
# ----------------------------------------------------------
evaluator_rmse = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="rmse"
)

evaluator_r2 = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="r2"
)

rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print("=== Evaluation du modele ===")
print("RMSE :", rmse)
print("R2   :", r2)

# ----------------------------------------------------------
# 8) Fin du script
# ----------------------------------------------------------
spark.stop()

