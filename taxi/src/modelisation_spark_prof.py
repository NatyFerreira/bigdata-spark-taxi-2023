"""
===========================================================
MODELISATION SPARK - Version professeur
===========================================================

Modele simple de regression lineaire pour predire le montant
d'une course de taxi a partir de quelques variables
explicatives. Version conforme au cours.
===========================================================
"""

# ----------------------------------------------------------
# 0) Importations
# ----------------------------------------------------------
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# ----------------------------------------------------------
# 1) SparkSession
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("modelisation_prof") \
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
# 3) Selection des features
# ----------------------------------------------------------
features = [
    "trip_distance",
    "passenger_count",
    "PULocationID",
    "DOLocationID"
]

assembler = VectorAssembler(
    inputCols=features,
    outputCol="features"
)

df_ml = assembler.transform(df).select("features", "fare_amount")

# ----------------------------------------------------------
# 4) Train/test split
# ----------------------------------------------------------
train, test = df_ml.randomSplit([0.8, 0.2], seed=42)

# ----------------------------------------------------------
# 5) Modele de regression lineaire
# ----------------------------------------------------------
lr = LinearRegression(
    featuresCol="features",
    labelCol="fare_amount"
)

model = lr.fit(train)

# ----------------------------------------------------------
# 6) Evaluation
# ----------------------------------------------------------
pred = model.transform(test)

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

rmse = evaluator_rmse.evaluate(pred)
r2 = evaluator_r2.evaluate(pred)

print("=== Evaluation du modele (version professeur) ===")
print("RMSE :", rmse)
print("R²   :", r2)

# ----------------------------------------------------------
# 7) Fin
# ----------------------------------------------------------
spark.stop()

