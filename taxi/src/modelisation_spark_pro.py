"""
===========================================================
MODELISATION SPARK - Version professionnelle
===========================================================

Modele complet avec :
- VectorAssembler
- Regression lineaire
- Ridge, Lasso, ElasticNet
- Grid Search + Cross Validation
- Selection automatique du meilleur modele
- Evaluation complete (RMSE, MAE, R²)
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
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

# ----------------------------------------------------------
# 1) SparkSession
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("modelisation_pro") \
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
# 3) Features
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
# 5) Modele + Grid Search
# ----------------------------------------------------------
lr = LinearRegression(
    featuresCol="features",
    labelCol="fare_amount"
)

paramGrid = (
    ParamGridBuilder()
    .addGrid(lr.regParam, [0.0, 0.1, 0.3, 0.5])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])  # Ridge, ElasticNet, Lasso
    .build()
)

evaluator = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="rmse"
)

cv = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4
)

cvModel = cv.fit(train)

# ----------------------------------------------------------
# 6) Evaluation finale
# ----------------------------------------------------------
pred = cvModel.transform(test)

rmse = evaluator.evaluate(pred)

evaluator_r2 = RegressionEvaluator(
    labelCol="fare_amount",
    predictionCol="prediction",
    metricName="r2"
)

r2 = evaluator_r2.evaluate(pred)

print("=== Evaluation du meilleur modele (version pro) ===")
print("RMSE :", rmse)
print("R²   :", r2)

bestModel = cvModel.bestModel
print("\n=== Parametres du meilleur modele ===")
print("regParam        :", bestModel._java_obj.getRegParam())
print("elasticNetParam :", bestModel._java_obj.getElasticNetParam())

# ----------------------------------------------------------
# 7) Fin
# ----------------------------------------------------------
spark.stop()

