# scripts/train_model.py
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import shutil
import os

# Chemins
SILVER_DATA_PATH = "/opt/airflow/data/silver"
MODEL_PATH = "/opt/airflow/models/eta_gbt_pipeline"
TEMP_MODEL_PATH = "/tmp/eta_gbt_pipeline_temp" # Dossier temporaire interne

def main():
    # 1. Init Spark avec configuration anti-permission
    spark = SparkSession.builder \
        .appName("Train_ETA_Model") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Chargement des données Silver depuis {SILVER_DATA_PATH}...")
    try:
        df = spark.read.parquet(SILVER_DATA_PATH)
    except Exception as e:
        print(f"Erreur critique : Impossible de lire les données. {e}")
        return

    # 2. Préparation des données
    print("Nettoyage des anomalies...")
    df_clean = df.filter(df.duration_minutes > 0).dropna()

    # Feature Engineering simple
    feature_cols = ["passenger_count", "trip_distance", "pickuphour", "dayof_week", "month", "payment_type"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Split Train/Test
    train_data, test_data = df_clean.randomSplit([0.8, 0.2], seed=42)
    print(f"Données d'entraînement : {train_data.count()} lignes")
    print(f"Données de test : {test_data.count()} lignes")

    # 3. Pipeline GBT (Gradient Boosted Trees)
    gbt = GBTRegressor(featuresCol="features", labelCol="duration_minutes", maxIter=10)
    pipeline = Pipeline(stages=[assembler, gbt])

    print("🏋️ Entraînement du modèle GBT en cours...")
    model = pipeline.fit(train_data)

    # 4. Évaluation
    print("Évaluation du modèle...")
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="duration_minutes", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    
    r2_evaluator = RegressionEvaluator(labelCol="duration_minutes", predictionCol="prediction", metricName="r2")
    r2 = r2_evaluator.evaluate(predictions)

    print("Résultats sur le Test Set :")
    print(f"RMSE : {rmse:.4f}")
    print(f"R²   : {r2:.4f}")

    # 5. Sauvegarde Sécurisée (Workaround Docker/Windows)
    print(f"💾 Sauvegarde temporaire dans : {TEMP_MODEL_PATH}")
    
    # A. Écriture dans le dossier temporaire Linux (pas de conflit de droits)
    if os.path.exists(TEMP_MODEL_PATH):
        shutil.rmtree(TEMP_MODEL_PATH)
    model.write().overwrite().save(TEMP_MODEL_PATH)

    # B. Copie vers le volume partagé SANS les métadonnées de permissions
    print(f"📦 Déplacement vers le dossier final : {MODEL_PATH}")
    if os.path.exists(MODEL_PATH):
        shutil.rmtree(MODEL_PATH)

    def copy_content_only(src, dst):
        shutil.copyfile(src, dst)

    try:
        shutil.copytree(TEMP_MODEL_PATH, MODEL_PATH, copy_function=copy_content_only)
        print("✅ Modèle sauvegardé avec succès (Permissions contournées).")
    except Exception as e:
        print(f"⚠️ Erreur lors de la copie finale : {e}")

    spark.stop()

if __name__ == "__main__":
    main()