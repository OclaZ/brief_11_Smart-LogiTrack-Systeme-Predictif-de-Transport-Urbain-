# scripts/train_model.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import sys
import os

def clean_anomalies(df):
    """
    Fonction de nettoyage issue de l'EDA.
    Filtre les valeurs aberrantes (durée, distance, montant).
    """
    # Limites définies dans l'EDA
    MIN_DURATION = 1
    MAX_DURATION = 240
    MIN_FARE = 0
    
    return df.filter(
        (col("duration_minutes") >= MIN_DURATION) & 
        (col("duration_minutes") <= MAX_DURATION) & 
        (col("fare_amount") > MIN_FARE) &
        (col("trip_distance") > 0)
    )

def main():
    # 1. Initialisation de la Session Spark
    spark = SparkSession.builder \
        .appName("Taxi_ETA_Training_GBT") \
        .getOrCreate()
    
    # --- CHEMIN DOCKER POUR LES DONNEES SILVER ---
    silver_path = "/opt/airflow/data/silver"
    
    print(f"Chargement des données Silver depuis {silver_path}...")
    try:
        # Vérification basique si le dossier existe ou non
        df_silver = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"❌ Erreur lors du chargement des données : {e}")
        # Arrête le script avec erreur pour qu'Airflow le détecte
        sys.exit(1)

    # 2. Nettoyage Préalable
    print("Nettoyage des anomalies...")
    df_clean = clean_anomalies(df_silver)
    
    # Suppression des valeurs nulles dans les colonnes utilisées
    features_cols = ["trip_distance", "passenger_count", "pickuphour", "dayof_week", "month"]
    df_clean = df_clean.dropna(subset=features_cols + ["duration_minutes"])

    # 3. Split Train / Test
    train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
    print(f"Données d'entraînement : {train_df.count()} lignes")
    print(f"Données de test : {test_df.count()} lignes")

    # 4. Construction du Pipeline
    # Etape A : Assemblage des features
    assembler = VectorAssembler(
        inputCols=features_cols,
        outputCol="features_raw"
    )
    
    # Etape B : Standardisation
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    
    # Etape C : Modèle GBT
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="duration_minutes",
        maxIter=100,
        stepSize=0.1,
        seed=42
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    # 5. Entraînement
    print("🏋️ Entraînement du modèle GBT en cours...")
    model = pipeline.fit(train_df)
    
    # 6. Évaluation
    print("Évaluation du modèle...")
    predictions = model.transform(test_df)
    
    evaluator_rmse = RegressionEvaluator(
        labelCol="duration_minutes",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    evaluator_r2 = RegressionEvaluator(
        labelCol="duration_minutes",
        predictionCol="prediction",
        metricName="r2"
    )
    
    rmse = evaluator_rmse.evaluate(predictions)
    r2 = evaluator_r2.evaluate(predictions)
    
    print(f"Résultats sur le Test Set :")
    print(f"RMSE : {rmse:.4f}")
    print(f"R²   : {r2:.4f}")

    # 7. Sauvegarde du Modèle
    # --- CHEMIN DOCKER POUR SAUVEGARDER LE MODELE ---
    model_path = "/opt/airflow/models/eta_gbt_pipeline"
    print(f"💾 Sauvegarde du modèle dans : {model_path}")
    
    # overwrite() permet d'écraser une ancienne version
    model.write().overwrite().save(model_path)
    
    print("✅ Modèle sauvegardé avec succès.")
    spark.stop()

if __name__ == "__main__":
    main()