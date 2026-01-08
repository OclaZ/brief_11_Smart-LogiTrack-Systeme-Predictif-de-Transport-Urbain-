from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import sys

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
    
    print("Chargement des données Silver...")
    # Assurez-vous que ce chemin pointe bien vers vos données nettoyées/enrichies
    try:
        df_silver = spark.read.parquet("data/silver")
    except Exception as e:
        print(f"Erreur lors du chargement des données : {e}")
        sys.exit(1)

    # 2. Nettoyage Préalable
    # Note : Le filtrage des lignes se fait avant le Pipeline (qui traite les colonnes)
    print("Nettoyage des anomalies...")
    df_clean = clean_anomalies(df_silver)
    
    # Suppression des éventuelles valeurs nulles dans les features utilisées
    features_cols = ["trip_distance", "passenger_count", "pickuphour", "dayof_week", "month"]
    df_clean = df_clean.dropna(subset=features_cols + ["duration_minutes"])

    # 3. Split Train / Test
    train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
    print(f"Données d'entraînement : {train_df.count()} lignes")
    print(f"Données de test : {test_df.count()} lignes")

    # 4. Construction du Pipeline
    # Etape A : Assemblage des features en un vecteur unique
    assembler = VectorAssembler(
        inputCols=features_cols,
        outputCol="features_raw"
    )
    
    # Etape B : Standardisation (Moyenne et Ecart-type) - Comme fait dans l'EDA
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,
        withStd=True
    )
    
    # Etape C : Modèle GBT (Gradient Boosted Tree)
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol="duration_minutes",
        maxIter=100,      # Peut être ajusté
        stepSize=0.1,     # Learning rate
        seed=42
    )
    
    # Création de l'objet Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    # 5. Entraînement
    print("Entraînement du modèle GBT en cours...")
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
    # On sauvegarde le Pipeline complet (preprocessing + modèle)
    model_path = "models/eta_gbt_pipeline"
    print(f"Sauvegarde du modèle dans : {model_path}")
    
    # overwrite() permet d'écraser une ancienne version si elle existe
    model.write().overwrite().save(model_path)
    
    spark.stop()

if __name__ == "__main__":
    main()