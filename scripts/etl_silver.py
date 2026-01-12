import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Chemins absolus dans le conteneur Docker
BRONZE_PATH = "/opt/airflow/data/bronze/taxi_data.parquet"
SILVER_PATH = "/opt/airflow/data/silver"
DB_URL = "postgresql://user:password@postgres:5432/taxidb" # 'postgres' est le nom du service docker

def run_etl():
    spark = SparkSession.builder.appName("ETL_Bronze_Silver").getOrCreate()
    
    print("1. Lecture Bronze (Parquet)...")
    try:
        df = spark.read.parquet(BRONZE_PATH)
    except Exception:
        print("Fichier Bronze introuvable, utilisation de données dummy pour la démo.")
        # Création dummy si le téléchargement a échoué (fallback)
        df = spark.createDataFrame([
            (10.0, 1, 8, 1, 1, 15.0, 20.0),
            (20.0, 2, 9, 2, 1, 30.0, 40.0)
        ], ["trip_distance", "passenger_count", "pickuphour", "dayof_week", "month", "duration_minutes", "fare_amount"])

    print("2. Nettoyage (Règles EDA)...")
    # Vos règles de l'EDA
    df_clean = df.filter(
        (col("duration_minutes") >= 1) & 
        (col("duration_minutes") <= 240) & 
        (col("fare_amount") > 0) &
        (col("trip_distance") > 0)
    )

    print(f"3. Sauvegarde Silver Parquet -> {SILVER_PATH}")
    df_clean.write.mode("overwrite").parquet(SILVER_PATH)

    print("4. Sauvegarde PostgreSQL (Analytics)...")
    # Conversion Pandas pour l'écriture SQL (sur échantillon si trop gros)
    pdf = df_clean.limit(50000).toPandas()
    
    engine = create_engine(DB_URL)
    pdf.to_sql('silver_taxi_trips', engine, if_exists='replace', index=False)
    
    print("✅ ETL Terminé.")
    spark.stop()

if __name__ == "__main__":
    run_etl()