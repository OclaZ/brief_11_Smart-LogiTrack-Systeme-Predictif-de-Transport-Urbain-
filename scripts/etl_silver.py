# scripts/etl_silver.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, round, hour, dayofweek, month
from sqlalchemy import create_engine
import pandas as pd
import os
import shutil

# Chemins
BRONZE_PATH = "/opt/airflow/data/bronze/taxi_data.parquet"
SILVER_PATH = "/opt/airflow/data/silver"
TEMP_SILVER_PATH = "/tmp/spark_silver_temp" 
DB_URL = "postgresql://user:password@postgres:5432/taxidb"

def run_etl():
    # Config Spark
    spark = SparkSession.builder \
        .appName("ETL_Bronze_Silver") \
        .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
        .getOrCreate()
    
    print("1. Lecture Bronze...")
    try:
        df = spark.read.parquet(BRONZE_PATH)
    except Exception as e:
        print(f"Erreur lecture: {e}")
        return

    print("2. Feature Engineering...")
    df = df.withColumn("duration_minutes", round((unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60, 2))
    df = df.withColumn("pickuphour", hour("tpep_pickup_datetime")) \
           .withColumn("dayof_week", dayofweek("tpep_pickup_datetime")) \
           .withColumn("month", month("tpep_pickup_datetime"))

    print("3. Nettoyage...")
    df_clean = df.filter(
        (col("duration_minutes") >= 1) & 
        (col("duration_minutes") <= 240) & 
        (col("fare_amount") > 0) &
        (col("trip_distance") > 0)
    )

    # --- ÉCRITURE ET DÉPLACEMENT SÉCURISÉ ---
    print(f"4a. Ecriture temporaire dans {TEMP_SILVER_PATH}...")
    if os.path.exists(TEMP_SILVER_PATH):
        shutil.rmtree(TEMP_SILVER_PATH)
    
    df_clean.write.mode("overwrite").parquet(TEMP_SILVER_PATH)

    print(f"4b. Déplacement vers {SILVER_PATH} (Sans métadonnées)...")
    if os.path.exists(SILVER_PATH):
        shutil.rmtree(SILVER_PATH)
    
    # FONCTION MAGIQUE : On copie SEULEMENT le contenu, pas les permissions
    def copy_content_only(src, dst):
        shutil.copyfile(src, dst)

    try:
        # copy_function force l'utilisation de copyfile (pas de chmod)
        shutil.copytree(TEMP_SILVER_PATH, SILVER_PATH, copy_function=copy_content_only)
        print("   -> Copie terminée avec succès.")
    except Exception as e:
        print(f"   -> Attention (non bloquant) : {e}")

    print("5. Sauvegarde PostgreSQL...")
    pdf = df_clean.limit(50000).toPandas()
    engine = create_engine(DB_URL)
    pdf.to_sql('silver_taxi_trips', engine, if_exists='replace', index=False)
    
    print("✅ ETL Terminé avec succès.")
    spark.stop()

if __name__ == "__main__":
    run_etl()