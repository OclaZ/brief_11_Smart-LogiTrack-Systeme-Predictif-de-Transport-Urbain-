import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, dayofweek, month
from sqlalchemy import create_engine
import pandas as pd

# 1. Initialize Spark
def get_spark_session():
    return SparkSession.builder \
        .appName("TransportProject") \
        .getOrCreate()

# 2. Bronze Step: Read Raw Data
def process_bronze(input_file, output_path):
    spark = get_spark_session()
    
    print(f"Reading raw data from {input_file}...")
    df = spark.read.parquet(input_file)
    
    # Save as Bronze (Raw copy)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Bronze data saved to {output_path}")

# 3. Silver Step: Clean and Enrich
def process_silver(bronze_path, silver_path, db_connection_str):
    spark = get_spark_session()
    df = spark.read.parquet(bronze_path)

    # --- Feature Engineering (from your notebook) ---
    # Calculate Duration in minutes
    df = df.withColumn(
        "duration_minutes",
        (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60
    )

    # Extract Time Features
    df = df.withColumn("pickuphour", hour(col("tpep_pickup_datetime"))) \
           .withColumn("dayof_week", dayofweek(col("tpep_pickup_datetime"))) \
           .withColumn("month", month(col("tpep_pickup_datetime")))

    # --- Filtering (Cleaning Rules) ---
    # Filter 1: Duration > 0 and < 4 hours (240 mins) to remove errors
    # Filter 2: Distance > 0 and < 200 miles
    df_clean = df.filter(
        (col("duration_minutes") > 0) & 
        (col("duration_minutes") < 240) & 
        (col("trip_distance") > 0) & 
        (col("trip_distance") < 200) &
        (col("passenger_count") > 0)
    )

    # Save to Parquet (Silver Layer)
    df_clean.write.mode("overwrite").parquet(silver_path)
    print(f"Silver data saved to {silver_path}")

    # --- Load to PostgreSQL ---
    # We convert to Pandas for easier SQL insertion (suitable for project size)
    # In Big Data, we would use JDBC, but this is simpler for you now.
    pdf = df_clean.limit(10000).toPandas() # Limit to avoid crashing memory if file is huge
    
    engine = create_engine(db_connection_str)
    pdf.to_sql('silver_taxi_trips', engine, if_exists='replace', index=False)
    print("Data successfully written to PostgreSQL table 'silver_taxi_trips'")

if __name__ == "__main__":
    # Arguments passed from Airflow
    step = sys.argv[1]
    
    if step == "bronze":
        process_bronze("/opt/airflow/data/dataset.parquet", "/opt/airflow/data/bronze")
    elif step == "silver":
        # Connection string to the 'postgres' service defined in docker-compose
        db_url = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
        process_silver("/opt/airflow/data/bronze", "/opt/airflow/data/silver", db_url)