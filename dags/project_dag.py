from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

def download_data():
    # URL stable pour un petit dataset (Yellow Taxi Jan 2022)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
    save_path = "/opt/airflow/data/bronze"
    os.makedirs(save_path, exist_ok=True)
    
    print("Téléchargement...")
    response = requests.get(url)
    with open(f"{save_path}/taxi_data.parquet", "wb") as f:
        f.write(response.content)

with DAG('smart_logitrack_pipeline', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    
    t1 = PythonOperator(
        task_id='download_dataset',
        python_callable=download_data
    )
    
    t2 = BashOperator(
        task_id='etl_bronze_to_silver',
        bash_command='python /opt/airflow/scripts/etl_silver.py'
    )
    
    t3 = BashOperator(
        task_id='train_model',
        bash_command='python /opt/airflow/scripts/train_model.py'
    )
    
    t1 >> t2 >> t3