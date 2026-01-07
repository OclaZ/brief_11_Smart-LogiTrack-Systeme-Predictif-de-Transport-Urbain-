from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Default settings for the DAG
default_args = {
    'owner': 'hamza',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(
    'taxi_prediction_pipeline',
    default_args=default_args,
    schedule_interval=None, # Run manually for now
    catchup=False
) as dag:

    # Task 1: Ingestion (Bronze)
    task_bronze = BashOperator(
        task_id='ingest_bronze',
        bash_command='python /opt/airflow/dags/process_data.py bronze'
    )

    # Task 2: Cleaning (Silver)
    task_silver = BashOperator(
        task_id='process_silver',
        bash_command='python /opt/airflow/dags/process_data.py silver'
    )

    # Task 3: Machine Learning (We will add this later!)
    task_train_model = BashOperator(
        task_id='train_model',
        bash_command='echo "Training model placeholder"'
    )

    # Set the order: Bronze -> Silver -> Train
    task_bronze >> task_silver >> task_train_model