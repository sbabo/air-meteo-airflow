from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='air_meteo_pipeline',
    default_args=default_args,
    description='Complete pipeline Air + Meteo',
    schedule_interval='@daily',
    start_date=datetime(2025, 12, 1),
    catchup=False
) as dag:
    
    ingestion = DockerOperator(
        task_id='ingest_data',
        image='air-meteo-ingestion:latest',
        command='python scripts/ingestion_weather.py && python scripts/ingestion_pollution.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='air-meteo-network',
        auto_remove=True
    )
    
    transformation = DockerOperator(
        task_id='run_dbt',
        image='air-meteo-dbt:latest',
        command='dbt run',
        docker_url='unix://var/run/docker.sock',
        network_mode='air-meteo-network',
        auto_remove=True
    )
    ingestion >> transformation