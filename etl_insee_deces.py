from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from oeci.utils.insee_loader import (
    download_insee_file,
    load_to_postgres
)

default_args = {
    'owner': 'DATA-IA',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_insee_deces_histo",
    default_args=default_args,
    description="Import initial du fichier historique INSEE des décès",
    schedule_interval=None,
    catchup=False,
    tags=["insee", "deces", "historique"]
)

download = PythonOperator(
    task_id="download_insee_file",
    python_callable=download_insee_file,
    op_kwargs={"mode": "historical"},
    provide_context=True,
    dag=dag
)

load = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    provide_context=True,
    dag=dag
)

download >> load
