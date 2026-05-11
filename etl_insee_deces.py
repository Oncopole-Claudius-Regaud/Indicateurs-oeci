import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.insee_loader import (
    download_insee_file,
    load_to_postgres
)

default_args = {
    'owner': 'DATA-IA',
    'start_date': datetime(2026, 1, 1),  # date fixe
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_insee_deces_monthly",
    default_args=default_args,
    description="Import du fichier mensuel INSEE des décès",
    schedule_interval="0 6 10 * *",  # chaque 10 du mois à 6h00
    catchup=False,
    tags=["insee", "deces", "mensuel"]
)

download = PythonOperator(
    task_id="download_insee_file",
    python_callable=download_insee_file,
    op_kwargs={"mode": "monthly"},
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
