import sys
import os
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from utils.loader import load_to_postgresql
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "DATA-IA",
    "start_date": datetime(2025, 9, 8),
    "retries": 0,
}

with DAG(
    dag_id="etl_osiris_to_oeci",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["oeci", "patients", "copy", "PROD"],
) as dag:

    copy_task = PythonOperator(
        task_id='extract_osiris_to_oeci',
        python_callable=load_to_postgresql,
   
    )

    copy_task
