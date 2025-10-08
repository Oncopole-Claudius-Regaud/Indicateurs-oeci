
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from utils.email_notifier import notify_failure
from oeci.utils.loader import load_to_postgresql
from utils.extract import extract_all_data_streaming
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
