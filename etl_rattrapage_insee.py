from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# importe la fonction du module (alias fourni)
from oeci.utils.process_insee_rattrapage_full_replace_strict import (
    process_insee_rattrapage_full_replace  # alias de *_strict
)

with DAG(
    dag_id="insee_month_rattrapage_full_replace_strict",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["oeci","insee","rattrapage"],
) as dag:

    full_replace = PythonOperator(
        task_id="process_insee_rattrapage_full_replace_strict",
        python_callable=process_insee_rattrapage_full_replace,
        op_kwargs={
            # Laisse vide pour autodétection, ou précise le chemin exact :
            "input_dir": "/home/administrateur/airflow/dags/oeci/insee_month_rattrapage",
        },
    )
