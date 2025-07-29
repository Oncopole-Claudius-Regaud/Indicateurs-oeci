from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from oeci.utils.db import connect_to_iris
from oeci.utils.extract import extract_all_data
from oeci.utils.loader import load_to_postgresql
from oeci.utils.logger import configure_logger
#from oeci.utils.email_notifier import notify_failure
from airflow.stats import Stats

# Configuration du logger
configure_logger()

# Fonction d'extraction OECI
def extract_data_from_iris_oeci(**kwargs):
    try:
        conn = connect_to_iris()
        cursor = conn.cursor()
        patient_data, admission_data, diagnostic_data, traitement_data = extract_all_data(cursor)
        return patient_data, admission_data, diagnostic_data, traitement_data
    except Exception as e:
        Stats.incr("custom.task_failure.extract_data_from_iris_oeci")
        raise e

# Arguments du DAG
default_args = {
    'owner': 'DATA-IA',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 3),
    'email': ['data.alerte@iuct-oncopole.fr'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# Définition du DAG
dag = DAG(
    'etl_oeci_patients',
    default_args=default_args,
    description='Extraction des patients et admissions pour OECI depuis IRIS vers PostgreSQL',
    schedule_interval=None,
    catchup=False,
    tags=["oeci", "extraction", "iris"]
)

# Task 1 : extraction
extract_task = PythonOperator(
    task_id='extract_data_from_iris',
    python_callable=extract_data_from_iris_oeci,
    provide_context=True,
    do_xcom_push=True,
    dag=dag
)

# Task 2 : chargement dans PostgreSQL
load_task = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    provide_context=True,
    dag=dag
)

# Exécution en chaîne
extract_task >> load_task
