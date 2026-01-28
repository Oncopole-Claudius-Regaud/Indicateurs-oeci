# ==============================================================================
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Importation des fonctions de traitement
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from kaplan_meier_processing import (
    extract_and_clean_data_task, 
    calculate_kaplan_meier_task, 
    load_to_db_task
)

# --- Paramètres par défaut ---
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

# Liste fixe des organes à traiter
ORGANE_LIST = [
    "SEIN",
    "UROLOGIE",
    "GYNECOLOGIE",
    "ORL, VADS",
    "PEAU",
]

# L'ID de connexion PostgreSQL à utiliser
POSTGRES_CONN_ID = "postgres_test" 

with DAG(
    dag_id='kaplan_meier_analysis_production',
    default_args=DEFAULT_ARGS,
    schedule=None, 
    catchup=False,
    tags=['production', 'survie', 'datamart']
) as dag:

    # Définition des paramètres dynamiques
    DATE_DEBUT_OBS_PARAM = "{{ dag_run.conf.get('date_debut_obs', '2020') }}"
    DATE_FIN_OBS_PARAM = "{{ dag_run.conf.get('date_fin_obs', macros.datetime.now().year) }}"

    # Boucle pour créer les tasks par organe
    for organe in ORGANE_LIST:

        # 1. Extraction et Nettoyage
        extract_task = PythonOperator(
            task_id=f'extract_and_clean_data_{organe}',
            python_callable=extract_and_clean_data_task,
            op_kwargs={
                'organe': organe,
                'date_debut_obs': DATE_DEBUT_OBS_PARAM,
                'date_fin_obs': DATE_FIN_OBS_PARAM,
            },
        )

        # 2. Calcul du Kaplan-Meier
        calculate_task = PythonOperator(
            task_id=f'calculate_kaplan_meier_{organe}',
            python_callable=calculate_kaplan_meier_task,
        )

        # 3. Chargement des données de la Courbe
        load_curve_task = PythonOperator(
            task_id=f'store_curve_data_{organe}',
            python_callable=load_to_db_task,
            op_kwargs={
                'table_name': f'datamart_km_curve_{organe}',
            },
        )

        # 4. Chargement des Indicateurs Clés
        load_indicators_task = PythonOperator(
            task_id=f'store_key_indicators_{organe}',
            python_callable=load_to_db_task,
            op_kwargs={
                'table_name': f'datamart_km_key_indicators_{organe}',
            },
        )

        # Définition des dépendances
        extract_task >> calculate_task
        calculate_task >> [load_curve_task, load_indicators_task]

