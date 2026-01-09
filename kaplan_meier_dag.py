# ==============================================================================
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Importation des fonctions de traitement (à adapter si l'import vient de 'utils.db')
# Exemple si vous deviez importer le hook : 
# from utils.db import get_postgres_hook
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

# L'ID de connexion PostgreSQL à utiliser
# Nous pouvons le laisser implicite pour utiliser la Variable.get dans le processing,
# ou le définir ici si l'on veut le passer explicitement :
POSTGRES_CONN_ID = "postgres_test" 

with DAG(
    dag_id='kaplan_meier_analysis_production',
    default_args=DEFAULT_ARGS,
    schedule=None, 
    catchup=False,
    tags=['production', 'survie', 'datamart']
) as dag:
    
    # Définition des paramètres dynamiques
    ORGANE_PARAM = "{{ dag_run.conf.get('organe', 'SEIN') }}"
    DATE_DEBUT_OBS_PARAM = "{{ dag_run.conf.get('date_debut_obs', '2000') }}"
    DATE_FIN_OBS_PARAM = "{{ dag_run.conf.get('date_fin_obs', '2025') }}"
    DATE_FILTRE_DIAG = "2010-01-01" 
    
    
    # 1. Extraction et Nettoyage
    extract_data = PythonOperator(
        task_id='extract_and_clean_data',
        python_callable=extract_and_clean_data_task,
        op_kwargs={
            'organe': ORGANE_PARAM,
            'date_debut_obs': DATE_DEBUT_OBS_PARAM,
            'date_fin_obs': DATE_FIN_OBS_PARAM,
            # 'conn_id': POSTGRES_CONN_ID, # Optionnel : si vous voulez forcer l'ID de connexion
        },
    )

    # 2. Calcul du Kaplan-Meier et Structuration des Résultats
    calculate_km = PythonOperator(
        task_id='calculate_kaplan_meier',
        python_callable=calculate_kaplan_meier_task,
        op_kwargs={
            'date_debut_observation_filtre': DATE_FILTRE_DIAG,
        },
    )

    # 3. Chargement des données de la Courbe
    load_curve = PythonOperator(
        task_id='store_curve_data',
        python_callable=load_to_db_task,
        op_kwargs={
            'table_name': 'datamart_km_curve',
            # 'conn_id': POSTGRES_CONN_ID, # Optionnel
        },
    )

    # 4. Chargement des Indicateurs Clés
    load_indicators = PythonOperator(
        task_id='store_key_indicators',
        python_callable=load_to_db_task,
        op_kwargs={
            'table_name': 'datamart_km_key_indicators',
            # 'conn_id': POSTGRES_CONN_ID, # Optionnel
        },
    )

    # Définition de l'ordre d'exécution du DAG
    extract_data >> calculate_km
    calculate_km >> [load_curve, load_indicators]
