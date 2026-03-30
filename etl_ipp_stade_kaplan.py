import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), "utils"))
from kaplan_meier_processing import (
    calculate_kaplan_meier_task,
    load_to_db_task,
)
from ipp_stade_tasks import (
    extract_ipp_task,
    push_pdf_task,
    run_tnm_extraction_task,
    fetch_csv_task,
    load_ipp_stade_task,
    refresh_view_task,
    extract_and_clean_data_for_organe_task,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

ORGANE_CONFIG = {
    "SEIN": "sein",
    "UROLOGIE": "urologie",
    "GYNECOLOGIE": "gynecologie",
    "ORL, VADS": "orl",
    "PEAU": "peau",
    "SYSTEME HEMATOPOIETIQUE": "hemato",
    "OS ET TISSUS MOUS": "sarcom",
}

POSTGRES_CONN_ID = "postgres_test"

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ipp_stade_kaplan_meier_pipeline",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["production", "survie", "stade", "tnm", "llm", "datamart"],
) as dag:

    DATE_DEBUT_OBS = "{{ dag_run.conf.get('date_debut_obs', '2020') }}"
    DATE_FIN_OBS = "{{ dag_run.conf.get('date_fin_obs', (macros.datetime.now().year - 1) ~ '-12-31') }}"

    # ------------------------------------------------------------------
    # 1. Extraction des IPP depuis v_statut_vital
    # ------------------------------------------------------------------
    t_extract_ipp = PythonOperator(
        task_id="extract_ipp_from_statut_vital",
        python_callable=extract_ipp_task,
        op_kwargs={
            "date_debut_obs": DATE_DEBUT_OBS,
            "conn_id": POSTGRES_CONN_ID,
        },
    )

    # ------------------------------------------------------------------
    # 2. Push PDF + JSON metadata vers le serveur distant
    # ------------------------------------------------------------------
    t_push_pdf = PythonOperator(
        task_id="push_pdf_to_remote_server",
        python_callable=push_pdf_task,
        op_kwargs={
            "remote_host": "srvlakehouse",
            "remote_port": 22,
            "remote_user": "administrateur",
            "remote_dir": "/home/administrateur/pdf_llm",
            "ssh_password_var_key": "password_serverlakehouse",
            "remote_script": "/opt/push_pdf_llm.py",
            "source_dir": "/opt/PDF",
            "target_host": "10.210.22.130",
            "target_port": 22,
            "target_user": "administrateur",
            "target_password_var_key": "password_clidatadsin",
        },
    )

    # ------------------------------------------------------------------
    # 3. Lancement du script d'extraction TNM/stade sur le serveur distant
    # ------------------------------------------------------------------
    t_run_tnm = PythonOperator(
        task_id="run_tnm_regex_extraction",
        python_callable=run_tnm_extraction_task,
        op_kwargs={
            "remote_host": "10.210.22.130",
            "remote_port": 22,
            "remote_user": "administrateur",
            "remote_script": (
                "/home/administrateur/airflow/dags/LLM_Extract_PDF/"
                "extract_tnm_stage_by_ipp.py"
            ),
            "remote_data_dir": "/home/administrateur/pdf_llm",
            "ssh_password_var_key": "password_clidatadsin",
        },
    )

    # ------------------------------------------------------------------
    # 4. Rapatriement du CSV vers Airflow
    # ------------------------------------------------------------------
    t_fetch_csv = PythonOperator(
        task_id="fetch_tnm_csv",
        python_callable=fetch_csv_task,
        op_kwargs={
            "remote_host": "10.210.22.130",
            "remote_port": 22,
            "remote_user": "administrateur",
            "remote_csv_path": "/home/administrateur/pdf_llm/ipp_stage_results.csv",
            "local_csv_path": "/tmp/ipp_stage_results.csv",
            "ssh_password_var_key": "password_clidatadsin",
        },
    )

    # ------------------------------------------------------------------
    # 5. Chargement dans datamart_oeci_survie.ipp_stade
    # ------------------------------------------------------------------
    t_load_stade = PythonOperator(
        task_id="load_ipp_stade_to_db",
        python_callable=load_ipp_stade_task,
        op_kwargs={
            "local_csv_path": "/tmp/ipp_stage_results.csv",
            "conn_id": POSTGRES_CONN_ID,
        },
    )

    # ------------------------------------------------------------------
    # 6. Refresh de la vue v_statut_vital
    # ------------------------------------------------------------------
    t_refresh_view = PythonOperator(
        task_id="refresh_view_statut_vital",
        python_callable=refresh_view_task,
        op_kwargs={"conn_id": POSTGRES_CONN_ID},
    )

    # ------------------------------------------------------------------
    # 7-N. Pipeline Kaplan-Meier par organe (parallèle après refresh)
    # ------------------------------------------------------------------
    km_terminal_tasks = []

    for organe, organe_slug in ORGANE_CONFIG.items():

        t_extract = PythonOperator(
            task_id=f"extract_and_clean_data_{organe_slug}",
            python_callable=extract_and_clean_data_for_organe_task,
            op_kwargs={
                "organe": organe,
                "date_debut_obs": DATE_DEBUT_OBS,
                "date_fin_obs": DATE_FIN_OBS,
                "conn_id": POSTGRES_CONN_ID,
            },
        )

        t_km = PythonOperator(
            task_id=f"calculate_kaplan_meier_{organe_slug}",
            python_callable=calculate_kaplan_meier_task,
            op_kwargs={
                "date_debut_obs": DATE_DEBUT_OBS,
                "date_fin_obs": DATE_FIN_OBS,
            },
        )

        t_load_curve = PythonOperator(
            task_id=f"store_curve_data_{organe_slug}",
            python_callable=load_to_db_task,
            op_kwargs={
                "table_name": f"datamart_km_curve_{organe_slug}",
                "organe": organe,
                "date_debut_obs": DATE_DEBUT_OBS,
                "date_fin_obs": DATE_FIN_OBS,
                "conn_id": POSTGRES_CONN_ID,
            },
        )

        t_load_kpi = PythonOperator(
            task_id=f"store_key_indicators_{organe_slug}",
            python_callable=load_to_db_task,
            op_kwargs={
                "table_name": f"datamart_km_key_indicators_{organe_slug}",
                "organe": organe,
                "date_debut_obs": DATE_DEBUT_OBS,
                "date_fin_obs": DATE_FIN_OBS,
                "conn_id": POSTGRES_CONN_ID,
            },
        )

        t_refresh_view >> t_extract >> t_km >> [t_load_curve, t_load_kpi]
        km_terminal_tasks.extend([t_load_curve, t_load_kpi])

    # ------------------------------------------------------------------
    # Chaîne principale
    # ------------------------------------------------------------------
    (
        t_extract_ipp
        >> t_push_pdf
        >> t_run_tnm
        >> t_fetch_csv
        >> t_load_stade
        >> t_refresh_view
    )
