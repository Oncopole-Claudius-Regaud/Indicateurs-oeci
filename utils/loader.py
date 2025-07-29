import logging
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


def load_to_postgresql(**kwargs):
    logging.info("Début du chargement OECI dans PostgreSQL")

    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    logging.info(f"[ETL] Utilisation de la connexion PostgreSQL : {conn_id}")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)

    conn = pg_hook.get_conn()
    cur = conn.cursor()

    ti = kwargs["ti"]
    patient_data, admission_data, diagnostic_data, traitement_data = ti.xcom_pull(task_ids="extract_data_from_iris")

    # --- CHARGEMENT TABLE oeci.patients_trackcare ---
    if patient_data:
        logging.info(f"Insertion de {len(patient_data)} patients")
        execute_values(cur, """
            INSERT INTO oeci.patients_trackcare (
                ipp_ocr, ipp_chu, nom, prenom, date_naissance, sexe, date_dc, source_deces
            ) VALUES %s
            ON CONFLICT (ipp_ocr) DO NOTHING
        """, [
            (
                p["ipp_ocr"],
                p["ipp_chu"],
                p["nom"],
                p["prenom"],
                p["date_naissance"],
                p["sexe"],
                p["date_dc"],
                p["source_deces"]
            ) for p in patient_data
        ])
        conn.commit()

    # --- CHARGEMENT TABLE oeci.admissions ---
    if admission_data:
        logging.info(f"Insertion de {len(admission_data)} admissions")
        execute_values(cur, """
            INSERT INTO oeci.admissions (
                ipp_ocr, visit_episode_id, visit_start_date,
                visit_end_date, visit_status,
                visit_functional_unit, visit_type
            ) VALUES %s
        """, [
            (
                a["ipp_ocr"],
                a["visit_episode_id"],
                a["visit_start_date"],
                a["visit_end_date"],
                a["visit_status"],
                a["visit_functional_unit"],
                a["visit_type"]
            ) for a in admission_data
        ])
        conn.commit()
    
    # --- CHARGEMENT TABLE oeci.diagnostics ---
    if diagnostic_data:
        logging.info(f"Insertion de {len(diagnostic_data)} diagnostics")
        execute_values(cur, """
            INSERT INTO oeci.diagnostics (
                ipp_ocr, date_diagnostic, code_cim, libelle_cim,
                diagnostic_status, diagnostic_deleted_flag,
                date_diagnostic_created_at, date_diagnostic_updated_at,
                date_diagnostic_end, date_consultation,
                date_consultation_created, premiere_consultation_flag, source
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """, [
            (
                d["ipp_ocr"],
                d["date_diagnostic"],
                d["code_cim"],
                d["libelle_cim"],
                d["diagnostic_status"],
                d["diagnostic_deleted_flag"],
                d["date_diagnostic_created_at"],
                d["date_diagnostic_updated_at"],
                d["date_diagnostic_end"],
                d["date_consultation"],
                d["date_consultation_created"],
                d["premiere_consultation_flag"],
                d["source"]
            ) for d in diagnostic_data
        ])
        conn.commit()

    # --- CHARGEMENT TABLE oeci.traitements ---
    if traitement_data:
        logging.info(f"Insertion de {len(traitement_data)} traitements")
        execute_values(cur, """
            INSERT INTO oeci.traitements (
                ipp_ocr, date_debut_traitement, date_fin_traitement,
                dci_code, dci_libelle, forme_libelle, source
            ) VALUES %s
            ON CONFLICT DO NOTHING
        """, [
            (
                t["ipp_ocr"],
                t["date_debut_traitement"],
                t["date_fin_traitement"],
                t["dci_code"],
                t["dci_libelle"],
                t["forme_libelle"],
                t["source"]
            ) for t in traitement_data
        ])
        conn.commit()


    logging.info("Chargement terminé avec succès")
