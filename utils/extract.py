import logging
from oeci.utils.sql_loader import load_sql
from oeci.utils.patients import get_patient_ids
from oeci.utils.helpers import make_serializable


def extract_patient_data(cursor):
    logging.info("Début de l'extraction patient_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_patients_trackcare.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    patient_data = []
    for row in rows:
        patient_data.append({
            "ipp_ocr": row.ipp_ocr,
            "ipp_chu": row.ipp_chu or "",
            "nom": row.nom or "",
            "prenom": row.prenom or "",
            "date_naissance": row.date_naissance,
            "sexe": row.sexe,
            "date_dc": row.date_dc,
            "source_deces": row.source_deces or "TKC"
        })

    return patient_data


def extract_admission_data(cursor):
    logging.info("Début de l'extraction admission_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_visits_trackcare.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    admission_data = []
    for row in rows:
        admission_data.append({
            "ipp_ocr": row.ipp_ocr,
            "visit_episode_id": row.visit_episode_id,
            "visit_start_date": row.visit_start_date,
            "visit_end_date": row.visit_end_date,
            "visit_status": row.visit_status,
            "visit_functional_unit": row.visit_functional_unit,
            "visit_type": row.visit_type
        })

    return admission_data


def extract_diagnostic_data(cursor):
    logging.info("Début de l'extraction diagnostic_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_diagnostics_trackcare.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    diagnostic_data = []
    for row in rows:
        diagnostic_data.append({
            "ipp_ocr": row.ipp_ocr,
            "date_diagnostic": row.date_diagnostic,
            "code_cim": row.code_cim,
            "libelle_cim": row.libelle_cim,
            "diagnostic_status": row.diagnostic_status,
            "diagnostic_deleted_flag": row.diagnostic_deleted_flag,
            "date_diagnostic_created_at": row.date_diagnostic_created_at,
            "date_diagnostic_updated_at": row.date_diagnostic_updated_at,
            "date_diagnostic_end": row.date_diagnostic_end,
            "date_consultation": row.date_consultation,
            "date_consultation_created": row.date_consultation_created,
            "premiere_consultation_flag": row.premiere_consultation_flag,
            "source": row.source
        })

    return diagnostic_data


def extract_traitement_data(cursor):
    logging.info("Début de l'extraction traitement_data")

    patient_ids = get_patient_ids()
    patient_list_sql = ", ".join(f"'{pid}'" for pid in patient_ids)

    sql_template = load_sql("extract_traitements_trackcare.sql")
    sql = sql_template.format(patient_list=patient_list_sql)

    cursor.execute(sql)
    rows = cursor.fetchall()

    traitement_data = []
    for row in rows:
        traitement_data.append({
            "ipp_ocr": row.ipp_ocr,
            "date_debut_traitement": row.date_debut_traitement,
            "date_fin_traitement": row.date_fin_traitement,
            "dci_code": row.dci_code,
            "dci_libelle": row.dci_libelle,
            "forme_libelle": row.forme_libelle,
            "source": row.source
        })

    return traitement_data


def extract_all_data(cursor):
    """
    Orchestration des extractions OECI.
    Retourne les données sous forme de listes de dictionnaires serialisables (pour stockage ou XCom).
    """
    patient_data = extract_patient_data(cursor)
    admission_data = extract_admission_data(cursor)
    diagnostic_data = extract_diagnostic_data(cursor)
    traitement_data = extract_traitement_data(cursor)

    return (
        make_serializable(patient_data),
        make_serializable(admission_data),
        make_serializable(diagnostic_data),
        make_serializable(traitement_data)
    )
