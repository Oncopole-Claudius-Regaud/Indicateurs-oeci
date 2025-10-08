import logging
import json
import os
from typing import Iterable, Dict, Any, List, Tuple

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from psycopg2.extras import execute_values

# utilise TA fonction
from utils.helpers import compute_diagnostic_hash

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
OUTPUT_DIR = "/tmp/etl_iris"
BATCH_SIZE = 5_000


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def none_if_empty(v):
    """Transforme '' en None, laisse les autres valeurs intactes."""
    return None if (v is None or (isinstance(v, str) and v.strip() == "")) else v


def to_bool_or_none(v):
    """Convertit différentes représentations en bool ou None."""
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"true", "t", "1", "y", "yes", "oui"}:
        return True
    if s in {"false", "f", "0", "n", "no", "non"}:
        return False
    return None


def _rows_from_ndjson(path: str) -> Iterable[Dict[str, Any]]:
    """Lit un fichier NDJSON (.jsonl) ligne par ligne."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def _rows_from_json_array(path: str) -> Iterable[Dict[str, Any]]:
    """Fallback anciens .json (tableau complet) – itère sans tout garder en RAM."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
        for obj in data:
            yield obj


def _stream_rows(basename: str) -> Iterable[Dict[str, Any]]:
    """
    Renvoie un itérateur sur /tmp/etl_iris/<basename>.jsonl (prioritaire),
    sinon essaie /tmp/etl_iris/<basename>.json, sinon itérateur vide.
    """
    ndjson_path = os.path.join(OUTPUT_DIR, f"{basename}.jsonl")
    json_path   = os.path.join(OUTPUT_DIR, f"{basename}.json")

    if os.path.exists(ndjson_path):
        return _rows_from_ndjson(ndjson_path)
    elif os.path.exists(json_path):
        return _rows_from_json_array(json_path)
    else:
        logging.warning(f"[ETL] Fichier introuvable pour {basename} (ni .jsonl ni .json).")
        return iter(())


def _flush_values(cur, sql_stmt: str, buffer: List[Tuple], label: str = "", commit_conn=None):
    """Flush un buffer de tuples via execute_values, commit éventuel, clear, log."""
    if not buffer:
        return
    execute_values(cur, sql_stmt, buffer)
    if commit_conn:
        commit_conn.commit()
    if label:
        logging.info(f"[ETL] {label}: {len(buffer)} lignes traitées")
    buffer.clear()


# --------------------------------------------------------------------
# Main
# --------------------------------------------------------------------
def load_to_postgresql(**kwargs):
    logging.info("Début du chargement OECI dans PostgreSQL")

    # Connexion Postgres via Airflow
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    logging.info(f"[ETL] Utilisation de la connexion PostgreSQL : {conn_id}")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    # ---------------- PATIENTS (stream + batch) ----------------
    pg_cur.execute("TRUNCATE TABLE oeci.patients_trackcare CASCADE;")
    patient_buffer: List[Tuple] = []
    seen_ipp = set()
    for p in _stream_rows("patients"):
        ipp = p.get("ipp_ocr")
        if not ipp or ipp in seen_ipp:
            continue
        seen_ipp.add(ipp)
        patient_buffer.append((
            ipp,
            none_if_empty(p.get("ipp_chu")),
            none_if_empty(p.get("nom")),
            none_if_empty(p.get("prenom")),
            none_if_empty(p.get("date_of_birth")),
            none_if_empty(p.get("gender")),
            none_if_empty(p.get("date_of_death")),
            none_if_empty(p.get("birth_city")),
        ))
		
        if len(patient_buffer) >= BATCH_SIZE:
            _flush_values(pg_cur, """
                INSERT INTO oeci.patients_trackcare (
                    ipp_ocr, ipp_chu, nom, prenom, date_naissance, sexe, date_dc, ville_naissance
                ) VALUES %s
                ON CONFLICT (ipp_ocr) DO UPDATE
                SET
                  ipp_chu         = COALESCE(EXCLUDED.ipp_chu, oeci.patients_trackcare.ipp_chu),
                  nom             = COALESCE(EXCLUDED.nom, oeci.patients_trackcare.nom),
                  prenom          = COALESCE(EXCLUDED.prenom, oeci.patients_trackcare.prenom),
                  date_naissance  = COALESCE(EXCLUDED.date_naissance, oeci.patients_trackcare.date_naissance),
                  sexe            = COALESCE(EXCLUDED.sexe, oeci.patients_trackcare.sexe),
                  date_dc         = COALESCE(EXCLUDED.date_dc, oeci.patients_trackcare.date_dc),
                  ville_naissance = COALESCE(NULLIF(EXCLUDED.ville_naissance, ''), oeci.patients_trackcare.ville_naissance)
            """, patient_buffer, label="patients (batch)", commit_conn=pg_conn)
    _flush_values(pg_cur, """
        INSERT INTO oeci.patients_trackcare (
            ipp_ocr, ipp_chu, nom, prenom, date_naissance, sexe, date_dc, ville_naissance
        ) VALUES %s
        ON CONFLICT (ipp_ocr) DO UPDATE
        SET
          ipp_chu         = COALESCE(EXCLUDED.ipp_chu, oeci.patients_trackcare.ipp_chu),
          nom             = COALESCE(EXCLUDED.nom, oeci.patients_trackcare.nom),
          prenom          = COALESCE(EXCLUDED.prenom, oeci.patients_trackcare.prenom),
          date_naissance  = COALESCE(EXCLUDED.date_naissance, oeci.patients_trackcare.date_naissance),
          sexe            = COALESCE(EXCLUDED.sexe, oeci.patients_trackcare.sexe),
          date_dc         = COALESCE(EXCLUDED.date_dc, oeci.patients_trackcare.date_dc),
          ville_naissance = COALESCE(NULLIF(EXCLUDED.ville_naissance, ''), oeci.patients_trackcare.ville_naissance)
    """, patient_buffer, label="patients (final)", commit_conn=pg_conn)

    # ---------------- ADMISSIONS (stream + batch) ----------------
    pg_cur.execute("TRUNCATE TABLE oeci.admissions CASCADE;")
    admission_buffer: List[Tuple] = []
    seen_adm = set()
    for a in _stream_rows("visits"):
        key = (a.get("ipp_ocr"), a.get("visit_episode_id"))
        if not key[0] or not key[1] or key in seen_adm:
            continue
        seen_adm.add(key)
        admission_buffer.append((
            a.get("ipp_ocr"),
            none_if_empty(a.get("visit_episode_id")),
            none_if_empty(a.get("visit_start_date")),
            none_if_empty(a.get("visit_end_date")),
            none_if_empty(a.get("visit_status")),
            none_if_empty(a.get("visit_functional_unit")),
            none_if_empty(a.get("visit_type")),
        ))
		
        if len(admission_buffer) >= BATCH_SIZE:
            _flush_values(pg_cur, """
                INSERT INTO oeci.admissions (
                    ipp_ocr, visit_episode_id, visit_start_date, visit_end_date,
                    visit_status, visit_functional_unit, visit_type
                ) VALUES %s
                ON CONFLICT DO NOTHING
            """, admission_buffer, label="admissions (batch)", commit_conn=pg_conn)
    _flush_values(pg_cur, """
        INSERT INTO oeci.admissions (
            ipp_ocr, visit_episode_id, visit_start_date, visit_end_date,
            visit_status, visit_functional_unit, visit_type
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """, admission_buffer, label="admissions (final)", commit_conn=pg_conn)

    # ---------------- DIAGNOSTICS (stream + batch + hash) ----------------
    pg_cur.execute("TRUNCATE TABLE oeci.diagnostics CASCADE;")
    diag_buffer: List[Tuple] = []
    # set facultatif si tu veux limiter les tentatives d'insert en doublon
    seen_diag = set()

    for d in _stream_rows("diagnostic"):
        # Dictionnaire pour le CALCUL DU HASH (aligné avec ta logique côté helpers)
        row_dict = {
            "ipp_ocr": d.get("ipp_ocr"),
            "concept_id": d.get("concept_id"),
            "diagnostic_source_value": d.get("diagnostic_source_value") or d.get("condition_source_value"),
            "diagnostic_concept_label": d.get("diagnostic_concept_label") or d.get("condition_concept_label"),
            "libelle_cim_reference": d.get("libelle_cim") or d.get("libelle_cim_reference"),
            "diagnostic_start_date": d.get("diagnostic_start_date") or d.get("condition_start_date") or d.get("date_diagnostic"),
            "diagnostic_end_date": d.get("diagnostic_end_date") or d.get("condition_end_date"),
            "diagnostic_status": d.get("diagnostic_status") or d.get("condition_status"),
            "diagnostic_deleted_flag": d.get("diagnostic_deleted_flag") or d.get("condition_deleted_flag"),
            "diagnostic_create_date": d.get("diagnostic_create_date") or d.get("condition_create_date"),
            "cim_created_at": d.get("cim_created_at"),
            "cim_updated_at": d.get("cim_updated_at") or d.get("diagnostic_update_date") or d.get("condition_update_date"),
            "cim_active_from": d.get("cim_active_from"),
            "cim_active_to": d.get("cim_active_to"),
            "code_morphologique": d.get("code_morphologique"),
            
        }
        if not row_dict["ipp_ocr"]:
            continue

        # dédup logique amont (facultatif)
        dedup_key = (row_dict["ipp_ocr"], row_dict["diagnostic_source_value"], row_dict["diagnostic_start_date"])
        if dedup_key in seen_diag:
            continue
        seen_diag.add(dedup_key)

        # HASH via ta fonction utilitaire
        diag_hash = compute_diagnostic_hash(row_dict)

        # Prépare la ligne pour la TABLE OECI (mapping vers ses colonnes)
        diag_buffer.append((
            row_dict["ipp_ocr"],
            none_if_empty(row_dict["diagnostic_start_date"]),          # date_diagnostic
            none_if_empty(row_dict["diagnostic_source_value"]),        # code_cim
            none_if_empty(row_dict["diagnostic_concept_label"]),       # libelle_cim
            none_if_empty(row_dict["diagnostic_status"]),
            to_bool_or_none(row_dict["diagnostic_deleted_flag"]),
            none_if_empty(row_dict["diagnostic_create_date"]),
            none_if_empty(row_dict["cim_updated_at"]),                 # date_diagnostic_updated_at
            none_if_empty(row_dict["diagnostic_end_date"]),
            none_if_empty(d.get("code_morphologique") or d.get("code_morphologique")),
            diag_hash,                                                # <- NEW
        ))
		
        if len(diag_buffer) >= BATCH_SIZE:
            _flush_values(pg_cur, """
                INSERT INTO oeci.diagnostics (
                    ipp_ocr, date_debut_diagnostic, code_cim, libelle_cim,
                    diagnostic_status, diagnostic_deleted_flag,
                    date_diagnostic_created_at, date_diagnostic_updated_at, date_diagnostic_end,
                    code_morphologique, diagnostic_hash
                ) VALUES %s
                ON CONFLICT (diagnostic_hash) DO NOTHING
            """, diag_buffer, label="diagnostics (batch)", commit_conn=pg_conn)

    _flush_values(pg_cur, """
        INSERT INTO oeci.diagnostics (
            ipp_ocr, date_debut_diagnostic, code_cim, libelle_cim,
            diagnostic_status, diagnostic_deleted_flag,
            date_diagnostic_created_at, date_diagnostic_updated_at, date_diagnostic_end,
            code_morphologique, diagnostic_hash
        ) VALUES %s
        ON CONFLICT (diagnostic_hash) DO NOTHING
    """, diag_buffer, label="diagnostics (final)", commit_conn=pg_conn)

    # ---------------- TRAITEMENTS (stream + batch) ----------------
    pg_cur.execute("TRUNCATE TABLE oeci.traitements CASCADE;")
    trt_buffer: List[Tuple] = []
    seen_trt = set()

    for t in _stream_rows("treatments"):
        key = (t.get("ipp_ocr"), t.get("dci_code"), t.get("date_debut_traitement"), t.get("date_fin_traitement"))
        if not key[0] or key in seen_trt:
            continue
        seen_trt.add(key)
        trt_buffer.append((
            t.get("ipp_ocr"),
            none_if_empty(t.get("date_debut_traitement")),
            none_if_empty(t.get("date_fin_traitement")),
            none_if_empty(t.get("dci_code")),
            none_if_empty(t.get("dci_libelle")),
            none_if_empty(t.get("forme_libelle")),
            none_if_empty(t.get("source")) or "TKC",
            t.get("visit_iep"),
        ))
        if len(trt_buffer) >= BATCH_SIZE:
            _flush_values(pg_cur, """
                INSERT INTO oeci.traitements (
                    ipp_ocr, date_debut_traitement, date_fin_traitement,
                    dci_code, dci_libelle, forme_libelle, source, visit_iep
                ) VALUES %s
                ON CONFLICT DO NOTHING
            """, trt_buffer, label="traitements (batch)", commit_conn=pg_conn)

    _flush_values(pg_cur, """
        INSERT INTO oeci.traitements (
            ipp_ocr, date_debut_traitement, date_fin_traitement,
            dci_code, dci_libelle, forme_libelle, source, visit_iep
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """, trt_buffer, label="traitements (final)", commit_conn=pg_conn)

    # ---------------- CLEANUP ----------------
    pg_cur.close()
    pg_conn.close()
    logging.info("Chargement terminé avec succès")


