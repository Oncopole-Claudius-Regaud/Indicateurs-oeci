import logging
import json
import os
import sys
import csv
from typing import Iterable, Dict, Any, List, Tuple
import pandas as pd
from psycopg2.extras import execute_values

# utilise TA fonction
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from psycopg2.extras import execute_values
# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
OUTPUT_DIR = "/tmp/etl_iris"
BATCH_SIZE = 5_000
TMP_DIR = "/tmp/etl_iris"
TMP_FILE_TRT = os.path.join(TMP_DIR, "treatments.csv")


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
    json_path = os.path.join(OUTPUT_DIR, f"{basename}.json")

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


def _split_schema_table(full_table_name: str) -> Tuple[str, str]:
    """Extrait schema/table depuis `schema.table` (schema=public par défaut)."""
    if "." in full_table_name:
        schema, table = full_table_name.split(".", 1)
        return schema, table
    return "public", full_table_name


def _to_pg_value(v):
    """Normalise une valeur pour insertion PostgreSQL."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return none_if_empty(v)


def _load_jsonl_to_table(pg_conn, pg_cur, jsonl_path: str, target_table: str):
    """
    Charge un JSONL dans une table PostgreSQL en mappant les clés JSON
    (insensibles à la casse) aux colonnes de la table cible.
    """
    logging.info(f"[ETL] Début du chargement de {target_table} depuis {jsonl_path}...")

    if not os.path.exists(jsonl_path):
        raise FileNotFoundError(f"Fichier source introuvable: {jsonl_path}")

    schema_name, table_name = _split_schema_table(target_table)
    pg_cur.execute(
        """
        SELECT column_name, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema_name, table_name),
    )
    col_meta = pg_cur.fetchall()
    if not col_meta:
        raise ValueError(f"Table cible introuvable ou sans colonnes: {target_table}")

    stream = _rows_from_ndjson(jsonl_path)
    first_row = next(stream, None)
    if first_row is None:
        logging.warning(f"[ETL] Fichier vide: {jsonl_path}")
        pg_cur.execute(f"TRUNCATE TABLE {target_table} CASCADE;")
        pg_conn.commit()
        return

    source_cols = {str(k).lower() for k in first_row.keys()}
    table_cols = [r[0] for r in col_meta]
    insert_cols = [c for c in table_cols if c.lower() in source_cols]

    required_missing = [
        col_name for col_name, is_nullable, col_default in col_meta
        if col_name.lower() not in source_cols and is_nullable == "NO" and col_default is None
    ]
    if required_missing:
        raise ValueError(
            f"Colonnes obligatoires manquantes dans {jsonl_path} pour {target_table}: {required_missing}"
        )
    if not insert_cols:
        raise ValueError(f"Aucune colonne commune entre {jsonl_path} et {target_table}")

    cols_sql = ", ".join(insert_cols)
    insert_sql = f"INSERT INTO {target_table} ({cols_sql}) VALUES %s"

    pg_cur.execute(f"TRUNCATE TABLE {target_table} CASCADE;")
    pg_conn.commit()

    buffer: List[Tuple] = []
    inserted = 0

    def row_to_tuple(row_dict: Dict[str, Any]) -> Tuple:
        normalized = {str(k).lower(): _to_pg_value(v) for k, v in row_dict.items()}
        return tuple(normalized.get(c.lower()) for c in insert_cols)

    buffer.append(row_to_tuple(first_row))
    for row in stream:
        buffer.append(row_to_tuple(row))
        if len(buffer) >= BATCH_SIZE:
            execute_values(pg_cur, insert_sql, buffer)
            pg_conn.commit()
            inserted += len(buffer)
            logging.info(f"[ETL] {target_table}: {inserted:,} lignes insérées...")
            buffer.clear()

    if buffer:
        execute_values(pg_cur, insert_sql, buffer)
        pg_conn.commit()
        inserted += len(buffer)

    logging.info(f"[ETL] Chargement terminé dans {target_table}: {inserted:,} lignes.")


# --------------------------------------------------------------------
# CHIMIOTHERAPIE depuis OSIRIS → fichier → COPY
# --------------------------------------------------------------------
def extract_treatments_to_file(pg_conn):
    """Extraction progressive depuis osiris.CHIMIOTHERAPIE vers un CSV local."""
    logging.info(" Début de l'extraction depuis osiris.chimiotherapie (stream)...")

    os.makedirs(TMP_DIR, exist_ok=True)
    pg_cur = pg_conn.cursor()
    pg_cur.execute("""
        DECLARE trt_cursor CURSOR FOR
        SELECT
            num_doss,
            jour,
            dat_admini,
            cod_categ_proto,
            cod_typ_proto,
            num_pdt,
            nom_pdt,
            cod_voie,
            uf_real,
            lib_uf_real,
            dose_tot,
            nom_proto,
            nom_moda,
            ce_etat_chimio
        FROM osiris.chimiotherapie
    """)

    total = 0
    with open(TMP_FILE_TRT, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, delimiter="|", quoting=csv.QUOTE_MINIMAL)
        while True:
            pg_cur.execute(f"FETCH FORWARD {BATCH_SIZE} FROM trt_cursor;")
            rows = pg_cur.fetchall()
            if not rows:
                break
            writer.writerows(rows)
            total += len(rows)
            logging.info(f" {total:,} lignes extraites...")
    pg_cur.execute("CLOSE trt_cursor;")
    pg_conn.commit()
    pg_cur.close()
    logging.info(f" Extraction terminée : {total:,} lignes écrites dans {TMP_FILE_TRT}")
    return TMP_FILE_TRT


def load_treatments_from_file(pg_conn):
    """Chargement du fichier CSV vers la table oeci.chimiotherapie."""
    logging.info(f"🚀 Chargement vers oeci.chimiotherapie depuis {TMP_FILE_TRT}...")

    pg_cur = pg_conn.cursor()
    pg_cur.execute("TRUNCATE TABLE oeci.chimiotherapie CASCADE;")
    pg_conn.commit()

    with open(TMP_FILE_TRT, "r", encoding="utf-8") as f:
        pg_cur.copy_expert("""
            COPY oeci.chimiotherapie (
                num_doss,  jour, dat_admini,
                cod_categ_proto, cod_typ_proto, num_pdt, nom_pdt,
                cod_voie, uf_real, lib_uf_real, dose_tot,
                nom_proto, nom_moda, ce_etat_chimio
            )
            FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL '', HEADER false)
        """, f)
    pg_conn.commit()
    pg_cur.close()
    logging.info(" Chargement complet dans oeci.chimiotherapie.")


# --------------------------------------------------------------------
# MAIN
# --------------------------------------------------------------------
def load_to_postgresql(**kwargs):
    logging.info("Début du chargement OECI dans PostgreSQL")

    # Connexion Postgres via Airflow
    conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    logging.info(f"[ETL] Utilisation de la connexion PostgreSQL : {conn_id}")
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = pg_hook.get_conn()
    pg_cur = pg_conn.cursor()

    # ---------------- PATIENTS ----------------
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

    # ---------------- ADMISSIONS ----------------
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
            none_if_empty(a.get("visit_code_unit")),
            none_if_empty(a.get("visit_responsible_unit_desc")),
            none_if_empty(a.get("visit_start_time")),
            none_if_empty(a.get("visit_end_time")),
        ))
        if len(admission_buffer) >= BATCH_SIZE:
            _flush_values(pg_cur, """
                INSERT INTO oeci.admissions (
                    ipp_ocr, visit_episode_id, visit_start_date, visit_end_date,
                    visit_status, visit_functional_unit, visit_type, visit_code_unit, visit_responsible_unit_desc, visit_start_time, visit_end_time
                ) VALUES %s
                ON CONFLICT DO NOTHING
            """, admission_buffer, label="admissions (batch)", commit_conn=pg_conn)
    _flush_values(pg_cur, """
        INSERT INTO oeci.admissions (
            ipp_ocr, visit_episode_id, visit_start_date, visit_end_date,
            visit_status, visit_functional_unit, visit_type, visit_code_unit, visit_responsible_unit_desc, visit_start_time, visit_end_time
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """, admission_buffer, label="admissions (final)", commit_conn=pg_conn)

    # ---------------- DIAGNOSTICS ----------------
    pg_cur.execute("TRUNCATE TABLE oeci.diagnostics CASCADE;")

    diag_buffer: List[Tuple] = []
    seen_diag = set()

    for d in _stream_rows("diagnostic"):
        row_dict = {
            "ipp_ocr": d.get("ipp_ocr"),
            "diagnostic_source_value": d.get("diagnostic_source_value") or d.get("condition_source_value"),
            "diagnostic_concept_label": d.get("diagnostic_concept_label") or d.get("condition_concept_label"),
            "diagnostic_start_date": d.get("diagnostic_start_date") or d.get("condition_start_date") or d.get("date_diagnostic"),
            "diagnostic_end_date": d.get("diagnostic_end_date") or d.get("condition_end_date"),
            "diagnostic_status": d.get("diagnostic_status") or d.get("condition_status"),
            "diagnostic_create_date": d.get("diagnostic_create_date") or d.get("condition_create_date"),
            "cim_updated_at": d.get("cim_updated_at") or d.get("diagnostic_update_date") or d.get("condition_update_date"),
            "code_morphologique": d.get("code_morphologique"),
        }

        if not row_dict["ipp_ocr"]:
            continue

        # dédoublonnage applicatif
        dedup_key = (
            row_dict["ipp_ocr"],
            row_dict["diagnostic_source_value"],
            row_dict["diagnostic_start_date"],
        )
        if dedup_key in seen_diag:
            continue
        seen_diag.add(dedup_key)

        diag_buffer.append((
            row_dict["ipp_ocr"],
            none_if_empty(row_dict["diagnostic_start_date"]),
            none_if_empty(row_dict["diagnostic_source_value"]),
            none_if_empty(row_dict["diagnostic_concept_label"]),
            none_if_empty(row_dict["diagnostic_status"]),
            none_if_empty(row_dict["diagnostic_create_date"]),
            none_if_empty(row_dict["cim_updated_at"]),
            none_if_empty(row_dict["diagnostic_end_date"]),
            none_if_empty(row_dict["code_morphologique"]),
        ))

        if len(diag_buffer) >= BATCH_SIZE:
            _flush_values(pg_cur, """
                INSERT INTO oeci.diagnostics (
                    ipp_ocr,
                    date_diagnostic,
                    code_cim,
                    libelle_cim,
                    diagnostic_status,
                    date_diagnostic_created_at,
                    date_diagnostic_updated_at,
                    date_diagnostic_end,
                    code_morphologique
                ) VALUES %s
            """, diag_buffer, label="diagnostics (batch)", commit_conn=pg_conn)

    _flush_values(pg_cur, """
        INSERT INTO oeci.diagnostics (
            ipp_ocr,
            date_diagnostic,
            code_cim,
            libelle_cim,
            diagnostic_status,
            date_diagnostic_created_at,
            date_diagnostic_updated_at,
            date_diagnostic_end,
            code_morphologique
        ) VALUES %s
    """, diag_buffer, label="diagnostics (final)", commit_conn=pg_conn)


    # ---------------- RADIO_THERAPIE ----------------
    logging.info(" Début du chargement de oeci.radioth depuis osiris.radioth...")

    cols = ["ipp_ocr", "rana_duedate", "rana_activitycode"]
    cols_csv = ", ".join(cols)

    pg_cur.execute("TRUNCATE TABLE oeci.radioth CASCADE;")
    pg_conn.commit()

    sql = f"""
        INSERT INTO oeci.radioth ({cols_csv})
        SELECT {cols_csv}
        FROM osiris.radioth;
    """
    logging.info(f"[ETL] Insertion des colonnes {cols_csv} depuis osiris.radioth...")
    pg_cur.execute(sql)
    pg_conn.commit()

    logging.info(f"Chargement terminé : {pg_cur.rowcount} lignes insérées dans oeci.radioth.")

    # ---------------- CHIRURGIE ----------------
    logging.info("Début du chargement de oeci.chirurgie depuis /tmp/etl_iris/chirurgie.jsonl...")

    chir_path = "/tmp/etl_iris/chirurgie.jsonl"
    pg_table = "oeci.chirurgie"
    cols_target = ["ipp_ocr", "nom_interv", "dat_deb_reel", "dat_fin_reel", "patient_key", "code_ccam"]
    cols_csv = ", ".join(cols_target)

    # --- Vérification du fichier source
    if not os.path.exists(chir_path):
        raise FileNotFoundError(f"Fichier {chir_path} introuvable.")

    # --- Lecture du fichier JSONL
    df_chir = pd.read_json(chir_path, lines=True)
    logging.info(f"{len(df_chir)} lignes lues depuis {chir_path}")
    logging.info(f"Colonnes détectées : {list(df_chir.columns)}")

    # --- Renommage des colonnes pour correspondre à la table cible
    rename_map = {
        "P_CODE": "ipp_ocr",
        "I_LABEL": "nom_interv",
        "I_PLANNED_START": "dat_deb_reel",
        "I_PLANNED_END": "dat_fin_reel",
        "I_PATIENT_KEY": "patient_key",
        "IN_CODE": "code_ccam",
        "I_STATE": "i_state",
    }
    df_chir.rename(columns=rename_map, inplace=True)

    # --- Contrôle + typage i_state
    if "i_state" not in df_chir.columns:
        logging.warning("⚠️ Colonne I_STATE absente du JSONL : aucune ligne ne sera gardée (filtre i_state non null).")
        df_chir["i_state"] = pd.NA

    df_chir["i_state"] = pd.to_numeric(df_chir["i_state"], errors="coerce").astype("Int64")

    # --- Filtre : i_state doit être NON NULL et != -1
    before = len(df_chir)
    df_chir = df_chir[df_chir["i_state"].notna() & (df_chir["i_state"] != -1)]
    removed = before - len(df_chir)
    logging.info(f"🧹 {removed} lignes supprimées (i_state NULL ou i_state = -1).")

    # --- Conversion des dates -> STRING 'YYYY-MM-DD' (gère epoch millisecondes + strings)
    for col in ["dat_deb_reel", "dat_fin_reel"]:
        if col in df_chir.columns:
            s = df_chir[col]

            if pd.api.types.is_numeric_dtype(s):
                dt = pd.to_datetime(s, unit="ms", errors="coerce")
            else:
                raw = s.astype(str).replace("nan", "").fillna("").str.strip()
                dt = pd.to_datetime(raw, errors="coerce")

            df_chir[col] = dt.dt.strftime("%Y-%m-%d")
            df_chir.loc[dt.isna(), col] = None

    # --- Autres colonnes en string (hors dates)
    for col in df_chir.columns:
        if col not in ["dat_deb_reel", "dat_fin_reel"]:
            df_chir[col] = df_chir[col].astype(str).replace("nan", "").fillna("").str.strip()

    # --- Nettoyage final : remplacer NaN/NaT par None
    df_chir = df_chir.where(pd.notnull(df_chir), None)

    # --- Vérification colonnes requises avant insert
    missing = [c for c in cols_target if c not in df_chir.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes pour l'insertion dans {pg_table}: {missing}")

    # --- Chargement PostgreSQL
    pg_cur.execute(f"TRUNCATE TABLE {pg_table} CASCADE;")
    pg_conn.commit()

    records = df_chir[cols_target].to_records(index=False)
    buffer = []
    count_total = 0

    for row in records:
        buffer.append(tuple(row))
        if len(buffer) >= BATCH_SIZE:
            execute_values(
                pg_cur,
                f"INSERT INTO {pg_table} ({cols_csv}) VALUES %s",
                buffer
            )
            pg_conn.commit()
            count_total += len(buffer)
            logging.info(f"{count_total:,} lignes insérées ...")
            buffer.clear()

    # Dernier lot
    if buffer:
        execute_values(
            pg_cur,
            f"INSERT INTO {pg_table} ({cols_csv}) VALUES %s",
            buffer
        )
        pg_conn.commit()
        count_total += len(buffer)

    logging.info(f"Chargement terminé : {count_total:,} lignes insérées dans {pg_table}.")



    # ---------------- ORDONNANCE_SORTIE ----------------
    _load_jsonl_to_table(
        pg_conn=pg_conn,
        pg_cur=pg_cur,
        jsonl_path="/tmp/etl_iris/ordonnance_sortie.jsonl",
        target_table="oeci.ordonnance_sortie",
    )

    #----------------RDV--------------
    logging.info(" Début du chargement de oeci.rdv depuis osiris.rdv...")

    # même ordre et nommage que la table osiris.rdv
    cols = ["ipp_ocr", "date_rdv", "libelle_examen", "date_booked"]
    cols_csv = ", ".join(cols)

    #  on vide la table cible avant d'insérer
    pg_cur.execute("TRUNCATE TABLE oeci.rdv CASCADE;")
    pg_conn.commit()

    # on insère depuis la source osiris.rdv
    sql = f"""
           INSERT INTO oeci.rdv ({cols_csv})
           SELECT {cols_csv}
           FROM osiris.rdv;
           """
    logging.info(f"[ETL] Insertion des colonnes {cols_csv} depuis osiris.rdv...")
    pg_cur.execute(sql)
    pg_conn.commit()

    logging.info(f"Chargement terminé : {pg_cur.rowcount} lignes insérées dans oeci.rdv.")
	
	
	# --------------------------------------------------------------------
	# TRAITEMENTS depuis OSIRIS → fichier → COPY
    # --------------------------------------------------------------------

    extract_treatments_to_file(pg_conn)
    load_treatments_from_file(pg_conn)


    # ---------------- CLEANUP ----------------
    pg_cur.close()
    pg_conn.close()
    logging.info("Chargement OECI terminé avec succès")
