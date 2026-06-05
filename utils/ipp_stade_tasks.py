from __future__ import annotations

import json
import logging
import os
import re
import shlex
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers SSH (paramiko)
# ---------------------------------------------------------------------------

def _get_ssh_client(
    host: str,
    port: int,
    user: str,
    password_var_key: str,
) -> "paramiko.SSHClient":
    """Ouvre une connexion SSH avec le mot de passe stocké dans les variables Airflow."""
    import paramiko  # import local pour éviter une dépendance au démarrage

    password = Variable.get(password_var_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=user,
        password=password,
        timeout=30,
        allow_agent=False,
        look_for_keys=False,
    )
    return client


def _scp_put_files(
    sftp: "paramiko.SFTPClient",
    local_paths: list[str],
    remote_dir: str,
) -> None:
    """Envoie une liste de fichiers locaux vers remote_dir via SFTP."""
    try:
        sftp.stat(remote_dir)
    except FileNotFoundError:
        sftp.mkdir(remote_dir)

    for local_path in local_paths:
        remote_path = remote_dir.rstrip("/") + "/" + os.path.basename(local_path)
        logger.info("SFTP put %s -> %s", local_path, remote_path)
        sftp.put(local_path, remote_path)


# ---------------------------------------------------------------------------
# 1. Extraction des IPP depuis v_statut_vital
# ---------------------------------------------------------------------------


def _ipp_records_from_df(df: pd.DataFrame) -> list[dict[str, Optional[str]]]:
    records: list[dict[str, Optional[str]]] = []
    for _, row in df.iterrows():
        ipp = str(row.get("ipp_ocr", "")).strip()
        if not ipp:
            continue
        records.append(
            {
                "ipp": ipp,
                "organe": None if pd.isna(row.get("organe")) else str(row.get("organe")).strip() or None,
                "code_cim": None if pd.isna(row.get("code_cim")) else str(row.get("code_cim")).strip() or None,
                "date_diag_tkc": None if pd.isna(row.get("date_diag_tkc")) else str(row.get("date_diag_tkc")),
                "date_diag_dcc": None if pd.isna(row.get("date_diag_dcc")) else str(row.get("date_diag_dcc")),
            }
        )
    return records

def _extract_ipp_df(
    date_debut_obs: str,
    date_fin_obs: str,
    conn_id: str,
    only_missing_stage: bool,
) -> pd.DataFrame:
    hook = PostgresHook(postgres_conn_id=conn_id)
    start_date = str(date_debut_obs)
    if len(start_date) == 4 and start_date.isdigit():
        start_date = f"{start_date}-01-01"
    end_date = str(date_fin_obs)
    if len(end_date) == 4 and end_date.isdigit():
        end_date = f"{end_date}-12-31"
    stage_filter = ""
    if only_missing_stage:
        stage_filter = """
        AND NULLIF(BTRIM(COALESCE(stage::text, '')), '') IS NULL
        """

    query = f"""
    SELECT DISTINCT ON (ipp_ocr)
        ipp_ocr,
        organe,
        code_cim,
        date_diag_tkc::date AS date_diag_tkc,
        date_diag_dcc::date AS date_diag_dcc
    FROM datamart_oeci_survie.v_statut_vital
    WHERE
        organe IS NOT NULL
        AND code_cim IS NOT NULL
        AND (
            UPPER(BTRIM(organe::text)) = 'SEIN'
            OR (
                UPPER(BTRIM(organe::text)) = 'UROLOGIE'
                AND LEFT(UPPER(BTRIM(code_cim::text)), 3) = 'C61'
            )
            OR (
                UPPER(BTRIM(organe::text)) = 'PEAU'
                AND LEFT(UPPER(BTRIM(code_cim::text)), 3) = 'C43'
            )
        )
        AND COALESCE(date_diag_tkc, date_diag_dcc) IS NOT NULL
        AND COALESCE(date_diag_tkc, date_diag_dcc)::date >= DATE '{start_date}'
        AND COALESCE(date_diag_tkc, date_diag_dcc)::date <= DATE '{end_date}'
        AND ipp_ocr IS NOT NULL
        AND ipp_ocr <> ''
        {stage_filter}
    ORDER BY
        ipp_ocr,
        COALESCE(date_diag_tkc, date_diag_dcc) DESC NULLS LAST,
        date_diag_tkc DESC NULLS LAST,
        date_diag_dcc DESC NULLS LAST
    ;
    """

    conn = hook.get_conn()
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def extract_ipp_task(date_debut_obs: str, date_fin_obs: str, conn_id: str = "postgres_test", **kwargs) -> None:
    """
    Extrait tous les IPP distincts depuis datamart_oeci_survie.v_statut_vital
    dont date_diag_tkc (ou date_diag_dcc) est dans la fenetre demandee.

    Pousse le résultat en XCom : liste JSON des IPP (strings).
    """
    df = _extract_ipp_df(
        date_debut_obs=date_debut_obs,
        date_fin_obs=date_fin_obs,
        conn_id=conn_id,
        only_missing_stage=False,
    )

    ipp_list = df["ipp_ocr"].astype(str).str.strip().tolist()
    logger.info("IPP extraits : %d", len(ipp_list))

    ti = kwargs["ti"]
    ti.xcom_push(key="ipp_list", value=ipp_list)
    ti.xcom_push(key="ipp_records", value=_ipp_records_from_df(df))


def extract_ipp_without_stage_task(
    date_debut_obs: str,
    date_fin_obs: str,
    conn_id: str = "postgres_test",
    **kwargs,
) -> None:
    """
    Extrait uniquement les IPP dont la colonne stade est absente dans
    datamart_oeci_survie.v_statut_vital, dans la fenetre demandee.
    """
    df = _extract_ipp_df(
        date_debut_obs=date_debut_obs,
        date_fin_obs=date_fin_obs,
        conn_id=conn_id,
        only_missing_stage=True,
    )

    ipp_list = df["ipp_ocr"].astype(str).str.strip().tolist()
    logger.info("IPP sans stade extraits : %d", len(ipp_list))

    ti = kwargs["ti"]
    ti.xcom_push(key="ipp_list", value=ipp_list)
    ti.xcom_push(key="ipp_records", value=_ipp_records_from_df(df))


# ---------------------------------------------------------------------------
# 2. Push PDF + JSON metadata vers le serveur distant
# ---------------------------------------------------------------------------

def push_pdf_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    ssh_password_var_key: str,
    ipp_task_id: str = "extract_ipp_from_statut_vital",
    remote_script: str = "/opt/push_pdf_llm.py",
    source_dir: str = "/opt/PDF",
    stage_dir: str = "/home/administrateur/pdf_llm_stage",
    link_mode: str = "symlink",
    remote_python_bin: str = "python3",
    remote_tmp_dir: str = "/tmp",
    remote_progress_every: int = 200,
    remote_command_timeout: Optional[int] = None,
    **kwargs,
) -> None:
    """
    Lance sur le lakehouse le script /opt/push_pdf_llm.

    Le script distant lit la liste d'IPP produite par la task précédente,
    scanne les PDF/JSON sur le lakehouse, puis materialise uniquement les
    couples eligibles dans un dossier de staging local.
    """
    ti = kwargs["ti"]
    ipp_list: list[str] = ti.xcom_pull(
        task_ids=ipp_task_id,
        key="ipp_list",
    )
    if not ipp_list:
        logger.warning("Aucun IPP reçu en XCom – rien à envoyer.")
        return

    logger.info("IPP reçus depuis XCom : %d", len(ipp_list))

    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)

    local_ipp_file: Optional[str] = None
    remote_ipp_file: Optional[str] = None
    sftp = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="ipp_list_",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            json.dump({"ipp_list": ipp_list}, tmp, ensure_ascii=False)
            local_ipp_file = tmp.name

        remote_ipp_file = (
            f"{remote_tmp_dir.rstrip('/')}/"
            f"{os.path.basename(local_ipp_file)}"
        )

        sftp = client.open_sftp()
        sftp.put(local_ipp_file, remote_ipp_file)
        logger.info("Liste IPP copiée sur %s:%s", remote_host, remote_ipp_file)

        cmd = " ".join(
            [
                shlex.quote(remote_python_bin),
                shlex.quote(remote_script),
                "--ipp-file",
                shlex.quote(remote_ipp_file),
                "--local-dir",
                shlex.quote(source_dir),
                "--stage-dir",
                shlex.quote(stage_dir),
                "--link-mode",
                shlex.quote(link_mode),
                "--clean-stage-dir",
                "--progress-every",
                shlex.quote(str(remote_progress_every)),
            ]
        )

        logger.info(
            "Commande distante sur %s : %s %s --ipp-file %s ...",
            remote_host,
            remote_python_bin,
            remote_script,
            remote_ipp_file,
        )
        _, stdout, stderr = client.exec_command(
            cmd,
            timeout=remote_command_timeout,
            get_pty=True,
        )
        stdout_txt = stdout.read().decode("utf-8", errors="replace")
        stderr_txt = stderr.read().decode("utf-8", errors="replace")
        exit_status = stdout.channel.recv_exit_status()

        if stdout_txt:
            stdout_tail = "\n".join(stdout_txt.strip().splitlines()[-40:])
            if stdout_tail:
                logger.info("STDOUT push_pdf (tail):\n%s", stdout_tail)
        if stderr_txt:
            stderr_tail = "\n".join(stderr_txt.strip().splitlines()[-40:])
            if stderr_tail:
                logger.warning("STDERR push_pdf (tail):\n%s", stderr_tail)

        if exit_status != 0:
            error_excerpt = (stderr_txt or stdout_txt).strip()[:1000]
            raise RuntimeError(
                f"Le script distant {remote_script} a terminé avec le code {exit_status}. "
                f"Détail: {error_excerpt}"
            )
    finally:
        if sftp is not None:
            if remote_ipp_file:
                try:
                    sftp.remove(remote_ipp_file)
                except Exception:
                    pass
            sftp.close()
        if local_ipp_file and os.path.exists(local_ipp_file):
            os.unlink(local_ipp_file)
        client.close()

    logger.info("Staging PDF distant termine avec succes.")


# ---------------------------------------------------------------------------
# 3. Exécution du script d'extraction TNM sur le serveur distant
# ---------------------------------------------------------------------------

def run_tnm_extraction_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_script: str,
    remote_data_dir: str,
    ssh_password_var_key: str,
    ipp_task_id: str = "extract_ipp_from_statut_vital",
    remote_output_dir: Optional[str] = None,
    remote_python_bin: str = "python3",
    remote_csv_name: str = "ipp_stage_results.csv",
    remote_tmp_dir: str = "/tmp",
    remote_command_timeout: Optional[int] = None,
    **kwargs,
) -> None:
    """
    Lance extract_tnm_stage_by_ipp.py sur le serveur distant via SSH.
    Le script reçoit remote_data_dir comme répertoire d'entrée et écrit le CSV
    dans remote_output_dir.
    """
    ti = kwargs["ti"]
    ipp_records: list[dict[str, Optional[str]]] = ti.xcom_pull(
        task_ids=ipp_task_id,
        key="ipp_records",
    ) or []

    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    local_metadata_file: Optional[str] = None
    remote_metadata_file: Optional[str] = None
    sftp = None
    try:
        output_dir = remote_output_dir or remote_data_dir
        output_csv_path = f"{output_dir.rstrip('/')}/{remote_csv_name}"
        if ipp_records:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                prefix="ipp_metadata_",
                delete=False,
                encoding="utf-8",
            ) as tmp:
                json.dump({"ipp_records": ipp_records}, tmp, ensure_ascii=False)
                local_metadata_file = tmp.name

            remote_metadata_file = (
                f"{remote_tmp_dir.rstrip('/')}/"
                f"{os.path.basename(local_metadata_file)}"
            )
            sftp = client.open_sftp()
            sftp.put(local_metadata_file, remote_metadata_file)
            logger.info("Métadonnées IPP copiées sur %s:%s", remote_host, remote_metadata_file)

        cmd = (
            f"mkdir -p {shlex.quote(output_dir)} && "
            f"rm -f {shlex.quote(output_csv_path)} && "
            f"{shlex.quote(remote_python_bin)} {shlex.quote(remote_script)} "
            f"{shlex.quote(remote_data_dir)} "
            f"--output-dir {shlex.quote(output_dir)} "
            f"--ipp-strategy baseline "
            f"--log-level INFO "
            f"--csv-name {shlex.quote(remote_csv_name)}"
        )
        if remote_metadata_file:
            cmd += f" --ipp-metadata-file {shlex.quote(remote_metadata_file)}"
        logger.info("Commande SSH : %s", cmd)
        _, stdout, stderr = client.exec_command(
            cmd,
            timeout=remote_command_timeout,
            get_pty=True,
        )
        stdout_txt = stdout.read().decode("utf-8", errors="replace")
        stderr_txt = stderr.read().decode("utf-8", errors="replace")
        exit_status = stdout.channel.recv_exit_status()

        if stdout_txt:
            stdout_tail = "\n".join(stdout_txt.strip().splitlines()[-40:])
            if stdout_tail:
                logger.info("STDOUT regex (tail):\n%s", stdout_tail)
        if stderr_txt:
            stderr_tail = "\n".join(stderr_txt.strip().splitlines()[-40:])
            if stderr_tail:
                logger.warning("STDERR regex (tail):\n%s", stderr_tail)

        if exit_status != 0:
            error_excerpt = (stderr_txt or stdout_txt).strip()[:1000]
            raise RuntimeError(
                f"Le script TNM a terminé avec le code {exit_status}. "
                f"Détail: {error_excerpt}"
            )
        logger.info("Extraction TNM terminée avec succès.")
    finally:
        if sftp is not None:
            if remote_metadata_file:
                try:
                    sftp.remove(remote_metadata_file)
                except Exception:
                    pass
            sftp.close()
        if local_metadata_file and os.path.exists(local_metadata_file):
            os.unlink(local_metadata_file)
        client.close()


# ---------------------------------------------------------------------------
# 4. Rapatriement du CSV
# ---------------------------------------------------------------------------

def fetch_csv_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_csv_path: str,
    local_csv_path: str,
    ssh_password_var_key: str,
    **kwargs,
) -> None:
    """Télécharge le CSV résultant depuis le serveur distant vers Airflow."""
    local_dir = os.path.dirname(local_csv_path)
    if local_dir:
        os.makedirs(local_dir, exist_ok=True)

    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    try:
        sftp = client.open_sftp()
        try:
            logger.info("SFTP get %s -> %s", remote_csv_path, local_csv_path)
            sftp.get(remote_csv_path, local_csv_path)
        finally:
            sftp.close()
    finally:
        client.close()
    logger.info("CSV récupéré : %s", local_csv_path)


def cleanup_remote_dir_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_dir: str,
    ssh_password_var_key: str,
    remote_command_timeout: Optional[int] = None,
    **kwargs,
) -> None:
    """Vide le contenu d'un répertoire distant sans supprimer le dossier racine."""
    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    try:
        cmd = (
            f"mkdir -p {shlex.quote(remote_dir)} && "
            f"find {shlex.quote(remote_dir)} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} +"
        )
        logger.info("Nettoyage distant : %s", cmd)
        _, stdout, stderr = client.exec_command(cmd, timeout=remote_command_timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_txt = stdout.read().decode("utf-8", errors="replace")
        stderr_txt = stderr.read().decode("utf-8", errors="replace")

        if stdout_txt.strip():
            logger.info("STDOUT cleanup:\n%s", stdout_txt.strip())
        if stderr_txt.strip():
            logger.warning("STDERR cleanup:\n%s", stderr_txt.strip())

        if exit_status != 0:
            error_excerpt = (stderr_txt or stdout_txt).strip()[:1000]
            raise RuntimeError(
                f"Le nettoyage distant de {remote_dir} a terminé avec le code {exit_status}. "
                f"Détail: {error_excerpt}"
            )
    finally:
        client.close()

    logger.info("Nettoyage distant terminé : %s", remote_dir)


# ---------------------------------------------------------------------------
# 5. Chargement dans datamart_oeci_survie.ipp_stade
# ---------------------------------------------------------------------------

# Mapping stade texte → libelle canonique
STAGE_DIGIT_MAPPING = {
    "1": "I",
    "2": "II",
    "3": "III",
    "4": "IV",
}


def _normalize_stage(raw: object) -> Optional[str]:
    """Convertit 'IIA' ou 'Stage IIA (Mx)' → 'Stage IIA'."""
    if raw is None or pd.isna(raw):
        return None
    raw = str(raw).strip()
    if not raw or raw.lower() in {"null", "nan"}:
        return None
    clean = raw.split("(")[0].strip().upper()
    clean = re.sub(r"^(STADE|STAGE)\s*", "", clean).strip()
    clean = clean.replace("AJCC", "").strip()
    clean = STAGE_DIGIT_MAPPING.get(clean, clean)
    if not clean or not re.fullmatch(r"0|IV|III[ABC]?|II[ABC]?|I[ABC]?", clean):
        return None
    return f"Stage {clean}"


def _normalize_text(raw: object) -> Optional[str]:
    if raw is None or pd.isna(raw):
        return None
    value = str(raw).strip()
    if not value or value.lower() in {"null", "nan", "nat"}:
        return None
    return value


def _to_db_value(raw: object) -> object:
    if raw is None:
        return None
    if isinstance(raw, list):
        return raw
    try:
        if pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass
    return raw


def _parse_date_value(raw: object) -> Optional[str]:
    value = _normalize_text(raw)
    if value is None:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _parse_bool_value(raw: object) -> Optional[bool]:
    value = _normalize_text(raw)
    if value is None:
        return None
    lowered = value.lower()
    if lowered in {"true", "t", "1", "yes", "y", "oui"}:
        return True
    if lowered in {"false", "f", "0", "no", "n", "non"}:
        return False
    return None


def _parse_int_value(raw: object) -> Optional[int]:
    value = _normalize_text(raw)
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_text_array(raw: object) -> Optional[list[str]]:
    value = _normalize_text(raw)
    if value is None:
        return None

    if value.startswith("[") and value.endswith("]"):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                items = [_normalize_text(item) for item in parsed]
                cleaned = [item for item in items if item]
                return cleaned or None
        except Exception:
            pass

    normalized = value.replace("|", ",").replace(";", ",")
    items = [_normalize_text(item) for item in normalized.split(",")]
    cleaned = [item for item in items if item]
    return cleaned or None


INTEGER_TYPE_RANGES = {
    "smallint": (-32768, 32767),
    "integer": (-2147483648, 2147483647),
    "bigint": (-9223372036854775808, 9223372036854775807),
}

INTEGER_DF_COLUMNS = {
    "documents_seen": "documents_seen_int",
    "documents_with_stage": "documents_with_stage_int",
    "grade_sbr": "grade_sbr_int",
    "sbr_tubule_score": "sbr_tubule_score_int",
    "sbr_nuclear_score": "sbr_nuclear_score_int",
    "sbr_mitotic_score": "sbr_mitotic_score_int",
    "er_percent": "er_percent_int",
    "pr_percent": "pr_percent_int",
    "pdl1_cps_value": "pdl1_cps_value_int",
}


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _fetch_table_column_types(cur, schema: str, table: str) -> dict[str, str]:
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        """,
        (schema, table),
    )
    return {column_name: data_type for column_name, data_type in cur.fetchall()}


def _ensure_ipp_text_column(cur, schema: str, table: str) -> dict[str, str]:
    column_types = _fetch_table_column_types(cur, schema, table)
    ipp_type = column_types.get("ipp")
    if ipp_type in INTEGER_TYPE_RANGES:
        full_table = f"{_quote_ident(schema)}.{_quote_ident(table)}"
        logger.warning(
            "Conversion de %s.ipp de %s vers text pour éviter les débordements d'identifiants IPP.",
            full_table,
            ipp_type,
        )
        cur.execute(
            f"""
            ALTER TABLE {full_table}
            ALTER COLUMN ipp TYPE text USING ipp::text
            """
        )
        column_types["ipp"] = "text"
    return column_types


def _nullify_integer_range_overflows(
    df: pd.DataFrame,
    column_types: dict[str, str],
) -> None:
    for db_column, df_column in INTEGER_DF_COLUMNS.items():
        data_type = column_types.get(db_column)
        if data_type not in INTEGER_TYPE_RANGES or df_column not in df.columns:
            continue

        lower_bound, upper_bound = INTEGER_TYPE_RANGES[data_type]
        values = df[df_column]
        invalid_mask = values.notna() & (
            (values < lower_bound) | (values > upper_bound)
        )
        if not invalid_mask.any():
            continue

        sample_columns = ["ipp", df_column]
        if db_column in df.columns:
            sample_columns.insert(1, db_column)
        samples = (
            df.loc[invalid_mask, sample_columns]
            .head(20)
            .rename(
                columns={
                    db_column: "raw_value",
                    df_column: "parsed_value",
                }
            )
            .to_dict(orient="records")
        )
        logger.warning(
            "Valeurs hors plage pour %s (%s) dans ipp_stade: %d valeur(s) remplacee(s) par NULL. "
            "Plage attendue: %s..%s. Exemples ipp/raw_value/parsed_value: %s",
            db_column,
            data_type,
            int(invalid_mask.sum()),
            lower_bound,
            upper_bound,
            samples,
        )
        df.loc[invalid_mask, df_column] = None


def _log_insert_failure_diagnostics(
    cur,
    insert_sql: str,
    rows: list[tuple],
    export_columns: list[str],
) -> None:
    from psycopg2.extras import execute_values

    integer_columns = set(INTEGER_DF_COLUMNS.keys())

    for row_number, row_values in enumerate(rows, start=1):
        cur.execute("SAVEPOINT ipp_stade_row_diag")
        try:
            execute_values(cur, insert_sql, [row_values], page_size=1)
        except Exception as row_error:
            cur.execute("ROLLBACK TO SAVEPOINT ipp_stade_row_diag")
            cur.execute("RELEASE SAVEPOINT ipp_stade_row_diag")

            row_dict = dict(zip(export_columns, row_values))
            integer_values = {
                column: row_dict.get(column)
                for column in export_columns
                if column in integer_columns
            }
            logger.error(
                "Echec insertion ipp_stade sur la ligne %d: ipp=%s, erreur=%s, colonnes_int=%s, ligne_complete=%s",
                row_number,
                row_dict.get("ipp"),
                row_error,
                integer_values,
                row_dict,
            )
            return
        else:
            cur.execute("ROLLBACK TO SAVEPOINT ipp_stade_row_diag")
            cur.execute("RELEASE SAVEPOINT ipp_stade_row_diag")

    logger.error(
        "Echec insertion ipp_stade en bulk, mais aucune ligne isolee n'a reproduit l'erreur."
    )


def _series_or_default(df: pd.DataFrame, column: str, default: object = None) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([default] * len(df), index=df.index, dtype="object")


def _drop_embedded_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Supprime les lignes du corps CSV qui correspondent en réalité au header.
    """
    if df.empty:
        return df

    text_columns = [column for column in df.columns if isinstance(column, str)]
    if not text_columns:
        return df

    lowered = (
        df[text_columns]
        .fillna("")
        .astype(str)
        .apply(lambda series: series.str.strip().str.lower())
    )

    match_count = pd.Series(0, index=df.index, dtype="int64")
    for column in text_columns:
        match_count = match_count.add((lowered[column] == column.lower()).astype("int64"))

    ipp_header_mask = pd.Series(False, index=df.index)
    if "ipp" in lowered.columns:
        ipp_header_mask = lowered["ipp"].isin({"ipp", "ipp_ocr"})

    header_mask = ipp_header_mask | (match_count >= 3)
    removed = int(header_mask.sum())
    if removed:
        logger.warning(
            "Suppression de %d ligne(s) de header parasite dans le CSV avant insertion.",
            removed,
        )
        return df.loc[~header_mask].copy()

    return df


def _fetch_statut_vital_metadata(pg_conn, ipps: list[str]) -> pd.DataFrame:
    if not ipps:
        return pd.DataFrame(
            columns=["ipp", "organe", "code_cim", "date_diag_tkc", "date_diag_dcc"]
        )

    query = """
        WITH ranked AS (
            SELECT
                ipp_ocr::text AS ipp,
                organe::text AS organe,
                code_cim::text AS code_cim,
                date_diag_tkc::date AS date_diag_tkc,
                date_diag_dcc::date AS date_diag_dcc,
                ROW_NUMBER() OVER (
                    PARTITION BY ipp_ocr
                    ORDER BY
                        COALESCE(date_diag_tkc, date_diag_dcc) DESC NULLS LAST,
                        date_diag_tkc DESC NULLS LAST,
                        date_diag_dcc DESC NULLS LAST
                ) AS rn
            FROM datamart_oeci_survie.v_statut_vital
            WHERE
                ipp_ocr = ANY(%s)
                AND ipp_ocr IS NOT NULL
                AND ipp_ocr <> ''
                AND organe IS NOT NULL
                AND code_cim IS NOT NULL
                AND (
                    UPPER(BTRIM(organe::text)) = 'SEIN'
                    OR (
                        UPPER(BTRIM(organe::text)) = 'UROLOGIE'
                        AND LEFT(UPPER(BTRIM(code_cim::text)), 3) = 'C61'
                    )
                    OR (
                        UPPER(BTRIM(organe::text)) = 'PEAU'
                        AND LEFT(UPPER(BTRIM(code_cim::text)), 3) = 'C43'
                    )
                )
        )
        SELECT
            ipp,
            organe,
            code_cim,
            date_diag_tkc,
            date_diag_dcc
        FROM ranked
        WHERE rn = 1
    """
    return pd.read_sql_query(query, pg_conn, params=(ipps,))


def load_ipp_stade_task(
    local_csv_path: str,
    conn_id: str = "postgres_test",
    **kwargs,
) -> None:
    """
    Lit le CSV ipp_stage_results.csv, enrichit chaque ligne avec les métadonnées
    de v_statut_vital, puis effectue un UPSERT complet dans
    datamart_oeci_survie.ipp_stade.
    """
    from psycopg2.extras import execute_values

    df = pd.read_csv(local_csv_path, dtype=str)
    logger.info("CSV chargé : %d lignes", len(df))

    if df.empty:
        logger.warning("CSV vide, rien à charger.")
        return

    df = _drop_embedded_header_rows(df)
    if df.empty:
        logger.warning("CSV ne contient que des headers parasites, rien à charger.")
        return

    # Nettoyage
    df["ipp"] = df["ipp"].fillna("").astype(str).str.strip()
    df = df[df["ipp"] != ""]
    if df.empty:
        logger.warning("CSV sans IPP valide, rien à charger.")
        return

    df["stage"] = _series_or_default(df, "stage")
    df["stade_norm"] = df["stage"].apply(_normalize_stage)
    df["tnm_raw"] = _series_or_default(df, "tnm_raw")
    df["t"] = _series_or_default(df, "t")
    df["n"] = _series_or_default(df, "n")
    df["m"] = _series_or_default(df, "m")
    df["document_date_fmt"] = _series_or_default(df, "document_date").apply(_parse_date_value)
    df["source_pdf"] = _series_or_default(df, "source_pdf")
    df["status"] = _series_or_default(df, "status")
    df["reason"] = _series_or_default(df, "reason")
    df["selection_reason"] = _series_or_default(df, "selection_reason")
    df["document_kind"] = _series_or_default(df, "document_kind")
    df["tnm_context"] = _series_or_default(df, "tnm_context")
    df["treatment_detected_bool"] = _series_or_default(df, "treatment_detected").apply(_parse_bool_value)
    df["treatment_keywords_arr"] = _series_or_default(df, "treatment_keywords").apply(_parse_text_array)
    df["surgery_detected_bool"] = _series_or_default(df, "surgery_detected").apply(_parse_bool_value)
    df["chemo_detected_bool"] = _series_or_default(df, "chemo_detected").apply(_parse_bool_value)
    df["radiotherapy_detected_bool"] = _series_or_default(df, "radiotherapy_detected").apply(_parse_bool_value)
    df["metastasis_detected_bool"] = _series_or_default(df, "metastasis_detected").apply(_parse_bool_value)
    df["documents_seen_int"] = _series_or_default(df, "documents_seen").apply(_parse_int_value)
    df["documents_with_stage_int"] = _series_or_default(df, "documents_with_stage").apply(_parse_int_value)
    df["stage_confidence"] = _series_or_default(df, "stage_confidence")
    df["histology_type"] = _series_or_default(df, "histology_type")
    df["grade_sbr_int"] = _series_or_default(df, "grade_sbr").apply(_parse_int_value)
    df["sbr_tubule_score_int"] = _series_or_default(df, "sbr_tubule_score").apply(_parse_int_value)
    df["sbr_nuclear_score_int"] = _series_or_default(df, "sbr_nuclear_score").apply(_parse_int_value)
    df["sbr_mitotic_score_int"] = _series_or_default(df, "sbr_mitotic_score").apply(_parse_int_value)
    df["er_percent_int"] = _series_or_default(df, "er_percent").apply(_parse_int_value)
    df["er_intensity"] = _series_or_default(df, "er_intensity")
    df["er_status"] = _series_or_default(df, "er_status")
    df["pr_percent_int"] = _series_or_default(df, "pr_percent").apply(_parse_int_value)
    df["pr_intensity"] = _series_or_default(df, "pr_intensity")
    df["pr_status"] = _series_or_default(df, "pr_status")
    df["hormone_receptor_status_project"] = _series_or_default(df, "hormone_receptor_status_project")
    df["her2_ihc_score"] = _series_or_default(df, "her2_ihc_score")
    df["her2_ish_result"] = _series_or_default(df, "her2_ish_result")
    df["her2_status"] = _series_or_default(df, "her2_status")
    df["her2_qualification_project"] = _series_or_default(df, "her2_qualification_project")
    df["pdl1_cps_value_int"] = _series_or_default(df, "pdl1_cps_value").apply(_parse_int_value)
    df["pdl1_cps_status_project"] = _series_or_default(df, "pdl1_cps_status_project")
    df["breast_anapath_sources"] = _series_or_default(df, "breast_anapath_sources")

    hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = hook.get_conn()
    schema  = "datamart_oeci_survie"
    table   = "ipp_stade"
    full_table = f"{schema}.{table}"
    export_columns = [
        "ipp",
        "organe",
        "code_cim",
        "date_diag_tkc",
        "date_diag_dcc",
        "stage",
        "tnm_raw",
        "t",
        "n",
        "m",
        "document_date",
        "source_pdf",
        "status",
        "reason",
        "selection_reason",
        "document_kind",
        "tnm_context",
        "treatment_detected",
        "treatment_keywords",
        "surgery_detected",
        "chemo_detected",
        "radiotherapy_detected",
        "metastasis_detected",
        "documents_seen",
        "documents_with_stage",
        "last_update",
        "stage_confidence",
        "histology_type",
        "grade_sbr",
        "sbr_tubule_score",
        "sbr_nuclear_score",
        "sbr_mitotic_score",
        "er_percent",
        "er_intensity",
        "er_status",
        "pr_percent",
        "pr_intensity",
        "pr_status",
        "hormone_receptor_status_project",
        "her2_ihc_score",
        "her2_ish_result",
        "her2_status",
        "her2_qualification_project",
        "pdl1_cps_value",
        "pdl1_cps_status_project",
        "breast_anapath_sources",
    ]

    try:
        metadata_df = _fetch_statut_vital_metadata(
            pg_conn,
            df["ipp"].drop_duplicates().tolist(),
        )
        if not metadata_df.empty:
            metadata_df["ipp"] = metadata_df["ipp"].astype(str).str.strip()
            metadata_df["date_diag_tkc"] = metadata_df["date_diag_tkc"].apply(_parse_date_value)
            metadata_df["date_diag_dcc"] = metadata_df["date_diag_dcc"].apply(_parse_date_value)

        df = df.merge(metadata_df, on="ipp", how="left", suffixes=("_csv", ""))

        if df["ipp"].duplicated().any():
            logger.warning(
                "IPP dupliqués détectés dans le CSV enrichi, conservation de la dernière ligne par IPP."
            )
            df = df.drop_duplicates(subset=["ipp"], keep="last")

        with pg_conn.cursor() as cur:
            column_types = _ensure_ipp_text_column(cur, schema, table)
            _nullify_integer_range_overflows(df, column_types)

            last_update = datetime.utcnow()
            rows = []
            for _, row in df.iterrows():
                record = (
                    row["ipp"],
                    _normalize_text(row.get("organe")),
                    _normalize_text(row.get("code_cim")),
                    row.get("date_diag_tkc"),
                    row.get("date_diag_dcc"),
                    row.get("stade_norm"),
                    _normalize_text(row.get("tnm_raw")),
                    _normalize_text(row.get("t")),
                    _normalize_text(row.get("n")),
                    _normalize_text(row.get("m")),
                    row.get("document_date_fmt"),
                    _normalize_text(row.get("source_pdf")),
                    _normalize_text(row.get("status")),
                    _normalize_text(row.get("reason")),
                    _normalize_text(row.get("selection_reason")),
                    _normalize_text(row.get("document_kind")),
                    _normalize_text(row.get("tnm_context")),
                    row.get("treatment_detected_bool"),
                    row.get("treatment_keywords_arr"),
                    row.get("surgery_detected_bool"),
                    row.get("chemo_detected_bool"),
                    row.get("radiotherapy_detected_bool"),
                    row.get("metastasis_detected_bool"),
                    row.get("documents_seen_int"),
                    row.get("documents_with_stage_int"),
                    last_update,
                    _normalize_text(row.get("stage_confidence")),
                    _normalize_text(row.get("histology_type")),
                    row.get("grade_sbr_int"),
                    row.get("sbr_tubule_score_int"),
                    row.get("sbr_nuclear_score_int"),
                    row.get("sbr_mitotic_score_int"),
                    row.get("er_percent_int"),
                    _normalize_text(row.get("er_intensity")),
                    _normalize_text(row.get("er_status")),
                    row.get("pr_percent_int"),
                    _normalize_text(row.get("pr_intensity")),
                    _normalize_text(row.get("pr_status")),
                    _normalize_text(row.get("hormone_receptor_status_project")),
                    _normalize_text(row.get("her2_ihc_score")),
                    _normalize_text(row.get("her2_ish_result")),
                    _normalize_text(row.get("her2_status")),
                    _normalize_text(row.get("her2_qualification_project")),
                    row.get("pdl1_cps_value_int"),
                    _normalize_text(row.get("pdl1_cps_status_project")),
                    _normalize_text(row.get("breast_anapath_sources")),
                )
                rows.append(tuple(_to_db_value(value) for value in record))

            if rows:
                enriched_csv_path = str(
                    Path(local_csv_path).with_name(
                        f"{Path(local_csv_path).stem}_enriched.csv"
                    )
                )
                pd.DataFrame(rows, columns=export_columns).to_csv(
                    enriched_csv_path,
                    index=False,
                )
                logger.info("CSV enrichi écrit : %s", enriched_csv_path)

                insert_sql = f"""
                    INSERT INTO {full_table} (
                        ipp,
                        organe,
                        code_cim,
                        date_diag_tkc,
                        date_diag_dcc,
                        stage,
                        tnm_raw,
                        t,
                        n,
                        m,
                        document_date,
                        source_pdf,
                        status,
                        reason,
                        selection_reason,
                        document_kind,
                        tnm_context,
                        treatment_detected,
                        treatment_keywords,
                        surgery_detected,
                        chemo_detected,
                        radiotherapy_detected,
                        metastasis_detected,
                        documents_seen,
                        documents_with_stage,
                        last_update,
                        stage_confidence,
                        histology_type,
                        grade_sbr,
                        sbr_tubule_score,
                        sbr_nuclear_score,
                        sbr_mitotic_score,
                        er_percent,
                        er_intensity,
                        er_status,
                        pr_percent,
                        pr_intensity,
                        pr_status,
                        hormone_receptor_status_project,
                        her2_ihc_score,
                        her2_ish_result,
                        her2_status,
                        her2_qualification_project,
                        pdl1_cps_value,
                        pdl1_cps_status_project,
                        breast_anapath_sources
                    )
                    VALUES %s
                    ON CONFLICT (ipp) DO UPDATE
                        SET organe                = EXCLUDED.organe,
                            code_cim              = EXCLUDED.code_cim,
                            date_diag_tkc         = EXCLUDED.date_diag_tkc,
                            date_diag_dcc         = EXCLUDED.date_diag_dcc,
                            stage                 = EXCLUDED.stage,
                            tnm_raw               = EXCLUDED.tnm_raw,
                            t                     = EXCLUDED.t,
                            n                     = EXCLUDED.n,
                            m                     = EXCLUDED.m,
                            document_date         = EXCLUDED.document_date,
                            source_pdf            = EXCLUDED.source_pdf,
                            status                = EXCLUDED.status,
                            reason                = EXCLUDED.reason,
                            selection_reason      = EXCLUDED.selection_reason,
                            document_kind         = EXCLUDED.document_kind,
                            tnm_context           = EXCLUDED.tnm_context,
                            treatment_detected    = EXCLUDED.treatment_detected,
                            treatment_keywords    = EXCLUDED.treatment_keywords,
                            surgery_detected      = EXCLUDED.surgery_detected,
                            chemo_detected        = EXCLUDED.chemo_detected,
                            radiotherapy_detected = EXCLUDED.radiotherapy_detected,
                            metastasis_detected   = EXCLUDED.metastasis_detected,
                            documents_seen        = EXCLUDED.documents_seen,
                            documents_with_stage  = EXCLUDED.documents_with_stage,
                            last_update           = EXCLUDED.last_update,
                            stage_confidence      = EXCLUDED.stage_confidence,
                            histology_type        = EXCLUDED.histology_type,
                            grade_sbr             = EXCLUDED.grade_sbr,
                            sbr_tubule_score      = EXCLUDED.sbr_tubule_score,
                            sbr_nuclear_score     = EXCLUDED.sbr_nuclear_score,
                            sbr_mitotic_score     = EXCLUDED.sbr_mitotic_score,
                            er_percent            = EXCLUDED.er_percent,
                            er_intensity          = EXCLUDED.er_intensity,
                            er_status             = EXCLUDED.er_status,
                            pr_percent            = EXCLUDED.pr_percent,
                            pr_intensity          = EXCLUDED.pr_intensity,
                            pr_status             = EXCLUDED.pr_status,
                            hormone_receptor_status_project = EXCLUDED.hormone_receptor_status_project,
                            her2_ihc_score        = EXCLUDED.her2_ihc_score,
                            her2_ish_result       = EXCLUDED.her2_ish_result,
                            her2_status           = EXCLUDED.her2_status,
                            her2_qualification_project = EXCLUDED.her2_qualification_project,
                            pdl1_cps_value        = EXCLUDED.pdl1_cps_value,
                            pdl1_cps_status_project = EXCLUDED.pdl1_cps_status_project,
                            breast_anapath_sources = EXCLUDED.breast_anapath_sources
                """
                cur.execute("SAVEPOINT ipp_stade_bulk_insert")
                try:
                    execute_values(cur, insert_sql, rows, page_size=500)
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT ipp_stade_bulk_insert")
                    cur.execute("RELEASE SAVEPOINT ipp_stade_bulk_insert")
                    _log_insert_failure_diagnostics(
                        cur,
                        insert_sql,
                        rows,
                        export_columns,
                    )
                    raise
                else:
                    cur.execute("RELEASE SAVEPOINT ipp_stade_bulk_insert")
                logger.info("INSERT %d lignes dans %s", len(rows), full_table)
            else:
                logger.warning("Aucune ligne à insérer.")

        pg_conn.commit()
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


# ---------------------------------------------------------------------------
# 6. Refresh de la vue v_statut_vital
# ---------------------------------------------------------------------------

def refresh_view_task(conn_id: str = "postgres_test", **kwargs) -> None:
    """
    Rafraîchit la vue (matérialisée ou ordinaire) v_statut_vital afin qu'elle
    intègre la colonne 'stade' nouvellement alimentée depuis ipp_stade.

    - Si c'est une vue matérialisée  → REFRESH MATERIALIZED VIEW CONCURRENTLY
    - Sinon (vue ordinaire)          → pas d'action nécessaire (select retourne
      toujours les données fraîches), on logue juste.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            # Vérifie si c'est une vue matérialisée
            cur.execute(
                """
                SELECT relkind
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'datamart_oeci_survie'
                  AND c.relname = 'v_statut_vital'
                """
            )
            row = cur.fetchone()
            if row and row[0] == "m":
                logger.info("Refresh MATERIALIZED VIEW datamart_oeci_survie.v_statut_vital")
                cur.execute(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY "
                    "datamart_oeci_survie.v_statut_vital;"
                )
                conn.commit()
                logger.info("Refresh terminé.")
            else:
                logger.info(
                    "v_statut_vital est une vue ordinaire – pas de refresh nécessaire."
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 7. Wrapper extract_and_clean pour le pipeline KM (remplace l'appel direct)
# ---------------------------------------------------------------------------

def extract_and_clean_data_for_organe_task(
    organe: str,
    date_debut_obs: str,
    date_fin_obs: str,
    conn_id: str = "postgres_test",
    **kwargs,
) -> None:
    """
    Adapte extract_and_clean_data_task pour qu'elle pousse le résultat en XCom
    de façon compatible avec calculate_kaplan_meier_task qui lit l'upstream.

    La version originale retournait df.to_json() – on conserve ce comportement
    mais on l'encapsule ici pour que la task_id upstream soit cohérente.
    """
    from kaplan_meier_processing import extract_and_clean_data_task

    result_json = extract_and_clean_data_task(
        organe=organe,
        date_debut_obs=date_debut_obs,
        date_fin_obs=date_fin_obs,
        conn_id=conn_id,
    )
    # Le résultat est retourné et Airflow le stocke automatiquement en XCom
    # (return value → XCom key "return_value")
    return result_json
