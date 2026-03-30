from __future__ import annotations

import json
import logging
import os
import shlex
import tempfile
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

def extract_ipp_task(date_debut_obs: str, conn_id: str = "postgres_test", **kwargs) -> None:
    """
    Extrait tous les IPP distincts depuis datamart_oeci_survie.v_statut_vital
    dont date_diag_tkc (ou date_diag_dcc) >= 2020-01-01.

    Pousse le résultat en XCom : liste JSON des IPP (strings).
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    year = str(date_debut_obs)[:4]

    query = f"""
    SELECT DISTINCT ipp_ocr
    FROM datamart_oeci_survie.v_statut_vital
    WHERE
        organe IS NOT NULL
        AND code_cim IS NOT NULL
        AND COALESCE(date_diag_tkc, date_diag_dcc) IS NOT NULL
        AND EXTRACT(YEAR FROM COALESCE(date_diag_tkc, date_diag_dcc))::int >= {int(year)}
        AND ipp_ocr IS NOT NULL
        AND ipp_ocr <> ''
    ;
    """

    conn = hook.get_conn()
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    ipp_list = df["ipp_ocr"].astype(str).str.strip().tolist()
    logger.info("IPP extraits : %d", len(ipp_list))

    ti = kwargs["ti"]
    ti.xcom_push(key="ipp_list", value=ipp_list)


# ---------------------------------------------------------------------------
# 2. Push PDF + JSON metadata vers le serveur distant
# ---------------------------------------------------------------------------

def push_pdf_task(
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_dir: str,
    ssh_password_var_key: str,
    remote_script: str = "/opt/push_pdf_llm.py",
    source_dir: str = "/opt/PDF",
    target_host: str = "10.210.22.130",
    target_port: int = 22,
    target_user: str = "administrateur",
    target_password_var_key: str = "password_clidatadsin",
    remote_python_bin: str = "python3",
    remote_tmp_dir: str = "/tmp",
    **kwargs,
) -> None:
    """
    Lance sur le lakehouse le script /opt/push_pdf_llm.

    Le script distant lit la liste d'IPP produite par la task précédente,
    scanne les PDF/JSON sur le lakehouse, puis envoie les fichiers sur la
    machine cible via SFTP.
    """
    ti = kwargs["ti"]
    ipp_list: list[str] = ti.xcom_pull(
        task_ids="extract_ipp_from_statut_vital", key="ipp_list"
    )
    if not ipp_list:
        logger.warning("Aucun IPP reçu en XCom – rien à envoyer.")
        return

    logger.info("IPP reçus depuis XCom : %d", len(ipp_list))

    target_password = Variable.get(target_password_var_key)
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
                f"REMOTE_TARGET_PASSWORD={shlex.quote(target_password)}",
                shlex.quote(remote_python_bin),
                shlex.quote(remote_script),
                "--ipp-file",
                shlex.quote(remote_ipp_file),
                "--local-dir",
                shlex.quote(source_dir),
                "--remote-host",
                shlex.quote(target_host),
                "--remote-port",
                shlex.quote(str(target_port)),
                "--remote-user",
                shlex.quote(target_user),
                "--remote-dir",
                shlex.quote(remote_dir),
                "--remote-password-env",
                "REMOTE_TARGET_PASSWORD",
                "--verbose",
            ]
        )

        logger.info(
            "Commande distante sur %s : %s %s --ipp-file %s ...",
            remote_host,
            remote_python_bin,
            remote_script,
            remote_ipp_file,
        )
        _, stdout, stderr = client.exec_command(cmd, timeout=3600)
        exit_status = stdout.channel.recv_exit_status()
        stdout_txt = stdout.read().decode("utf-8", errors="replace")
        stderr_txt = stderr.read().decode("utf-8", errors="replace")

        if stdout_txt:
            logger.info("STDOUT push_pdf:\n%s", stdout_txt)
        if stderr_txt:
            logger.warning("STDERR push_pdf:\n%s", stderr_txt)

        if exit_status != 0:
            raise RuntimeError(
                f"Le script distant {remote_script} a terminé avec le code {exit_status}. "
                f"Stderr: {stderr_txt[:500]}"
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

    logger.info("Push PDF distant terminé avec succès.")


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
    **kwargs,
) -> None:
    """
    Lance extract_tnm_stage_by_ipp.py sur le serveur distant via SSH.
    Le script reçoit remote_data_dir comme répertoire d'entrée et de sortie.
    """
    client = _get_ssh_client(remote_host, remote_port, remote_user, ssh_password_var_key)
    try:
        cmd = (
            f"python3 {remote_script} "
            f"{remote_data_dir} "
            f"--output-dir {remote_data_dir} "
            f"--ipp-strategy baseline "
            f"--log-level INFO"
        )
        logger.info("Commande SSH : %s", cmd)
        _, stdout, stderr = client.exec_command(cmd, timeout=1800)
        exit_status = stdout.channel.recv_exit_status()
        stdout_txt = stdout.read().decode("utf-8", errors="replace")
        stderr_txt = stderr.read().decode("utf-8", errors="replace")

        if stdout_txt:
            logger.info("STDOUT:\n%s", stdout_txt)
        if stderr_txt:
            logger.warning("STDERR:\n%s", stderr_txt)

        if exit_status != 0:
            raise RuntimeError(
                f"Le script TNM a terminé avec le code {exit_status}. "
                f"Stderr: {stderr_txt[:500]}"
            )
        logger.info("Extraction TNM terminée avec succès.")
    finally:
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


# ---------------------------------------------------------------------------
# 5. Chargement dans datamart_oeci_survie.ipp_stade
# ---------------------------------------------------------------------------

# Mapping stade texte → code court pour la colonne VARCHAR de la table
STAGE_MAPPING = {
    "Stage 0":    "0",
    "Stage I":    "I",
    "Stage IIA":  "IIA",
    "Stage IIB":  "IIB",
    "Stage IIIA": "IIIA",
    "Stage IIIB": "IIIB",
    "Stage IIIC": "IIIC",
    "Stage IV":   "IV",
}


def _normalize_stage(raw: str) -> Optional[str]:
    """Convertit 'Stage IIA (Mx)' → 'IIA', None si non reconnu."""
    if not raw or raw == "null":
        return None
    # Retire le suffixe "(Mx)" éventuel
    clean = raw.split("(")[0].strip()
    return STAGE_MAPPING.get(clean, clean[:20] if clean else None)


def load_ipp_stade_task(
    local_csv_path: str,
    conn_id: str = "postgres_test",
    **kwargs,
) -> None:
    """
    Lit le CSV ipp_stage_results.csv et effectue un UPSERT dans
    datamart_oeci_survie.ipp_stade (colonnes : ipp, date_diagnostic, organe, stade).

    La table est supposée exister avec les colonnes visibles en pièce jointe.
    On fait un DELETE + INSERT par batch pour rester idempotent.
    """
    from psycopg2.extras import execute_values

    df = pd.read_csv(local_csv_path, dtype=str)
    logger.info("CSV chargé : %d lignes", len(df))

    if df.empty:
        logger.warning("CSV vide, rien à charger.")
        return

    # Nettoyage
    df["ipp"] = df["ipp"].fillna("").str.strip()
    df = df[df["ipp"] != ""]
    df["stade_norm"] = df["stage"].apply(_normalize_stage)

    # date_diagnostic : on utilise document_date (format YYYYMMDD → DATE)
    def parse_doc_date(v: str) -> Optional[str]:
        if not v or v == "null":
            return None
        v = str(v).strip()
        if len(v) == 8 and v.isdigit():
            return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
        return None

    df["date_diag_fmt"] = df["document_date"].apply(parse_doc_date)

    # organe : non présent dans le CSV de sortie du script regex, on met NULL
    # (sera rempli lors du refresh vue si nécessaire)
    organe_col = df["organe"] if "organe" in df.columns else pd.Series([""] * len(df))

    hook = PostgresHook(postgres_conn_id=conn_id)
    pg_conn = hook.get_conn()
    schema  = "datamart_oeci_survie"
    table   = "ipp_stade"
    full_table = f"{schema}.{table}"

    try:
        with pg_conn.cursor() as cur:
            # UPSERT : on supprime d'abord les IPP présents dans ce batch
            ipps = df["ipp"].tolist()
            cur.execute(
                f"DELETE FROM {full_table} WHERE ipp = ANY(%s)",
                (ipps,),
            )
            logger.info("DELETE pour %d IPP dans %s", len(ipps), full_table)

            rows = []
            for _, row in df.iterrows():
                stade = row.get("stade_norm")
                if not stade:
                    continue
                rows.append((
                    row["ipp"],
                    row.get("date_diag_fmt"),
                    row.get("organe", None) or None,
                    stade,
                ))

            if rows:
                insert_sql = f"""
                    INSERT INTO {full_table} (ipp, date_diagnostic, organe, stade)
                    VALUES %s
                    ON CONFLICT (ipp) DO UPDATE
                        SET date_diagnostic = EXCLUDED.date_diagnostic,
                            organe          = EXCLUDED.organe,
                            stade           = EXCLUDED.stade
                """
                execute_values(cur, insert_sql, rows, page_size=500)
                logger.info("INSERT %d lignes dans %s", len(rows), full_table)
            else:
                logger.warning("Aucun stade valide à insérer.")

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
