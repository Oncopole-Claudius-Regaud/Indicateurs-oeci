
from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_matches_tsv(
    postgres_conn_id: str,
    tsv_path: str,
    schema: str = "oeci",
    table: str = "insee_deces_matches_raw",
) -> None:
    input_path = Path(tsv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"matches file not found: {input_path}")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur, open(input_path, "r", encoding="utf-8") as handle:
        cur.execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS {schema};
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                nip text,
                ipp_ocr text,
                id text,
                date_deces text,
                probas_rf text,
                probas_nn text,
                upper text
            );
            TRUNCATE TABLE {schema}.{table};
            """
        )
        cur.copy_expert(
            f"""
            COPY {schema}.{table} (nip, ipp_ocr, id, date_deces, probas_rf, probas_nn, upper)
            FROM STDIN
            WITH (FORMAT CSV, HEADER true, DELIMITER E'\\t', NULL '')
            """,
            handle,
        )
        conn.commit()


def execute_sql_file(postgres_conn_id: str, sql_path: str) -> None:
    sql = Path(sql_path).read_text(encoding="utf-8")
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

