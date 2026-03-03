from pathlib import Path
from airflow.providers.postgres.hooks.postgres import PostgresHook

def export_sql_to_csv(postgres_conn_id: str, sql_path: str, output_csv: str) -> None:
    sql = Path(sql_path).read_text(encoding="utf-8").strip().rstrip(";")
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with hook.get_conn() as conn, conn.cursor() as cur, open(output_path, "w", encoding="utf-8") as handle:
        cur.copy_expert(f"COPY ({sql}) TO STDOUT WITH CSV HEADER", handle)
        conn.commit()
