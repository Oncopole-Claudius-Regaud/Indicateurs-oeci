# ==============================================================================

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
from io import StringIO
import numpy as np
from lifelines import KaplanMeierFitter
from sqlalchemy import text

# ==============================================================================
# Utils DB
# ==============================================================================

def get_postgres_hook(conn_id=None):
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)

# ==============================================================================
# 1. Extraction & nettoyage
# ==============================================================================

def extract_and_clean_data_task(organe, date_debut_obs, date_fin_obs, conn_id=None):
    hook = get_postgres_hook(conn_id)
    FULL_TABLE_PATH = "datamart_oeci_survie.v_statut_vital"

    query = f"""
    WITH patient_min_annee AS (
        SELECT ipp_ocr, MIN(annee) AS annee_debut_suivi
        FROM {FULL_TABLE_PATH}
        WHERE organe = '{organe}'
        GROUP BY ipp_ocr
    ),
    patient_statut_final AS (
        SELECT DISTINCT ON (t1.ipp_ocr)
            t1.ipp_ocr, t1.statut_vital, t1.annee
        FROM {FULL_TABLE_PATH} t1
        WHERE t1.organe = '{organe}'
          AND t1.annee <= SUBSTRING('{date_fin_obs}' FROM 1 FOR 4)::int
        ORDER BY t1.ipp_ocr, t1.annee DESC, t1.date_derniere_nouvelle DESC
    )
    SELECT t_base.*
    FROM {FULL_TABLE_PATH} t_base
    JOIN patient_min_annee min_annee
        ON t_base.ipp_ocr = min_annee.ipp_ocr
    JOIN patient_statut_final final_statut
        ON t_base.ipp_ocr = final_statut.ipp_ocr
    WHERE t_base.organe = '{organe}'
      AND min_annee.annee_debut_suivi >= SUBSTRING('{date_debut_obs}' FROM 1 FOR 4)::int
      AND final_statut.statut_vital <> 'PDV'
      AND t_base.annee <= SUBSTRING('{date_fin_obs}' FROM 1 FOR 4)::int
    ORDER BY t_base.ipp_ocr, t_base.annee;
    """

    # ✅ pandas + SQLAlchemy : passer une Connection, pas l'Engine
    conn = hook.get_conn()
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    # Nettoyage Python
    df["ipp_ocr"] = df["ipp_ocr"].fillna("")
    df["ipp_prefix"] = df["ipp_ocr"].str[:4]

    masque_final = (
        (df["ipp_prefix"] >= "2000")
        & df["date_diag_tkc"].notna()
        & df["date_derniere_nouvelle"].notna()
    )

    df_final = df[masque_final].copy()
    return df_final.to_json(date_format="iso")

# ==============================================================================
# 2. Calcul Kaplan-Meier
# ==============================================================================

def calculate_kaplan_meier_task(ti, **kwargs):
    upstream_task_id = list(ti.task.upstream_task_ids)[0]
    df_json = ti.xcom_pull(task_ids=upstream_task_id)

    if not df_json:
        raise ValueError("Aucune donnée XCom reçue")

    df = pd.read_json(StringIO(df_json))
    df["date_diag_tkc"] = pd.to_datetime(df["date_diag_tkc"])
    df["date_derniere_nouvelle"] = pd.to_datetime(df["date_derniere_nouvelle"])

    df["time_years"] = (
        (df["date_derniere_nouvelle"] - df["date_diag_tkc"])
        .dt.days / 365.25
    )

    df["event_observed"] = np.where(
        df["statut_vital"] == "Décédé", 1, 0
    )

    kmf = KaplanMeierFitter()
    kmf.fit(df["time_years"], df["event_observed"])

    curve_df = kmf.survival_function_.join(
        kmf.confidence_interval_survival_function_
    ).reset_index()

    curve_df.columns = [
        "time_years", "survival_rate", "ic_lower", "ic_upper"
    ]

    key_indicators = []
    for t in [1, 5, 10]:
        try:
            surv = kmf.survival_function_at_times(t).iloc[0] * 100
            ci = kmf.confidence_interval_survival_function_.loc[:t].iloc[-1] * 100
            key_indicators.append({
                "time_point": t,
                "survival_rate": round(surv, 2),
                "ic_low_pct": round(ci.iloc[0], 2),
                "ic_high_pct": round(ci.iloc[1], 2),
            })
        except Exception:
            pass

    return {
        "curve_data": curve_df.to_json(orient="records"),
        "key_indicators": key_indicators,
    }

# ==============================================================================
# 3. Chargement en base
# ==============================================================================

def load_to_db_task(ti, table_name, conn_id=None, **kwargs):
    """
    Charge les résultats Kaplan-Meier dans PostgreSQL en psycopg2 pur (execute_values).
    - TRUNCATE la table cible
    - INSERT bulk
    - Respecte les colonnes du DDL (IDs et run_date en DEFAULT)
    """
    from psycopg2.extras import execute_values

    pg_hook = get_postgres_hook(conn_id)

    upstream_task_id = list(ti.task.upstream_task_ids)[0]
    results = ti.xcom_pull(task_ids=upstream_task_id)

    if not results:
        print("Aucun résultat récupéré depuis XCom. Fin de la tâche.")
        return

    schema = "datamart_oeci_survie"
    full_table = f"{schema}.{table_name}"

    # ⚠️ organe et dates obs viennent du DAG via op_kwargs
    organe = kwargs.get("organe")
    if not organe:
        raise ValueError("Paramètre 'organe' manquant dans op_kwargs (DAG).")

    date_debut_obs = kwargs.get("date_debut_obs")
    date_fin_obs = kwargs.get("date_fin_obs")

    pg_conn = pg_hook.get_conn()
    try:
        with pg_conn.cursor() as cur:
            # 1) TRUNCATE
            cur.execute(f"TRUNCATE TABLE {full_table};")
            print(f"🧹 Table vidée : {full_table}")

            # 2) Préparer INSERT selon la table
            if table_name.startswith("datamart_km_curve"):
                # DDL attend:
                # time_years, survival_rate, ic_lower, ic_upper, organe, date_start_obs, date_end_obs
                curve_df = pd.read_json(StringIO(results["curve_data"]))
                curve_df = curve_df.replace({np.nan: None})

                insert_cols = [
                    "time_years",
                    "survival_rate",
                    "ic_lower",
                    "ic_upper",
                    "organe",
                    "date_start_obs",
                    "date_end_obs",
                ]

                rows = []
                for r in curve_df.itertuples(index=False):
                    rows.append((
                        r.time_years,
                        r.survival_rate,
                        r.ic_lower,
                        r.ic_upper,
                        organe,
                        date_debut_obs,
                        date_fin_obs,
                    ))

                insert_sql = f"""
                    INSERT INTO {full_table} ({", ".join(insert_cols)})
                    VALUES %s
                """

                if rows:
                    execute_values(cur, insert_sql, rows, page_size=1000)
                print(f"✅ Insert curve: {len(rows)} lignes → {full_table}")

            elif table_name.startswith("datamart_km_key_indicators"):
                # DDL attend:
                # time_point, survival_rate, ic_range, ic_low_pct, ic_high_pct, organe
                kpi_df = pd.DataFrame(results["key_indicators"])
                if not kpi_df.empty:
                    kpi_df = kpi_df.replace({np.nan: None})

                insert_cols = [
                    "time_point",
                    "survival_rate",
                    "ic_range",
                    "ic_low_pct",
                    "ic_high_pct",
                    "organe",
                ]

                rows = []
                for r in kpi_df.itertuples(index=False):
                    rows.append((
                        int(r.time_point) if r.time_point is not None else None,
                        r.survival_rate,
                        getattr(r, "ic_range", None),   # présent si ton calculate le renvoie
                        getattr(r, "ic_low_pct", None),
                        getattr(r, "ic_high_pct", None),
                        organe,
                    ))

                insert_sql = f"""
                    INSERT INTO {full_table} ({", ".join(insert_cols)})
                    VALUES %s
                """

                if rows:
                    execute_values(cur, insert_sql, rows, page_size=1000)
                print(f"✅ Insert KPI: {len(rows)} lignes → {full_table}")

            else:
                raise ValueError(f"Table inconnue : {table_name}")

        pg_conn.commit()

    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()


