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
    SELECT
        ipp_ocr,
        code_cim,
        organe,
        date_diag_tkc,
        date_diag_dcc,
        date_derniere_nouvelle,
        statut_vital
    FROM {FULL_TABLE_PATH}
    WHERE organe = '{organe}'
      AND organe IS NOT NULL
      AND code_cim IS NOT NULL
      AND COALESCE(date_diag_tkc, date_diag_dcc) IS NOT NULL
      AND EXTRACT(YEAR FROM COALESCE(date_diag_tkc, date_diag_dcc))::int
            = SUBSTRING('{date_debut_obs}' FROM 1 FOR 4)::int
    ;
    """

    conn = hook.get_conn()
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    df["ipp_ocr"] = df["ipp_ocr"].fillna("")
    return df.to_json(date_format="iso")


# ==============================================================================
# 2. Calcul Kaplan-Meier
# ==============================================================================

def calculate_kaplan_meier_task(ti, **kwargs):
    upstream_task_id = list(ti.task.upstream_task_ids)[0]
    df_json = ti.xcom_pull(task_ids=upstream_task_id)

    if not df_json:
        raise ValueError("Aucune donnée XCom reçue")

    df = pd.read_json(StringIO(df_json))

    end_obs = pd.to_datetime(kwargs.get("date_fin_obs"), errors="coerce")

    # Dates (dayfirst=True utile si dcc est au format 29/08/2023)
    df["date_diag_tkc"] = pd.to_datetime(df.get("date_diag_tkc"), errors="coerce")
    df["date_diag_dcc"] = pd.to_datetime(df.get("date_diag_dcc"), errors="coerce", dayfirst=True)
    df["date_derniere_nouvelle"] = pd.to_datetime(df.get("date_derniere_nouvelle"), errors="coerce")

    # Date diag ref : TKC sinon DCC
    df["date_diag_ref"] = df["date_diag_tkc"].fillna(df["date_diag_dcc"])

    # Garder exploitable
    df = df[df["date_diag_ref"].notna() & df["date_derniere_nouvelle"].notna()].copy()

    # Censure à date_fin_obs : fin de suivi = min(dernière nouvelle, fin obs)
    if pd.notna(end_obs):
        df["date_end_followup"] = df["date_derniere_nouvelle"].where(
            df["date_derniere_nouvelle"] <= end_obs, end_obs
        )
    else:
        df["date_end_followup"] = df["date_derniere_nouvelle"]

    # 1 ligne par IPP :
    # - diag = min
    # - fin suivi = max (après censure)
    # - statut_final = statut de la ligne avec dernière_nouvelle max (avant censure)
    idx_last = df.groupby("ipp_ocr")["date_derniere_nouvelle"].idxmax()
    statut_last = df.loc[idx_last, ["ipp_ocr", "statut_vital", "date_derniere_nouvelle"]].set_index("ipp_ocr")

    df_patient = df.groupby("ipp_ocr").agg(
        date_diag_ref=("date_diag_ref", "min"),
        date_end_followup=("date_end_followup", "max"),
    ).join(statut_last[["statut_vital", "date_derniere_nouvelle"]], how="left").reset_index()

    # Event dans la fenêtre : si Décédé ET décès (approché) <= fin_obs
    # (ici on suppose que "date_derniere_nouvelle" correspond à la date de décès quand statut=Décédé)
    if pd.notna(end_obs):
        df_patient["event_observed"] = np.where(
            (df_patient["statut_vital"] == "Décédé") & (df_patient["date_derniere_nouvelle"] <= end_obs),
            1, 0
        )
    else:
        df_patient["event_observed"] = np.where(df_patient["statut_vital"] == "Décédé", 1, 0)

    # Temps
    df_patient["time_years"] = (
        (df_patient["date_end_followup"] - df_patient["date_diag_ref"]).dt.days / 365.25
    )
    df_patient = df_patient[df_patient["time_years"].notna() & (df_patient["time_years"] >= 0)].copy()

    if df_patient.empty:
        raise ValueError("Après nettoyage, aucune ligne exploitable pour Kaplan-Meier.")

    kmf = KaplanMeierFitter()
    kmf.fit(df_patient["time_years"], event_observed=df_patient["event_observed"])

    curve_df = (
        kmf.survival_function_
        .join(kmf.confidence_interval_survival_function_)
        .reset_index()
    )
    curve_df.columns = ["time_years", "survival_rate", "ic_lower", "ic_upper"]

    key_indicators = []
    for t in [1, 5, 10]:
        try:
            surv = float(kmf.survival_function_at_times(t).iloc[0]) * 100
            ci = kmf.confidence_interval_survival_function_.loc[:t].iloc[-1] * 100
            key_indicators.append({
                "time_point": t,
                "survival_rate": round(surv, 2),
                "ic_low_pct": round(float(ci.iloc[0]), 2),
                "ic_high_pct": round(float(ci.iloc[1]), 2),
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

    # ✅ Correction: tes colonnes sont varchar(4) -> on stocke l'année uniquement
    date_start_obs_year = str(date_debut_obs)[:4] if date_debut_obs is not None else None
    date_end_obs_year = str(date_fin_obs)[:4] if date_fin_obs is not None else None

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
                        date_start_obs_year,
                        date_end_obs_year,
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
                        getattr(r, "ic_range", None),
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



