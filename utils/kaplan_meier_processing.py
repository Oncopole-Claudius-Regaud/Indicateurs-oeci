# ==============================================================================

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
from io import StringIO
import numpy as np
from lifelines import KaplanMeierFitter

# ==============================================================================
# Utils DB
# ==============================================================================

def get_postgres_hook(conn_id=None):
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)


def get_db_engine(hook):
    return hook.get_sqlalchemy_engine()

# ==============================================================================
# 1. Extraction & nettoyage
# ==============================================================================

def extract_and_clean_data_task(
    organe,
    date_debut_obs,
    date_fin_obs,
    conn_id=None
):
    hook = get_postgres_hook(conn_id)
    engine = get_db_engine(hook)

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

    df = pd.read_sql_query(query, engine)

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

def load_to_db_task(
    ti,
    table_name,
    conn_id=None,
    **kwargs
):
    hook = get_postgres_hook(conn_id)
    engine = get_db_engine(hook)

    upstream_task_id = list(ti.task.upstream_task_ids)[0]
    results = ti.xcom_pull(task_ids=upstream_task_id)

    if not results:
        return

    if table_name.startswith("datamart_km_curve"):
        df = pd.read_json(StringIO(results["curve_data"]))
        df["date_start_obs"] = kwargs.get("date_debut_obs")
        df["date_end_obs"] = kwargs.get("date_fin_obs")

    elif table_name.startswith("datamart_km_key_indicators"):
        df = pd.DataFrame(results["key_indicators"])

    else:
        raise ValueError(f"Table inconnue : {table_name}")

    df["run_date"] = datetime.now()

    df.to_sql(
        table_name,
        engine,
        schema="datamart_oeci_survie",
        if_exists="append",
        index=False,
    )

    print(f"✅ Chargement OK → {table_name} ({len(df)} lignes)")
