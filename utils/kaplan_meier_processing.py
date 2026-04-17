# ==============================================================================
# kaplan_meier_processing.py  (version mise à jour – ajout colonne "stade")
# ==============================================================================

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
from io import StringIO
import numpy as np
from lifelines import KaplanMeierFitter
import re
import unicodedata

# ==============================================================================
# Utils DB
# ==============================================================================

def get_postgres_hook(conn_id=None):
    if not conn_id:
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)


def _normalize_status(value):
    if pd.isna(value):
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    normalized = normalized.encode("ascii", "ignore").decode("ascii")
    return " ".join(normalized.strip().lower().split())


def _normalize_stage_label(value):
    if pd.isna(value):
        return ""
    clean = str(value).strip()
    if not clean or clean.lower() in {"null", "nan"}:
        return ""
    clean = clean.split("(")[0].strip().upper()
    clean = re.sub(r"^(STADE|STAGE)\s+", "", clean).strip()
    clean = clean.replace("AJCC", "").strip()
    clean = {"1": "I", "2": "II", "3": "III", "4": "IV"}.get(clean, clean)
    return f"Stage {clean}" if clean else ""


def _collapse_stage_label_for_breast(value):
    normalized = _normalize_stage_label(value)
    if not normalized:
        return ""

    match = re.match(r"^Stage\s+(0|IV|III|II|I)([A-D]?)$", normalized, re.IGNORECASE)
    if not match:
        return normalized

    major_stage = match.group(1).upper()
    return f"Stage {major_stage}"


def _death_in_observation_window_mask(df_patient, start_obs, end_obs):
    death_mask = df_patient["is_deceased_status"]
    if pd.notna(start_obs):
        death_mask = death_mask & (df_patient["date_derniere_nouvelle"] >= start_obs)
    if pd.notna(end_obs):
        death_mask = death_mask & (df_patient["date_derniere_nouvelle"] <= end_obs)
    return death_mask.fillna(False)


def _compute_final_patient_counts(df_patient, start_obs, end_obs):
    if df_patient.empty:
        return {
            "nb_decedes_fin_courbe": 0,
            "nb_vivants_fin_courbe": 0,
            "nb_pdv_fin_courbe": 0,
        }

    is_deceased = _death_in_observation_window_mask(df_patient, start_obs, end_obs)

    if pd.notna(end_obs):
        is_alive = (~is_deceased) & (df_patient["date_derniere_nouvelle"] > end_obs)
        is_pdv = (~is_deceased) & (~is_alive)
    else:
        is_pdv = (~is_deceased) & df_patient["is_pdv_status"]
        is_alive = (~is_deceased) & (~is_pdv)

    return {
        "nb_decedes_fin_courbe": int(is_deceased.sum()),
        "nb_vivants_fin_courbe": int(is_alive.sum()),
        "nb_pdv_fin_courbe": int(is_pdv.sum()),
    }


def _compute_km_outputs(df_patient, start_obs, end_obs, stade=None):
    if df_patient.empty:
        return [], [], {
            "nb_decedes_fin_courbe": 0,
            "nb_vivants_fin_courbe": 0,
            "nb_pdv_fin_courbe": 0,
        }

    final_patient_counts = _compute_final_patient_counts(df_patient, start_obs, end_obs)

    kmf = KaplanMeierFitter()
    kmf.fit(df_patient["time_years"], event_observed=df_patient["event_observed"])

    curve_df = (
        kmf.survival_function_
        .join(kmf.confidence_interval_survival_function_)
        .reset_index()
    )
    curve_df.columns = ["time_years", "survival_rate", "ic_lower", "ic_upper"]
    if stade is not None:
        curve_df["stade"] = stade

    key_indicators = []
    max_followup_years = float(df_patient["time_years"].max())
    for t in [1, 5, 10]:
        if t > max_followup_years:
            kpi = {
                "time_point": t,
                "survival_rate": None,
                "ic_low_pct": None,
                "ic_high_pct": None,
            }
        else:
            try:
                surv = float(kmf.survival_function_at_times(t).iloc[0]) * 100
                ci = kmf.confidence_interval_survival_function_.loc[:t].iloc[-1] * 100
                kpi = {
                    "time_point": t,
                    "survival_rate": round(surv, 2),
                    "ic_low_pct": round(float(ci.iloc[0]), 2),
                    "ic_high_pct": round(float(ci.iloc[1]), 2),
                }
            except Exception:
                kpi = {
                    "time_point": t,
                    "survival_rate": None,
                    "ic_low_pct": None,
                    "ic_high_pct": None,
                }

        kpi.update(final_patient_counts)
        if stade is not None:
            kpi["stade"] = stade
        key_indicators.append(kpi)

    return curve_df.to_dict(orient="records"), key_indicators, final_patient_counts

# ==============================================================================
# 1. Extraction & nettoyage
# ==============================================================================

def extract_and_clean_data_task(organe, date_debut_obs, date_fin_obs, conn_id=None):
    """
    Extrait les données de survie depuis v_statut_vital pour un organe donné.
    Inclut désormais la colonne 'stade' (nullable) issue de la jointure avec ipp_stade.
    Retourne un JSON (string) stocké en XCom.
    """
    hook = get_postgres_hook(conn_id)
    FULL_TABLE_PATH = "datamart_oeci_survie.v_statut_vital"

    query = f"""
    SELECT
        v.ipp_ocr,
        v.code_cim,
        v.organe,
        v.date_diag_tkc,
        v.date_diag_dcc,
        v.date_derniere_nouvelle,
        v.statut_vital,
        v.stage AS stade
    FROM {FULL_TABLE_PATH} v
    WHERE v.organe = '{organe}'
      AND v.organe IS NOT NULL
      AND v.code_cim IS NOT NULL
      AND COALESCE(v.date_diag_tkc, v.date_diag_dcc) IS NOT NULL
      AND EXTRACT(YEAR FROM COALESCE(v.date_diag_tkc, v.date_diag_dcc))::int
            = SUBSTRING('{date_debut_obs}' FROM 1 FOR 4)::int
    ;
    """

    conn = hook.get_conn()
    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    df["ipp_ocr"] = df["ipp_ocr"].fillna("")
    if "stade" in df.columns:
        stage_normalizer = (
            _collapse_stage_label_for_breast
            if str(organe).strip().upper() == "SEIN"
            else _normalize_stage_label
        )
        df["stade"] = df["stade"].apply(stage_normalizer)
    else:
        df["stade"] = ""
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

    start_obs = pd.to_datetime(kwargs.get("date_debut_obs"), errors="coerce")
    end_obs = pd.to_datetime(kwargs.get("date_fin_obs"), errors="coerce")

    df["date_diag_tkc"] = pd.to_datetime(df.get("date_diag_tkc"), errors="coerce")
    df["date_diag_dcc"] = pd.to_datetime(df.get("date_diag_dcc"), errors="coerce", dayfirst=True)
    df["date_derniere_nouvelle"] = pd.to_datetime(df.get("date_derniere_nouvelle"), errors="coerce")
    df["date_diag_ref"] = df["date_diag_tkc"].fillna(df["date_diag_dcc"])

    df = df[df["date_diag_ref"].notna() & df["date_derniere_nouvelle"].notna()].copy()

    if pd.notna(end_obs):
        df["date_end_followup"] = df["date_derniere_nouvelle"].where(
            df["date_derniere_nouvelle"] <= end_obs, end_obs
        )
    else:
        df["date_end_followup"] = df["date_derniere_nouvelle"]

    idx_last = df.groupby("ipp_ocr")["date_derniere_nouvelle"].idxmax()
    statut_last = df.loc[idx_last, ["ipp_ocr", "statut_vital", "date_derniere_nouvelle"]].set_index("ipp_ocr")

    # Récupère le stade par IPP (prend le premier non-vide)
    stade_by_ipp = (
        df[df["stade"].notna() & (df["stade"] != "")]
        .drop_duplicates("ipp_ocr")
        .set_index("ipp_ocr")["stade"]
    ) if "stade" in df.columns else pd.Series(dtype=str)

    df_patient = df.groupby("ipp_ocr").agg(
        date_diag_ref=("date_diag_ref", "min"),
        date_end_followup=("date_end_followup", "max"),
    ).join(statut_last[["statut_vital", "date_derniere_nouvelle"]], how="left").reset_index()

    # Rattache le stade
    df_patient = df_patient.join(stade_by_ipp.rename("stade"), on="ipp_ocr", how="left")
    df_patient["stade"] = df_patient["stade"].fillna("")

    df_patient["statut_vital_norm"] = df_patient["statut_vital"].apply(_normalize_status)
    df_patient["is_deceased_status"] = df_patient["statut_vital_norm"].str.contains(
        r"\bdecede\b|\bdeces\b", regex=True, na=False,
    )
    df_patient["is_pdv_status"] = df_patient["statut_vital_norm"].str.contains(
        r"\bpdv\b|perdu de vue|perdue de vue|lost to follow up|lost to follow-up",
        regex=True, na=False,
    )

    death_in_window = _death_in_observation_window_mask(df_patient, start_obs, end_obs)
    df_patient["event_observed"] = np.where(death_in_window, 1, 0)

    df_patient["time_years"] = (
        (df_patient["date_end_followup"] - df_patient["date_diag_ref"]).dt.days / 365.25
    )
    df_patient = df_patient[df_patient["time_years"].notna() & (df_patient["time_years"] >= 0)].copy()

    if df_patient.empty:
        raise ValueError("Après nettoyage, aucune ligne exploitable pour Kaplan-Meier.")

    final_patient_counts = _compute_final_patient_counts(df_patient, start_obs, end_obs)
    total_fin_courbe = sum(final_patient_counts.values())
    print(
        f"Compteurs fin de courbe – decedes: {final_patient_counts['nb_decedes_fin_courbe']}, "
        f"vivants: {final_patient_counts['nb_vivants_fin_courbe']}, "
        f"pdv: {final_patient_counts['nb_pdv_fin_courbe']}, "
        f"total: {total_fin_courbe}"
    )

    # Distribution des stades pour info
    stade_dist = df_patient["stade"].value_counts().to_dict()
    print(f"Distribution des stades : {stade_dist}")

    curve_rows, key_indicators, _ = _compute_km_outputs(df_patient, start_obs, end_obs)

    curve_rows_by_stade = []
    key_indicators_by_stade = []
    known_stades = sorted(value for value in df_patient["stade"].dropna().unique() if value != "")
    for stade in known_stades:
        df_stage = df_patient[df_patient["stade"] == stade].copy()
        stage_curve_rows, stage_kpis, stage_counts = _compute_km_outputs(
            df_stage,
            start_obs,
            end_obs,
            stade=stade,
        )
        curve_rows_by_stade.extend(stage_curve_rows)
        key_indicators_by_stade.extend(stage_kpis)
        print(
            f"Stade {stade} â€“ decedes: {stage_counts['nb_decedes_fin_courbe']}, "
            f"vivants: {stage_counts['nb_vivants_fin_courbe']}, "
            f"pdv: {stage_counts['nb_pdv_fin_courbe']}, "
            f"patients: {len(df_stage)}"
        )

    # Stade majoritaire de la cohorte (pour alimenter la colonne stade des tables KM)
    stade_majoritaire = (
        df_patient[df_patient["stade"] != ""]["stade"].mode().iloc[0]
        if not df_patient[df_patient["stade"] != ""].empty
        else None
    )

    return {
        "curve_data": pd.DataFrame(curve_rows).to_json(orient="records"),
        "curve_data_by_stade": (
            pd.DataFrame(curve_rows_by_stade).to_json(orient="records")
            if curve_rows_by_stade
            else None
        ),
        "key_indicators": key_indicators,
        "key_indicators_by_stade": key_indicators_by_stade,
        "final_patient_counts": final_patient_counts,
        "stade_majoritaire": stade_majoritaire,
    }


# ==============================================================================
# 3. Chargement en base (avec colonne stade)
# ==============================================================================

def load_to_db_task(ti, table_name, conn_id=None, **kwargs):
    """
    Charge les résultats Kaplan-Meier dans PostgreSQL.
    Gère désormais la colonne 'stade' si elle est présente dans la table cible.
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

    organe = kwargs.get("organe")
    if not organe:
        raise ValueError("Paramètre 'organe' manquant dans op_kwargs (DAG).")

    date_debut_obs = kwargs.get("date_debut_obs")
    date_fin_obs = kwargs.get("date_fin_obs")

    date_start_obs_year = str(date_debut_obs)[:4] if date_debut_obs is not None else None
    date_end_obs_year = str(date_fin_obs)[:4] if date_fin_obs is not None else None

    stade_majoritaire = results.get("stade_majoritaire")

    pg_conn = pg_hook.get_conn()
    try:
        with pg_conn.cursor() as cur:
            # Colonnes disponibles dans la table cible
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                """,
                (schema, table_name),
            )
            available_cols = {row[0] for row in cur.fetchall()}
            stage_col = "stade" if "stade" in available_cols else "stage" if "stage" in available_cols else None
            has_stage_col = stage_col is not None
            has_stade_col = has_stage_col

            if has_stage_col:
                print(f"✅ Colonne 'stade' détectée dans {full_table}")
            else:
                print(f"ℹ️ Colonne 'stade' absente de {full_table} – ignorée")

            # TRUNCATE
            cur.execute(f"TRUNCATE TABLE {full_table};")
            print(f"🧹 Table vidée : {full_table}")

            # ---------------------------------------------------------------
            # Courbe KM
            # ---------------------------------------------------------------
            if table_name.startswith("datamart_km_curve"):
                curve_json = (
                    results.get("curve_data_by_stade")
                    if has_stade_col and results.get("curve_data_by_stade")
                    else results["curve_data"]
                )
                curve_df = pd.read_json(StringIO(curve_json))
                curve_df = curve_df.replace({np.nan: None})

                insert_cols = [
                    "time_years", "survival_rate", "ic_lower", "ic_upper",
                    "organe", "date_start_obs", "date_end_obs",
                ]
                if has_stade_col:
                    insert_cols.append(stage_col)

                rows = []
                for r in curve_df.itertuples(index=False):
                    row = [
                        r.time_years, r.survival_rate, r.ic_lower, r.ic_upper,
                        organe, date_start_obs_year, date_end_obs_year,
                    ]
                    if has_stade_col:
                        row.append(getattr(r, "stade", None) or stade_majoritaire)
                    rows.append(tuple(row))

                if rows:
                    execute_values(
                        cur,
                        f"INSERT INTO {full_table} ({', '.join(insert_cols)}) VALUES %s",
                        rows,
                        page_size=1000,
                    )
                print(f"✅ Insert curve: {len(rows)} lignes → {full_table}")

            # ---------------------------------------------------------------
            # Indicateurs clés KM
            # ---------------------------------------------------------------
            elif table_name.startswith("datamart_km_key_indicators"):
                kpi_rows = (
                    results.get("key_indicators_by_stade")
                    if has_stade_col and results.get("key_indicators_by_stade")
                    else results["key_indicators"]
                )
                kpi_df = pd.DataFrame(kpi_rows)
                if not kpi_df.empty:
                    kpi_df = kpi_df.replace({np.nan: None})

                count_cols = [
                    "nb_decedes_fin_courbe",
                    "nb_vivants_fin_courbe",
                    "nb_pdv_fin_courbe",
                ]
                can_store_counts = all(col in available_cols for col in count_cols)

                final_counts = results.get("final_patient_counts", {})

                def _to_str_or_none(value):
                    return str(value) if value is not None else None

                insert_cols = [
                    "time_point", "survival_rate", "ic_range",
                    "ic_low_pct", "ic_high_pct", "organe",
                ]
                if can_store_counts:
                    insert_cols.extend(count_cols)
                if has_stade_col:
                    insert_cols.append(stage_col)

                rows = []
                for r in kpi_df.itertuples(index=False):
                    row = [
                        int(r.time_point) if r.time_point is not None else None,
                        r.survival_rate,
                        getattr(r, "ic_range", None),
                        getattr(r, "ic_low_pct", None),
                        getattr(r, "ic_high_pct", None),
                        organe,
                    ]
                    if can_store_counts:
                        row.extend([
                            _to_str_or_none(getattr(r, "nb_decedes_fin_courbe", final_counts.get("nb_decedes_fin_courbe"))),
                            _to_str_or_none(getattr(r, "nb_vivants_fin_courbe", final_counts.get("nb_vivants_fin_courbe"))),
                            _to_str_or_none(getattr(r, "nb_pdv_fin_courbe", final_counts.get("nb_pdv_fin_courbe"))),
                        ])
                    if has_stade_col:
                        row.append(getattr(r, "stade", None) or stade_majoritaire)
                    rows.append(tuple(row))

                if rows:
                    execute_values(
                        cur,
                        f"INSERT INTO {full_table} ({', '.join(insert_cols)}) VALUES %s",
                        rows,
                        page_size=1000,
                    )
                print(f"✅ Insert KPI: {len(rows)} lignes → {full_table}")

            else:
                raise ValueError(f"Table inconnue : {table_name}")

        pg_conn.commit()

    except Exception:
        pg_conn.rollback()
        raise
    finally:
        pg_conn.close()
