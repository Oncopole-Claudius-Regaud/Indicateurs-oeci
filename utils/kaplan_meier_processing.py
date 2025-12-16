# ==============================================================================
# Fichier : kaplan_meier_processing.py
# MISE À JOUR : Utilisation des Hooks Airflow pour la connexion DB
# ==============================================================================

import pandas as pd
# Importation standard pour les Hooks/Variables Airflow
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import json
from io import StringIO
import numpy as np
from lifelines import KaplanMeierFitter

# --- Définition de la fonction Hook (Simulée pour l'exemple) ---
# NOTE: Dans votre environnement réel, cette fonction doit être importée de 'utils.db'
def get_postgres_hook(conn_id=None):
    """Récupère un hook PostgreSQL via Airflow Variable (ou fallback postgres_test)."""
    # L'ID de connexion réelle est récupérée ici
    if not conn_id:
         # Simuler la récupération de la variable si non passée
         # ATTENTION : Si vous utilisez vraiment Variable.get, vous devez l'importer et cela nécessite une connexion DB Airflow.
        conn_id = Variable.get("target_pg_conn_id", default_var="postgres_test")
    return PostgresHook(postgres_conn_id=conn_id)

def get_db_engine(hook):
    """Crée et retourne l'objet engine SQLAlchemy à partir du hook."""
    try:
        engine = hook.get_sqlalchemy_engine()
        print("✅ Moteur SQLAlchemy créé via Hook.")
        return engine
    except Exception as e:
        print(f" Erreur lors de la création du moteur SQLAlchemy via Hook : {e}")
        raise


def extract_and_clean_data_task(
    organe, date_debut_obs, date_fin_obs,
    conn_id=None # Ajout du conn_id pour la flexibilité
):
    """
    1. Extrait les données brutes depuis PostgreSQL via Hook.
    2. Applique les nettoyages initiaux.
    3. Sérialise le DataFrame pour XCom.
    """
    hook = get_postgres_hook(conn_id)
    engine = get_db_engine(hook)
    
    SCHEMA_NAME = 'datamart_oeci_survie'
    TABLE_NAME = 'v_statut_vital'
    FULL_TABLE_PATH = f"{SCHEMA_NAME}.{TABLE_NAME}"
    
    # --- VOTRE CODE SQL (avec variables d'entrée) ---
    query = f"""
    WITH patient_min_annee AS (
        SELECT
            ipp_ocr, MIN(annee) AS annee_debut_suivi
        FROM {FULL_TABLE_PATH} 
        WHERE organe = '{organe}'
        GROUP BY ipp_ocr
    ),
    patient_statut_final AS (
        SELECT DISTINCT ON (t1.ipp_ocr)
            t1.ipp_ocr, t1.statut_vital, t1.annee
        FROM {FULL_TABLE_PATH} t1
        WHERE 
            t1.organe = '{organe}'
            AND t1.annee <= SUBSTRING('{date_fin_obs}' FROM 1 FOR 4)::int
        ORDER BY 
            t1.ipp_ocr, t1.annee DESC, t1.date_derniere_nouvelle DESC
    )
    
    SELECT t_base.*
    FROM {FULL_TABLE_PATH} t_base
    JOIN patient_min_annee min_annee ON t_base.ipp_ocr = min_annee.ipp_ocr
    JOIN patient_statut_final final_statut ON t_base.ipp_ocr = final_statut.ipp_ocr

    WHERE 
        t_base.organe = '{organe}'
        AND min_annee.annee_debut_suivi >= SUBSTRING('{date_debut_obs}' FROM 1 FOR 4)::int
        AND NOT (final_statut.statut_vital = 'PDV')
        AND t_base.annee <= SUBSTRING('{date_fin_obs}' FROM 1 FOR 4)::int
    ORDER BY t_base.ipp_ocr, t_base.annee;
    """
    # -----------------------------------------------

    print(f"Extraction des données pour l'organe {organe}...")
    df_travail = pd.read_sql_query(query, engine)
    
    # --- VOTRE CODE DE NETTOYAGE ET FILTRAGE PYTHON ---
    
    df_travail['ipp_ocr'] = df_travail['ipp_ocr'].fillna('')
    df_travail['ipp_prefix'] = df_travail['ipp_ocr'].str[0:4]
    CRITERE_FILTRAGE_IPP = '2000'
    masque_ipp_valide = df_travail['ipp_prefix'] >= CRITERE_FILTRAGE_IPP
    masque_dates_valides = (
        df_travail['date_diag_tkc'].notna() & 
        df_travail['date_derniere_nouvelle'].notna()
    )
    masque_final = masque_ipp_valide & masque_dates_valides
    df_survie_km_factorise = df_travail[masque_final].copy()

    print(f"✅ Nettoyage terminé. {len(df_survie_km_factorise)} lignes retenues.")
    
    # Sérialisation du DF en JSON pour le transport via XCom
    return df_survie_km_factorise.to_json(date_format='iso')


def calculate_kaplan_meier_task(ti, date_debut_observation_filtre, **kwargs):
    """
    Désérialise le DataFrame, effectue l'analyse KM, et structure les résultats.
    (Aucun changement majeur ici, la logique de calcul reste la même)
    """
    
    df_json = ti.xcom_pull(task_ids='extract_and_clean_data')
    if not df_json:
        raise ValueError("Le DataFrame n'a pas été récupéré par XCom.")
        
    df_km_final = pd.read_json(StringIO(df_json))
    
    # Conversion et filtrage de date
    df_km_final['date_diag_tkc'] = pd.to_datetime(df_km_final['date_diag_tkc'], errors='coerce')
    df_km_final['date_derniere_nouvelle'] = pd.to_datetime(df_km_final['date_derniere_nouvelle'], errors='coerce')

    DATE_OBS_FILTRE = pd.to_datetime(date_debut_observation_filtre)
    df_km_final = df_km_final[
        df_km_final['date_diag_tkc'] >= DATE_OBS_FILTRE
    ].copy()

    # Calcul de la Durée de Survie et Événement
    df_km_final['time_years'] = (
        df_km_final['date_derniere_nouvelle'] - df_km_final['date_diag_tkc']
    ).dt.days / 365.25

    EVENT_STATUS = 'Décédé' 
    df_km_final['event_observed'] = np.where(
        df_km_final['statut_vital'] == EVENT_STATUS, 
        1, 
        0
    )

    # Ajustement du modèle KM
    kmf = KaplanMeierFitter()
    kmf.fit(
        durations=df_km_final['time_years'], 
        event_observed=df_km_final['event_observed']
    )
    
    # --- 1. Préparation des Données de la COURBE ---
    curve_df = kmf.survival_function_.copy()
    ci_df = kmf.confidence_interval_survival_function_.copy()
    
    curve_data = curve_df.merge(ci_df, left_index=True, right_index=True).reset_index()

    curve_data.columns = [
        'time_years', 'survival_rate', 'ic_lower', 'ic_upper'
    ]
    
    curve_data_json = curve_data.to_json(orient='records')
    
    # --- 2. Préparation des Indicateurs CLÉS ---
    times = [1.0, 5.0, 10.0]
    key_indicators = []
    
    for t in times:
        try:
            taux = kmf.survival_function_at_times(t).iloc[0]
            ic_df_temp = kmf.confidence_interval_survival_function_.reindex(
                kmf.confidence_interval_survival_function_.index.union([t])
            )
            ic_df_interpole = ic_df_temp.ffill()

            ci_lower = ic_df_interpole.loc[t].iloc[0]
            ci_upper = ic_df_interpole.loc[t].iloc[1]
            
            survival_pct = float(f"{taux * 100:.2f}")
            ic_low_pct = float(f"{ci_lower * 100:.2f}")
            ic_high_pct = float(f"{ci_upper * 100:.2f}")
            
            key_indicators.append({
                'time_point': int(t),
                'survival_rate': survival_pct, 
                'ic_range': f"[{ic_low_pct} % - {ic_high_pct} %]",
                'ic_low_pct': ic_low_pct,
                'ic_high_pct': ic_high_pct
            })
        except Exception:
            pass

    return {
        'curve_data': curve_data_json,
        'key_indicators': key_indicators
    }

def load_to_db_task(ti, table_name, conn_id=None, **kwargs):
    """
    Récupère les résultats de KM et les charge dans la table PostgreSQL cible via Hook.
    """
    
    hook = get_postgres_hook(conn_id)
    engine = get_db_engine(hook) # Récupérer l'engine pour to_sql

    results = ti.xcom_pull(task_ids='calculate_kaplan_meier')
    
    if not results:
        print("Aucune donnée de résultats récupérée. Fin de la tâche.")
        return

    # Récupérer les paramètres du DAG pour la traçabilité
    ORGAN = kwargs['dag_run'].conf.get('organe', 'UNKNOWN')
    DATE_DEB = kwargs['dag_run'].conf.get('date_debut_obs', 'UNKNOWN')
    DATE_FIN = kwargs['dag_run'].conf.get('date_fin_obs', 'UNKNOWN')
    
    # Déterminer la donnée à charger
    if table_name == 'datamart_km_curve':
        df_to_load = pd.read_json(StringIO(results['curve_data']), orient='records')
        df_to_load['date_start_obs'] = DATE_DEB
        df_to_load['date_end_obs'] = DATE_FIN
        
    elif table_name == 'datamart_km_key_indicators':
        df_to_load = pd.DataFrame(results['key_indicators'])
        
    else:
        raise ValueError(f"Nom de table inconnu : {table_name}")

    # Ajouter les colonnes de traçabilité communes
    df_to_load['organe'] = ORGAN
    df_to_load['run_date'] = datetime.now() 

    # Chargement dans la base de données
    df_to_load.to_sql(
        table_name, 
        engine, 
        if_exists='append', 
        index=False,
        schema='datamart_oeci_survie'
    )
    print(f"✅ Chargement de {len(df_to_load)} lignes réussi dans la table {table_name}.")