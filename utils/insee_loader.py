import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from io import StringIO
import os
from oeci.utils.db import get_postgres_hook
import re

# === CONFIGURATION ===
DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/fichier-des-personnes-decedees/"
LOCAL_DOWNLOAD_PATH = "/tmp/insee_deces_latest.txt"
TABLE_NAME = "oeci.insee_ref"


# === UTILS ===
def get_previous_month_date():
    today = datetime.today()
    first = today.replace(day=1)
    prev_month = first - timedelta(days=1)
    return prev_month.year, f"{prev_month.month:02d}"


def get_postgres_conn():
    return get_postgres_hook().get_conn()


# === TASK: DOWNLOAD ===
def download_insee_file(mode="monthly", **context):
    logging.info(f"Mode de téléchargement : {mode}")

    if mode == "historical":
        filename_pattern = "deces-2024.txt"  # ou le bon nom exact du fichier historique
    else:
        year, month = get_previous_month_date()
        filename_pattern = f"deces-{year}-m{month}.txt"

    logging.info(f"Recherche du fichier : {filename_pattern} depuis l'API Data.gouv")

    response = requests.get(DATASET_API_URL)
    if response.status_code != 200:
        raise Exception(f"Échec de récupération des métadonnées du dataset INSEE ({response.status_code})")

    data = response.json()
    resources = data.get("resources", [])

    target_url = None
    for res in resources:
        if filename_pattern in res.get("title", ""):
            target_url = res.get("url")
            break

    if not target_url:
        raise Exception(f"Fichier {filename_pattern} introuvable dans les ressources de l'API.")

    logging.info(f"Téléchargement du fichier depuis : {target_url}")
    file_response = requests.get(target_url)
    if file_response.status_code != 200:
        raise Exception(f"Échec du téléchargement ({file_response.status_code})")

    with open(LOCAL_DOWNLOAD_PATH, "wb") as f:
        f.write(file_response.content)

    logging.info(f"Fichier sauvegardé localement à : {LOCAL_DOWNLOAD_PATH}")


def parse_ligne_fixe(ligne: str) -> dict:
    try:
        nom_prenoms = ligne[0:80].strip()
        sexe = ligne[80]  # 1 (homme) ou 2 (femme)
        insee_bloc = ligne[80:95]  # date + code INSEE
        lieu_naissance = ligne[95:135].strip()
        commune_naissance = ligne[94:124].strip()

        # (tu peux commenter la ligne ci-dessous pour l'instant)
        date_deces = ligne[154:162].strip()

        nom, prenoms = "", ""
        if "*" in nom_prenoms:
            parts = nom_prenoms.split("*")
            nom = parts[0].strip()
            prenoms = parts[1].strip() if len(parts) > 1 else ""
            prenoms = re.sub(r"[^A-ZÉÈÀÂÎÔÙÛÇa-zéèàâîôùûç\s-]+$", "", prenoms.strip())
        # Extraire date naissance sans le sexe
        date_naissance = insee_bloc[1:9]
        code_insee = insee_bloc[9:14].strip()

        return {
            "nom": nom,
            "prenoms": prenoms,
            "sexe":sexe,
            "date_naissance": date_naissance,
            "code_insee_lieu_naissance": code_insee,
            "commune_naissance": commune_naissance,
            "date_deces": date_deces,
        }

    except Exception as e:
        logging.warning(f"Erreur de parsing ligne: {e}")
        return {}


# === TASK: TRANSFORM ===
def transform_insee_file():
    logging.info("Début du parsing du fichier INSEE format fixe...")
    lignes = []
    with open(LOCAL_DOWNLOAD_PATH, "r", encoding="utf-8") as f:
        for i, ligne in enumerate(f):
            if i < 5:
                logging.info(f"Ligne {i} brute (len={len(ligne)}): {repr(ligne)}")
            lignes.append(parse_ligne_fixe(ligne))

    logging.info("Exemple de lignes brutes :")
    for ligne in lignes[:5]:
        logging.info(ligne)


    df = pd.DataFrame(lignes)
    if df.empty:
        logging.error("Le DataFrame est vide après parsing. Arrêt.")
        return ""
    
    logging.info("Aperçu colonne date_naissance :")
    logging.info(df["date_naissance"].head(10).to_string())
    
    # Nettoyage des colonnes date en string
    df["date_naissance"] = df["date_naissance"].astype(str).str.strip()
    df["date_deces"] = df["date_deces"].astype(str).str.strip()

    # Log pour voir les dates cheloues
    bad_deces = df[~df["date_deces"].str.fullmatch(r"\d{8}")]
    if not bad_deces.empty:
        logging.warning("ignes avec date_deces invalide (extrait) :")
        logging.warning(bad_deces[["nom", "prenoms", "date_deces"]].head().to_string())

    # ✅ Filtrage robuste
    avant = len(df)
    df = df[df["date_deces"].str.fullmatch(r"\d{8}")]
    logging.info(f"Lignes supprimées pour date_deces invalide : {avant - len(df)}")

    # Pareil pour date_naissance
    avant = len(df)
    df = df[df["date_naissance"].str.fullmatch(r"\d{8}")]
    df = df[~df["date_naissance"].str[4:6].isin(["00"])]
    df = df[~df["date_naissance"].str[6:8].isin(["00"])]
    logging.info(f"Lignes supprimées pour date_naissance invalide : {avant - len(df)}")

    #  Conversion sécurisée
    df["date_naissance"] = pd.to_datetime(df["date_naissance"], format="%Y%m%d", errors="coerce").dt.date
    df["date_deces"] = pd.to_datetime(df["date_deces"], format="%Y%m%d", errors="coerce").dt.date

    # Dernier nettoyage
    df = df.dropna(subset=["date_naissance", "date_deces"])

    # Date d'import
    df["date_import"] = datetime.today().date()

    logging.info(df.head(5).to_string())
    logging.info(f"Nombre de lignes après parsing : {len(df)}")

    buffer = StringIO()
    df[["nom", "prenoms", "sexe", "date_naissance", "code_insee_lieu_naissance", "commune_naissance", "date_deces", "date_import"]].to_csv(
        buffer, index=False, header=False, sep=";"
    )
    buffer.seek(0)
    return buffer.getvalue()


def load_to_postgres(mode="monthly", **context):
    csv_data = transform_insee_file()
    conn = get_postgres_conn()
    cur = conn.cursor()

    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        nom TEXT,
        prenoms TEXT,
        sexe TEXT,
        date_naissance DATE,
        code_insee_lieu_naissance TEXT,
        commune_naissance TEXT,
        date_deces DATE,
        date_import DATE
    )
    """
    cur.execute(create_stmt)
    conn.commit()

    if mode == "historical":
        logging.info("Mode 'historical' détecté → TRUNCATE table avant insertion")
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        conn.commit()

    else:
        logging.info("Mode 'monthly' → insertion incrémentale (pas de TRUNCATE)")

    # Chargement dans une table temporaire
    cur.execute("""
        CREATE TEMP TABLE tmp_insee_ref (
            nom TEXT,
            prenoms TEXT,
            sexe TEXT,
            date_naissance DATE,
            code_insee_lieu_naissance TEXT,
            commune_naissance TEXT,
            date_deces DATE,
            date_import DATE
        )
    """)
    conn.commit()

    cur.copy_expert(
        sql="COPY tmp_insee_ref FROM STDIN WITH CSV DELIMITER ';'",
        file=StringIO(csv_data)
    )
    conn.commit()

    if mode == "monthly":
        # Insertion uniquement des nouveaux décès
        cur.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT * FROM tmp_insee_ref t
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} r
                WHERE
                    r.nom = t.nom
                    AND r.prenoms = t.prenoms
                    AND r.date_naissance = t.date_naissance
                    AND r.date_deces = t.date_deces
            )
        """)
        conn.commit()
    else:
        # Mode historique → insérer tout (déjà tronqué)
        cur.execute(f"""
            INSERT INTO {TABLE_NAME}
            SELECT * FROM tmp_insee_ref
        """)
        conn.commit()

    cur.close()
    conn.close()
    logging.info(f"{len(csv_data.splitlines())} lignes traitées et insérées selon le mode '{mode}'.")
