import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from io import StringIO
import os
from oeci.utils.db import get_postgres_hook
import re
import time
import csv

# === HELPERS API DATA.GOUV (liste puis dernier dispo) ===


HTTP_HEADERS = {"User-Agent": "oeci-insee-loader/1.0"}
MONTHLY_RE = re.compile(r"^deces-(\d{4})-m(\d{2})\.txt$", re.I)
ANNUAL_RE  = re.compile(r"^deces-(\d{4})\.txt$", re.I)

def _get_dataset_json(max_retries=3, backoff=2.0):
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(DATASET_API_URL, headers=HTTP_HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            logging.warning(f"[INSEE] tentative {attempt}/{max_retries} échec: {e}")
            time.sleep(backoff * attempt)
    raise Exception(f"[INSEE] Échec de récupération des métadonnées INSEE: {last_err}")

def _iter_resources():
    data = _get_dataset_json()
    for res in data.get("resources", []):
        name = (res.get("title") or res.get("name") or res.get("file_name") or "").strip()
        url  = (res.get("url") or (res.get("latest") or {}).get("url") or "").strip()
        yield name, url

def _latest_monthly_resource():
    """
    Retourne ((year, month), name, url) du dernier fichier mensuel disponible.
    On se base uniquement sur le pattern de nommage 'deces-YYYY-mMM.txt'.
    """
    candidates = []
    for name, url in _iter_resources():
        m = MONTHLY_RE.match(name)
        if m and url:
            y, mm = int(m.group(1)), int(m.group(2))
            candidates.append(((y, mm), name, url))
    if not candidates:
        raise Exception("[INSEE] Aucun fichier mensuel 'deces-YYYY-mMM.txt' trouvé.")
    # tri décroissant par (année, mois)
    candidates.sort(key=lambda x: (x[0][0], x[0][1]), reverse=True)
    return candidates[0]  # ((y,m), name, url)

def _latest_annual_resource():
    """
    Optionnel: retourne (year, name, url) du dernier fichier annuel disponible.
    """
    candidates = []
    for name, url in _iter_resources():
        m = ANNUAL_RE.match(name)
        if m and url:
            y = int(m.group(1))
            candidates.append((y, name, url))
    if not candidates:
        raise Exception("[INSEE] Aucun fichier annuel 'deces-YYYY.txt' trouvé.")
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0]

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


# === TASK: DOWNLOAD (nouvelle version) ===
def download_insee_file(mode="monthly", **context):
    """
    monthly (par défaut) : liste les ressources et télécharge le DERNIER mensuel disponible.
    historical          : liste et prend le DERNIER annuel disponible.
    """
    logging.info(f"[INSEE] Mode de téléchargement : {mode}")

    if mode == "historical":
        y, name, url = _latest_annual_resource()
        filename = name or f"deces-{y}.txt"
    else:
        (y, m), name, url = _latest_monthly_resource()
        filename = name or f"deces-{y}-m{m:02d}.txt"

    if not url:
        raise Exception("[INSEE] Ressource trouvée mais URL vide.")

    logging.info(f"[INSEE] Téléchargement: {filename} ← {url}")
    resp = requests.get(url, headers=HTTP_HEADERS, timeout=60)
    if resp.status_code != 200 or not resp.content:
        raise Exception(f"[INSEE] Échec du téléchargement ({resp.status_code}).")

    with open(LOCAL_DOWNLOAD_PATH, "wb") as f:
        f.write(resp.content)

    logging.info(f"[INSEE] Fichier sauvegardé: {LOCAL_DOWNLOAD_PATH} (source: {filename})")

    # Optionnel: tracer dans XCom le nom de la ressource utilisée
    try:
        ti = context.get("ti")
        if ti:
            ti.xcom_push(key="insee_filename", value=filename)
    except Exception:
        pass


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
    df[["nom", "prenoms", "sexe", "date_naissance", "code_insee_lieu_naissance", "commune_naissance", "date_deces"]].to_csv(
        buffer, index=False, header=False, sep=";"
    )
    buffer.seek(0)
    return buffer.getvalue()


def load_to_postgres(mode="monthly", **context):
    csv_data = transform_insee_file()  # <= 7 colonnes (on a nettoyé)
    conn = get_postgres_conn()
    cur = conn.cursor()

    # 1) Table cible (7 colonnes)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            nom TEXT,
            prenoms TEXT,
            sexe TEXT,
            date_naissance DATE,
            code_insee_lieu_naissance TEXT,
            commune_naissance TEXT,
            date_deces DATE
        )
    """)
    conn.commit()

    if mode == "historical":
        logging.info("Mode 'historical' → TRUNCATE table avant insertion")
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        conn.commit()
    else:
        logging.info("Mode 'monthly' → insertion incrémentale (pas de TRUNCATE)")

    # 2) (RE)création table TEMP (même session)
    cur.execute("DROP TABLE IF EXISTS tmp_insee_ref;")
    cur.execute("""
        CREATE TEMP TABLE tmp_insee_ref (
            nom TEXT,
            prenoms TEXT,
            sexe TEXT,
            date_naissance DATE,
            code_insee_lieu_naissance TEXT,
            commune_naissance TEXT,
            date_deces DATE
        ) ON COMMIT PRESERVE ROWS
    """)

    # Sanity: vérifie que la temp existe bien dans cette session
    cur.execute("SELECT to_regclass('pg_temp.tmp_insee_ref')")
    exists = cur.fetchone()[0]
    logging.info(f"[INSEE] temp exists? {exists is not None} (regclass={exists})")

    # 3) COPY dans la table TEMP (7 colonnes)
    #    si tu as un buffer produit par csv, assure-toi d'un seek(0)
    out_buf = StringIO()
    rdr = csv.reader(StringIO(csv_data), delimiter=';')
    wtr = csv.writer(out_buf, delimiter=';', lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    for row in rdr:
        if not row or all(c == "" for c in row):
            continue
        row7 = (row + [""] * 7)[:7]  # garde 7 colonnes
        wtr.writerow(row7)
    out_buf.seek(0)

    cur.copy_expert(
        sql="""
            COPY tmp_insee_ref (
                nom, prenoms, sexe, date_naissance,
                code_insee_lieu_naissance, commune_naissance, date_deces
            )
            FROM STDIN
            WITH (FORMAT CSV, DELIMITER ';', QUOTE '"', ESCAPE '"', NULL '');
        """,
        file=out_buf
    )
    # Ici on peut commit : la temp persiste (PRESERVE ROWS)
    conn.commit()

    # 4) INSERT explicite (pas de SELECT *)
    if mode == "monthly":
        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (
                nom, prenoms, sexe, date_naissance,
                code_insee_lieu_naissance, commune_naissance, date_deces
            )
            SELECT
                t.nom, t.prenoms, t.sexe, t.date_naissance,
                t.code_insee_lieu_naissance, t.commune_naissance, t.date_deces
            FROM tmp_insee_ref t
            WHERE NOT EXISTS (
                SELECT 1 FROM {TABLE_NAME} r
                WHERE r.nom = t.nom
                  AND r.prenoms = t.prenoms
                  AND r.date_naissance = t.date_naissance
                  AND r.date_deces IS NOT DISTINCT FROM t.date_deces
            );
        """)
    else:
        cur.execute(f"""
            INSERT INTO {TABLE_NAME} (
                nom, prenoms, sexe, date_naissance,
                code_insee_lieu_naissance, commune_naissance, date_deces
            )
            SELECT
                nom, prenoms, sexe, date_naissance,
                code_insee_lieu_naissance, commune_naissance, date_deces
            FROM tmp_insee_ref;
        """)

    conn.commit()

    # (optionnel) cleanup
    cur.execute("DROP TABLE IF EXISTS tmp_insee_ref;")
    conn.commit()

    cur.close()
    conn.close()

