import logging
import requests
from datetime import datetime, timedelta
from io import StringIO
import os
from utils.db import get_postgres_hook
import re
import time
import csv

# ==============================================================================
# Helpers API data.gouv (liste puis dernier dispo)
# ==============================================================================

HTTP_HEADERS = {"User-Agent": "oeci-insee-loader/1.0"}
MONTHLY_RE = re.compile(r"^deces-(\d{4})-m(\d{2})\.txt$", re.I)
ANNUAL_RE  = re.compile(r"^deces-(\d{4})\.txt$", re.I)

DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/fichier-des-personnes-decedees/"
LOCAL_DOWNLOAD_PATH = "/tmp/insee_deces_latest.txt"
TABLE_NAME = "ref_source_externe.insee_ref"

CHUNK_SIZE = 10_000
DATE8_RE = re.compile(r"^\d{8}$")


def get_postgres_conn():
    return get_postgres_hook().get_conn()


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
    Pattern: deces-YYYY-mMM.txt
    """
    candidates = []
    for name, url in _iter_resources():
        m = MONTHLY_RE.match(name)
        if m and url:
            y, mm = int(m.group(1)), int(m.group(2))
            candidates.append(((y, mm), name, url))
    if not candidates:
        raise Exception("[INSEE] Aucun fichier mensuel 'deces-YYYY-mMM.txt' trouvé.")
    candidates.sort(key=lambda x: (x[0][0], x[0][1]), reverse=True)
    return candidates[0]


def _latest_annual_resource():
    """
    Retourne (year, name, url) du dernier fichier annuel disponible.
    Pattern: deces-YYYY.txt
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


# ==============================================================================
# Task: DOWNLOAD
# ==============================================================================

def download_insee_file(mode="monthly", **context):
    """
    monthly (défaut) : télécharge le DERNIER mensuel disponible.
    historical       : télécharge le DERNIER annuel disponible.
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


# ==============================================================================
# Parsing ligne fixe
# ==============================================================================

def parse_ligne_fixe(ligne: str) -> dict:
    try:
        nom_prenoms = ligne[0:80].strip()
        sexe = ligne[80]  # 1 (homme) ou 2 (femme)
        insee_bloc = ligne[80:95]  # sexe + date naissance + code INSEE (selon spec)
        commune_naissance = ligne[94:124].strip()

        date_deces = ligne[154:162].strip()

        nom, prenoms = "", ""
        if "*" in nom_prenoms:
            parts = nom_prenoms.split("*")
            nom = parts[0].strip()
            prenoms = parts[1].strip() if len(parts) > 1 else ""
            prenoms = re.sub(r"[^A-ZÉÈÀÂÎÔÙÛÇa-zéèàâîôùûç\s-]+$", "", prenoms.strip())

        # Extraire date naissance sans le sexe (ton code d'origine: insee_bloc[1:9])
        date_naissance = insee_bloc[1:9]
        code_insee = insee_bloc[9:14].strip()

        return {
            "nom": nom,
            "prenoms": prenoms,
            "sexe": sexe,
            "date_naissance": date_naissance,
            "code_insee_lieu_naissance": code_insee,
            "commune_naissance": commune_naissance,
            "date_deces": date_deces,
        }

    except Exception as e:
        logging.warning(f"Erreur de parsing ligne: {e}")
        return {}


def _yyyymmdd_to_date(s: str):
    """Convertit 'YYYYMMDD' -> date ISO 'YYYY-MM-DD'. Retourne None si invalide."""
    if not s or not DATE8_RE.fullmatch(s):
        return None
    if s[4:6] == "00" or s[6:8] == "00":
        return None
    try:
        return datetime.strptime(s, "%Y%m%d").date().isoformat()
    except Exception:
        return None


def iter_insee_rows(filepath: str, encoding="utf-8"):
    """
    Générateur: lit le fichier INSEE et yield des tuples (7 colonnes) propres.
    On sort les dates en 'YYYY-MM-DD' pour que COPY -> DATE fonctionne.
    """
    with open(filepath, "r", encoding=encoding, errors="replace") as f:
        for ligne in f:
            d = parse_ligne_fixe(ligne)
            if not d:
                continue

            dn = _yyyymmdd_to_date(str(d.get("date_naissance", "")).strip())
            dd = _yyyymmdd_to_date(str(d.get("date_deces", "")).strip())
            if dn is None or dd is None:
                continue

            yield (
                (d.get("nom") or "").strip(),
                (d.get("prenoms") or "").strip(),
                (d.get("sexe") or "").strip(),
                dn,
                (d.get("code_insee_lieu_naissance") or "").strip(),
                (d.get("commune_naissance") or "").strip(),
                dd,
            )


# ==============================================================================
# Task: LOAD (streaming + chunks + COPY toutes les 10 000)
# ==============================================================================

def load_to_postgres(mode="monthly", **context):
    conn = get_postgres_conn()
    cur = conn.cursor()

    # 1) Table cible
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
        logging.info("[INSEE] Mode 'historical' → TRUNCATE table avant insertion")
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        conn.commit()
    else:
        logging.info("[INSEE] Mode 'monthly' → insertion incrémentale (pas de TRUNCATE)")

    # 2) Table TEMP (dans la même session)
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
    conn.commit()

    def flush_chunk(buf: StringIO, rows_in_buf: int) -> int:
        """COPY -> temp, INSERT -> target, puis TRUNCATE temp."""
        if rows_in_buf == 0:
            return 0

        buf.seek(0)

        cur.copy_expert(
            sql="""
                COPY tmp_insee_ref (
                    nom, prenoms, sexe, date_naissance,
                    code_insee_lieu_naissance, commune_naissance, date_deces
                )
                FROM STDIN
                WITH (FORMAT CSV, DELIMITER ';', QUOTE '"', ESCAPE '"', NULL '');
            """,
            file=buf
        )
        conn.commit()

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
        inserted = cur.rowcount
        conn.commit()

        cur.execute("TRUNCATE TABLE tmp_insee_ref;")
        conn.commit()

        return inserted

    total_read = 0
    total_inserted = 0

    buf = StringIO()
    writer = csv.writer(buf, delimiter=";", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    rows_in_buf = 0

    # Si tu as des soucis d'encodage, remplace par encoding="latin-1"
    for row in iter_insee_rows(LOCAL_DOWNLOAD_PATH, encoding="utf-8"):
        writer.writerow(row)
        rows_in_buf += 1
        total_read += 1

        if rows_in_buf >= CHUNK_SIZE:
            inserted = flush_chunk(buf, rows_in_buf)
            total_inserted += inserted
            logging.info(f"[INSEE] flush chunk: rows={rows_in_buf}, inserted={inserted}, total_read={total_read}, total_inserted={total_inserted}")

            buf = StringIO()
            writer = csv.writer(buf, delimiter=";", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
            rows_in_buf = 0

    # dernier chunk
    inserted = flush_chunk(buf, rows_in_buf)
    total_inserted += inserted
    logging.info(f"[INSEE] fin: total_read={total_read}, total_inserted={total_inserted}")

    # cleanup
    cur.execute("DROP TABLE IF EXISTS tmp_insee_ref;")
    conn.commit()

    cur.close()
    conn.close()
