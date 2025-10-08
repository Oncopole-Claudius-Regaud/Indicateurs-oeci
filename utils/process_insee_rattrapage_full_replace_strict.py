import os
import logging
import re
from io import StringIO
from glob import glob
from pathlib import Path

import pandas as pd
from oeci.utils.db import get_postgres_hook

TABLE_NAME = "oeci.insee_ref"

# Dossiers possibles (tu peux aussi passer input_dir via op_kwargs du DAG)
CANDIDATE_DIRS = [
    os.environ.get("OECI_INSEE_INPUT_DIR"),
    str(Path(__file__).resolve().parent / "insee_month_rattrapage"),
    "/home/administrateur/airflow/dags/oeci/insee_month_rattrapage",
]

FILE_PATTERNS = ["*.csv", "deces*.csv", "Deces*.csv"]

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")

# ---------------- DB ----------------
def get_postgres_conn():
    return get_postgres_hook().get_conn()

# ------------- CSV READ -------------
def _read_csv(path: str) -> pd.DataFrame:
    """Lecture simple, essaie TAB puis ';' puis ','. Tout en str pour préserver les zéros."""
    for sep in ("\t", ";", ","):
        try:
            df = pd.read_csv(
                path, sep=sep, encoding="utf-8",
                dtype=str, keep_default_na=False, na_values=[""], engine="python"
            )
            if df.shape[1] > 1:
                logger.info(f"[READ] {os.path.basename(path)} sep='{sep}' utf-8 -> {df.shape}")
                return df
        except Exception:
            continue
    # Dernier recours: autodetect
    df = pd.read_csv(path, sep=None, engine="python", dtype=str, keep_default_na=False, na_values=[""])
    logger.info(f"[READ] {os.path.basename(path)} autodetect -> {df.shape}")
    return df

# ----------- TRANSFORM --------------
REQUIRED_INPUT = [
    "nomprenom", "sexe", "datenaiss", "lieunaiss", "commnaiss", "datedeces"
]

OUTPUT_COLS = [
    "nom","prenoms","sexe","date_naissance",
    "code_insee_lieu_naissance","commune_naissance","date_deces"
]

def _clean_tail(x: str) -> str:
    """Trim + supprime / ou _ en fin + collapse espaces."""
    x = (x or "").strip()
    x = re.sub(r"[/_]+$", "", x)
    x = re.sub(r"\s{2,}", " ", x)
    return x

def _split_nom_prenoms(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Split 'NOM*PRENOMS' en 2 colonnes avec nettoyage.
    ⚠️ 1ère partie => nom, 2ème => prenoms (exigence utilisateur).
    """
    def split_one(s: str):
        s = str(s)
        if "*" in s:
            a, b = s.split("*", 1)
            return _clean_tail(a), _clean_tail(b)
        # fallback doux si pas d'étoile
        parts = re.split(r"[;\s]+", s.strip(), maxsplit=1)
        if len(parts) == 2:
            return _clean_tail(parts[0]), _clean_tail(parts[1])
        return _clean_tail(s), ""
    pairs = series.astype(str).apply(split_one)
    nom = pairs.apply(lambda t: t[0])       # ← 1ère valeur -> nom
    prenoms = pairs.apply(lambda t: t[1])   # ← 2ème valeur -> prenoms
    return nom, prenoms

def transform_insee_df_strict(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Logique stricte d’origine (sans date_import) :
    - dates 'YYYYMMDD' validées (8 chiffres, mois/jour != '00')
    - conversion en DATE
    - filtre des lignes invalides
    """
    df = df_in.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    logger.info(f"[HEADERS] Colonnes vues: {list(df.columns)}")

    # Vérifie présence minimale
    missing = [c for c in REQUIRED_INPUT if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes d'entrée manquantes: {missing}")

    # Construire nom/prenoms depuis 'nomprenom' (NOM*PRENOMS)
    nom, prenoms = _split_nom_prenoms(df["nomprenom"])

    # Base DataFrame selon cibles
    out = pd.DataFrame({
        "nom": nom,
        "prenoms": prenoms,
        "sexe": df["sexe"].astype(str).str.strip(),
        "date_naissance": df["datenaiss"].astype(str).str.strip(),
        "code_insee_lieu_naissance": df["lieunaiss"].astype(str).str.strip(),
        "commune_naissance": df["commnaiss"].astype(str).str.strip(),
        "date_deces": df["datedeces"].astype(str).str.strip(),
    })

    # Logs d'aperçu
    logger.info("Exemple de lignes brutes (après mapping direct) :")
    logger.info(out.head(5).to_string())

    # --- logique stricte sur les dates ---
    logger.info("Aperçu colonne date_naissance :")
    logger.info(out["date_naissance"].head(10).to_string())

    # 1) garder seulement les dates 8 chiffres
    before = len(out)
    bad_deces = ~out["date_deces"].str.fullmatch(r"\d{8}")
    if bad_deces.any():
        logger.warning("lignes avec date_deces invalide (extrait) :")
        logger.warning(out.loc[bad_deces, ["nom","prenoms","date_deces"]].head().to_string())
    out = out[out["date_deces"].str.fullmatch(r"\d{8}")]
    logger.info(f"Lignes supprimées pour date_deces invalide : {before - len(out)}")

    before = len(out)
    out = out[out["date_naissance"].str.fullmatch(r"\d{8}")]
    out = out[~out["date_naissance"].str[4:6].isin(["00"])]
    out = out[~out["date_naissance"].str[6:8].isin(["00"])]
    logger.info(f"Lignes supprimées pour date_naissance invalide : {before - len(out)}")

    # 2) conversions en DATE
    out["date_naissance"] = pd.to_datetime(out["date_naissance"], format="%Y%m%d", errors="coerce").dt.date
    out["date_deces"]     = pd.to_datetime(out["date_deces"],     format="%Y%m%d", errors="coerce").dt.date

    # 3) drop des NA
    out = out.dropna(subset=["date_naissance", "date_deces"])

    logger.info(out.head(5).to_string())
    logger.info(f"Nombre de lignes après parsing : {len(out)}")

    return out[OUTPUT_COLS]

# --------- COMBINE & LOAD ----------
def _resolve_input_dir(input_dir: str | None) -> str:
    if input_dir and os.path.isdir(input_dir):
        return input_dir
    for d in CANDIDATE_DIRS:
        if d and os.path.isdir(d):
            logger.info(f"[PATH] Using input_dir={d}")
            return d
    raise FileNotFoundError(f"Aucun dossier d'entrée trouvé. Candidates: {CANDIDATE_DIRS}")

def _list_files(input_dir: str) -> list[str]:
    paths = []
    for pat in FILE_PATTERNS:
        paths.extend(glob(os.path.join(input_dir, pat)))
    return sorted(set(paths))

def build_combined_dataframe_strict(input_dir: str | None = None) -> pd.DataFrame:
    input_dir = _resolve_input_dir(input_dir)
    files = _list_files(input_dir)
    if not files:
        logger.warning(f"[INSEE] Aucun CSV trouvé dans {input_dir}.")
        return pd.DataFrame(columns=OUTPUT_COLS)

    logger.info(f"[INSEE] {len(files)} fichier(s) détecté(s) dans {input_dir}.")
    combined = []
    cumul = 0
    for i, path in enumerate(files, 1):
        logger.info(f"[INSEE] ({i}/{len(files)}) Lecture CSV : {path}")
        raw = _read_csv(path)
        df = transform_insee_df_strict(raw)
        n = len(df)
        cumul += n
        logger.info(f"[ACCUM] +{n} ligne(s) depuis {os.path.basename(path)} | cumul = {cumul}")
        if n > 0:
            combined.append(df)

    if not combined:
        logger.warning("[INSEE] Aucun enregistrement utilisable après transform.")
        return pd.DataFrame(columns=OUTPUT_COLS)

    final_df = pd.concat(combined, ignore_index=True)
    logger.info(f"[ACCUM] DF final combiné : {len(final_df)} ligne(s).")
    return final_df

def load_full_replace_to_postgres(df: pd.DataFrame):
    if df.empty:
        logger.error("[LOAD] DF final vide. Abandon du TRUNCATE pour éviter d'effacer la table par erreur.")
        return {"inserted": 0, "truncated": False}

    conn = get_postgres_conn()
    cur = conn.cursor()
    try:
        # PAS DE CREATE TABLE — la table existe déjà
        logger.info(f"[LOAD] TRUNCATE {TABLE_NAME} …")
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
        conn.commit()

        ordered = ["nom","prenoms","sexe","date_naissance","code_insee_lieu_naissance","commune_naissance","date_deces"]
        df_copy = df.copy()
        # convertir les DATE en str ISO pour COPY
        for c in ["date_naissance","date_deces"]:
            df_copy[c] = df_copy[c].astype(str)

        buf = StringIO()
        df_copy[ordered].to_csv(buf, index=False, header=False, sep=";")
        buf.seek(0)

        cur.copy_expert(sql=f"COPY {TABLE_NAME} FROM STDIN WITH CSV DELIMITER ';'", file=buf)
        inserted = len(df_copy)
        conn.commit()
        logger.info(f"[LOAD] Insertion terminée : {inserted} ligne(s).")
        return {"inserted": inserted, "truncated": True}
    except Exception as e:
        conn.rollback()
        logger.exception(f"[LOAD] Échec du chargement : {e}")
        raise
    finally:
        cur.close()
        conn.close()

# ----------- ENTRYPOINT ------------
def process_insee_rattrapage_full_replace_strict(input_dir: str | None = None):
    df = build_combined_dataframe_strict(input_dir=input_dir)
    result = load_full_replace_to_postgres(df)
    return {"rows_final_df": int(len(df)), **result}

# Alias rétro-compatible
process_insee_rattrapage_full_replace = process_insee_rattrapage_full_replace_strict

