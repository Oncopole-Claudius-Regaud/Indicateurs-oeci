import argparse
import logging
import re
from pathlib import Path

import requests


DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/fichier-des-personnes-decedees/"
HTTP_HEADERS = {"User-Agent": "oeci-insee-backfill/1.0"}
MONTHLY_RE = re.compile(r"^deces-(\d{4})-m(\d{2})\.txt$", re.I)
ANNUAL_RE = re.compile(r"^deces-(\d{4})\.txt$", re.I)


def _parse_cutoff(value: str) -> tuple[int, int]:
    m = re.fullmatch(r"(\d{4})-(\d{2})", value.strip())
    if not m:
        raise ValueError("Le cutoff doit être au format YYYY-MM, ex: 2026-04")
    year = int(m.group(1))
    month = int(m.group(2))
    if month < 1 or month > 12:
        raise ValueError("Le mois doit être entre 01 et 12")
    return year, month


def _fetch_resources() -> list[tuple[str, str]]:
    resp = requests.get(DATASET_API_URL, headers=HTTP_HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    out = []
    for res in data.get("resources", []):
        name = (res.get("title") or res.get("name") or res.get("file_name") or "").strip()
        url = (res.get("url") or (res.get("latest") or {}).get("url") or "").strip()
        if name and url:
            out.append((name, url))
    return out


def _should_keep(name: str, cutoff_year: int, cutoff_month: int) -> bool:
    mm = MONTHLY_RE.match(name)
    if mm:
        year = int(mm.group(1))
        month = int(mm.group(2))
        return (year, month) <= (cutoff_year, cutoff_month)

    aa = ANNUAL_RE.match(name)
    if aa:
        year = int(aa.group(1))
        return year <= cutoff_year

    return False


def download_backfill(output_dir: Path, cutoff: str, overwrite: bool = False) -> dict:
    cutoff_year, cutoff_month = _parse_cutoff(cutoff)
    output_dir.mkdir(parents=True, exist_ok=True)

    resources = _fetch_resources()
    selected = [(name, url) for (name, url) in resources if _should_keep(name, cutoff_year, cutoff_month)]
    selected.sort(key=lambda x: x[0].lower())

    downloaded = 0
    skipped_existing = 0
    failed = 0

    logging.info("Ressources candidates: %d", len(selected))
    for idx, (name, url) in enumerate(selected, start=1):
        target = output_dir / name
        if target.exists() and not overwrite:
            skipped_existing += 1
            logging.info("[%d/%d] existe déjà, skip: %s", idx, len(selected), target.name)
            continue

        try:
            logging.info("[%d/%d] téléchargement: %s", idx, len(selected), name)
            resp = requests.get(url, headers=HTTP_HEADERS, timeout=120)
            resp.raise_for_status()
            if not resp.content:
                raise ValueError("contenu vide")
            target.write_bytes(resp.content)
            downloaded += 1
        except Exception as exc:
            failed += 1
            logging.warning("échec %s: %s", name, exc)

    return {
        "selected": len(selected),
        "downloaded": downloaded,
        "skipped_existing": skipped_existing,
        "failed": failed,
        "output_dir": str(output_dir),
        "cutoff": f"{cutoff_year:04d}-{cutoff_month:02d}",
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Télécharge les fichiers INSEE décès (annuels + mensuels) jusqu'à un cutoff YYYY-MM."
    )
    parser.add_argument(
        "--output-dir",
        default="insee_month_rattrapage",
        help="Dossier de sortie (défaut: insee_month_rattrapage)",
    )
    parser.add_argument(
        "--cutoff",
        default="2026-04",
        help="Dernier mois inclus au format YYYY-MM (défaut: 2026-04)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Réécrit les fichiers existants",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s - %(message)s")
    result = download_backfill(
        output_dir=Path(args.output_dir),
        cutoff=args.cutoff,
        overwrite=args.overwrite,
    )
    logging.info("Résumé: %s", result)


if __name__ == "__main__":
    main()
