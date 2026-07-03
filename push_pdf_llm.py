#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass
class Candidate:
    json_path: Path
    pdf_path: Path
    ipp: Optional[str]
    json_ok: bool
    pdf_exists: bool
    eligible: bool
    reason: str
    uploaded: bool = False
    upload_error: Optional[str] = None


def setup_logger(verbose: bool) -> logging.Logger:
    logger = logging.getLogger("push_pdf_llm")
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)

    return logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filtre les couples *.json.txt + *.pdf selon une liste d'IPP "
            "puis les envoie sur un serveur distant via SFTP."
        )
    )
    parser.add_argument(
        "--local-dir",
        default="/opt/PDF",
        help="Repertoire local contenant les fichiers *.json.txt et *.pdf",
    )
    parser.add_argument(
        "--remote-host",
        default="10.210.22.130",
        help="Hote SSH distant",
    )
    parser.add_argument(
        "--remote-port",
        type=int,
        default=22,
        help="Port SSH distant",
    )
    parser.add_argument(
        "--remote-user",
        default="administrateur",
        help="Utilisateur SSH distant",
    )
    parser.add_argument(
        "--remote-dir",
        default="/home/administrateur/pdf_llm",
        help="Repertoire cible sur le serveur distant",
    )
    parser.add_argument(
        "--stage-dir",
        help="Repertoire local de staging sur la machine courante",
    )
    parser.add_argument(
        "--link-mode",
        choices=["symlink", "hardlink", "copy"],
        default="symlink",
        help="Mode de materialisation dans --stage-dir",
    )
    parser.add_argument(
        "--clean-stage-dir",
        action="store_true",
        help="Vide --stage-dir avant de recreer les liens/fichiers",
    )
    parser.add_argument(
        "--airflow-password-key",
        default="password_clidatadsin",
        help="Cle Airflow Variable contenant le mot de passe SSH de destination",
    )
    parser.add_argument(
        "--remote-password",
        help="Mot de passe SSH de destination. Prioritaire sur les autres sources.",
    )
    parser.add_argument(
        "--remote-password-env",
        default="REMOTE_TARGET_PASSWORD",
        help="Nom de la variable d'environnement contenant le mot de passe SSH de destination",
    )
    parser.add_argument(
        "--ipp-file",
        help=(
            "Fichier contenant les IPP a cibler. "
            "Formats acceptes: JSON array, JSON object avec ipp_list, ou texte separe par virgules/retours ligne."
        ),
    )
    parser.add_argument(
        "--ipp-json",
        help="Chaine JSON contenant la liste des IPP, par ex. '[\"202303219\",\"202303264\"]'",
    )
    parser.add_argument(
        "--ipp",
        action="append",
        default=[],
        help="IPP individuel. Option repetable.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Affiche les couples trouves sans upload",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche ce qui serait envoye sans executer l'upload",
    )
    parser.add_argument(
        "--report-path",
        help="Chemin d'un fichier JSON de rapport",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=200,
        help="Frequence des logs de progression pendant l'upload",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Active les logs DEBUG",
    )
    return parser.parse_args()


def normalize_ipps(values: Iterable[object]) -> set[str]:
    normalized: set[str] = set()
    for value in values:
        if value is None:
            continue
        ipp = str(value).strip()
        if ipp:
            normalized.add(ipp)
    return normalized


def parse_ipp_payload(raw: str) -> set[str]:
    raw = raw.strip()
    if not raw:
        return set()

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        tokens = [
            token.strip()
            for token in raw.replace(";", ",").replace("\n", ",").split(",")
        ]
        return normalize_ipps(tokens)

    if isinstance(payload, list):
        return normalize_ipps(payload)

    if isinstance(payload, dict):
        if "ipp_list" in payload and isinstance(payload["ipp_list"], list):
            return normalize_ipps(payload["ipp_list"])
        if "ipps" in payload and isinstance(payload["ipps"], list):
            return normalize_ipps(payload["ipps"])

    raise ValueError("Format IPP non supporte")


def load_target_ipps(args: argparse.Namespace) -> set[str]:
    target_ipps: set[str] = set()

    if args.ipp_json:
        target_ipps.update(parse_ipp_payload(args.ipp_json))

    if args.ipp_file:
        raw = Path(args.ipp_file).read_text(encoding="utf-8", errors="replace")
        target_ipps.update(parse_ipp_payload(raw))

    if args.ipp:
        target_ipps.update(normalize_ipps(args.ipp))

    if not target_ipps:
        raise SystemExit(
            "Aucun IPP fourni. Utilisez --ipp-file, --ipp-json ou --ipp."
        )

    return target_ipps


def parse_ipp_from_json(json_txt_path: Path) -> tuple[Optional[str], bool, str]:
    try:
        raw = json_txt_path.read_text(encoding="utf-8", errors="replace").strip()
        if not raw:
            return None, False, "JSON vide"
        payload = json.loads(raw)
        ipp = (payload.get("Patient") or {}).get("IPP")
        if ipp is None:
            return None, True, "IPP absent"
        return str(ipp).strip(), True, "OK"
    except json.JSONDecodeError as exc:
        return None, False, f"JSON invalide: {exc}"
    except Exception as exc:
        return None, False, f"Erreur lecture JSON: {exc}"


def associated_pdf(json_txt_path: Path) -> Path:
    name = str(json_txt_path)
    if name.endswith(".json.txt"):
        return Path(name[: -len(".json.txt")] + ".pdf")
    return json_txt_path.with_suffix(".pdf")


def collect_candidates(local_dir: Path, target_ipps: set[str]) -> tuple[list[Candidate], list[Candidate]]:
    candidates: list[Candidate] = []
    eligible: list[Candidate] = []

    for json_file in sorted(local_dir.glob("*.json.txt")):
        ipp, json_ok, message = parse_ipp_from_json(json_file)
        pdf_file = associated_pdf(json_file)
        pdf_exists = pdf_file.exists()

        if not json_ok:
            candidates.append(
                Candidate(
                    json_path=json_file,
                    pdf_path=pdf_file,
                    ipp=ipp,
                    json_ok=json_ok,
                    pdf_exists=pdf_exists,
                    eligible=False,
                    reason=message,
                )
            )
            continue

        if not ipp:
            candidates.append(
                Candidate(
                    json_path=json_file,
                    pdf_path=pdf_file,
                    ipp=ipp,
                    json_ok=json_ok,
                    pdf_exists=pdf_exists,
                    eligible=False,
                    reason="IPP manquant",
                )
            )
            continue

        if ipp not in target_ipps:
            candidates.append(
                Candidate(
                    json_path=json_file,
                    pdf_path=pdf_file,
                    ipp=ipp,
                    json_ok=json_ok,
                    pdf_exists=pdf_exists,
                    eligible=False,
                    reason="IPP non cible",
                )
            )
            continue

        if not pdf_exists:
            candidates.append(
                Candidate(
                    json_path=json_file,
                    pdf_path=pdf_file,
                    ipp=ipp,
                    json_ok=json_ok,
                    pdf_exists=pdf_exists,
                    eligible=False,
                    reason="PDF associe introuvable",
                )
            )
            continue

        candidate = Candidate(
            json_path=json_file,
            pdf_path=pdf_file,
            ipp=ipp,
            json_ok=json_ok,
            pdf_exists=pdf_exists,
            eligible=True,
            reason="OK",
        )
        candidates.append(candidate)
        eligible.append(candidate)

    return candidates, eligible


def clear_directory(target_dir: Path) -> None:
    for child in target_dir.iterdir():
        if child.is_symlink() or child.is_file():
            child.unlink()
        elif child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink(missing_ok=True)


def stage_candidates(
    candidates: list[Candidate],
    stage_dir: Path,
    link_mode: str,
    clean_stage_dir: bool,
    progress_every: int,
    logger: logging.Logger,
) -> tuple[int, int]:
    stage_dir.mkdir(parents=True, exist_ok=True)
    if clean_stage_dir:
        clear_directory(stage_dir)

    ok = 0
    ko = 0

    for index, candidate in enumerate(candidates, start=1):
        try:
            if link_mode == "symlink":
                os.symlink(candidate.json_path, stage_dir / candidate.json_path.name)
                os.symlink(candidate.pdf_path, stage_dir / candidate.pdf_path.name)
            elif link_mode == "hardlink":
                os.link(candidate.json_path, stage_dir / candidate.json_path.name)
                os.link(candidate.pdf_path, stage_dir / candidate.pdf_path.name)
            else:
                shutil.copy2(candidate.json_path, stage_dir / candidate.json_path.name)
                shutil.copy2(candidate.pdf_path, stage_dir / candidate.pdf_path.name)

            candidate.uploaded = True
            ok += 1
            should_log_progress = (
                index == 1
                or index == len(candidates)
                or (progress_every > 0 and index % progress_every == 0)
            )
            if should_log_progress:
                logger.info(
                    "Progress staging %d/%d - OK:%d KO:%d - dernier IPP=%s",
                    index,
                    len(candidates),
                    ok,
                    ko,
                    candidate.ipp,
                )
        except Exception as exc:
            candidate.upload_error = str(exc)
            ko += 1
            logger.error("Echec staging %s : %s", candidate.json_path.name, exc)

    return ok, ko


def sftp_mkdir_p(sftp, remote_dir: str) -> None:
    remote_dir = remote_dir.rstrip("/")
    if not remote_dir:
        return

    parts = [part for part in remote_dir.split("/") if part]
    current = "/" if remote_dir.startswith("/") else ""

    for part in parts:
        if current in ("", "/"):
            current = f"{current}{part}" if current == "/" else part
        else:
            current = f"{current}/{part}"

        try:
            sftp.stat(current)
        except FileNotFoundError:
            sftp.mkdir(current)


def upload_candidates(
    candidates: list[Candidate],
    remote_host: str,
    remote_port: int,
    remote_user: str,
    remote_dir: str,
    remote_password: str,
    progress_every: int,
    logger: logging.Logger,
) -> tuple[int, int]:
    import paramiko

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=remote_host,
        port=remote_port,
        username=remote_user,
        password=remote_password,
        timeout=30,
        allow_agent=False,
        look_for_keys=False,
    )

    ok = 0
    ko = 0
    try:
        sftp = client.open_sftp()
        try:
            sftp_mkdir_p(sftp, remote_dir)

            for index, candidate in enumerate(candidates, start=1):
                try:
                    remote_json = f"{remote_dir.rstrip('/')}/{candidate.json_path.name}"
                    remote_pdf = f"{remote_dir.rstrip('/')}/{candidate.pdf_path.name}"

                    sftp.put(str(candidate.json_path), remote_json)
                    sftp.put(str(candidate.pdf_path), remote_pdf)
                    candidate.uploaded = True
                    ok += 1
                    should_log_progress = (
                        index == 1
                        or index == len(candidates)
                        or (progress_every > 0 and index % progress_every == 0)
                    )
                    if should_log_progress:
                        logger.info(
                            "Progress upload %d/%d - OK:%d KO:%d - dernier IPP=%s",
                            index,
                            len(candidates),
                            ok,
                            ko,
                            candidate.ipp,
                        )
                except Exception as exc:
                    candidate.upload_error = str(exc)
                    ko += 1
                    logger.error("Echec upload %s : %s", candidate.json_path.name, exc)
        finally:
            sftp.close()
    finally:
        client.close()

    return ok, ko


def resolve_remote_password(args: argparse.Namespace) -> str:
    if args.remote_password:
        return args.remote_password

    if args.remote_password_env:
        env_value = os.environ.get(args.remote_password_env)
        if env_value:
            return env_value

    if args.airflow_password_key:
        try:
            from airflow.models import Variable
        except Exception as exc:
            raise SystemExit(
                "Impossible de lire la variable Airflow de mot de passe. "
                "Passez --remote-password ou exportez "
                f"{args.remote_password_env}. Détail: {exc}"
            ) from exc

        try:
            return Variable.get(args.airflow_password_key)
        except Exception as exc:
            raise SystemExit(
                f"Lecture de la Variable Airflow impossible pour la cle "
                f"{args.airflow_password_key}: {exc}"
            ) from exc

    raise SystemExit(
        "Aucun mot de passe de destination disponible. "
        "Utilisez --remote-password, --remote-password-env ou --airflow-password-key."
    )


def write_report(report_path: Path, args: argparse.Namespace, candidates: list[Candidate]) -> None:
    report = {
        "local_dir": args.local_dir,
        "remote_host": args.remote_host,
        "remote_port": args.remote_port,
        "remote_user": args.remote_user,
        "remote_dir": args.remote_dir,
        "airflow_password_key": args.airflow_password_key,
        "candidates": [
            {
                **asdict(candidate),
                "json_path": str(candidate.json_path),
                "pdf_path": str(candidate.pdf_path),
            }
            for candidate in candidates
        ],
    }
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    logger = setup_logger(args.verbose)

    local_dir = Path(args.local_dir)
    if not local_dir.exists() or not local_dir.is_dir():
        raise SystemExit(f"Repertoire local introuvable: {local_dir}")

    target_ipps = load_target_ipps(args)
    logger.info("IPP cibles recus : %d", len(target_ipps))
    logger.info("Scan du dossier : %s", local_dir)

    candidates, eligible = collect_candidates(local_dir, target_ipps)
    logger.info("Fichiers JSON detectes : %d", len(candidates))
    logger.info("Paires eligibles : %d", len(eligible))

    if args.list_only:
        for candidate in candidates:
            logger.info(
                "- %s -> %s | ipp=%s | pdf_exists=%s | eligible=%s | reason=%s",
                candidate.json_path.name,
                candidate.pdf_path.name,
                candidate.ipp,
                candidate.pdf_exists,
                candidate.eligible,
                candidate.reason,
            )
        if args.report_path:
            write_report(Path(args.report_path), args, candidates)
        return 0

    if args.dry_run:
        for candidate in eligible:
            logger.info(
                "[DRY-RUN] IPP=%s : %s + %s",
                candidate.ipp,
                candidate.json_path.name,
                candidate.pdf_path.name,
            )
        if args.report_path:
            write_report(Path(args.report_path), args, candidates)
        return 0

    if not eligible:
        logger.warning("Aucun fichier a envoyer.")
        if args.report_path:
            write_report(Path(args.report_path), args, candidates)
        return 0

    if args.stage_dir:
        ok, ko = stage_candidates(
            candidates=eligible,
            stage_dir=Path(args.stage_dir),
            link_mode=args.link_mode,
            clean_stage_dir=args.clean_stage_dir,
            progress_every=args.progress_every,
            logger=logger,
        )
        logger.info("Staging termine - OK:%d KO:%d", ok, ko)
    else:
        remote_password = resolve_remote_password(args)

        ok, ko = upload_candidates(
            candidates=eligible,
            remote_host=args.remote_host,
            remote_port=args.remote_port,
            remote_user=args.remote_user,
            remote_dir=args.remote_dir,
            remote_password=remote_password,
            progress_every=args.progress_every,
            logger=logger,
        )

        logger.info("Upload termine - OK:%d KO:%d", ok, ko)

    if args.report_path:
        write_report(Path(args.report_path), args, candidates)

    if ko:
        raise SystemExit(1)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
