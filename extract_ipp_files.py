from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract all files for one IPP into a target folder.")
    parser.add_argument("--source-dir", default="/opt/PDF", help="Folder containing *.json.txt and *.pdf files.")
    parser.add_argument("--target-dir", default="/opt/pdf_test", help="Destination folder.")
    parser.add_argument("--ipp", required=True, help="IPP to extract.")
    parser.add_argument("--clean-target", action="store_true", help="Delete existing files in target dir before copy.")
    parser.add_argument(
        "--date-basis",
        choices=["established", "validation"],
        default="established",
        help=(
            "Date field used for filtering: established=Episode.StartDate; "
            "validation=Document.CreateDate/UpdateDate."
        ),
    )
    parser.add_argument(
        "--min-established-date",
        default="2019-01-01",
        help="Minimum established date (YYYY-MM-DD) when --date-basis=established.",
    )
    parser.add_argument(
        "--min-validation-date",
        default="2020-01-01",
        help="Minimum validation date (YYYY-MM-DD) when --date-basis=validation.",
    )
    return parser.parse_args()


def load_metadata(metadata_path: Path) -> dict:
    raw_bytes = metadata_path.read_bytes()
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw_bytes.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = exc
    raise ValueError(f"Unable to decode {metadata_path}: {last_error}")


def metadata_to_ipp(metadata: dict, metadata_path: Path) -> str:
    ipp = metadata.get("Patient", {}).get("IPP") or metadata.get("IPP") or metadata_path.name.split("_")[0]
    return str(ipp).strip()


def metadata_to_pdf_path(metadata_path: Path) -> Path:
    suffix = ".json.txt"
    if metadata_path.name.lower().endswith(suffix):
        return metadata_path.with_name(metadata_path.name[: -len(suffix)] + ".pdf")
    return metadata_path.with_suffix(".pdf")


def parse_yyyymmdd(raw: object) -> Optional[datetime]:
    token = str(raw or "").strip()
    if not token:
        return None
    token = token[:8]
    if len(token) != 8 or not token.isdigit():
        return None
    try:
        return datetime.strptime(token, "%Y%m%d")
    except ValueError:
        return None


def metadata_established_date(metadata: dict) -> Optional[datetime]:
    return parse_yyyymmdd(metadata.get("Episode", {}).get("StartDate"))


def metadata_validation_date(metadata: dict) -> Optional[datetime]:
    create_date = parse_yyyymmdd(metadata.get("Document", {}).get("CreateDate"))
    update_date = parse_yyyymmdd(metadata.get("Document", {}).get("UpdateDate"))
    if create_date and update_date:
        return create_date if create_date <= update_date else update_date
    return create_date or update_date


def clean_dir(path: Path) -> None:
    for child in path.iterdir():
        if child.is_file():
            child.unlink()


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    target_dir = Path(args.target_dir)
    target_ipp = str(args.ipp).strip()

    if not source_dir.exists():
        print(f"ERROR: source dir does not exist: {source_dir}")
        return 1

    target_dir.mkdir(parents=True, exist_ok=True)
    if args.clean_target:
        clean_dir(target_dir)

    min_established = datetime.strptime(args.min_established_date, "%Y-%m-%d")
    min_validation = datetime.strptime(args.min_validation_date, "%Y-%m-%d")

    metadata_files = sorted(source_dir.glob("*.json.txt"))
    copied_json = 0
    copied_pdf = 0
    missing_pdf = 0
    decode_errors = 0
    filtered_by_date = 0

    for metadata_path in metadata_files:
        try:
            metadata = load_metadata(metadata_path)
        except Exception:
            decode_errors += 1
            continue

        ipp = metadata_to_ipp(metadata, metadata_path)
        if ipp != target_ipp:
            continue

        if args.date_basis == "established":
            doc_date = metadata_established_date(metadata)
            if doc_date is None or doc_date < min_established:
                filtered_by_date += 1
                continue
        else:
            doc_date = metadata_validation_date(metadata)
            if doc_date is None or doc_date < min_validation:
                filtered_by_date += 1
                continue

        dest_json = target_dir / metadata_path.name
        shutil.copy2(metadata_path, dest_json)
        copied_json += 1

        pdf_path = metadata_to_pdf_path(metadata_path)
        if pdf_path.exists():
            dest_pdf = target_dir / pdf_path.name
            shutil.copy2(pdf_path, dest_pdf)
            copied_pdf += 1
        else:
            missing_pdf += 1

    print(f"IPP={target_ipp}")
    print(f"source={source_dir}")
    print(f"target={target_dir}")
    print(f"copied_json={copied_json}")
    print(f"copied_pdf={copied_pdf}")
    print(f"missing_pdf={missing_pdf}")
    print(f"decode_errors={decode_errors}")
    print(f"filtered_by_date={filtered_by_date}")
    print(f"date_basis={args.date_basis}")
    print(f"min_established_date={args.min_established_date}")
    print(f"min_validation_date={args.min_validation_date}")

    if copied_json == 0:
        print("WARNING: no metadata matched this IPP.")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
