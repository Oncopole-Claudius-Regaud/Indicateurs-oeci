from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List distinct metadata labels from JSON sidecars."
    )
    parser.add_argument(
        "input_dir",
        nargs="?",
        default="/PDF",
        help="Directory containing *.json.txt metadata files (default: /PDF)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="If > 0, show only top N values per field",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict | None:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw.decode(enc))
        except Exception:
            continue
    return None


def norm(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def print_counter(title: str, counter: Counter[str], top: int) -> None:
    print(f"\n=== {title} ===")
    if not counter:
        print("(no values found)")
        return
    items = counter.most_common(top) if top and top > 0 else sorted(
        counter.items(), key=lambda kv: (-kv[1], kv[0].lower())
    )
    for value, count in items:
        label = value if value else "<EMPTY>"
        print(f"{count:8d} | {label}")
    print(f"Total distinct: {len(counter)}")


def main() -> int:
    args = parse_args()
    base = Path(args.input_dir)
    if not base.exists():
        print(f"Input directory not found: {base}")
        return 1

    files = sorted(base.rglob("*.json.txt"))
    if not files:
        print(f"No *.json.txt files found under: {base}")
        return 1

    type_desc = Counter()
    format_desc = Counter()
    prescription_desc = Counter()
    bad_files = 0

    for path in files:
        payload = load_json(path)
        if payload is None:
            bad_files += 1
            continue
        doc = payload.get("Document", {}) if isinstance(payload, dict) else {}
        type_desc[norm(doc.get("TypeDescription"))] += 1
        format_desc[norm(doc.get("FormatComDesc"))] += 1
        prescription_desc[norm(doc.get("PrescriptionDesc"))] += 1

    print(f"Scanned files: {len(files)}")
    print(f"Unreadable JSON: {bad_files}")
    print_counter("TypeDescription", type_desc, args.top)
    print_counter("FormatComDesc", format_desc, args.top)
    print_counter("PrescriptionDesc", prescription_desc, args.top)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

