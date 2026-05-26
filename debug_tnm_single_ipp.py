from __future__ import annotations

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Optional

try:
    import fitz  # type: ignore
except ImportError:  # pragma: no cover
    fitz = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError:  # pragma: no cover
    PdfReader = None


LOGGER = logging.getLogger("tnm_debug")

TNM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyra]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyra]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyra]{0,4})?m(?:x|0|1[abc]?)?)?",
    re.IGNORECASE,
)
TNM_LOOSE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyra]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:[\s\S]{0,120}?)"
    r"((?:[cpyra]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:[\s\S]{0,80}?)"
    r"((?:[cpyra]{0,4})?m(?:x|0|1[abc]?))",
    re.IGNORECASE,
)
T_COMPONENT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyra]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
N_COMPONENT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyra]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
M_COMPONENT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyra]{0,4})?m(?:x|0|1[abc]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
EXPLICIT_STAGE_PATTERN = re.compile(r"\b(?:stade|stage)\s*(?:ajcc\s*)?(0|iv|iii[abc]?|ii[abc]?|i[abc]?|1|2|3|4)\b", re.IGNORECASE)
BRESLOW_PATTERN = re.compile(
    r"(?:\bbreslow(?:\s*(?:de|:|=))?\s*([0-9]+(?:[.,][0-9]+)?)\s*mm\b|"
    r"\b([0-9]+(?:[.,][0-9]+)?)\s*mm\s+d['’][eé]paisseur\s+selon\s+breslow\b)",
    re.IGNORECASE,
)
METASTASIS_PATTERN = re.compile(
    r"\b(metast|oligometast|secondaire[s]?\s+(hepatiq|osseu|pulmon|cerebr)|"
    r"atteinte\s+metastatique|maladie\s+metastatique)",
    re.IGNORECASE,
)
METASTASIS_NEGATION_PATTERN = re.compile(r"\b(pas\s+de|sans|absence\s+de|aucun(?:e)?|pas\s+d['e])\b", re.IGNORECASE)
METASTASIS_EXPLICIT_NEGATIVE_CONTEXT_PATTERN = re.compile(
    r"\b(?:absence\s+de|sans|pas\s+de|aucun(?:e)?)\b[\s\S]{0,60}\bmetast",
    re.IGNORECASE,
)
REGIONAL_NODAL_CONTEXT_PATTERN = re.compile(
    r"\b(ganglion(?:naire)?|ad[ée]nom[ée]galie|adenopathie|inguinal|axillaire|iliaque)\b",
    re.IGNORECASE,
)
NO_OTHER_SECONDARY_LOCATION_PATTERN = re.compile(
    r"\b(pas\s+d['’]autre\s+localisation\s+secondaire|pas\s+autre\s+localisation\s+secondaire|"
    r"aucune?\s+autre\s+localisation\s+secondaire|dedouane?\s+toute\s+localisation\s+secondaire|"
    r"d[eé]douanant\s+toute\s+localisation\s+secondaire|"
    r"pas\s+d['’]autre\s+localisation\s+a\s+distance)\b",
    re.IGNORECASE,
)
SECONDARY_LOCATION_NEGATED_PATTERN = re.compile(
    r"\b(?:aucun(?:e)?|sans|absence\s+de|pas\s+de|pas\s+d['’])\b[\s\S]{0,40}\blocalisation\s+secondaire(?:s)?\b|"
    r"\blocalisation\s+secondaire(?:s)?\b[\s\S]{0,40}\b(?:aucun(?:e)?|sans|absence\s+de|pas\s+de|pas\s+d['’])\b",
    re.IGNORECASE,
)
ANESTHESIA_DOC_PATTERN = re.compile(r"\bdossier\s+anesth[eé]sie\b", re.IGNORECASE)
DCIS_PATTERN = re.compile(r"\b(ccis|dcis|carcinome\s+canalaire\s+in\s+situ|carcinome\s+intracanalaire)\b", re.IGNORECASE)
IN_SITU_PATTERN = re.compile(r"\bin\s+situ\b", re.IGNORECASE)
NO_INVASION_PATTERN = re.compile(
    r"\b(absence\s+de\s+contingent\s+infiltrant|sans\s+contingent\s+infiltrant|"
    r"pas\s+de\s+contingent\s+infiltrant|absence\s+d['e]\s+invasion|"
    r"absence\s+d['e]\s+infiltration|non\s+infiltrant|non\s+invasif|"
    r"pas\s+de\s+composante\s+invasive|absence\s+de\s+composante\s+invasive|"
    r"absence\s+de\s+foyer\s+infiltrant|absence\s+de\s+carcinome\s+invasif|"
    r"absence\s+de\s+carcinome\s+infiltrant|pas\s+d['e]\s+argument\s+pour\s+une\s+infiltration|"
    r"pas\s+d['e]\s+argument\s+pour\s+une\s+invasion)\b",
    re.IGNORECASE,
)
INVASION_EXCLUSION_PATTERN = re.compile(
    r"\b(micro[\s-]?invasion|micro[\s-]?invasif|carcinome\s+infiltrant|carcinome\s+invasif|"
    r"composante\s+infiltrante|composante\s+invasive|foyer\s+invasif|foyer\s+infiltrant|"
    r"contingent\s+infiltrant|invasion\s+stromale)\b",
    re.IGNORECASE,
)
NODAL_NEGATIVE_PATTERN = re.compile(
    r"\b(absence\s+de\s+metastase\s+ganglionnaire|sans\s+metastase\s+ganglionnaire|"
    r"ganglion\s+sentinelle\s+negatif|pas\s+de\s+metastase\s+ganglionnaire|"
    r"aucune?\s+metastase\s+ganglionnaire|0\s*/\s*[1-9]\d*|"
    r"pas\s+mis\s+en\s+[eé]vidence\s+(?:d['’]\s*|de\s+)ad[ée]nom[ée]galie(?:s)?(?:\s+axillaire(?:s)?)?|"
    r"ganglion(?:naire)?s?[\s\S]{0,80}sans\s+[eé]l[eé]ment\s+suspect|"
    r"aires?\s+ganglionnaires?\s+axillaires?\s+vierges?)\b",
    re.IGNORECASE,
)
NODAL_POSITIVE_PATTERN = re.compile(
    r"\b(metastase\s+ganglionnaire|adenopathie[s]?\s+secondaire[s]?|envahissement\s+ganglionnaire|"
    r"atteinte\s+ganglionnaire)\b",
    re.IGNORECASE,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug TNM regex extraction for one IPP.")
    parser.add_argument("input_dir", help="Folder containing *.json.txt and matching *.pdf files.")
    parser.add_argument("--ipp", required=True, help="IPP to debug.")
    parser.add_argument("--context-window", type=int, default=100, help="Context chars around each match.")
    parser.add_argument("--show-text", action="store_true", help="Print full normalized text for each document.")
    parser.add_argument(
        "--only-stage-hits",
        action="store_true",
        default=True,
        help="Only print documents where TNM patterns matched and produced stage output (default behavior).",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(levelname)s | %(message)s")


def normalize_text(text: str) -> str:
    for bad, good in {"\u00a0": " ", "\u00ad": "", "\ufb01": "fi", "\ufb02": "fl", "\r": "\n"}.items():
        text = text.replace(bad, good)
    return re.sub(r"[ \t]+", " ", text)


def require_pdf_backend() -> None:
    if fitz is None and PdfReader is None:
        raise RuntimeError("No PDF backend found. Install 'pymupdf' or 'PyPDF2'.")


def load_metadata(metadata_path: Path) -> dict:
    raw_bytes = metadata_path.read_bytes()
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw_bytes.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = exc
    raise ValueError(f"Unable to decode metadata JSON file {metadata_path}. Last error: {last_error}")


def metadata_to_ipp(metadata: dict, metadata_path: Path) -> str:
    ipp = metadata.get("Patient", {}).get("IPP") or metadata.get("IPP") or metadata_path.name.split("_")[0]
    return str(ipp).strip()


def metadata_to_date(metadata: dict) -> str:
    for value in (
        metadata.get("Episode", {}).get("StartDate"),
        metadata.get("Document", {}).get("CreateDate"),
        metadata.get("Document", {}).get("UpdateDate"),
    ):
        if value:
            return str(value)[:8]
    return "null"


def metadata_to_pdf_path(metadata_path: Path) -> Path:
    suffix = ".json.txt"
    if metadata_path.name.lower().endswith(suffix):
        return metadata_path.with_name(metadata_path.name[: -len(suffix)] + ".pdf")
    return metadata_path.with_suffix(".pdf")


def detect_document_kind(metadata: dict, metadata_path: Path, pdf_path: Path) -> str:
    haystack = " ".join(
        [
            metadata_path.name,
            pdf_path.name,
            str(metadata.get("Document", {}).get("FileName", "")),
            str(metadata.get("Document", {}).get("PDFDocumentName", "")),
            str(metadata.get("Document", {}).get("TypeDescription", "")),
            str(metadata.get("Document", {}).get("FormatComDesc", "")),
            str(metadata.get("Document", {}).get("PrescriptionDesc", "")),
        ]
    ).lower()
    if "rcp" in haystack:
        return "rcp"
    if "anapath" in haystack or "path" in haystack or "anatomo" in haystack:
        return "pathology"
    if "consult" in haystack or "crcssur" in haystack:
        return "consultation"
    return "other"


def is_excluded_document(metadata: dict) -> bool:
    fields = [
        str(metadata.get("Document", {}).get("PrescriptionDesc", "")),
        str(metadata.get("Document", {}).get("TypeDescription", "")),
        str(metadata.get("Document", {}).get("FormatComDesc", "")),
    ]
    haystack = " ".join(fields)
    return bool(ANESTHESIA_DOC_PATTERN.search(haystack))


def detect_metastasis_signal(text: str) -> str:
    for match in METASTASIS_PATTERN.finditer(text):
        start = max(0, match.start() - 180)
        end = min(len(text), match.end() + 120)
        prefix = text[start:match.start()]
        around = text[start:end]
        if METASTASIS_NEGATION_PATTERN.search(prefix):
            continue
        if METASTASIS_EXPLICIT_NEGATIVE_CONTEXT_PATTERN.search(around):
            continue
        if SECONDARY_LOCATION_NEGATED_PATTERN.search(around):
            continue
        # Do not promote regional nodal disease to Stage IV when the report explicitly
        # states there is no other secondary location.
        if REGIONAL_NODAL_CONTEXT_PATTERN.search(around) and NO_OTHER_SECONDARY_LOCATION_PATTERN.search(text):
            continue
        return "yes"
    return "no"


def normalize_explicit_stage(token: str) -> str:
    token = token.strip().upper()
    token = {"1": "I", "2": "II", "3": "III", "4": "IV"}.get(token, token)
    return f"Stage {token}"


def extract_explicit_stage(text: str) -> Optional[str]:
    match = EXPLICIT_STAGE_PATTERN.search(text)
    if not match:
        return None
    return normalize_explicit_stage(match.group(1))


def infer_stage_zero_from_pathology(text: str, document_kind: str) -> Optional[str]:
    if document_kind != "pathology":
        return None
    strong_in_situ_signal = bool(DCIS_PATTERN.search(text))
    contextual_in_situ_signal = bool(IN_SITU_PATTERN.search(text) and NO_INVASION_PATTERN.search(text))
    if not (strong_in_situ_signal or contextual_in_situ_signal):
        return None
    if INVASION_EXCLUSION_PATTERN.search(text):
        return None
    return "Stage 0"


def extract_pdf_text(pdf_path: Path) -> str:
    if fitz is not None:
        chunks: list[str] = []
        with fitz.open(pdf_path) as document:
            for page in document:
                chunks.append(page.get_text("text"))
        return normalize_text("\n".join(chunks))
    if PdfReader is not None:
        chunks = []
        reader = PdfReader(str(pdf_path))
        for page in reader.pages:
            chunks.append(page.extract_text() or "")
        return normalize_text("\n".join(chunks))
    raise RuntimeError("No PDF backend available.")


def normalize_tnm_component(value: str, axis: str) -> str:
    value = (value or "").lower().strip().replace(" ", "")
    if not value:
        return ""
    idx = value.find(axis)
    return value[idx:] if idx >= 0 else value


def compute_stage(t_value: str, n_value: str, m_value: str) -> str:
    t = normalize_tnm_component(t_value, "t")
    n = normalize_tnm_component(n_value, "n")
    m = normalize_tnm_component(m_value, "m") or "mx"
    logic_n = "n0" if n == "nx" else n
    logic_m = "m0" if m == "mx" else m

    if logic_m.startswith("m1"):
        return "Stage IV"
    if t == "tis" and logic_n == "n0" and logic_m == "m0":
        return "Stage 0"
    if t in {"t1", "t1a", "t1b", "t1c", "t1mi"} and logic_n == "n0" and logic_m == "m0":
        return "Stage I"
    if ((t in {"t0", "t1", "t1a", "t1b", "t1c", "t1mi"} and logic_n in {"n1", "n1mi"}) or (t.startswith("t2") and logic_n == "n0")) and logic_m == "m0":
        return "Stage IIA"
    if ((t.startswith("t2") and logic_n == "n1") or (t.startswith("t3") and logic_n == "n0")) and logic_m == "m0":
        return "Stage IIB"
    if ((t.startswith(("t0", "t1", "t2")) and logic_n == "n2") or (t.startswith("t3") and logic_n in {"n1", "n1mi", "n2"})) and logic_m == "m0":
        return "Stage IIIA"
    if t.startswith("t4") and logic_n in {"n0", "n1", "n2"} and logic_m == "m0":
        return "Stage IIIB"
    if logic_n == "n3" and logic_m == "m0":
        return "Stage IIIC"
    return "null"


def context_around(text: str, start: int, end: int, window: int) -> str:
    s = max(0, start - window)
    e = min(len(text), end + window)
    return text[s:e].replace("\n", " ")


def log_matches(name: str, pattern: re.Pattern, text: str, window: int) -> list[re.Match]:
    matches = list(pattern.finditer(text))
    LOGGER.info("%s: %s match(es)", name, len(matches))
    for i, match in enumerate(matches, start=1):
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        LOGGER.info("  %s#%s raw='%s'", name, i, raw)
        LOGGER.info("  %s#%s context='%s'", name, i, context_around(text, match.start(), match.end(), window))
    return matches


def log_tnm_interpretation(name: str, matches: list[re.Match]) -> None:
    if not matches:
        return
    LOGGER.info("%s candidates and computed stages:", name)
    for match in matches:
        t = normalize_tnm_component(match.group(1) or "", "t")
        n = normalize_tnm_component(match.group(2) or "", "n")
        m = normalize_tnm_component(match.group(3) or "", "m") or "mx"
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        LOGGER.info("  raw='%s' -> t=%s n=%s m=%s stage=%s", raw, t, n, m, compute_stage(t, n, m))


def tnm_rows(name: str, matches: list[re.Match]) -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    t_token_pattern = re.compile(
        r"(?<![A-Za-z0-9])((?:[cpyra]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))(?![A-Za-z0-9])",
        re.IGNORECASE,
    )
    t_irm_pattern = re.compile(
        r"(?<![A-Za-z0-9])((?:[cpyra]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
        r"(?:[\s,;:()\\/-]{0,12})irm\b",
        re.IGNORECASE,
    )
    for match in matches:
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        t = normalize_tnm_component(match.group(1) or "", "t")
        n = normalize_tnm_component(match.group(2) or "", "n")
        m = normalize_tnm_component(match.group(3) or "", "m") or "mx"

        if name == "TNM_LOOSE_PATTERN":
            irm_t_candidates = [normalize_tnm_component(item.group(1), "t") for item in t_irm_pattern.finditer(raw)]
            if irm_t_candidates:
                t = irm_t_candidates[-1]
            else:
                all_t_candidates = [normalize_tnm_component(item.group(1), "t") for item in t_token_pattern.finditer(raw)]
                if all_t_candidates:
                    t = all_t_candidates[-1]

        rows.append((name, raw, t, n, m))
    return rows


def parse_breslow_mm(raw_value: str) -> Optional[float]:
    token = (raw_value or "").strip().replace(",", ".")
    if not token:
        return None
    if "." not in token and token.startswith("0") and len(token) > 1:
        # Common OCR/forms style: "06 mm" -> 0.6 mm, "025 mm" -> 0.25 mm
        return int(token) / (10 ** (len(token) - 1))
    try:
        return float(token)
    except ValueError:
        return None


def extract_breslow_raw_value(match: re.Match) -> Optional[str]:
    return match.group(1) or match.group(2)


def breslow_t_category(mm: float) -> str:
    if mm <= 1.0:
        return "t1"
    if mm <= 2.0:
        return "t2"
    if mm <= 4.0:
        return "t3"
    return "t4"


def infer_n_from_nodal_context(text: str) -> str:
    has_negative = bool(NODAL_NEGATIVE_PATTERN.search(text))
    has_positive = bool(NODAL_POSITIVE_PATTERN.search(text))
    if has_negative and not has_positive:
        return "n0"
    return "nx"


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    require_pdf_backend()

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        LOGGER.error("Input directory does not exist: %s", input_dir)
        return 1

    metadata_files = sorted(input_dir.glob("*.json.txt"))
    if not metadata_files:
        LOGGER.error("No metadata files found in: %s", input_dir)
        return 1

    target_ipp = str(args.ipp).strip()
    selected: list[tuple[Path, dict, Path]] = []
    for metadata_path in metadata_files:
        metadata = load_metadata(metadata_path)
        ipp = metadata_to_ipp(metadata, metadata_path)
        if ipp == target_ipp and not is_excluded_document(metadata):
            selected.append((metadata_path, metadata, metadata_to_pdf_path(metadata_path)))

    if not selected:
        LOGGER.warning("No files found for IPP=%s", target_ipp)
        return 0

    selected.sort(key=lambda row: metadata_to_date(row[1]))
    LOGGER.info("IPP=%s | documents found=%s", target_ipp, len(selected))

    for idx, (metadata_path, metadata, pdf_path) in enumerate(selected, start=1):
        if not pdf_path.exists():
            if not args.only_stage_hits:
                LOGGER.warning("PDF missing: %s", pdf_path)
            continue

        try:
            text = extract_pdf_text(pdf_path)
        except Exception as exc:  # pragma: no cover
            if not args.only_stage_hits:
                LOGGER.exception("PDF extraction failed for %s: %s", pdf_path, exc)
            continue

        if args.show_text:
            LOGGER.info("FULL TEXT START\n%s\nFULL TEXT END", text)

        document_kind = detect_document_kind(metadata, metadata_path, pdf_path)
        metastasis_detected = detect_metastasis_signal(text)
        explicit_stage = extract_explicit_stage(text)
        stage_zero = infer_stage_zero_from_pathology(text, document_kind)

        if document_kind in {"rcp", "consultation"} and metastasis_detected == "yes":
            LOGGER.info("------------------------------------------------------------")
            LOGGER.info(
                "Document %s/%s | PDF=%s | date=%s | mode=metastatic_first | kind=%s",
                idx,
                len(selected),
                pdf_path.name,
                metadata_to_date(metadata),
                document_kind,
            )
            LOGGER.info("match=METASTASIS_PATTERN | stage=Stage IV")
            return 0

        if explicit_stage is not None:
            LOGGER.info("------------------------------------------------------------")
            LOGGER.info(
                "Document %s/%s | PDF=%s | date=%s | mode=explicit_stage | kind=%s",
                idx,
                len(selected),
                pdf_path.name,
                metadata_to_date(metadata),
                document_kind,
            )
            LOGGER.info("match=EXPLICIT_STAGE_PATTERN | stage=%s", explicit_stage)
            return 0

        if stage_zero is not None:
            LOGGER.info("------------------------------------------------------------")
            LOGGER.info(
                "Document %s/%s | PDF=%s | date=%s | mode=pathology_stage_zero | kind=%s",
                idx,
                len(selected),
                pdf_path.name,
                metadata_to_date(metadata),
                document_kind,
            )
            LOGGER.info("match=DCIS/IN_SITU_RULE | stage=%s", stage_zero)
            return 0

        tnm_matches = list(TNM_PATTERN.finditer(text))
        if tnm_matches:
            hit_rows = tnm_rows("TNM_PATTERN", tnm_matches)
            chosen_mode = "strict"
        else:
            tnm_loose_matches = list(TNM_LOOSE_PATTERN.finditer(text))
            hit_rows = tnm_rows("TNM_LOOSE_PATTERN", tnm_loose_matches)
            chosen_mode = "loose_fallback"

        if not hit_rows:
            breslow_matches = list(BRESLOW_PATTERN.finditer(text))
            if breslow_matches:
                LOGGER.info("------------------------------------------------------------")
                LOGGER.info(
                    "Document %s/%s | PDF=%s | date=%s | mode=breslow_fallback | kind=%s",
                    idx,
                    len(selected),
                    pdf_path.name,
                    metadata_to_date(metadata),
                    document_kind,
                )
                for match in breslow_matches:
                    raw = re.sub(r"\s+", " ", match.group(0)).strip()
                    raw_value = extract_breslow_raw_value(match)
                    if raw_value is None:
                        continue
                    mm = parse_breslow_mm(raw_value)
                    if mm is None:
                        LOGGER.info("match=BRESLOW_PATTERN | raw='%s' | parse_error", raw)
                        continue
                    t = breslow_t_category(mm)
                    n = infer_n_from_nodal_context(text)
                    stage = compute_stage(t, n, "m0")
                    LOGGER.info("match=BRESLOW_PATTERN | raw='%s' | breslow_mm=%.3f | T=%s N=%s M=m0 | stage=%s", raw, mm, t, n, stage)
                return 0
            if not args.only_stage_hits:
                LOGGER.info("No TNM/Breslow hit | PDF=%s", pdf_path.name)
            continue

        LOGGER.info("------------------------------------------------------------")
        LOGGER.info(
            "Document %s/%s | PDF=%s | date=%s | mode=%s | kind=%s",
            idx,
            len(selected),
            pdf_path.name,
            metadata_to_date(metadata),
            chosen_mode,
            document_kind,
        )
        for pattern_name, raw, t, n, m in hit_rows:
            stage = compute_stage(t, n, m)
            LOGGER.info("match=%s | raw='%s' | T=%s N=%s M=%s | stage=%s", pattern_name, raw, t, n, m, stage)
        return 0

        if not args.only_stage_hits:
            log_matches("EXPLICIT_STAGE_PATTERN", EXPLICIT_STAGE_PATTERN, text, args.context_window)
            log_matches("T_COMPONENT_PATTERN", T_COMPONENT_PATTERN, text, args.context_window)
            log_matches("N_COMPONENT_PATTERN", N_COMPONENT_PATTERN, text, args.context_window)
            log_matches("M_COMPONENT_PATTERN", M_COMPONENT_PATTERN, text, args.context_window)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
