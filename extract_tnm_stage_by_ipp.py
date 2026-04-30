from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

try:
    import fitz  # type: ignore
except ImportError:  # pragma: no cover
    fitz = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError:  # pragma: no cover
    PdfReader = None


LOGGER = logging.getLogger("tnm_stage")
NULL_VALUE = "null"

TNM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyra]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyra]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyra]{0,4})?m(?:x|0|1[abc]?)?)?",
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
    r"(?<![A-Za-z0-9])((?:[cpyra]{0,4})?m(?:x|0|1[abc]?)?)(?![A-Za-z0-9])",
    re.IGNORECASE,
)
TREATMENT_PATTERN = re.compile(
    r"\b(chirurg(?:ie|ical|ien|e|ee|ees)|mastectom|tumorectom|lumpectom|curage|"
    r"chimiotherap|radiochimiotherap|radiotherap|curieth|curietherap|hormonotherap|"
    r"immunotherap|traitement|post[\s-]?operatoire|post[\s-]?op|apres\s+chirurg|"
    r"a\s+recu|a\s+beneficie|en\s+cours\s+de\s+traitement|sous\s+chimiotherap|"
    r"sous\s+radiotherap)\b",
    re.IGNORECASE,
)
SURGERY_PATTERN = re.compile(
    r"\b(chirurg(?:ie|ical|ien|e|ee|ees)|mastectom|tumorectom|lumpectom|curage|"
    r"oper(?:ation|e|ee|ees)?|intervention|exerese)\b",
    re.IGNORECASE,
)
CHEMO_PATTERN = re.compile(r"\b(chimiotherap|neoadjuv|adjuv)\b", re.IGNORECASE)
RADIOTHERAPY_PATTERN = re.compile(r"\b(radiotherap|radiochimiotherap|irradiat|curieth|curietherap)\b", re.IGNORECASE)
METASTASIS_PATTERN = re.compile(
    r"\b(metast|oligometast|secondaire[s]?\s+(hepatiq|osseu|pulmon|cerebr|ganglion)|"
    r"localisation[s]?\s+secondaire[s]?|atteinte\s+metastatique|maladie\s+metastatique)",
    re.IGNORECASE,
)
METASTASIS_NEGATION_PATTERN = re.compile(r"\b(pas\s+de|sans|absence\s+de|aucun(?:e)?|pas\s+d['e])\b", re.IGNORECASE)
EXPLICIT_STAGE_PATTERN = re.compile(r"\b(?:stade|stage)\s*(?:ajcc\s*)?(0|iv|iii[abc]?|ii[abc]?|i[abc]?|1|2|3|4)\b", re.IGNORECASE)
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
GLEASON_PATTERN = re.compile(
    r"\bgleason\s*(?:score\s*)?(\d{1,2})(?:\s*\(\s*([345])\s*\+\s*([345])\s*\))?",
    re.IGNORECASE,
)


@dataclass
class IppMetadata:
    ipp: str
    organe: str
    code_cim: str


@dataclass
class TnmCandidate:
    raw: str
    t: str
    n: str
    m: str
    stage: str
    context: str


@dataclass
class DocumentResult:
    ipp: str
    metadata_file: str
    pdf_file: str
    document_date: str
    visit_number: str
    text_length: int
    tnm_raw: str
    t: str
    n: str
    m: str
    stage: str
    status: str
    reason: str
    all_tnm_matches: str
    document_kind: str
    tnm_context: str
    treatment_detected: str
    treatment_keywords: str
    surgery_detected: str
    chemo_detected: str
    radiotherapy_detected: str
    metastasis_detected: str


@dataclass
class IppResult:
    ipp: str
    stage: str
    tnm_raw: str
    t: str
    n: str
    m: str
    document_date: str
    source_pdf: str
    status: str
    reason: str
    selection_reason: str
    document_kind: str
    tnm_context: str
    treatment_detected: str
    treatment_keywords: str
    surgery_detected: str
    chemo_detected: str
    radiotherapy_detected: str
    metastasis_detected: str
    documents_seen: int
    documents_with_stage: int
    last_update: str


@dataclass
class MetadataIndex:
    ipp: str
    metadata_file: Path
    document_date: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract TNM and derive generic stage per distinct IPP.")
    parser.add_argument("input_dir", help="Folder containing *.pdf and *.json.txt files.")
    parser.add_argument("--output-dir", default=None, help="Output folder. Defaults to the input folder.")
    parser.add_argument("--ipp-metadata-file", default=None, help="JSON file with ipp/organe/code_cim metadata.")
    parser.add_argument("--ipp-strategy", choices=["baseline", "highest", "latest"], default="baseline")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--csv-name", default="ipp_stage_results.csv")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(levelname)s | %(message)s")


def require_pdf_backend() -> None:
    if fitz is None and PdfReader is None:
        raise RuntimeError("No PDF backend found. Install 'pymupdf' or 'PyPDF2' before running this script.")


def normalize_text(text: str) -> str:
    for bad, good in {"\u00a0": " ", "\u00ad": "", "\ufb01": "fi", "\ufb02": "fl", "\r": "\n"}.items():
        text = text.replace(bad, good)
    return re.sub(r"[ \t]+", " ", text)


def load_ipp_metadata_map(path: Optional[str]) -> dict[str, IppMetadata]:
    if not path:
        return {}
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    records = raw.get("ipp_records", raw if isinstance(raw, list) else [])
    mapping: dict[str, IppMetadata] = {}
    for row in records:
        ipp = str(row.get("ipp", "")).strip()
        if not ipp:
            continue
        mapping[ipp] = IppMetadata(
            ipp=ipp,
            organe=str(row.get("organe") or "").strip(),
            code_cim=str(row.get("code_cim") or "").strip(),
        )
    return mapping


def normalize_tnm_component(value: str, axis: str) -> str:
    value = (value or "").lower().strip().replace(" ", "")
    if not value:
        return ""
    index = value.find(axis)
    return value[index:] if index >= 0 else value


def t_group(t_value: str) -> str:
    t_value = normalize_tnm_component(t_value, "t")
    if t_value == "tis":
        return "tis"
    if t_value in {"t1", "t1mi", "t1a", "t1b", "t1c"}:
        return "t1"
    if t_value.startswith("t2"):
        return "t2"
    if t_value.startswith("t3"):
        return "t3"
    if t_value.startswith("t4"):
        return "t4"
    if t_value == "t0":
        return "t0"
    return t_value


def n_group(n_value: str) -> str:
    n_value = normalize_tnm_component(n_value, "n")
    if n_value == "n0":
        return "n0"
    if n_value == "n1mi":
        return "n1mi"
    if n_value.startswith("n1"):
        return "n1"
    if n_value.startswith("n2"):
        return "n2"
    if n_value.startswith("n3"):
        return "n3"
    return n_value


def m_group(m_value: str) -> str:
    m_value = normalize_tnm_component(m_value, "m")
    if not m_value:
        return "mx"
    if m_value == "mx":
        return "mx"
    if m_value.startswith("m1"):
        return "m1"
    if m_value == "m0":
        return "m0"
    return m_value


def metastatic_stage() -> str:
    return "Stage IV"


def detect_treatment_keywords(text: str) -> tuple[str, str]:
    matches = sorted({match.group(0).strip().lower() for match in TREATMENT_PATTERN.finditer(text)})
    if not matches:
        return "no", ""
    return "yes", " | ".join(matches[:20])


def detect_signal(pattern: re.Pattern, text: str) -> str:
    return "yes" if pattern.search(text) else "no"


def detect_metastasis_signal(text: str) -> str:
    for match in METASTASIS_PATTERN.finditer(text):
        start = max(0, match.start() - 80)
        prefix = text[start:match.start()]
        if METASTASIS_NEGATION_PATTERN.search(prefix):
            continue
        return "yes"
    return "no"


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
def classify_tnm_context(*tokens: str) -> str:
    prefixes = []
    for token, axis in zip(tokens, ("t", "n", "m")):
        token = (token or "").lower().replace(" ", "")
        index = token.find(axis)
        prefixes.append(token[:index] if index >= 0 else "")
    joined = "".join(prefixes)

    if "y" in joined or "r" in joined:
        if "p" in joined:
            return "post_treatment_pathologic"
        if "c" in joined:
            return "post_treatment_clinical"
        return "post_treatment"
    if "p" in joined:
        return "pathologic"
    if "c" in joined:
        return "clinical"
    return "unknown"


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


def load_metadata(metadata_path: Path) -> dict:
    raw_bytes = metadata_path.read_bytes()
    last_error: Optional[Exception] = None

    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw_bytes.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = exc

    raise ValueError(
        f"Unable to decode metadata JSON file {metadata_path} with supported encodings. "
        f"Last error: {last_error}"
    )


def metadata_to_ipp(metadata: dict, metadata_path: Path) -> str:
    ipp = (
        metadata.get("Patient", {}).get("IPP")
        or metadata.get("IPP")
        or metadata_path.name.split("_")[0]
    )
    ipp = str(ipp).strip()
    return ipp or NULL_VALUE


def metadata_to_date(metadata: dict) -> str:
    for value in (
        metadata.get("Episode", {}).get("StartDate"),
        metadata.get("Document", {}).get("CreateDate"),
        metadata.get("Document", {}).get("UpdateDate"),
    ):
        if value:
            return str(value)[:8]
    return NULL_VALUE


def metadata_to_visit_number(metadata: dict) -> str:
    for value in (
        metadata.get("Episode", {}).get("VisitNumber"),
        metadata.get("Episode", {}).get("NumeroEpisode"),
    ):
        if value:
            return str(value)
    return NULL_VALUE


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


def compute_stage(t_value: str, n_value: str, m_value: str) -> str:
    t_norm = t_group(t_value)
    n_norm = n_group(n_value)
    m_norm = m_group(m_value)
    logic_m = "m0" if m_norm == "mx" else m_norm

    if logic_m == "m1":
        stage = "Stage IV"
    elif t_norm == "tis" and n_norm == "n0" and logic_m == "m0":
        stage = "Stage 0"
    elif t_norm == "t1" and n_norm == "n0" and logic_m == "m0":
        stage = "Stage I"
    elif (
        (t_norm in {"t0", "t1"} and n_norm in {"n1mi", "n1"})
        or (t_norm == "t2" and n_norm == "n0")
    ) and logic_m == "m0":
        stage = "Stage IIA"
    elif (
        (t_norm == "t2" and n_norm == "n1")
        or (t_norm == "t3" and n_norm == "n0")
    ) and logic_m == "m0":
        stage = "Stage IIB"
    elif (
        (t_norm in {"t0", "t1", "t2"} and n_norm == "n2")
        or (t_norm == "t3" and n_norm in {"n1", "n1mi", "n2"})
    ) and logic_m == "m0":
        stage = "Stage IIIA"
    elif t_norm == "t4" and n_norm in {"n0", "n1", "n2"} and logic_m == "m0":
        stage = "Stage IIIB"
    elif n_norm == "n3" and logic_m == "m0":
        stage = "Stage IIIC"
    else:
        stage = NULL_VALUE

    if stage != NULL_VALUE and m_norm == "mx":
        return f"{stage} (Mx)"
    return stage


def extract_tnm_candidates(text: str, ipp_meta: Optional[IppMetadata]) -> list[TnmCandidate]:
    candidates: list[TnmCandidate] = []
    seen: set[tuple[str, str, str]] = set()

    for match in TNM_PATTERN.finditer(text):
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        t_token = match.group(1) or ""
        n_token = match.group(2) or ""
        m_token = match.group(3) or ""
        t_value = normalize_tnm_component(t_token, "t")
        n_value = normalize_tnm_component(n_token, "n")
        m_value = normalize_tnm_component(m_token, "m")
        key = (t_value, n_value, m_value)
        if key in seen:
            continue
        seen.add(key)

        candidates.append(
            TnmCandidate(
                raw=raw,
                t=t_value or NULL_VALUE,
                n=n_value or NULL_VALUE,
                m=m_value or "mx",
                stage=compute_stage(t_value, n_value, m_value),
                context=classify_tnm_context(t_token, n_token, m_token),
            )
        )
    return candidates


def stage_rank(stage: str) -> tuple[int, int]:
    if not stage or stage == NULL_VALUE:
        return (-1, -1)
    match = re.match(r"Stage\s+(IV|III|II|I|0)([A-D]?)", stage)
    if not match:
        return (-1, -1)
    major_order = {"0": 0, "I": 1, "II": 2, "III": 3, "IV": 4}
    letter_order = {"": 0, "A": 1, "B": 2, "C": 3, "D": 4}
    return major_order[match.group(1)], letter_order.get(match.group(2), 0)


def is_post_treatment_context(context: str) -> bool:
    return context.startswith("post_treatment")


def candidate_context_priority(
    candidate: TnmCandidate,
    preferred_contexts: Optional[list[str]] = None,
) -> int:
    if preferred_contexts is None:
        preferred_contexts = ["clinical", "pathologic", "unknown"]

    if candidate.context in preferred_contexts:
        return preferred_contexts.index(candidate.context)
    if is_post_treatment_context(candidate.context):
        return len(preferred_contexts) + 10
    return len(preferred_contexts) + 5


def document_preferred_contexts(
    document_kind: str,
    surgery_detected: str,
    chemo_detected: str,
    radiotherapy_detected: str,
) -> list[str]:
    if document_kind == "pathology":
        return ["pathologic", "clinical", "unknown"]

    if document_kind == "rcp":
        if surgery_detected == "yes" and chemo_detected == "no" and radiotherapy_detected == "no":
            return ["pathologic", "clinical", "unknown"]
        if (chemo_detected == "yes" or radiotherapy_detected == "yes") and surgery_detected == "no":
            return ["clinical", "unknown", "pathologic"]
        return ["clinical", "pathologic", "unknown"]

    if document_kind == "consultation":
        return ["clinical", "unknown", "pathologic"]

    return ["clinical", "pathologic", "unknown"]


def choose_best_candidate(
    candidates: Iterable[TnmCandidate],
    preferred_contexts: Optional[list[str]] = None,
) -> Optional[TnmCandidate]:
    candidates = list(candidates)
    if not candidates:
        return None

    return min(
        candidates,
        key=lambda candidate: (
            candidate_context_priority(candidate, preferred_contexts),
            -stage_rank(candidate.stage)[0],
            -stage_rank(candidate.stage)[1],
            -(1 if candidate.m not in {"", "mx"} else 0),
            -len(candidate.raw),
        ),
    )


def token_is_post_treatment(token: str, axis: str) -> bool:
    token = (token or "").lower().replace(" ", "")
    index = token.find(axis)
    prefix = token[:index] if index >= 0 else ""
    return "y" in prefix or "r" in prefix


def axis_unknown_value(axis: str) -> str:
    return f"{axis}x"


def choose_informative_axis(candidates: list[str], axis: str) -> str:
    for candidate in candidates:
        candidate = normalize_tnm_component(candidate, axis)
        if candidate and candidate != axis_unknown_value(axis):
            return candidate
    for candidate in candidates:
        candidate = normalize_tnm_component(candidate, axis)
        if candidate:
            return candidate
    return ""


def t_component_rank(value: str) -> tuple[int, int]:
    value = normalize_tnm_component(value, "t")
    if not value:
        return (99, 99)
    if value == "tis":
        return (0, 0)
    if value == "t0":
        return (1, 0)
    match = re.match(r"t([1-4])(mi|a|b|c|d)?", value)
    if not match:
        if value == "tx":
            return (99, 99)
        return (98, 98)
    suffix_order = {"mi": 0, "a": 1, "b": 2, "c": 3, "d": 4, None: 5}
    return int(match.group(1)) + 1, suffix_order.get(match.group(2), 5)


def choose_smallest_t(candidates: list[str]) -> str:
    normalized = [normalize_tnm_component(candidate, "t") for candidate in candidates if candidate]
    normalized = [candidate for candidate in normalized if candidate]
    if not normalized:
        return ""
    return min(normalized, key=t_component_rank)


def extract_axis_values(text: str, pattern: re.Pattern, axis: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()

    for match in pattern.finditer(text):
        token = match.group(1)
        if token_is_post_treatment(token, axis):
            continue
        normalized = normalize_tnm_component(token, axis)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)

    return values


def reconstruct_same_document_tnm(text: str) -> Optional[tuple[str, str, str, str]]:
    t_values = extract_axis_values(text, T_COMPONENT_PATTERN, "t")
    n_values = extract_axis_values(text, N_COMPONENT_PATTERN, "n")
    m_values = extract_axis_values(text, M_COMPONENT_PATTERN, "m")

    t_value = choose_smallest_t(t_values)
    n_value = choose_informative_axis(n_values, "n")
    m_value = choose_informative_axis(m_values, "m")

    if not (t_value and n_value and m_value):
        return None

    stage = compute_stage(t_value, n_value, m_value)
    if stage == NULL_VALUE:
        return None

    return t_value, n_value, m_value, stage


def parse_date_sort_key(value: str) -> str:
    return value if value and value != NULL_VALUE else "99999999"


def blank_stage_row(row: DocumentResult, reason: str, status: str = "no_pre_treatment_stage_found") -> DocumentResult:
    return replace(
        row,
        tnm_raw=NULL_VALUE,
        t=NULL_VALUE,
        n=NULL_VALUE,
        m=NULL_VALUE,
        stage=NULL_VALUE,
        status=status,
        reason=reason,
        tnm_context="unknown",
    )


def choose_best_document(results: list[DocumentResult], strategy: str) -> DocumentResult:
    valid = [row for row in results if row.stage != NULL_VALUE and not is_post_treatment_context(row.tnm_context)]
    if not valid:
        post_only = [row for row in results if row.stage != NULL_VALUE]
        if post_only:
            return blank_stage_row(
                sorted(post_only, key=lambda row: parse_date_sort_key(row.document_date))[-1],
                "Only post-treatment stages were found for this IPP",
            )
        return sorted(results, key=lambda row: parse_date_sort_key(row.document_date))[-1]

    if strategy == "latest":
        return max(valid, key=lambda row: (parse_date_sort_key(row.document_date), stage_rank(row.stage)))

    return max(valid, key=lambda row: (stage_rank(row.stage), parse_date_sort_key(row.document_date)))


def find_metadata_files(input_dir: Path) -> list[Path]:
    return sorted(input_dir.glob("*.json.txt"))


def index_metadata_file(metadata_path: Path) -> MetadataIndex:
    metadata = load_metadata(metadata_path)
    return MetadataIndex(
        ipp=metadata_to_ipp(metadata, metadata_path),
        metadata_file=metadata_path,
        document_date=metadata_to_date(metadata),
    )


def group_metadata_by_ipp(metadata_files: list[Path]) -> dict[str, list[MetadataIndex]]:
    grouped: dict[str, list[MetadataIndex]] = {}
    for metadata_path in metadata_files:
        index = index_metadata_file(metadata_path)
        grouped.setdefault(index.ipp, []).append(index)

    for ipp in grouped:
        grouped[ipp].sort(key=lambda row: parse_date_sort_key(row.document_date))
    return grouped


def metadata_to_pdf_path(metadata_path: Path) -> Path:
    suffix = ".json.txt"
    if metadata_path.name.lower().endswith(suffix):
        return metadata_path.with_name(metadata_path.name[: -len(suffix)] + ".pdf")
    return metadata_path.with_suffix(".pdf")


def build_document_result(metadata_path: Path, ipp_meta: Optional[IppMetadata]) -> DocumentResult:
    metadata = load_metadata(metadata_path)
    ipp = metadata_to_ipp(metadata, metadata_path)
    document_date = metadata_to_date(metadata)
    visit_number = metadata_to_visit_number(metadata)
    pdf_path = metadata_to_pdf_path(metadata_path)
    document_kind = detect_document_kind(metadata, metadata_path, pdf_path)

    if not pdf_path.exists():
        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=0,
            tnm_raw=NULL_VALUE,
            t=NULL_VALUE,
            n=NULL_VALUE,
            m=NULL_VALUE,
            stage=NULL_VALUE,
            status="missing_pdf",
            reason="PDF not found next to metadata file",
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="unknown",
            treatment_detected="no",
            treatment_keywords="",
            surgery_detected="no",
            chemo_detected="no",
            radiotherapy_detected="no",
            metastasis_detected="no",
        )

    try:
        text = extract_pdf_text(pdf_path)
    except Exception as exc:  # pragma: no cover - runtime/pdf dependent
        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=0,
            tnm_raw=NULL_VALUE,
            t=NULL_VALUE,
            n=NULL_VALUE,
            m=NULL_VALUE,
            stage=NULL_VALUE,
            status="pdf_extract_error",
            reason=str(exc),
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="unknown",
            treatment_detected="no",
            treatment_keywords="",
            surgery_detected="no",
            chemo_detected="no",
            radiotherapy_detected="no",
            metastasis_detected="no",
        )

    treatment_detected, treatment_keywords = detect_treatment_keywords(text)
    surgery_detected = detect_signal(SURGERY_PATTERN, text)
    chemo_detected = detect_signal(CHEMO_PATTERN, text)
    radiotherapy_detected = detect_signal(RADIOTHERAPY_PATTERN, text)
    metastasis_detected = detect_metastasis_signal(text)

    if document_kind in {"rcp", "consultation"} and metastasis_detected == "yes":
        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=len(text),
            tnm_raw="metastatic_signal",
            t=NULL_VALUE,
            n=NULL_VALUE,
            m="m1",
            stage=metastatic_stage(),
            status="stage_found",
            reason="Metastatic mention in consultation/RCP",
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="metastatic_clinical",
            treatment_detected=treatment_detected,
            treatment_keywords=treatment_keywords,
            surgery_detected=surgery_detected,
            chemo_detected=chemo_detected,
            radiotherapy_detected=radiotherapy_detected,
            metastasis_detected=metastasis_detected,
        )

    explicit_stage = extract_explicit_stage(text)
    if explicit_stage is not None:
        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=len(text),
            tnm_raw="explicit_stage_mention",
            t=NULL_VALUE,
            n=NULL_VALUE,
            m=NULL_VALUE,
            stage=explicit_stage,
            status="stage_found",
            reason="Explicit stage mention found in document",
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="explicit_stage",
            treatment_detected=treatment_detected,
            treatment_keywords=treatment_keywords,
            surgery_detected=surgery_detected,
            chemo_detected=chemo_detected,
            radiotherapy_detected=radiotherapy_detected,
            metastasis_detected=metastasis_detected,
        )

    direct_stage_zero = infer_stage_zero_from_pathology(text, document_kind)
    if direct_stage_zero is not None:
        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=len(text),
            tnm_raw="dcis_stage_zero_rule",
            t="tis",
            n=NULL_VALUE,
            m=NULL_VALUE,
            stage=direct_stage_zero,
            status="stage_found",
            reason="DCIS/CCIS or in situ without invasion -> Stage 0",
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="pathologic",
            treatment_detected=treatment_detected,
            treatment_keywords=treatment_keywords,
            surgery_detected=surgery_detected,
            chemo_detected=chemo_detected,
            radiotherapy_detected=radiotherapy_detected,
            metastasis_detected=metastasis_detected,
        )

    candidates = extract_tnm_candidates(text, ipp_meta)
    chosen = choose_best_candidate(
        candidates,
        document_preferred_contexts(
            document_kind,
            surgery_detected,
            chemo_detected,
            radiotherapy_detected,
        ),
    )

    if chosen is None:
        reconstructed = reconstruct_same_document_tnm(text)
        if reconstructed is not None:
            t_value, n_value, m_value, stage = reconstructed
            return DocumentResult(
                ipp=ipp,
                metadata_file=str(metadata_path),
                pdf_file=str(pdf_path),
                document_date=document_date,
                visit_number=visit_number,
                text_length=len(text),
                tnm_raw=f"{t_value.upper()} {n_value.upper()} {m_value.upper()}",
                t=t_value,
                n=n_value,
                m=m_value,
                stage=stage,
                status="stage_found",
                reason="TNM reconstructed from separated T/N/M mentions within the same document",
                all_tnm_matches="",
                document_kind=document_kind,
                tnm_context="unknown",
                treatment_detected=treatment_detected,
                treatment_keywords=treatment_keywords,
                surgery_detected=surgery_detected,
                chemo_detected=chemo_detected,
                radiotherapy_detected=radiotherapy_detected,
                metastasis_detected=metastasis_detected,
            )

        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=len(text),
            tnm_raw=NULL_VALUE,
            t=NULL_VALUE,
            n=NULL_VALUE,
            m=NULL_VALUE,
            stage=NULL_VALUE,
            status="no_tnm_found",
            reason="No TNM pattern or direct stage rule found in extracted PDF text",
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="unknown",
            treatment_detected=treatment_detected,
            treatment_keywords=treatment_keywords,
            surgery_detected=surgery_detected,
            chemo_detected=chemo_detected,
            radiotherapy_detected=radiotherapy_detected,
            metastasis_detected=metastasis_detected,
        )

    return DocumentResult(
        ipp=ipp,
        metadata_file=str(metadata_path),
        pdf_file=str(pdf_path),
        document_date=document_date,
        visit_number=visit_number,
        text_length=len(text),
        tnm_raw=chosen.raw,
        t=chosen.t,
        n=chosen.n,
        m=chosen.m,
        stage=chosen.stage,
        status="stage_found" if chosen.stage != NULL_VALUE else "tnm_found_stage_unknown",
        reason=(
            "TNM extracted and stage computed"
            if chosen.stage != NULL_VALUE
            else "TNM extracted but organ-specific stage mapping returned null"
        ),
        all_tnm_matches=" | ".join(candidate.raw for candidate in candidates),
        document_kind=document_kind,
        tnm_context=chosen.context,
        treatment_detected=treatment_detected,
        treatment_keywords=treatment_keywords,
        surgery_detected=surgery_detected,
        chemo_detected=chemo_detected,
        radiotherapy_detected=radiotherapy_detected,
        metastasis_detected=metastasis_detected,
    )


def has_valid_date(value: str) -> bool:
    return bool(value and value != NULL_VALUE)


def first_signal_date(results: list[DocumentResult], predicate) -> Optional[str]:
    dates = sorted(
        {
            row.document_date
            for row in results
            if predicate(row) and has_valid_date(row.document_date)
        }
    )
    return dates[0] if dates else None


def row_on_or_before(row: DocumentResult, reference_date: Optional[str]) -> bool:
    if reference_date is None or not has_valid_date(reference_date):
        return True
    if not has_valid_date(row.document_date):
        return False
    return parse_date_sort_key(row.document_date) <= parse_date_sort_key(reference_date)


def infer_first_treatment(results: list[DocumentResult]) -> tuple[str, Optional[str], Optional[str]]:
    first_surgery_date = first_signal_date(results, lambda row: row.surgery_detected == "yes")
    first_non_surgical_date = first_signal_date(
        results,
        lambda row: row.chemo_detected == "yes" or row.radiotherapy_detected == "yes",
    )

    if first_surgery_date and first_non_surgical_date:
        if first_surgery_date < first_non_surgical_date:
            return "surgery_first", first_surgery_date, first_non_surgical_date
        if first_non_surgical_date < first_surgery_date:
            return "non_surgical_first", first_surgery_date, first_non_surgical_date
        return "ambiguous_first_treatment", first_surgery_date, first_non_surgical_date

    if first_surgery_date:
        return "surgery_first", first_surgery_date, first_non_surgical_date
    if first_non_surgical_date:
        return "non_surgical_first", first_surgery_date, first_non_surgical_date
    return "unknown_first_treatment", first_surgery_date, first_non_surgical_date


def baseline_sort_key(row: DocumentResult, document_priority: dict[str, int]) -> tuple[str, int, int, int]:
    major, minor = stage_rank(row.stage)
    return (
        parse_date_sort_key(row.document_date),
        document_priority.get(row.document_kind, 9),
        -major,
        -minor,
    )


def pick_first_matching(
    ordered: list[DocumentResult],
    predicate,
    selection_reason: str,
    document_priority: Optional[dict[str, int]] = None,
) -> Optional[tuple[DocumentResult, str]]:
    matches = [row for row in ordered if predicate(row)]
    if not matches:
        return None

    priority = document_priority or {}
    chosen = sorted(matches, key=lambda row: baseline_sort_key(row, priority))[0]
    return chosen, selection_reason


def choose_baseline_document(
    results: list[DocumentResult],
    ipp_meta: Optional[IppMetadata],
) -> tuple[DocumentResult, str]:
    ordered = sorted(results, key=lambda row: parse_date_sort_key(row.document_date))
    valid_non_post = [
        row for row in ordered if row.stage != NULL_VALUE and not is_post_treatment_context(row.tnm_context)
    ]

    first_rcp = next((row for row in ordered if row.document_kind == "rcp"), None)
    if first_rcp is not None and first_rcp.metastasis_detected == "yes":
        return first_rcp, "first_rcp_metastatic_stage"

    treatment_mode, _, first_non_surgical_date = infer_first_treatment(ordered)

    if treatment_mode == "surgery_first":
        preferred = pick_first_matching(
            ordered,
            lambda row: (
                row.stage != NULL_VALUE
                and row.tnm_context == "pathologic"
                and not is_post_treatment_context(row.tnm_context)
            ),
            "surgery_first_pathologic_tnm",
            {"rcp": 0, "pathology": 1, "consultation": 2, "other": 3},
        )
        if preferred is not None:
            return preferred

        fallback = pick_first_matching(
            ordered,
            lambda row: row.stage != NULL_VALUE and not is_post_treatment_context(row.tnm_context),
            "surgery_first_non_post_treatment_fallback",
            {"rcp": 0, "pathology": 1, "consultation": 2, "other": 3},
        )
        if fallback is not None:
            return fallback

    if treatment_mode == "non_surgical_first":
        preferred = pick_first_matching(
            ordered,
            lambda row: (
                row.stage != NULL_VALUE
                and row.tnm_context in {"clinical", "explicit_stage", "metastatic_clinical"}
                and not is_post_treatment_context(row.tnm_context)
                and row_on_or_before(row, first_non_surgical_date)
            ),
            "non_surgical_first_clinical_tnm_before_treatment",
            {"consultation": 0, "rcp": 1, "other": 2, "pathology": 3},
        )
        if preferred is not None:
            return preferred

        fallback = pick_first_matching(
            ordered,
            lambda row: (
                row.stage != NULL_VALUE
                and row.tnm_context in {"clinical", "unknown", "explicit_stage", "metastatic_clinical"}
                and not is_post_treatment_context(row.tnm_context)
            ),
            "non_surgical_first_clinical_fallback",
            {"consultation": 0, "rcp": 1, "other": 2, "pathology": 3},
        )
        if fallback is not None:
            return fallback

    generic = pick_first_matching(
        ordered,
        lambda row: (
            row.stage != NULL_VALUE
            and row.tnm_context in {"clinical", "explicit_stage", "metastatic_clinical"}
            and not is_post_treatment_context(row.tnm_context)
        ),
        "first_clinical_tnm_fallback",
        {"consultation": 0, "rcp": 1, "other": 2, "pathology": 3},
    )
    if generic is not None:
        return generic

    generic = pick_first_matching(
        ordered,
        lambda row: (
            row.stage != NULL_VALUE
            and row.tnm_context == "pathologic"
            and not is_post_treatment_context(row.tnm_context)
        ),
        "first_pathologic_tnm_fallback",
        {"rcp": 0, "pathology": 1, "consultation": 2, "other": 3},
    )
    if generic is not None:
        return generic

    generic = pick_first_matching(
        ordered,
        lambda row: row.stage != NULL_VALUE and not is_post_treatment_context(row.tnm_context),
        "first_non_post_treatment_stage_fallback",
    )
    if generic is not None:
        return generic

    if valid_non_post:
        return valid_non_post[0], "first_valid_stage_last_resort"

    post_only = [row for row in ordered if row.stage != NULL_VALUE]
    if post_only:
        return (
            blank_stage_row(
                post_only[-1],
                "Only post-treatment stages were found for this IPP",
            ),
            "post_treatment_only_stage_excluded",
        )

    return ordered[-1], "no_valid_stage_found"


def build_ipp_result(
    rows: list[DocumentResult],
    strategy: str,
    ipp_meta: Optional[IppMetadata],
) -> IppResult:
    chosen, selection_reason = choose_baseline_document(rows, ipp_meta)
    if strategy == "latest":
        chosen = choose_best_document(rows, strategy)
        selection_reason = "latest_document"
    elif strategy == "highest":
        chosen = choose_best_document(rows, strategy)
        selection_reason = "highest_stage_document"

    documents_with_stage = sum(
        1 for row in rows if row.stage != NULL_VALUE and not is_post_treatment_context(row.tnm_context)
    )
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return IppResult(
        ipp=chosen.ipp,
        stage=chosen.stage,
        tnm_raw=chosen.tnm_raw,
        t=chosen.t,
        n=chosen.n,
        m=chosen.m,
        document_date=chosen.document_date,
        source_pdf=chosen.pdf_file,
        status=chosen.status,
        reason=chosen.reason,
        selection_reason=selection_reason,
        document_kind=chosen.document_kind,
        tnm_context=chosen.tnm_context,
        treatment_detected=chosen.treatment_detected,
        treatment_keywords=chosen.treatment_keywords,
        surgery_detected=chosen.surgery_detected,
        chemo_detected=chosen.chemo_detected,
        radiotherapy_detected=chosen.radiotherapy_detected,
        metastasis_detected=chosen.metastasis_detected,
        documents_seen=len(rows),
        documents_with_stage=documents_with_stage,
        last_update=run_timestamp,
    )


def write_csv(path: Path, rows: list[IppResult]) -> None:
    if not rows:
        return
    fieldnames = list(asdict(rows[0]).keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def document_match_count(row: DocumentResult) -> int:
    if row.all_tnm_matches:
        return len([part for part in row.all_tnm_matches.split(" | ") if part.strip()])
    if row.tnm_raw and row.tnm_raw != NULL_VALUE:
        return 1
    return 0


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        LOGGER.error("Input directory does not exist: %s", input_dir)
        return 1

    require_pdf_backend()

    metadata_files = find_metadata_files(input_dir)
    if not metadata_files:
        LOGGER.error("No *.json.txt metadata files found in %s", input_dir)
        return 1

    ipp_metadata_map = load_ipp_metadata_map(args.ipp_metadata_file)
    ipp_csv = output_dir / args.csv_name

    LOGGER.info("Found %s metadata files", len(metadata_files))
    grouped_metadata = group_metadata_by_ipp(metadata_files)
    LOGGER.info("Found %s distinct IPP", len(grouped_metadata))

    ipp_results: list[IppResult] = []
    running_total_matches = 0
    running_docs_with_match = 0

    for ipp_index, ipp in enumerate(sorted(grouped_metadata), start=1):
        metadata_entries = grouped_metadata[ipp]
        ipp_meta = ipp_metadata_map.get(ipp)

        LOGGER.info(
            "Processing IPP %s/%s: %s (%s documents) | organe=%s | cim=%s",
            ipp_index,
            len(grouped_metadata),
            ipp,
            len(metadata_entries),
            ipp_meta.organe if ipp_meta else "",
            ipp_meta.code_cim if ipp_meta else "",
        )

        document_results: list[DocumentResult] = []
        for metadata_entry in metadata_entries:
            result = build_document_result(metadata_entry.metadata_file, ipp_meta)
            document_results.append(result)
            match_count = document_match_count(result)
            LOGGER.info(
                "  date=%s | kind=%s | context=%s | stage=%s | matches=%s | surg=%s | chemo=%s | radio=%s | meta=%s | status=%s | file=%s",
                result.document_date,
                result.document_kind,
                result.tnm_context,
                result.stage,
                match_count,
                result.surgery_detected,
                result.chemo_detected,
                result.radiotherapy_detected,
                result.metastasis_detected,
                result.status,
                Path(result.pdf_file).name,
            )

        ipp_total_matches = sum(document_match_count(row) for row in document_results)
        ipp_docs_with_match = sum(1 for row in document_results if document_match_count(row) > 0)
        running_total_matches += ipp_total_matches
        running_docs_with_match += ipp_docs_with_match

        ipp_result = build_ipp_result(document_results, args.ipp_strategy, ipp_meta)
        ipp_results.append(ipp_result)
        write_csv(ipp_csv, ipp_results)

        LOGGER.info(
            "IPP match summary | ipp=%s | docs=%s | docs_with_match=%s | total_matches=%s | cumulative_docs_with_match=%s | cumulative_matches=%s",
            ipp,
            len(document_results),
            ipp_docs_with_match,
            ipp_total_matches,
            running_docs_with_match,
            running_total_matches,
        )
        LOGGER.info(
            "IPP selected | %s | stage=%s | date=%s | strategy=%s | tnm=%s",
            ipp_result.ipp,
            ipp_result.stage,
            ipp_result.document_date,
            ipp_result.selection_reason,
            ipp_result.tnm_raw,
        )

    LOGGER.info("Wrote %s", ipp_csv)
    LOGGER.info("Processed IPP count in this run lakehouse: %s", len(ipp_results))
    LOGGER.info("IPP summary for current run LakeHouse:")

    for row in ipp_results:
        print(
            f"{row.ipp},{row.stage},{row.tnm_raw},{row.document_date},"
            f"{row.documents_seen},{row.documents_with_stage},{row.status},"
            f"{row.selection_reason},{row.treatment_detected}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
