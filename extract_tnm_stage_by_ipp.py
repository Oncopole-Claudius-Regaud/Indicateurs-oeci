from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
import types
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
import debug_tnm_single_ipp as debug_engine

try:
    import fitz  # type: ignore
except ImportError:  # pragma: no cover
    fitz = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError:  # pragma: no cover
    PdfReader = None


LOGGER = logging.getLogger("tnm_stage")
VERSION_FLAG = "STABLE"
NULL_VALUE = "null"

TNM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyrai]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyrai]{0,4})?m(?:x|0|1[abc]?)?)?",
    re.IGNORECASE,
)
TNM_LOOSE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:[\s\S]{0,120}?)"
    r"((?:[cpyrai]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:[\s\S]{0,80}?)"
    r"((?:[cpyrai]{0,4})?m(?:x|0|1[abc]?))",
    re.IGNORECASE,
)
T_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
T_IRM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:[\s,;:()\\/-]{0,12})irm\b",
    re.IGNORECASE,
)
T_COMPONENT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
N_COMPONENT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
M_COMPONENT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?m(?:x|0|1[abc]?))(?![A-Za-z0-9])",
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
    r"\b(m[ée]tast|oligom[ée]tast|secondaire[s]?\s+(hepatiq|osseu|pulmon|cerebr)|"
    r"atteinte\s+m[ée]tastatique|maladie\s+m[ée]tastatique|"
    r"l[ée]sion[s]?\s+osseuse[s]?\s+multifocale[s]?|atteinte\s+osseuse)",
    re.IGNORECASE,
)
METASTASIS_NEGATION_PATTERN = re.compile(
    r"\b(pas\s+de|sans|absence\s+de|aucun(?:e)?|pas\s+d['e])\b",
    re.IGNORECASE,
)
METASTASIS_FIELD_NEGATION_PATTERN = re.compile(
    r"\bm[ée]tastas?(?:e|es|ique|iques)?\b\s*[:=-]\s*non\b",
    re.IGNORECASE,
)
METASTASIS_FORM_LABEL_PATTERN = re.compile(
    r"\btype\s+histologique\s*\(\s*primitif\s*,\s*m[ée]tastase\s+et\s+origine\s*\)\s*:\s*primitif\b",
    re.IGNORECASE,
)
METASTASIS_LOCAL_NEGATION_PATTERN = re.compile(
    r"\b(non|pas\s+de|sans|absence\s+de|aucun(?:e)?)\b",
    re.IGNORECASE,
)
RCP_CARTOUCHE_PATTERN = re.compile(
    r"\brcp\s+sein\s+diagnostique\b[\s\S]{0,160}\brcp\s+sein\s+post\s+chirurgical\b[\s\S]{0,160}\brcp\s+sein\s+m[ée]tastatique\b",
    re.IGNORECASE,
)
SERVICE_MENU_METASTASIS_PATTERN = re.compile(
    r"\b(pathologie\s+thyro[iï]dienne|tumeurs?\s+neuro[\s-]?endocrines?|h[ée]mopathies|m[ée]tastases?\s+osseuses)\b",
    re.IGNORECASE,
)
METASTASIS_EXPLICIT_NEGATIVE_CONTEXT_PATTERN = re.compile(
    r"\b(?:absence\s+de|sans|pas\s+de|aucun(?:e)?)\b[\s\S]{0,60}\bmetast",
    re.IGNORECASE,
)
NODAL_POSITIVE_PATTERN = re.compile(
    r"\b(metastase\s+ganglionnaire|metastases\s+ganglionnaires|adenopathie[s]?\s+secondaire[s]?|"
    r"envahissement\s+ganglionnaire|atteinte\s+ganglionnaire)\b",
    re.IGNORECASE,
)
NODAL_SUSPICIOUS_PATTERN = re.compile(
    r"\b(suspicion\s+(?:d[''’]\s*)?(?:atteinte\s+)?ganglionnaire|"
    r"atteinte\s+ganglionnaire\s+suspecte?|"
    r"ad[ée]nopathie[s]?\s+suspecte?s?|"
    r"ganglion(?:naire)?s?\s+suspecte?s?)\b",
    re.IGNORECASE,
)
NODAL_NEGATIVE_PATTERN = re.compile(
    r"\b(absence\s+de\s+metastase\s+ganglionnaire|sans\s+metastase\s+ganglionnaire|"
    r"absence\s+d[''’]\s*atteinte\s+ganglionnaire|absence\s+de\s+atteinte\s+ganglionnaire|"
    r"sans\s+atteinte\s+ganglionnaire|pas\s+d[''’]\s*atteinte\s+ganglionnaire|"
    r"pas\s+de\s+atteinte\s+ganglionnaire|"
    r"ganglion\s+sentinelle\s+negatif|pas\s+de\s+metastase\s+ganglionnaire|"
    r"aucune?\s+metastase\s+ganglionnaire|0\s*/\s*[1-9]\d*|"
    r"pas\s+mis\s+en\s+[eé]vidence\s+(?:d['']\s*|de\s+)ad[ée]nom[ée]galie(?:s)?(?:\s+axillaire(?:s)?)?|"
    r"ganglion(?:naire)?s?[\s\S]{0,80}sans\s+[eé]l[eé]ment\s+suspect|"
    r"aires?\s+ganglionnaires?\s+axillaires?\s+vierges?)\b",
    re.IGNORECASE,
)
NODAL_NEGATION_PATTERN = re.compile(
    r"\b(pas\s+d[''’]?|pas\s+de|sans|absence\s+d[''’]?|absence\s+de|aucun(?:e)?)\b",
    re.IGNORECASE,
)
PROSTATE_CONTEXT_PATTERN = re.compile(
    r"\b(prostate|prostatique|prostatectomie|biopsie[s]?\s+prostatique[s]?|"
    r"ad[ée]nocarcinome\s+prostatique|loge\s+prostatique|pirads|pi[\s-]?rads|"
    r"gleason|isup|psa)\b",
    re.IGNORECASE,
)
MELANOMA_CONTEXT_PATTERN = re.compile(r"\b(m[ée]lanome|breslow|clark|ssm)\b", re.IGNORECASE)
MELANOMA_WEAK_CERTAINTY_PATTERN = re.compile(
    r"\b(suspicion|suspecte?|possible|probable|douteux|douteuse|compatible\s+avec|[àa]\s+contr[oô]ler)\b",
    re.IGNORECASE,
)
MELANOMA_EXCLUSION_PATTERN = re.compile(
    r"\b(r[ée]actionnel|inflammatoire|stable\s+non\s+suspect|non\s+suspect|b[ée]nin|cicatriciel|post[\s-]?op[ée]ratoire|post[\s-]?th[ée]rapeutique)\b",
    re.IGNORECASE,
)
MELANOMA_NON_REGIONAL_NODAL_PATTERN = re.compile(
    r"\b(m[ée]diastin|hilaire|r[ée]tro[\s-]?p[ée]riton|lombo[\s-]?aort|para[\s-]?aort|ganglion\s+non\s+r[ée]gional|ad[ée]nopathie\s+[àa]\s+distance)\b",
    re.IGNORECASE,
)
MELANOMA_TRANSIT_SATELLITE_PATTERN = re.compile(
    r"\b(microsatellite|microsatellites|m[ée]tastase[s]?\s+satellite[s]?|nodule[s]?\s+satellite[s]?|m[ée]tastase[s]?\s+en\s+transit|l[ée]sion[s]?\s+en\s+transit|in[\s-]?transit)\b",
    re.IGNORECASE,
)
MELANOMA_SENTINEL_MAPPING_PATTERN = re.compile(
    r"\b(ganglion[s]?\s+(?:intens[ée]ment\s+)?fixant[s]?|radiotraceur|gamma\s+cam[ée]ra|"
    r"sonde\s+de\s+d[ée]tection|migration\s+cervicale|lymphoscintigraphie|"
    r"ganglion[s]?\s+sentinelle[s]?)\b",
    re.IGNORECASE,
)
MELANOMA_METASTASIS_CONFIRMED_PATTERN = re.compile(
    r"\b("
    r"hyperfixation|pet[\s-]?scanner|pet[\s-]?scan|"
    r"nodule[s]?\s+pulmonaire[s]?\s+(se\s+major|confirm|m[ée]tastat|malin|maligne|suspect)|"
    r"[ée]volutivit[eé]\s+pulmonaire|"
    r"m[ée]tastase[s]?\s+pulmonaire[s]?|"
    r"atteinte\s+m[ée]tastatique\s+(pulmonaire|h[eé]patique|osseuse|c[eé]r[eé]brale|visc[eé]rale)|"
    r"bilan\s+d['']extension\s+positif|"
    r"progression\s+m[ée]tastatique"
    r")\b",
    re.IGNORECASE,
)
MELANOMA_SURVEILLANCE_PATTERN = re.compile(
    r"\b("
    r"surveillance|r[eé]mission\s+compl[eè]te|contr[oô]le|suivi|"
    r"pas\s+de\s+signe\s+de\s+r[eé]cidive|absence\s+de\s+r[eé]cidive|"
    r"en\s+r[eé]mission|r[eé]mission\s+maintenue"
    r")\b",
    re.IGNORECASE,
)
IMAGING_EVIDENCE_PATTERN = re.compile(
    r"\b(scanner|irm|pet[\s-]?scan|pet[\s-]?scanner|tep|imagerie|bilan\s+d['']extension|echo(graphie)?)\b",
    re.IGNORECASE,
)
ULCERATION_PATTERN = re.compile(
    r"\b(ulc[eé]r[eé]|largement\s+ulc[eé]r[eé]|ulc[eé]ration)\b",
    re.IGNORECASE,
)
REGIONAL_NODAL_CONTEXT_PATTERN = re.compile(
    r"\b(ganglion(?:naire)?|ad[ée]nom[ée]galie|adenopathie|inguinal|axillaire|iliaque)\b",
    re.IGNORECASE,
)
DISTANT_SECONDARY_SITE_PATTERN = re.compile(
    r"\b(secondaire[s]?\s+(hepatiq|osseu|pulmon|cerebr)|a\s+distance|visceral(?:e|es)?)\b",
    re.IGNORECASE,
)
BREAST_REGIONAL_NODAL_MET_PATTERN = re.compile(
    r"\b(m[ée]tastase[s]?\s+ganglionnaire[s]?)\b[\s\S]{0,80}\b(axillaire[s]?|sus[\s-]?claviculaire[s]?|"
    r"sous[\s-]?claviculaire[s]?|mammaire[s]?\s+interne[s]?|sentinelle[s]?)\b|"
    r"\b(axillaire[s]?|sus[\s-]?claviculaire[s]?|sous[\s-]?claviculaire[s]?|"
    r"mammaire[s]?\s+interne[s]?|sentinelle[s]?)\b[\s\S]{0,80}\b(m[ée]tastase[s]?\s+ganglionnaire[s]?)\b",
    re.IGNORECASE,
)
BREAST_DISTANT_METASTASIS_PATTERN = re.compile(
    r"\b(m1[abc]?|m[ée]tastase[s]?\s+(h[ée]patique[s]?|pulmonaire[s]?|osseuse[s]?|c[eé]r[eé]brale[s]?|"
    r"visc[ée]rale[s]?|p[eé]riton[ée]ale[s]?|pleurale[s]?)|localisation\s+[àa]\s+distance|"
    r"ad[ée]nopathie[s]?\s+[àa]\s+distance|ganglion\s+non\s+r[ée]gional)\b",
    re.IGNORECASE,
)
NO_OTHER_SECONDARY_LOCATION_PATTERN = re.compile(
    r"\b(pas\s+d['']autre\s+localisation\s+secondaire|pas\s+autre\s+localisation\s+secondaire|"
    r"aucune?\s+autre\s+localisation\s+secondaire|dedouane?\s+toute\s+localisation\s+secondaire|"
    r"d[eé]douanant\s+toute\s+localisation\s+secondaire|"
    r"pas\s+d['']autre\s+localisation\s+a\s+distance)\b",
    re.IGNORECASE,
)
SECONDARY_LOCATION_NEGATED_PATTERN = re.compile(
    r"\b(?:aucun(?:e)?|sans|absence\s+de|pas\s+de|pas\s+d[''])\b[\s\S]{0,40}\blocalisation\s+secondaire(?:s)?\b|"
    r"\blocalisation\s+secondaire(?:s)?\b[\s\S]{0,40}\b(?:aucun(?:e)?|sans|absence\s+de|pas\s+de|pas\s+d[''])\b",
    re.IGNORECASE,
)
ANESTHESIA_DOC_PATTERN = re.compile(r"\bdossier\s+anesth[eé]sie\b", re.IGNORECASE)
EXPLICIT_STAGE_PATTERN = re.compile(r"\b(?:stade|stage)\s*(?:ajcc\s*)?(0|iv|iii[abc]?|ii[abc]?|i[abc]?|1|2|3|4)\b", re.IGNORECASE)
EXPLICIT_STAGE_FALSE_POSITIVE_PATTERN = re.compile(
    r"\b(ptose(?:\s+mammaire)?|oms)\b",
    re.IGNORECASE,
)
EXPLICIT_STAGE_ONCO_CONTEXT_PATTERN = re.compile(
    r"\b(cancer|carcinom|tumeur|oncolog|tnm|ajcc|m[ée]tast|invasi|ad[ée]nocarcinom)\b",
    re.IGNORECASE,
)
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
BRESLOW_PATTERN = re.compile(
    r"(?:\bbreslow(?:\s*(?:de|:|=))?\s*([0-9]+(?:[.,][0-9]+)?)\s*mm\b|"
    r"\b([0-9]+(?:[.,][0-9]+)?)\s*mm\s+d[''][eé]paisseur\s+selon\s+breslow\b)",
    re.IGNORECASE,
)
BREAST_CONTEXT_PATTERN = re.compile(
    r"\b(sein|mammaire|s[eé]nologie|mastectomie|tumorectomie|quadrantectomie|"
    r"carcinome\s+canalaire|carcinome\s+lobulaire|her2|recepteur\s+(estrog|progest)|"
    r"grade\s+(sbr|eln)|ganglion\s+sentinelle\s+axillaire)\b",
    re.IGNORECASE,
)
BREAST_PATHOLOGICAL_TNM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"(p\s*t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"(p?\s*n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"(p?\s*m(?:x|0|1[abc]?))?",
    re.IGNORECASE,
)


@dataclass
class IppMetadata:
    ipp: str
    organe: str
    code_cim: str
    date_diag_tkc: str = ""
    date_diag_dcc: str = ""


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
    stage_confidence: str = "high"


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
    stage_confidence: str = "high"


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
            date_diag_tkc=str(row.get("date_diag_tkc") or "").strip(),
            date_diag_dcc=str(row.get("date_diag_dcc") or "").strip(),
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
        start = max(0, match.start() - 180)
        end = min(len(text), match.end() + 120)
        prefix = text[start:match.start()]
        around = text[start:end]
        around_wide = text[max(0, match.start() - 420):min(len(text), match.end() + 420)]
        if RCP_CARTOUCHE_PATTERN.search(around_wide):
            continue
        if len(SERVICE_MENU_METASTASIS_PATTERN.findall(around_wide)) >= 2:
            continue
        if METASTASIS_NEGATION_PATTERN.search(prefix):
            continue
        if METASTASIS_LOCAL_NEGATION_PATTERN.search(around):
            continue
        if METASTASIS_FIELD_NEGATION_PATTERN.search(around):
            continue
        if METASTASIS_FORM_LABEL_PATTERN.search(around):
            continue
        if METASTASIS_EXPLICIT_NEGATIVE_CONTEXT_PATTERN.search(around):
            continue
        if SECONDARY_LOCATION_NEGATED_PATTERN.search(around):
            continue
        # Regional nodal metastatic wording should not be promoted to Stage IV
        # unless there is an explicit distant-secondary context.
        if REGIONAL_NODAL_CONTEXT_PATTERN.search(around) and not DISTANT_SECONDARY_SITE_PATTERN.search(around):
            continue
        if REGIONAL_NODAL_CONTEXT_PATTERN.search(around) and NO_OTHER_SECONDARY_LOCATION_PATTERN.search(text):
            continue
        return "yes"
    return "no"


def has_post_treatment_tnm_prefix(raw_tnm: str) -> bool:
    compact = re.sub(r"[\s\.\-_/,:;()]+", "", (raw_tnm or "").lower())
    return bool(
        re.search(
            r"y(?:p|c)?i?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?)"
            r"|y(?:p|c)?i?n(?:x|0|1mi|1[abc]?|2[ab]?|3[abc]?)"
            r"|y(?:p|c)?i?m(?:x|0|1[abc]?)",
            compact,
            re.IGNORECASE,
        )
    )


def is_breast_regional_nodal_only_metastasis(text: str) -> bool:
    if not BREAST_CONTEXT_PATTERN.search(text):
        return False
    if not BREAST_REGIONAL_NODAL_MET_PATTERN.search(text):
        return False
    if DISTANT_SECONDARY_SITE_PATTERN.search(text):
        return False
    return True


def breast_has_distant_metastasis_signal(text: str) -> bool:
    return bool(DISTANT_SECONDARY_SITE_PATTERN.search(text) or BREAST_DISTANT_METASTASIS_PATTERN.search(text))


def detect_nodal_positive_signal(text: str) -> str:
    for match in NODAL_POSITIVE_PATTERN.finditer(text):
        start = max(0, match.start() - 120)
        end = min(len(text), match.end() + 80)
        prefix = text[start:match.start()]
        context = text[start:end]
        if NODAL_NEGATION_PATTERN.search(prefix):
            continue
        if NODAL_SUSPICIOUS_PATTERN.search(context):
            continue
        return "yes"
    return "no"


def detect_nodal_uncertain_signal(text: str) -> str:
    for match in NODAL_SUSPICIOUS_PATTERN.finditer(text):
        start = max(0, match.start() - 120)
        prefix = text[start:match.start()]
        if NODAL_NEGATION_PATTERN.search(prefix):
            continue
        return "yes"
    return "no"


def detect_imaging_evidence(text: str, document_kind: str) -> bool:
    return document_kind == "radiology" or bool(IMAGING_EVIDENCE_PATTERN.search(text))


def detect_melanoma_nodal_signal(text: str) -> str:
    sentinel_mapping_only = bool(MELANOMA_SENTINEL_MAPPING_PATTERN.search(text))
    for match in MELANOMA_NON_REGIONAL_NODAL_PATTERN.finditer(text):
        prefix = text[max(0, match.start() - 160):match.start()]
        context = text[max(0, match.start() - 220):min(len(text), match.end() + 220)]
        if sentinel_mapping_only and not MELANOMA_METASTASIS_CONFIRMED_PATTERN.search(context):
            continue
        if NODAL_NEGATION_PATTERN.search(prefix):
            continue
        if MELANOMA_WEAK_CERTAINTY_PATTERN.search(context):
            continue
        if MELANOMA_EXCLUSION_PATTERN.search(context):
            continue
        if MELANOMA_SURVEILLANCE_PATTERN.search(context):
            continue
        return "non_regional"
    if MELANOMA_TRANSIT_SATELLITE_PATTERN.search(text) and not sentinel_mapping_only:
        return "positive"
    if detect_nodal_positive_signal(text) == "yes":
        return "positive"
    return "unknown"


def detect_melanoma_metastasis_confirmed(text: str) -> bool:
    if MELANOMA_SURVEILLANCE_PATTERN.search(text):
        past_markers = re.compile(
            r"\b(en\s+2\d{3}|trait[eé]\s+par|a\s+[eé]t[eé]|ancienne|ant[eé]rieure?|anciennement)\b",
            re.IGNORECASE,
        )
        for match in MELANOMA_METASTASIS_CONFIRMED_PATTERN.finditer(text):
            window_start = max(0, match.start() - 200)
            context = text[window_start:match.end() + 100]
            if MELANOMA_WEAK_CERTAINTY_PATTERN.search(context):
                continue
            if MELANOMA_EXCLUSION_PATTERN.search(context):
                continue
            if past_markers.search(context):
                continue
            return True
        return False
    for match in MELANOMA_METASTASIS_CONFIRMED_PATTERN.finditer(text):
        context = text[max(0, match.start() - 200):min(len(text), match.end() + 100)]
        if MELANOMA_WEAK_CERTAINTY_PATTERN.search(context):
            continue
        if MELANOMA_EXCLUSION_PATTERN.search(context):
            continue
        return True
    return False


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
    if any(k in haystack for k in ("scanner", "scannercr", "irm", "pet", "echograph", "radio", "imagerie")):
        return "radiology"
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
    if token == "0":
        return "Stage I"
    token = {"1": "I", "2": "II", "3": "III", "4": "IV"}.get(token, token)
    return f"Stage {token}"


def extract_explicit_stage(text: str) -> Optional[str]:
    match = EXPLICIT_STAGE_PATTERN.search(text)
    if not match:
        return None
    window_start = max(0, match.start() - 80)
    window_end = min(len(text), match.end() + 80)
    window = text[window_start:window_end]
    if EXPLICIT_STAGE_FALSE_POSITIVE_PATTERN.search(window):
        return None
    if not EXPLICIT_STAGE_ONCO_CONTEXT_PATTERN.search(window):
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
    return "Stage I"


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


FILENAME_DATE_PATTERN = re.compile(r"_(\d{8})(?:_\d+)?\.pdf$", re.IGNORECASE)


def extract_date_from_filename(filename: str) -> Optional[str]:
    match = FILENAME_DATE_PATTERN.search(filename)
    if not match:
        return None
    value = match.group(1)
    year = int(value[:4])
    month = int(value[4:6])
    day = int(value[6:8])
    if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
        return value
    return None


def metadata_to_date(metadata: dict, pdf_path: Optional[Path] = None) -> str:
    if pdf_path is not None:
        file_date = extract_date_from_filename(pdf_path.name)
        if file_date:
            return file_date
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
    def ocr_fallback() -> str:
        if fitz is None:
            return ""
        if shutil.which("tesseract") is None:
            return ""
        texts: list[str] = []
        try:
            with fitz.open(pdf_path) as document, tempfile.TemporaryDirectory() as tmpdir:
                for page_index, page in enumerate(document):
                    image_path = Path(tmpdir) / f"page_{page_index:04d}.png"
                    pix = page.get_pixmap(matrix=fitz.Matrix(2.5, 2.5), alpha=False)
                    pix.save(str(image_path))
                    cmd = ["tesseract", str(image_path), "stdout", "-l", "fra+eng", "--psm", "6"]
                    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
                    if proc.returncode == 0 and proc.stdout:
                        texts.append(proc.stdout)
        except Exception:
            return ""
        return normalize_text("\n".join(texts))

    if fitz is not None:
        chunks: list[str] = []
        with fitz.open(pdf_path) as document:
            for page in document:
                chunks.append(page.get_text("text"))
        native_text = normalize_text("\n".join(chunks))
        if len(native_text.strip()) >= 40:
            return native_text
        ocr_text = ocr_fallback()
        return ocr_text or native_text

    if PdfReader is not None:
        chunks = []
        reader = PdfReader(str(pdf_path))
        for page in reader.pages:
            chunks.append(page.extract_text() or "")
        native_text = normalize_text("\n".join(chunks))
        if len(native_text.strip()) >= 40:
            return native_text
        ocr_text = ocr_fallback()
        return ocr_text or native_text

    raise RuntimeError("No PDF backend available.")


def compute_stage(t_value: str, n_value: str, m_value: str) -> str:
    t_norm = t_group(t_value)
    n_norm = n_group(n_value)
    m_norm = m_group(m_value)
    logic_n = "n0" if n_norm == "nx" else n_norm
    logic_m = "m0" if m_norm == "mx" else m_norm

    if logic_m == "m1":
        stage = "Stage IV"
    elif t_norm == "tis" and logic_n == "n0" and logic_m == "m0":
        stage = "Stage I"
    elif t_norm == "t1" and logic_n == "n0" and logic_m == "m0":
        stage = "Stage I"
    elif (
        (t_norm in {"t0", "t1"} and logic_n in {"n1mi", "n1"})
        or (t_norm == "t2" and logic_n == "n0")
    ) and logic_m == "m0":
        stage = "Stage IIA"
    elif (
        (t_norm == "t2" and logic_n == "n1")
        or (t_norm == "t3" and logic_n == "n0")
    ) and logic_m == "m0":
        stage = "Stage IIB"
    elif (
        (t_norm in {"t0", "t1", "t2"} and logic_n == "n2")
        or (t_norm == "t3" and logic_n in {"n1", "n1mi", "n2"})
    ) and logic_m == "m0":
        stage = "Stage IIIA"
    elif t_norm == "t4" and logic_n in {"n0", "n1", "n2"} and logic_m == "m0":
        stage = "Stage IIIB"
    elif logic_n == "n3" and logic_m == "m0":
        stage = "Stage IIIC"
    else:
        stage = NULL_VALUE

    if stage != NULL_VALUE and m_norm == "mx":
        return f"{stage} (Mx)"
    return stage


def compute_melanoma_stage(t_value: str, n_value: str, m_value: str, ulcerated: bool) -> str:
    t = normalize_tnm_component(t_value, "t")
    n = normalize_tnm_component(n_value, "n")
    m = normalize_tnm_component(m_value, "m") or "mx"
    logic_n = n
    logic_m = m

    if logic_m == "mx":
        return NULL_VALUE
    if logic_m.startswith("m1"):
        return "Stage IV"
    if logic_n == "nx":
        return NULL_VALUE
    if t == "tis":
        return "Stage 0"
    if t in {"t1", "t1a"}:
        return "Stage IA" if logic_n == "n0" else "Stage IIIA"
    if t == "t1b":
        return "Stage IB" if logic_n == "n0" else "Stage IIIA"
    if t in {"t2", "t2a"}:
        return "Stage IB" if logic_n == "n0" else "Stage IIIA"
    if t == "t2b":
        return "Stage IIA" if logic_n == "n0" else "Stage IIIB"
    if t in {"t3", "t3a"}:
        return "Stage IIA" if logic_n == "n0" else "Stage IIIB"
    if t == "t3b":
        return "Stage IIB" if logic_n == "n0" else "Stage IIIB"
    if t in {"t4", "t4a"}:
        return "Stage IIB" if logic_n == "n0" else "Stage IIIB"
    if t == "t4b":
        if logic_n == "n0":
            return "Stage IIC"
        if logic_n in {"n1", "n1a", "n1b", "n2", "n2a", "n2b"}:
            return "Stage IIIB"
        if logic_n in {"n3", "n3a", "n3b", "n3c"}:
            return "Stage IIIC"
        return "Stage IIIB"
    return compute_stage(t_value, n_value, m_value)


def compute_breast_stage(t_value: str, n_value: str, m_value: str) -> str:
    t = normalize_tnm_component(t_value, "t")
    n = normalize_tnm_component(n_value, "n")
    if n.startswith("n3"):
        n = "n3"
    elif n.startswith("n2"):
        n = "n2"
    elif n.startswith("n1"):
        n = "n1"
    m = normalize_tnm_component(m_value, "m") or "mx"

    if m.startswith("m1"):
        return "Stage IV"
    if n in {"", "nx"} or m in {"", "mx"}:
        return NULL_VALUE
    return compute_stage(t, n, m)


def extract_tnm_candidates(text: str, ipp_meta: Optional[IppMetadata]) -> list[TnmCandidate]:
    candidates: list[TnmCandidate] = []
    seen: set[tuple[str, str, str]] = set()

    strict_matches = list(TNM_PATTERN.finditer(text))
    use_loose_fallback = not strict_matches
    pattern_matches = strict_matches if strict_matches else list(TNM_LOOSE_PATTERN.finditer(text))
    for match in pattern_matches:
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        t_token = match.group(1) or ""
        n_token = match.group(2) or ""
        m_token = match.group(3) or ""

        if use_loose_fallback:
            irm_t_candidates = [candidate.group(1) for candidate in T_IRM_PATTERN.finditer(raw)]
            if irm_t_candidates:
                t_token = irm_t_candidates[-1]
            else:
                all_t_candidates = [candidate.group(1) for candidate in T_TOKEN_PATTERN.finditer(raw)]
                if all_t_candidates:
                    t_token = all_t_candidates[-1]

        t_value = normalize_tnm_component(t_token, "t")
        n_value = normalize_tnm_component(n_token, "n")
        m_value = normalize_tnm_component(m_token, "m")
        key = (t_value, n_value, m_value)
        if key in seen:
            continue
        seen.add(key)

        stage_value = compute_stage(t_value, n_value, m_value)
        if (
            PROSTATE_CONTEXT_PATTERN.search(text)
            and n_value == "nx"
            and detect_nodal_uncertain_signal(text) == "yes"
        ):
            stage_value = NULL_VALUE

        candidates.append(
            TnmCandidate(
                raw=raw,
                t=t_value or NULL_VALUE,
                n=n_value or NULL_VALUE,
                m=m_value or "mx",
                stage=stage_value,
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


def parse_breslow_mm(raw_value: str) -> Optional[float]:
    token = (raw_value or "").strip().replace(",", ".")
    if not token:
        return None
    if "." not in token and token.startswith("0") and len(token) > 1:
        return int(token) / (10 ** (len(token) - 1))
    try:
        return float(token)
    except ValueError:
        return None


def extract_breslow_raw_value(match: re.Match) -> Optional[str]:
    return match.group(1) or match.group(2)


def breslow_t_category_with_ulceration(mm: float, ulcerated: bool) -> str:
    if mm <= 1.0:
        return "t1b" if ulcerated else "t1a"
    if mm <= 2.0:
        return "t2b" if ulcerated else "t2a"
    if mm <= 4.0:
        return "t3b" if ulcerated else "t3a"
    return "t4b" if ulcerated else "t4a"


def infer_n_from_nodal_context(text: str) -> str:
    if detect_nodal_positive_signal(text) == "yes":
        return "n1"
    if detect_nodal_uncertain_signal(text) == "yes":
        return "nx"
    return "n0"


def extract_breslow_stage(text: str, ulcerated: bool, m_value: str) -> Optional[tuple[str, str, str, str, str]]:
    values: list[tuple[float, str]] = []
    for match in BRESLOW_PATTERN.finditer(text):
        raw_value = extract_breslow_raw_value(match)
        if raw_value is None:
            continue
        mm = parse_breslow_mm(raw_value)
        if mm is None:
            continue
        values.append((mm, re.sub(r"\s+", " ", match.group(0)).strip()))
    if not values:
        return None

    mm, raw = max(values, key=lambda item: item[0])
    t_value = breslow_t_category_with_ulceration(mm, ulcerated)
    n_value = infer_n_from_nodal_context(text)
    stage = compute_melanoma_stage(t_value, n_value, m_value, ulcerated)
    if stage == NULL_VALUE:
        return None
    return raw, t_value, n_value, m_value, stage


def extract_melanoma_t_category_stage(text: str, metastasis_detected: str) -> Optional[tuple[str, str, str, str, str]]:
    if not MELANOMA_CONTEXT_PATTERN.search(text):
        return None
    if metastasis_detected == "yes":
        return None

    t_values = extract_axis_values(text, T_COMPONENT_PATTERN, "t")
    t_values = [value for value in t_values if value and value not in {"tx"}]
    if not t_values:
        return None

    t_value = max(t_values, key=t_component_rank)
    n_value = infer_n_from_nodal_context(text)
    m_value = "m0"
    stage = compute_melanoma_stage(t_value, n_value, m_value, ulcerated=False)
    if stage == NULL_VALUE:
        return None
    return f"{t_value.upper()} (melanoma T category inferred N0M0)", t_value, n_value, m_value, stage


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


def extract_prostate_t_only_stage(text: str, metastasis_detected: str) -> Optional[tuple[str, str, str, str, str]]:
    if not PROSTATE_CONTEXT_PATTERN.search(text):
        return None
    if metastasis_detected == "yes":
        return None
    has_nodal_positive = detect_nodal_positive_signal(text) == "yes"
    has_nodal_uncertain = detect_nodal_uncertain_signal(text) == "yes"
    if has_nodal_positive:
        return None

    irm_t_candidates = [
        normalize_tnm_component(item.group(1), "t")
        for item in T_IRM_PATTERN.finditer(text)
        if not token_is_post_treatment(item.group(1), "t")
    ]
    t_values = [value for value in irm_t_candidates if value and value not in {"tx"}]
    if not t_values:
        all_t_values = extract_axis_values(text, T_COMPONENT_PATTERN, "t")
        t_values = [value for value in all_t_values if value and value not in {"tx"}]
    if not t_values:
        return None

    t_value = max(t_values, key=t_component_rank)
    n_value = "nx" if has_nodal_uncertain else "n0"
    m_value = "m0"
    stage = NULL_VALUE if n_value == "nx" else compute_stage(t_value, n_value, m_value)
    if stage == NULL_VALUE:
        return f"{t_value.upper()} (prostate inferred NxM0)", t_value, n_value, m_value, stage

    raw = f"{t_value.upper()} (prostate inferred N0M0)"
    return raw, t_value, n_value, m_value, stage


def parse_date_sort_key(value: str) -> str:
    return value if value and value != NULL_VALUE else "99999999"


def normalize_diag_date_token(value: str) -> str:
    token = (value or "").strip()[:10].replace("-", "")
    return token if re.fullmatch(r"\d{8}", token) else ""


def is_on_or_after(date_value: str, ref_value: str) -> bool:
    if not (date_value and ref_value):
        return False
    return parse_date_sort_key(date_value) >= parse_date_sort_key(ref_value)


def is_within_days(date_value: str, ref_value: str, max_days: int) -> bool:
    try:
        d = datetime.strptime(date_value, "%Y%m%d")
        r = datetime.strptime(ref_value, "%Y%m%d")
    except Exception:
        return False
    return 0 <= (d - r).days <= max_days


def is_date_in_window(date_str: str, center_str: Optional[str], days: int = 62) -> bool:
    if not center_str or center_str == NULL_VALUE:
        return True
    try:
        d = datetime.strptime(date_str, "%Y%m%d")
        c = datetime.strptime(center_str, "%Y%m%d")
    except Exception:
        return False
    return abs((d - c).days) <= days


def is_date_in_forward_window(date_str: str, start_str: Optional[str], days: int = 90) -> bool:
    if not start_str or start_str == NULL_VALUE:
        return True
    try:
        d = datetime.strptime(date_str, "%Y%m%d")
        s = datetime.strptime(start_str, "%Y%m%d")
    except Exception:
        return False
    return 0 <= (d - s).days <= days


def extract_breast_pathological_tnm(text: str) -> Optional[tuple[str, str, str, str]]:
    matches = list(BREAST_PATHOLOGICAL_TNM_PATTERN.finditer(text))
    if not matches:
        return None
    best = None
    best_score = -1
    for match in matches:
        t = normalize_tnm_component(match.group(1) or "", "t")
        n = normalize_tnm_component(match.group(2) or "", "n")
        m = normalize_tnm_component(match.group(3) or "", "m") or "mx"
        score = sum(1 for v in (t, n, m) if v and v not in {"tx", "nx", "mx"})
        if score > best_score:
            best = (re.sub(r"\s+", " ", match.group(0)).strip(), t, n, m)
            best_score = score
    return best


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
    pdf_path = metadata_to_pdf_path(metadata_path)
    return MetadataIndex(
        ipp=metadata_to_ipp(metadata, metadata_path),
        metadata_file=metadata_path,
        document_date=metadata_to_date(metadata, pdf_path),
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
    pdf_path = metadata_to_pdf_path(metadata_path)
    document_date = metadata_to_date(metadata, pdf_path)
    visit_number = metadata_to_visit_number(metadata)
    document_kind = detect_document_kind(metadata, metadata_path, pdf_path)

    if is_excluded_document(metadata):
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
            status="filtered_out",
            reason="Document excluded: Dossier Anesthesie",
            all_tnm_matches="",
            document_kind=document_kind,
            tnm_context="excluded",
            treatment_detected="no",
            treatment_keywords="",
            surgery_detected="no",
            chemo_detected="no",
            radiotherapy_detected="no",
            metastasis_detected="no",
            stage_confidence="high",
        )

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
    metadata_organe = str(ipp_meta.organe).strip().upper() if ipp_meta is not None else ""
    is_prostate = bool(PROSTATE_CONTEXT_PATTERN.search(text)) or metadata_organe == "PROSTATE"
    is_breast = (
        bool(BREAST_CONTEXT_PATTERN.search(text)) or metadata_organe == "SEIN"
    ) and not is_prostate
    if metastasis_detected == "yes" and is_breast_regional_nodal_only_metastasis(text):
        metastasis_detected = "no"
    if is_breast and metastasis_detected == "yes" and not breast_has_distant_metastasis_signal(text):
        metastasis_detected = "no"
    is_melanoma = bool(MELANOMA_CONTEXT_PATTERN.search(text))
    melanoma_nodal_signal = detect_melanoma_nodal_signal(text) if is_melanoma else "unknown"
    has_imaging_evidence = detect_imaging_evidence(text, document_kind) if is_melanoma else False
    ulcerated = bool(ULCERATION_PATTERN.search(text)) if is_melanoma else False
    melanoma_meta_confirmed = detect_melanoma_metastasis_confirmed(text) if is_melanoma else False

    if is_melanoma and melanoma_meta_confirmed:
        metastasis_detected = "yes"
    elif is_melanoma and MELANOMA_SURVEILLANCE_PATTERN.search(text):
        metastasis_detected = "no"
    elif is_melanoma and metastasis_detected == "yes":
        if MELANOMA_WEAK_CERTAINTY_PATTERN.search(text) or MELANOMA_EXCLUSION_PATTERN.search(text):
            metastasis_detected = "no"

    if is_melanoma and melanoma_nodal_signal == "non_regional":
        metastasis_detected = "yes"

    if is_melanoma and melanoma_meta_confirmed:
        return DocumentResult(
            ipp=ipp,
            metadata_file=str(metadata_path),
            pdf_file=str(pdf_path),
            document_date=document_date,
            visit_number=visit_number,
            text_length=len(text),
            tnm_raw="melanoma_metastatic_signal_confirmed",
            t=NULL_VALUE,
            n=NULL_VALUE,
            m="m1",
            stage=metastatic_stage(),
            status="stage_found",
            reason="Melanoma metastasis confirmed",
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

    if metastasis_detected == "yes":
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
            reason="Metastatic mention found in document",
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

    if is_breast:
        breast_ptnm = extract_breast_pathological_tnm(text)
        if breast_ptnm is not None:
            raw_ptnm, t_ptnm, n_ptnm, m_ptnm = breast_ptnm
            return DocumentResult(
                ipp=ipp,
                metadata_file=str(metadata_path),
                pdf_file=str(pdf_path),
                document_date=document_date,
                visit_number=visit_number,
                text_length=len(text),
                tnm_raw=raw_ptnm,
                t=t_ptnm,
                n=n_ptnm,
                m=m_ptnm,
                stage=compute_breast_stage(t_ptnm, n_ptnm, m_ptnm),
                status="stage_found",
                reason="Breast pTN detected in text (priority over document kind)",
                all_tnm_matches="",
                document_kind=document_kind,
                tnm_context="breast_pathological_ptnm",
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

    if is_melanoma and metastasis_detected == "no" and melanoma_nodal_signal != "positive":
        m_value = "m0"
        breslow_stage = extract_breslow_stage(text, ulcerated, m_value)
        if breslow_stage is not None:
            raw_breslow, t_value, n_value, m_value, stage = breslow_stage
            return DocumentResult(
                ipp=ipp,
                metadata_file=str(metadata_path),
                pdf_file=str(pdf_path),
                document_date=document_date,
                visit_number=visit_number,
                text_length=len(text),
                tnm_raw=raw_breslow,
                t=t_value,
                n=n_value,
                m=m_value,
                stage=stage,
                status="stage_found",
                reason="Melanoma context: stage inferred from Breslow with implicit N0/M0 when no nodal/metastatic signal is found",
                all_tnm_matches="",
                document_kind=document_kind,
                tnm_context="breslow_fallback",
                treatment_detected=treatment_detected,
                treatment_keywords=treatment_keywords,
                surgery_detected=surgery_detected,
                chemo_detected=chemo_detected,
                radiotherapy_detected=radiotherapy_detected,
                metastasis_detected=metastasis_detected,
            )
        melanoma_t_stage = extract_melanoma_t_category_stage(text, metastasis_detected)
        if melanoma_t_stage is not None:
            raw_tnm, t_value, n_value, m_value, stage = melanoma_t_stage
            return DocumentResult(
                ipp=ipp,
                metadata_file=str(metadata_path),
                pdf_file=str(pdf_path),
                document_date=document_date,
                visit_number=visit_number,
                text_length=len(text),
                tnm_raw=raw_tnm,
                t=t_value,
                n=n_value,
                m=m_value,
                stage=stage,
                status="stage_found",
                reason="Melanoma context: stage inferred from explicit T category with implicit N0/M0",
                all_tnm_matches="",
                document_kind=document_kind,
                tnm_context="breslow_fallback",
                treatment_detected=treatment_detected,
                treatment_keywords=treatment_keywords,
                surgery_detected=surgery_detected,
                chemo_detected=chemo_detected,
                radiotherapy_detected=radiotherapy_detected,
                metastasis_detected=metastasis_detected,
            )

    candidates = extract_tnm_candidates(text, ipp_meta)
    if is_prostate:
        candidates = [
            candidate for candidate in candidates
            if not is_post_treatment_context(candidate.context)
            and not has_post_treatment_tnm_prefix(candidate.raw)
        ]
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
        prostate_t_only = extract_prostate_t_only_stage(text, metastasis_detected)
        if prostate_t_only is not None:
            raw_tnm, t_value, n_value, m_value, stage = prostate_t_only
            return DocumentResult(
                ipp=ipp,
                metadata_file=str(metadata_path),
                pdf_file=str(pdf_path),
                document_date=document_date,
                visit_number=visit_number,
                text_length=len(text),
                tnm_raw=raw_tnm,
                t=t_value,
                n=n_value,
                m=m_value,
                stage=stage,
                status="stage_found" if stage != NULL_VALUE else "tnm_found_stage_unknown",
                reason=(
                    "Prostate T-only context; inferred N0/M0 from same-document absence of positive nodal/metastatic signals"
                    if stage != NULL_VALUE
                    else "Prostate T-only context with suspicious nodal signal; kept N as Nx and did not derive stage"
                ),
                all_tnm_matches="",
                document_kind=document_kind,
                tnm_context="clinical",
                treatment_detected=treatment_detected,
                treatment_keywords=treatment_keywords,
                surgery_detected=surgery_detected,
                chemo_detected=chemo_detected,
                radiotherapy_detected=radiotherapy_detected,
                metastasis_detected=metastasis_detected,
            )

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

        fallback_m = "m0" if has_imaging_evidence else "mx" if is_melanoma else "m0"
        breslow_stage = extract_breslow_stage(text, ulcerated if is_melanoma else False, fallback_m)
        if breslow_stage is not None:
            raw_breslow, t_value, n_value, m_value, stage = breslow_stage
            return DocumentResult(
                ipp=ipp,
                metadata_file=str(metadata_path),
                pdf_file=str(pdf_path),
                document_date=document_date,
                visit_number=visit_number,
                text_length=len(text),
                tnm_raw=raw_breslow,
                t=t_value,
                n=n_value,
                m=m_value,
                stage=stage,
                status="stage_found",
                reason="Stage inferred from Breslow thickness fallback",
                all_tnm_matches="",
                document_kind=document_kind,
                tnm_context="breslow_fallback",
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

    final_stage, final_n, final_m, used_assumed_completion = derive_stage_from_partial_tnm(
        stage=chosen.stage,
        t_value=chosen.t,
        n_value=chosen.n,
        m_value=chosen.m,
        tnm_context=chosen.context,
        text=text,
        metastasis_detected=metastasis_detected,
    )

    # Alignement branche prostate debug:
    # si TNM présent mais non stadifiable, tenter l'inférence N0/M0 (hors signal nodal+/métastatique).
    if final_stage == NULL_VALUE and is_prostate and chosen.t not in {"", NULL_VALUE, "tx"}:
        n_known = final_n not in {"", NULL_VALUE, "nx"}
        m_known = normalize_tnm_component(final_m, "m") in {"m0", "m1", "m1a", "m1b", "m1c"}
        if (
            n_known
            or (
                detect_nodal_positive_signal(text) != "yes"
                and detect_nodal_uncertain_signal(text) != "yes"
            )
        ) and (m_known or metastasis_detected != "yes"):
            n_try = final_n if n_known else "n0"
            m_try = final_m if m_known else "m0"
            derived = compute_stage(chosen.t, n_try, m_try)
            if derived != NULL_VALUE:
                final_stage = derived
                final_n = n_try
                final_m = m_try
                used_assumed_completion = True

    return DocumentResult(
        ipp=ipp,
        metadata_file=str(metadata_path),
        pdf_file=str(pdf_path),
        document_date=document_date,
        visit_number=visit_number,
        text_length=len(text),
        tnm_raw=chosen.raw,
        t=chosen.t,
        n=final_n,
        m=final_m,
        stage=final_stage,
        status="stage_found" if final_stage != NULL_VALUE else "tnm_found_stage_unknown",
        reason=(
            "TNM extracted and stage computed"
            if final_stage != NULL_VALUE and not used_assumed_completion
            else (
                "Partial TNM inferred with assumed N0/M0 to derive baseline stage"
                if final_stage != NULL_VALUE and used_assumed_completion
                else "TNM extracted but organ-specific stage mapping returned null"
            )
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


def first_metastatic_signal_date(results: list[DocumentResult]) -> Optional[str]:
    dates = sorted(
        {
            row.document_date
            for row in results
            if has_valid_date(row.document_date)
            and (
                row.metastasis_detected == "yes"
                or row.tnm_context == "metastatic_clinical"
                or row.m.startswith("m1")
                or row.stage == "Stage IV"
            )
        }
    )
    return dates[0] if dates else None


def baseline_sort_key(row: DocumentResult, document_priority: dict[str, int]) -> tuple[str, int, int, int]:
    major, minor = stage_rank(row.stage)
    return (
        parse_date_sort_key(row.document_date),
        document_priority.get(row.document_kind, 9),
        -major,
        -minor,
    )


def tnm_completeness_score(row: DocumentResult) -> int:
    score = 0
    if row.t not in {"", NULL_VALUE, "tx"}:
        score += 1
    if row.n not in {"", NULL_VALUE, "nx"}:
        score += 1
    if row.m not in {"", NULL_VALUE, "mx"}:
        score += 1
    return score


def document_kind_priority(kind: str) -> int:
    priority = {
        "pathology": 0,
        "rcp": 1,
        "radiology": 2,
        "consultation": 3,
        "other": 4,
    }
    return priority.get(kind, 9)


def choose_surgery_first_pathologic_document(rows: list[DocumentResult]) -> Optional[DocumentResult]:
    candidates = [
        row
        for row in rows
        if row.tnm_context == "pathologic"
        and not is_post_treatment_context(row.tnm_context)
        and (
            row.stage != NULL_VALUE
            or (
                row.t not in {"", NULL_VALUE, "tx"}
                and row.n not in {"", NULL_VALUE, "nx"}
            )
        )
    ]
    if not candidates:
        return None

    pathology_dates = sorted(
        [row.document_date for row in rows if row.document_kind == "pathology" and row.document_date != NULL_VALUE]
    )
    first_pathology_date = pathology_dates[0] if pathology_dates else None
    if first_pathology_date is not None:
        pre_pathology_ptpn = [
            row
            for row in candidates
            if row_on_or_before(row, first_pathology_date)
            and row.t not in {"", NULL_VALUE, "tx"}
            and row.n not in {"", NULL_VALUE, "nx"}
        ]
        if pre_pathology_ptpn:
            return max(
                pre_pathology_ptpn,
                key=lambda row: (
                    tnm_completeness_score(row),
                    stage_rank(row.stage)[0],
                    stage_rank(row.stage)[1],
                    parse_date_sort_key(row.document_date),
                    1 if row.document_kind == "pathology" else 0,
                ),
            )

    # Surgery-first cohorts are expected to have the most informative baseline
    # in post-op pathology; prefer richer TNM and more specific stage labels.
    return max(
        candidates,
        key=lambda row: (
            tnm_completeness_score(row),
            stage_rank(row.stage)[0],
            stage_rank(row.stage)[1],
            parse_date_sort_key(row.document_date),
            1 if row.document_kind == "pathology" else 0,
        ),
    )


def derive_stage_from_partial_tnm(
    *,
    stage: str,
    t_value: str,
    n_value: str,
    m_value: str,
    tnm_context: str,
    text: str,
    metastasis_detected: str,
) -> tuple[str, str, str, bool]:
    if stage != NULL_VALUE:
        return stage, n_value, m_value, False

    t_known = t_value not in {"", NULL_VALUE, "tx"}
    n_known = n_value not in {"", NULL_VALUE, "nx"}
    m_known = m_value not in {"", NULL_VALUE, "mx"}
    if not t_known:
        return stage, n_value, m_value, False

    # Existing surgery-first/pathology behavior: pT + pN with missing M -> assume M0.
    if tnm_context == "pathologic" and n_known and not m_known:
        derived_stage = compute_stage(t_value, n_value, "m0")
        if derived_stage != NULL_VALUE:
            return derived_stage, n_value, "m0", True

    is_prostate_case = bool(PROSTATE_CONTEXT_PATTERN.search(text))
    if not is_prostate_case:
        return stage, n_value, m_value, False

    # New fallback for partially specified TNM (e.g., explicit T only):
    # if there is no positive nodal/metastatic signal in the same document,
    # assume N0/M0 to avoid waiting for a later document.
    if not n_known and (
        detect_nodal_positive_signal(text) == "yes" or detect_nodal_uncertain_signal(text) == "yes"
    ):
        return stage, n_value, m_value, False
    if not m_known and metastasis_detected == "yes":
        return stage, n_value, m_value, False

    inferred_n = n_value if n_known else "n0"
    inferred_m = m_value if m_known else "m0"
    derived_stage = compute_stage(t_value, inferred_n, inferred_m)
    if derived_stage == NULL_VALUE:
        return stage, n_value, m_value, False
    return derived_stage, inferred_n, inferred_m, True


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
    is_breast_case = bool(ipp_meta and str(ipp_meta.organe).strip().upper() == "SEIN")
    is_melanoma_case = bool(ipp_meta and str(ipp_meta.organe).strip().upper() == "MELANOME")

    if is_melanoma_case:
        valid = [row for row in ordered if row.stage != NULL_VALUE]
        metastatic_events = [row for row in valid if row.tnm_context == "metastatic_clinical" or row.stage == "Stage IV"]
        first_metastatic_date = min((row.document_date for row in metastatic_events), default=None)
        breslow_hits = [row for row in valid if row.tnm_context == "breslow_fallback"]
        if breslow_hits:
            if first_metastatic_date is not None:
                pre_meta = [
                    row for row in breslow_hits
                    if parse_date_sort_key(row.document_date) <= parse_date_sort_key(first_metastatic_date)
                ]
                if pre_meta:
                    chosen = min(pre_meta, key=lambda row: parse_date_sort_key(row.document_date))
                else:
                    chosen = min(breslow_hits, key=lambda row: parse_date_sort_key(row.document_date))
            else:
                chosen = min(breslow_hits, key=lambda row: parse_date_sort_key(row.document_date))

            window_hits = [
                row for row in ordered
                if row.tnm_context in {"breslow_fallback", "metastatic_clinical"}
                and is_date_in_forward_window(row.document_date, chosen.document_date, days=90)
            ]
            has_metastatic_window = any(
                row.tnm_context == "metastatic_clinical" or row.stage == "Stage IV"
                for row in window_hits
            )
            if has_metastatic_window:
                metastatic = min(
                    [row for row in window_hits if row.tnm_context == "metastatic_clinical" or row.stage == "Stage IV"],
                    key=lambda row: parse_date_sort_key(row.document_date),
                )
                return metastatic, "melanoma_breslow_plus90d_metastatic"

            n_values = [normalize_tnm_component(row.n, "n") for row in window_hits]
            if any(n in {"n1", "n1a", "n1b", "n2", "n2a", "n2b", "n3", "n3a", "n3b", "n3c"} for n in n_values):
                chosen_n = next(n for n in n_values if n in {"n1", "n1a", "n1b", "n2", "n2a", "n2b", "n3", "n3a", "n3b", "n3c"})
                return replace(chosen, n=chosen_n, m="m0", stage=compute_melanoma_stage(chosen.t, chosen_n, "m0", False)), "melanoma_breslow_plus90d_nodal_positive"

            return replace(
                chosen,
                n="n0",
                m="m0",
                stage=compute_melanoma_stage(chosen.t, "n0", "m0", False),
            ), "melanoma_breslow_plus90d_implicit_n0m0"

        non_meta = [row for row in valid if row.tnm_context != "metastatic_clinical" and row.stage != "Stage IV"]
        if non_meta:
            return min(non_meta, key=lambda row: parse_date_sort_key(row.document_date)), "melanoma_non_metastatic_fallback"

        non_iv = [row for row in valid if row.stage != "Stage IV"]
        if non_iv:
            return max(non_iv, key=lambda row: stage_rank(row.stage)), "melanoma_best_non_iv"

    if is_breast_case:
        breast_hits = [row for row in ordered if row.stage != NULL_VALUE]
        if breast_hits:
            diag_ref = ""
            if ipp_meta is not None:
                diag_ref = normalize_diag_date_token(ipp_meta.date_diag_tkc) or normalize_diag_date_token(ipp_meta.date_diag_dcc)
            diag_opt = diag_ref or None

            if diag_opt is not None:
                breast_ptnm_3m = [
                    row
                    for row in breast_hits
                    if row.tnm_context == "breast_pathological_ptnm"
                    and is_date_in_forward_window(row.document_date, diag_opt, days=90)
                ]
                if breast_ptnm_3m:
                    return min(breast_ptnm_3m, key=lambda row: parse_date_sort_key(row.document_date)), "breast_first_ptnm_within_3m_post_diag"

            breast_window = [row for row in breast_hits if is_date_in_window(row.document_date, diag_opt, days=62)]
            breast_pool = breast_window if breast_window else breast_hits

            m0_hits = [row for row in breast_pool if normalize_tnm_component(row.m, "m") == "m0"]
            metastatic_events_in_window = [
                row for row in breast_pool
                if row.tnm_context == "metastatic_clinical" or row.metastasis_detected == "yes"
            ]
            forced_m_value = "m0"
            if not m0_hits and metastatic_events_in_window:
                forced_m_value = "m1"

            if forced_m_value == "m1" and metastatic_events_in_window:
                chosen = min(metastatic_events_in_window, key=lambda row: parse_date_sort_key(row.document_date))
                return chosen, "breast_window_m1_priority"

            breast_pool_recent = sorted(
                breast_pool,
                key=lambda row: (parse_date_sort_key(row.document_date), row.document_kind),
                reverse=True,
            )

            for row in breast_pool_recent:
                if row.tnm_context == "breast_pathological_ptnm":
                    stage = compute_breast_stage(row.t, row.n, forced_m_value)
                    return replace(row, m=forced_m_value, stage=stage), "breast_window_ptnm_recent"

            for row in breast_pool_recent:
                if row.tnm_context in {"clinical", "unknown"} and row.document_kind in {"consultation", "rcp"}:
                    stage = compute_breast_stage(row.t, row.n, forced_m_value)
                    return replace(row, m=forced_m_value, stage=stage), "breast_window_recent_tnm_cs_rcp"

            non_meta_breast = [
                row for row in breast_hits
                if not (row.tnm_context == "metastatic_clinical" or row.metastasis_detected == "yes")
            ]
            if non_meta_breast:
                chosen = max(
                    non_meta_breast,
                    key=lambda row: (
                        parse_date_sort_key(row.document_date),
                        -({"pathology": 0, "rcp": 1, "consultation": 2, "radiology": 3, "other": 4}.get(row.document_kind, 9)),
                    ),
                )
                return chosen, "breast_fallback"

    valid_non_post = [
        row for row in ordered if row.stage != NULL_VALUE and not is_post_treatment_context(row.tnm_context)
    ]
    first_metastatic_date = first_metastatic_signal_date(ordered)
    if first_metastatic_date is not None:
        pre_metastatic = [
            row
            for row in valid_non_post
            if row_on_or_before(row, first_metastatic_date) and row.stage != "Stage IV"
        ]
        if pre_metastatic:
            pre_treatment = [row for row in pre_metastatic if not is_post_treatment_context(row.tnm_context)]
            pool = pre_treatment if pre_treatment else pre_metastatic
            chosen = min(
                pool,
                key=lambda row: (
                    parse_date_sort_key(row.document_date),
                    document_kind_priority(row.document_kind),
                    -tnm_completeness_score(row),
                ),
            )
            return chosen, "pre_metastatic_baseline_priority"

    first_rcp = next((row for row in ordered if row.document_kind == "rcp"), None)
    if first_rcp is not None and first_rcp.metastasis_detected == "yes":
        return first_rcp, "first_rcp_metastatic_stage"

    treatment_mode, _, first_non_surgical_date = infer_first_treatment(ordered)

    if treatment_mode == "surgery_first":
        preferred_pathology = choose_surgery_first_pathologic_document(ordered)
        if preferred_pathology is not None:
            return preferred_pathology, "surgery_first_pathologic_tnm_precise"

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

    structured = [
        row for row in valid_non_post
        if row.t not in {"", NULL_VALUE, "tx"}
        and row.n not in {"", NULL_VALUE, "nx"}
    ]
    if structured:
        chosen = min(
            structured,
            key=lambda row: (
                parse_date_sort_key(row.document_date),
                document_kind_priority(row.document_kind),
                -tnm_completeness_score(row),
            ),
        )
        return chosen, "structured_tnm_first_chronological"

    pathology = [row for row in valid_non_post if row.document_kind == "pathology"]
    if pathology:
        chosen = max(
            pathology,
            key=lambda row: (
                tnm_completeness_score(row),
                stage_rank(row.stage)[0],
                stage_rank(row.stage)[1],
            ),
        )
        return chosen, "pathology_best_tnm"

    if valid_non_post:
        chosen = max(
            valid_non_post,
            key=lambda row: (stage_rank(row.stage)[0], stage_rank(row.stage)[1]),
        )
        return chosen, "best_available_stage"

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
    debug_hits: Optional[list[dict]] = None,
    diagnosis_date: Optional[str] = None,
) -> IppResult:
    chosen, selection_reason = choose_baseline_document(rows, ipp_meta)
    if debug_hits:
        filtered_debug_hits = [
            hit for hit in debug_hits
            if not has_post_treatment_tnm_prefix(str(hit.get("raw", "")))
        ]
        debug_selected = debug_engine.select_initial_stage(filtered_debug_hits, diagnosis_date=diagnosis_date)
        if debug_selected is not None:
            hit, debug_reason = debug_selected
            hit_pdf = hit.get("pdf", "")
            hit_date = hit.get("date", NULL_VALUE)
            mapped = next(
                (row for row in rows if Path(row.pdf_file).name == hit_pdf and row.document_date == hit_date),
                None,
            )
            if mapped is not None:
                chosen = replace(
                    mapped,
                    tnm_raw=str(hit.get("raw", mapped.tnm_raw)),
                    t=str(hit.get("t", mapped.t)),
                    n=str(hit.get("n", mapped.n)),
                    m=str(hit.get("m", mapped.m)),
                    stage=str(hit.get("stage", mapped.stage)),
                )
                selection_reason = debug_reason
            else:
                chosen = replace(
                    rows[-1],
                    tnm_raw=str(hit.get("raw", NULL_VALUE)),
                    t=str(hit.get("t", NULL_VALUE)),
                    n=str(hit.get("n", NULL_VALUE)),
                    m=str(hit.get("m", NULL_VALUE)),
                    stage=str(hit.get("stage", NULL_VALUE)),
                    document_date=str(hit.get("date", NULL_VALUE)),
                    reason="Selected by debug engine",
                )
                selection_reason = debug_reason
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
        stage_confidence=chosen.stage_confidence,
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
        debug_hits: list[dict] = []
        debug_args = types.SimpleNamespace(only_stage_hits=True, show_text=False)
        total_docs = len(metadata_entries)
        for metadata_entry in metadata_entries:
            result = build_document_result(metadata_entry.metadata_file, ipp_meta)
            document_results.append(result)
            metadata = load_metadata(metadata_entry.metadata_file)
            pdf_path = metadata_to_pdf_path(metadata_entry.metadata_file)
            doc_hits = debug_engine.process_document(
                idx=len(document_results),
                total=total_docs,
                metadata=metadata,
                metadata_path=metadata_entry.metadata_file,
                pdf_path=pdf_path,
                args=debug_args,
            )
            debug_hits.extend(doc_hits)
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

        diagnosis_date = None
        if ipp_meta is not None:
            diagnosis_date = normalize_diag_date_token(ipp_meta.date_diag_tkc) or normalize_diag_date_token(ipp_meta.date_diag_dcc) or None
        ipp_result = build_ipp_result(
            document_results,
            args.ipp_strategy,
            ipp_meta,
            debug_hits=debug_hits,
            diagnosis_date=diagnosis_date,
        )
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

