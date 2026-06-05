from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    import fitz  # type: ignore
except ImportError:
    fitz = None

try:
    from PyPDF2 import PdfReader  # type: ignore
except ImportError:
    PdfReader = None


LOGGER = logging.getLogger("tnm_debug")
VERSION_FLAG = "STABLE"

# =============================================================================
# REGEX PATTERNS
# =============================================================================

TNM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyrai]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"((?:[cpyrai]{0,4})?m(?:x|0|1[abcd]?)?)?",
    re.IGNORECASE,
)
TNM_LOOSE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:[\s\S]{0,120}?)"
    r"((?:[cpyrai]{0,4})?n(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:[\s\S]{0,80}?)"
    r"((?:[cpyrai]{0,4})?m(?:x|0|1[abcd]?))",
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
    r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?m(?:x|0|1[abcd]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
EXPLICIT_STAGE_PATTERN = re.compile(
    r"\b(?:stade|stage)\s*(?:ajcc\s*)?(0|iv|iii[abc]?|ii[abc]?|i[abc]?|1|2|3|4)\b",
    re.IGNORECASE,
)
EXPLICIT_STAGE_FALSE_POSITIVE_PATTERN = re.compile(
    r"\b(ptose(?:\s+mammaire)?|oms)\b",
    re.IGNORECASE,
)
EXPLICIT_STAGE_ONCO_CONTEXT_PATTERN = re.compile(
    r"\b(cancer|carcinom|tumeur|oncolog|tnm|ajcc|m[ée]tast|invasi|ad[ée]nocarcinom)\b",
    re.IGNORECASE,
)
BRESLOW_PATTERN = re.compile(
    r"(?:\bbreslow(?:\s*(?:de|:|=))?\s*([0-9]+(?:[.,][0-9]+)?)\s*mm\b|"
    r"\b([0-9]+(?:[.,][0-9]+)?)\s*mm\s+d[''][eé]paisseur\s+selon\s+breslow\b)",
    re.IGNORECASE,
)
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
DCIS_PATTERN = re.compile(
    r"\b(ccis|dcis|carcinome\s+canalaire\s+in\s+situ|carcinome\s+intracanalaire)\b",
    re.IGNORECASE,
)
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
NODAL_POSITIVE_PATTERN = re.compile(
    r"\b(metastase\s+ganglionnaire|adenopathie[s]?\s+secondaire[s]?|envahissement\s+ganglionnaire|"
    r"atteinte\s+ganglionnaire)\b",
    re.IGNORECASE,
)
NODAL_SUSPICIOUS_PATTERN = re.compile(
    r"\b(suspicion\s+(?:d[''’]\s*)?(?:atteinte\s+)?ganglionnaire|"
    r"atteinte\s+ganglionnaire\s+suspecte?|"
    r"ad[ée]nopathie[s]?\s+suspecte?s?|"
    r"ganglion(?:naire)?s?\s+suspecte?s?)\b",
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
PROSTATE_SUMMARY_TNM_PATTERN = re.compile(
    r"\b(t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))\b[\s,;:()\\/\-\n]{0,30}"
    r"\b(n(?:x|0|1mi|1[abc]?|2[ab]?|3[abc]?|o))\b[\s,;:()\\/\-\n]{0,30}"
    r"\b(m(?:x|0|1[abc]?|o))\b",
    re.IGNORECASE,
)
MELANOMA_CONTEXT_PATTERN = re.compile(r"\b(m[ée]lanome|breslow|clark|ssm)\b", re.IGNORECASE)

# --- Sein / Sénologie ---
BREAST_CONTEXT_PATTERN = re.compile(
    r"\b(sein|mammaire|s[eé]nologie|mastectomie|tumorectomie|quadrantectomie|"
    r"carcinome\s+canalaire|carcinome\s+lobulaire|her2|recepteur\s+(estrog|progest)|"
    r"grade\s+(sbr|eln)|ganglion\s+sentinelle\s+axillaire)\b",
    re.IGNORECASE,
)
BREAST_STRONG_CONTEXT_PATTERN = re.compile(
    r"\b(mammaire|s[eé]nologie|mastectomie|tumorectomie|quadrantectomie|"
    r"carcinome\s+canalaire|carcinome\s+lobulaire|her2|recepteur\s+(estrog|progest)|"
    r"grade\s+(sbr|eln)|ganglion\s+sentinelle\s+axillaire)\b",
    re.IGNORECASE,
)
NON_CLINICAL_BREAST_MENU_PATTERN = re.compile(
    r"\boncophone\s+sein[\s-]?gyn[ée]cologie\b",
    re.IGNORECASE,
)
# pTNM pathologique post-chirurgie sein : préfixe p obligatoire
BREAST_PATHOLOGICAL_TNM_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"(p\s*t\s*(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"(p?\s*n\s*(?:x|0|1mi|1(?:[abc]|sn)?|2[ab]?|3[abc]?))"
    r"(?:\s*[/,;:=-]?\s*)"
    r"(p?\s*m\s*(?:x|0|1[abc]?))?",
    re.IGNORECASE,
)
BREAST_PATHOLOGICAL_T_ONLY_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(p\s*t\s*(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))(?![A-Za-z0-9])",
    re.IGNORECASE,
)
DEBUG_BREAST_HISTOLOGY_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("NST", re.compile(r"\b(carcinome\s+canalaire\s+infiltrant|carcinome\s+infiltrant(?:\s+du\s+sein(?:\s+\w+)?)?\s+de\s+type\s+non\s+sp[eé]cifique|carcinome\s+infiltrant\s+nst|carcinome\s+mammaire\s+infiltrant\s+nst|carcinome\s+invasif\s+nst|carcinome\s+infiltrant\s+sans\s+autre\s+sp[eé]cification|carcinome\s+canalaire\s+invasif|cci)\b", re.IGNORECASE)),
    ("LOBULAR", re.compile(r"\b(carcinome\s+lobulaire\s+infiltrant|carcinome\s+lobulaire\s+invasif|lobulaire\s+infiltrant|lobulaire\s+invasif|cli)\b", re.IGNORECASE)),
    ("MUCINOUS", re.compile(r"\b(mucineux|collo[iï]de)\b", re.IGNORECASE)),
    ("TUBULAR", re.compile(r"\btubuleux\b", re.IGNORECASE)),
    ("CRIBRIFORM", re.compile(r"\bcribriforme\b", re.IGNORECASE)),
    ("PAPILLARY", re.compile(r"\bpapillaire\b", re.IGNORECASE)),
    ("MICROPAPILLARY", re.compile(r"\bmicropapillaire\b", re.IGNORECASE)),
    ("METAPLASTIC", re.compile(r"\b(m[eé]taplasique|sarcomato[iï]de|m[eé]senchymateux|sarcome)\b", re.IGNORECASE)),
    ("APOCRINE", re.compile(r"\bapocrine\b", re.IGNORECASE)),
    ("NEUROENDOCRINE", re.compile(r"\bneuroendocrine\b", re.IGNORECASE)),
    ("OTHER_SPECIFIED", re.compile(r"\b(ad[eé]no[iï]de\s+kystique|s[eé]cr[eé]toire|m[eé]dullaire)\b", re.IGNORECASE)),
]
DEBUG_BREAST_IN_SITU_PATTERN = re.compile(r"\b(carcinome\s+(?:canalaire|lobulaire)?\s*in\s+situ|ccis|clis)\b", re.IGNORECASE)
DEBUG_BREAST_INFILTRATING_PATTERN = re.compile(r"\b(infiltrant|invasif|invasive)\b", re.IGNORECASE)
DEBUG_BREAST_HISTOLOGY_EXCLUSION_PATTERN = re.compile(r"\b(absence\s+de\s+carcinome\s+infiltrant|ccis\s+seul|clis\s+seul|in\s+situ\s+pur)\b", re.IGNORECASE)
DEBUG_BREAST_GRADE_PATTERN = re.compile(r"\b(?:grade(?:\s+(?:sbr|histopronostique|histologique|tumoral))?|sbr|scarff\s+bloom\s+richardson|elston(?:\s+et)?\s+ellis)\s*(?:de\s+)?(?:grade\s*)?(i{1,3}|[123])\b", re.IGNORECASE)
DEBUG_BREAST_GRADE_DETAIL_PATTERN = re.compile(r"\b(?:grade(?:\s+(?:sbr|histopronostique|histologique|tumoral))?|sbr|elston(?:\s+et)?\s+ellis)[^\n\r()]{0,80}\(([123])\s*,\s*([123])\s*,\s*([123])\)", re.IGNORECASE)
DEBUG_BREAST_ER_MARKER = r"(?:\bre\b|\ber\b|r[ée]cepteurs?\s+(?:des\s+|aux\s+)?[œo]strog[eéè]nes?|r[ée]cepteurs?\s+estrog[eé]niques?)"
DEBUG_BREAST_PR_MARKER = r"(?:\brp\b|\bpr\b|r[ée]cepteurs?\s+(?:de\s+la\s+|[àa]\s+la\s+)?progest[eéè]rone|r[ée]cepteurs?\s+progest[eéè]roniques?)"
DEBUG_BREAST_ER_PERCENT_PATTERN = re.compile(DEBUG_BREAST_ER_MARKER + r"[\s\S]{0,120}?\b(100|[1-9]?[0-9])\s*%", re.IGNORECASE)
DEBUG_BREAST_PR_PERCENT_PATTERN = re.compile(DEBUG_BREAST_PR_MARKER + r"[\s\S]{0,120}?\b(100|[1-9]?[0-9])\s*%", re.IGNORECASE)
DEBUG_BREAST_ER_INTENSITY_PATTERN = re.compile(DEBUG_BREAST_ER_MARKER + r"[\s\S]{0,140}?\bintensit[eé]\s*(?:[:=]?\s*)?(0|\+\+\+|\+\+|\+|faible|mod[ée]r[ée]e?|forte?|intense)", re.IGNORECASE)
DEBUG_BREAST_PR_INTENSITY_PATTERN = re.compile(DEBUG_BREAST_PR_MARKER + r"[\s\S]{0,140}?\bintensit[eé]\s*(?:[:=]?\s*)?(0|\+\+\+|\+\+|\+|faible|mod[ée]r[ée]e?|forte?|intense)", re.IGNORECASE)
DEBUG_BREAST_ER_POSITIVE_PATTERN = re.compile(r"\b(re\+|er\+|re\s+positif|er\s+positif|r[ée]cepteurs?\s+(?:aux\s+)?[œo]strog[eè]nes?\s+positifs?|hormonor[ée]cepteur\s+positif)\b", re.IGNORECASE)
DEBUG_BREAST_ER_NEGATIVE_PATTERN = re.compile(r"\b(re-|er-|re\s+n[ée]gatif|er\s+n[ée]gatif|r[ée]cepteurs?\s+(?:aux\s+)?[œo]strog[eè]nes?\s+n[ée]gatifs?)\b", re.IGNORECASE)
DEBUG_BREAST_PR_POSITIVE_PATTERN = re.compile(r"\b(rp\+|pr\+|rp\s+positif|pr\s+positif|r[ée]cepteurs?\s+(?:[àa]\s+la\s+)?progest[ée]rone\s+positifs?)\b", re.IGNORECASE)
DEBUG_BREAST_PR_NEGATIVE_PATTERN = re.compile(r"\b(rp-|pr-|rp\s+n[ée]gatif|pr\s+n[ée]gatif|r[ée]cepteurs?\s+(?:[àa]\s+la\s+)?progest[ée]rone\s+n[ée]gatifs?)\b", re.IGNORECASE)
DEBUG_BREAST_RH_POSITIVE_PATTERN = re.compile(r"\b(rh\+|hr\+|rh\s+positif|hr\s+positif|r[ée]cepteurs?\s+hormonaux\s+positifs?|hormonod[ée]pendant|luminal)\b", re.IGNORECASE)
DEBUG_BREAST_RH_NEGATIVE_PATTERN = re.compile(r"\b(rh-|hr-|rh\s+n[ée]gatif|hr\s+n[ée]gatif|r[ée]cepteurs?\s+hormonaux\s+n[ée]gatifs?|non\s+hormonod[ée]pendant)\b", re.IGNORECASE)
DEBUG_BREAST_TRIPLE_NEGATIVE_PATTERN = re.compile(r"\b(triple\s+n[ée]gatif|triple-negative|tnbc)\b", re.IGNORECASE)
DEBUG_BREAST_HER2_IHC_PATTERN = re.compile(r"\b(?:her[\s-]?2|erbb2|c-?erbb2)\b[\s\S]{0,180}?\b(?:score\s*)?(0\+?|1\+|2\+|3\+)", re.IGNORECASE)
DEBUG_BREAST_HER2_POSITIVE_PATTERN = re.compile(r"\b(her[\s-]?2\+|her[\s-]?2\s+positif|her[\s-]?2\s+amplifi[ée]|surexpression\s+her[\s-]?2|her[\s-]?2\s+surexprim[ée])\b", re.IGNORECASE)
DEBUG_BREAST_HER2_LOW_PATTERN = re.compile(r"\b(her[\s-]?2\s*low|her[\s-]?2-low|her[\s-]?2\s+faible|her[\s-]?2\s+1\+|her[\s-]?2\s+2\+\s+non\s+amplifi[ée])\b", re.IGNORECASE)
DEBUG_BREAST_HER2_NEGATIVE_PATTERN = re.compile(r"\b(her[\s-]?2-|her[\s-]?2\s+n[ée]gatif|her[\s-]?2\s+non\s+amplifi[ée])\b", re.IGNORECASE)
DEBUG_BREAST_HER2_ISH_AMPLIFIED_PATTERN = re.compile(r"\b(?:her[\s-]?2|erbb2|c-?erbb2)\b[\s\S]{0,160}?\b(?:ish|fish|cish|sish|hybridation\s+in\s+situ)\b[\s\S]{0,160}?\b(amplifi[ée]|amplification|ratio\s+amplifi[ée]|positif)\b", re.IGNORECASE)
DEBUG_BREAST_HER2_ISH_NOT_AMPLIFIED_PATTERN = re.compile(r"\b(?:her[\s-]?2|erbb2|c-?erbb2)\b[\s\S]{0,160}?\b(?:ish|fish|cish|sish|hybridation\s+in\s+situ)\b[\s\S]{0,160}?\b(non\s+amplifi[ée]|absence\s+d['’]amplification|n[ée]gatif)\b", re.IGNORECASE)
DEBUG_BREAST_HER2_ULTRALOW_PATTERN = re.compile(r"\b(ultra-?low|marquage\s+membranaire\s+tr[eè]s\s+faible|marquage\s+incomplet\s+faible|her[\s-]?2\s+0\s+avec\s+marquage\s+faible)\b", re.IGNORECASE)
DEBUG_BREAST_HER2_NULL_PATTERN = re.compile(r"\b(her[\s-]?2\s+0|ihc\s+0|absence\s+totale\s+de\s+marquage|aucun\s+marquage\s+membranaire|her[\s-]?2\s+nul)\b", re.IGNORECASE)
DEBUG_BREAST_PDL1_CPS_PATTERN = re.compile(r"\b(?:pd[\s-]?l1|pd\s*l1)\b[\s\S]{0,120}?\b(?:cps|combined\s+positive\s+score|score\s+combin[ée]\s+positif)\s*(?:[=:]?\s*|[<>≥≤]\s*)([0-9]+)", re.IGNORECASE)
# Grade SBR/Elston-Ellis
GRADE_SBR_PATTERN = re.compile(
    r"\b(?:grade|gr\.?)\s*(?:sbr\s*)?([1-3])\b",
    re.IGNORECASE,
)
# Marqueurs de néoadjuvant (chimio AVANT chirurgie sein)
NEOADJUVANT_PATTERN = re.compile(
    r"\b(n[eé]oadjuvant|chimioth[eé]rapie\s+premi[eè]re|traitement\s+premi[eè]r|"
    r"traitement\s+n[eé]oadjuvant|yptnm|ypt|ypn)\b",
    re.IGNORECASE,
)

# --- Mélanome ---
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
MELANOMA_M1D_PATTERN = re.compile(
    r"\b(c[eéè]r[eéè]bral|c[eéè]r[eéè]brale|c[eéè]r[eéè]brales|cerveau|enc[eéè]phal|m[eéè]ning[eéè]?)\b",
    re.IGNORECASE,
)
MELANOMA_M1B_PATTERN = re.compile(r"\b(pulmonaire|pulmonaires|poumon)\b", re.IGNORECASE)
MELANOMA_M1C_PATTERN = re.compile(
    r"\b(visc[ée]ral|visc[ée]rale|visc[ée]rales|foie|h[ée]patique|h[ée]patiques|"
    r"osseuse|osseuses|os|surr[ée]nale|p[ée]riton[ée]ale|pleurale)\b",
    re.IGNORECASE,
)
MELANOMA_M1A_PATTERN = re.compile(
    r"\b(ganglion\s+non\s+r[ée]gional|ganglionnaire\s+[àa]\s+distance|ad[ée]nopathie\s+[àa]\s+distance|"
    r"cutan[ée]e\s+[àa]\s+distance|sous[- ]?cutan[ée]e\s+[àa]\s+distance|musculaire\s+[àa]\s+distance|"
    r"m[ée]diastin|hilaire|r[ée]tro[- ]?p[ée]riton|lombo[- ]?aort|para[- ]?aort)\b",
    re.IGNORECASE,
)
MELANOMA_LDH_HIGH_PATTERN = re.compile(
    r"\bldh\b.{0,40}\b([eéè]lev[eéè]e?s?|augment[eéè]e?s?|sup[eéè]rieur(?:e)?s?\s+[àa]|haute?s?)\b",
    re.IGNORECASE,
)
MELANOMA_LDH_NORMAL_PATTERN = re.compile(
    r"\bldh\b.{0,40}\b(normale?s?|non\s+[eéè]lev[eéè]e?s?|dans\s+les\s+normes)\b",
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
MELANOMA_WEAK_CERTAINTY_PATTERN = re.compile(
    r"\b(suspicion|suspecte?|possible|probable|douteux|douteuse|compatible\s+avec|[àa]\s+contr[oô]ler)\b",
    re.IGNORECASE,
)
MELANOMA_EXCLUSION_PATTERN = re.compile(
    r"\b(r[ée]actionnel|inflammatoire|stable\s+non\s+suspect|non\s+suspect|b[ée]nin|cicatriciel|post[\s-]?op[ée]ratoire|post[\s-]?th[ée]rapeutique)\b",
    re.IGNORECASE,
)
MELANOMA_SENTINEL_MAPPING_PATTERN = re.compile(
    r"\b(ganglion[s]?\s+(?:intens[ée]ment\s+)?fixant[s]?|radiotraceur|gamma\s+cam[ée]ra|"
    r"sonde\s+de\s+d[ée]tection|migration\s+cervicale|lymphoscintigraphie|"
    r"ganglion[s]?\s+sentinelle[s]?)\b",
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
IMAGING_EVIDENCE_PATTERN = re.compile(
    r"\b(scanner|irm|pet[\s-]?scan|pet[\s-]?scanner|tep|imagerie|bilan\s+d['']extension|echo(graphie)?)\b",
    re.IGNORECASE,
)
ULCERATION_PATTERN = re.compile(
    r"\b(ulc[eé]r[eé]|largement\s+ulc[eé]r[eé]|ulc[eé]ration)\b",
    re.IGNORECASE,
)
ULCERATION_ABSENT_PATTERN = re.compile(
    r"\b(absence\s+d[''’]?\s*ulc[ée]ration|sans\s+ulc[ée]ration|non\s+ulc[ée]r[ée]|"
    r"ulc[ée]ration\s+absente|pas\s+d[''’]?\s*ulc[ée]ration)\b",
    re.IGNORECASE,
)

# --- Marqueurs de traitement (toutes localisations) ---
# Permettent de détecter si un document est POST-traitement
TREATMENT_STARTED_PATTERN = re.compile(
    r"\b("
    r"apr[eè]s\s+(chirurgie|op[eé]ration|traitement|chimioth[eé]rapie|radioth[eé]rapie|immunoth[eé]rapie)|"
    r"post[\s-]?op[eé]ratoire|post[\s-]?chirurgical|"
    r"apr[eè]s\s+ex[eé]r[eè]se|apr[eè]s\s+r[eé]section|"
    r"traitement\s+en\s+cours|sous\s+traitement|"
    r"cycle\s+[0-9]+|cure\s+[0-9]+|"
    r"r[eé]ponse\s+au\s+traitement|[eé]valuation\s+th[eé]rapeutique|"
    r"yptnm|ypt|ypn"
    r")\b",
    re.IGNORECASE,
)
# Marqueurs indiquant un document pré-thérapeutique (bilan initial)
PRE_TREATMENT_PATTERN = re.compile(
    r"\b("
    r"bilan\s+initial|bilan\s+d['']extension|diagnostic\s+initial|"
    r"premi[eè]re\s+consultation|nouvelle?\s+consultation|"
    r"bilan\s+pr[eé][\s-]?th[eé]rapeutique|bilan\s+pr[eé][\s-]?op[eé]ratoire|"
    r"avant\s+(traitement|chirurgie|op[eé]ration)|"
    r"[àa]\s+l['']admission|[àa]\s+l['']entr[eé]e"
    r")\b",
    re.IGNORECASE,
)


# =============================================================================
# DATE EXTRACTION FROM FILENAME
# =============================================================================

# Pattern général : dernier segment _YYYYMMDD avant .pdf (ou _N.pdf)
# Ex: 202007906_RESULTATSTEXTE_230299758_20210111.pdf → 20210111
# Ex: 202007906_HL7_902007906_20201112_1.pdf          → 20201112
# Ex: 202007906_HL7_902007906_20201112.pdf             → 20201112
FILENAME_DATE_PATTERN = re.compile(
    r"_(\d{8})(?:_\d+)?\.pdf$",
    re.IGNORECASE,
)


def extract_date_from_filename(filename: str) -> Optional[str]:
    """
    Extrait la date (YYYYMMDD) depuis le nom de fichier.

    Stratégie : on cherche le dernier segment _YYYYMMDD dans le nom,
    avant l'éventuel suffixe numérique (_1, _2, etc.) et l'extension .pdf.

    Cela donne la date réelle de l'événement clinique, indépendamment
    de la date d'intégration dans le DPI (souvent beaucoup plus tardive
    pour les documents HL7 intégrés en masse).

    Exemples :
      202007906_IUCTCRCSSUR_230299758_20210108.pdf    → 20210108
      202007906_HL7_902007906_20201112.pdf             → 20201112
      202007906_HL7_902007906_20201112_1.pdf           → 20201112
      202007906_HL7_902007906_20211021_1.pdf           → 20211021
    """
    match = FILENAME_DATE_PATTERN.search(filename)
    if match:
        candidate = match.group(1)
        # Validation basique : année plausible, mois 01-12, jour 01-31
        year = int(candidate[:4])
        month = int(candidate[4:6])
        day = int(candidate[6:8])
        if 2000 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
            return candidate
    return None


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Debug TNM regex extraction for one IPP.")
    parser.add_argument("input_dir", help="Folder containing *.json.txt and matching *.pdf files.")
    parser.add_argument("--ipp", required=True, help="IPP to debug.")
    parser.add_argument("--context-window", type=int, default=100)
    parser.add_argument("--show-text", action="store_true")
    parser.add_argument("--only-stage-hits", action="store_true", default=True)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(levelname)s | %(message)s")


# =============================================================================
# TEXT / PDF UTILITIES
# =============================================================================

def normalize_text(text: str) -> str:
    for bad, good in {"\u00a0": " ", "\u00ad": "", "\ufb01": "fi", "\ufb02": "fl", "\r": "\n"}.items():
        text = text.replace(bad, good)
    return re.sub(r"[ \t]+", " ", text)


def require_pdf_backend() -> None:
    if fitz is None and PdfReader is None:
        raise RuntimeError("No PDF backend found. Install 'pymupdf' or 'PyPDF2'.")


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


# =============================================================================
# METADATA UTILITIES
# =============================================================================

def load_metadata(metadata_path: Path) -> dict:
    raw_bytes = metadata_path.read_bytes()
    last_error: Optional[Exception] = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return json.loads(raw_bytes.decode(encoding))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            last_error = exc
    raise ValueError(f"Unable to decode {metadata_path}. Last error: {last_error}")


def metadata_to_ipp(metadata: dict, metadata_path: Path) -> str:
    ipp = metadata.get("Patient", {}).get("IPP") or metadata.get("IPP") or metadata_path.name.split("_")[0]
    return str(ipp).strip()


def metadata_to_date(metadata: dict, pdf_path: Optional[Path] = None) -> str:
    """
    Résolution de date avec priorité :
    1. Date extraite du NOM DE FICHIER PDF (date clinique réelle)
    2. Date métadata Episode.StartDate
    3. Date métadata Document.CreateDate / UpdateDate

    La date du nom de fichier est prioritaire car les documents intégrés
    en masse (HL7, import rétrospectif) ont une date metadata = date
    d'intégration, souvent des années après l'événement clinique réel.
    """
    # Priorité 1 : date dans le nom du fichier PDF
    if pdf_path is not None:
        fname_date = extract_date_from_filename(pdf_path.name)
        if fname_date:
            return fname_date

    # Priorité 2 : métadonnées
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


def metadata_to_diagnosis_date(metadata: dict) -> Optional[str]:
    # Use only explicit diagnosis fields. Do not fallback to episode/document dates
    # because they frequently represent integration/visit timestamps, not diagnosis.
    candidates = [
        metadata.get("date_diag_tkc"),
        metadata.get("date_diag_dcc"),
        metadata.get("date_diagnostic"),
        metadata.get("diagnostic_start_date"),
        metadata.get("Patient", {}).get("date_diag_tkc"),
        metadata.get("Patient", {}).get("date_diag_dcc"),
        metadata.get("Episode", {}).get("DiagnosisDate"),
    ]
    for value in candidates:
        if not value:
            continue
        token = str(value).strip()[:10].replace("-", "")
        if re.fullmatch(r"\d{8}", token):
            return token
    return None


def detect_document_kind(metadata: dict, metadata_path: Path, pdf_path: Path) -> str:
    """
    Détecte le type de document depuis les métadonnées et le nom de fichier.

    Types reconnus :
      - rcp          : Réunion de Concertation Pluridisciplinaire
      - pathology    : Anatomopathologie (anapath, compte-rendu histo)
      - consultation : Consultation, courrier de suivi
      - radiology    : Imagerie (scanner, IRM, PET, écho)
      - hospitalization : Compte-rendu d'hospitalisation
      - other        : Tout le reste
    """
    haystack = " ".join([
        metadata_path.name,
        pdf_path.name,
        str(metadata.get("Document", {}).get("FileName", "")),
        str(metadata.get("Document", {}).get("PDFDocumentName", "")),
        str(metadata.get("Document", {}).get("TypeDescription", "")),
        str(metadata.get("Document", {}).get("FormatComDesc", "")),
        str(metadata.get("Document", {}).get("PrescriptionDesc", "")),
    ]).lower()

    if "rcp" in haystack:
        return "rcp"
    if any(k in haystack for k in ("anapath", "anatomo", "patholog", "histo", "cytolog")):
        return "pathology"
    if any(k in haystack for k in ("scanner", "scannercr", "irm", "pet", "echograph", "radio", "imagerie")):
        return "radiology"
    if any(k in haystack for k in ("hospitalis", "crhospit", "hospitalisationcr")):
        return "hospitalization"
    if any(k in haystack for k in ("consult", "crcssur", "crcsnv", "lettres", "courrier")):
        return "consultation"
    return "other"


def is_excluded_document(metadata: dict) -> bool:
    fields = [
        str(metadata.get("Document", {}).get("PrescriptionDesc", "")),
        str(metadata.get("Document", {}).get("TypeDescription", "")),
        str(metadata.get("Document", {}).get("FormatComDesc", "")),
    ]
    return bool(ANESTHESIA_DOC_PATTERN.search(" ".join(fields)))


# =============================================================================
# SIGNAL DETECTION
# =============================================================================

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
        if REGIONAL_NODAL_CONTEXT_PATTERN.search(around) and not DISTANT_SECONDARY_SITE_PATTERN.search(around):
            continue
        if REGIONAL_NODAL_CONTEXT_PATTERN.search(around) and NO_OTHER_SECONDARY_LOCATION_PATTERN.search(text):
            continue
        return "yes"
    return "no"


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


def detect_melanoma_metastasis_confirmed(text: str) -> bool:
    """
    Détecte une métastase mélanome CONFIRMÉE (PET+, évolutivité pulmonaire…)
    par opposition à une simple mention historique dans un document de surveillance.
    """
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


def classify_melanoma_m_subtype(text: str) -> str:
    if MELANOMA_M1D_PATTERN.search(text):
        return "m1d"
    if MELANOMA_M1B_PATTERN.search(text):
        return "m1b"
    if MELANOMA_M1C_PATTERN.search(text):
        return "m1c"
    if MELANOMA_M1A_PATTERN.search(text):
        return "m1a"
    return "m1"


def classify_melanoma_ldh_status(text: str) -> str:
    if MELANOMA_LDH_HIGH_PATTERN.search(text):
        return "ldh_high"
    if MELANOMA_LDH_NORMAL_PATTERN.search(text):
        return "ldh_normal"
    return "ldh_unknown"


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
    if NODAL_NEGATIVE_PATTERN.search(text):
        return "negative"
    return "unknown"


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


def detect_post_treatment(text: str) -> bool:
    """
    Détecte si le document décrit une situation POST-traitement.
    Utile pour exclure les comptes-rendus de suivi lors de la recherche
    du TNM initial avant traitement.
    """
    return bool(TREATMENT_STARTED_PATTERN.search(text))


def detect_pre_treatment(text: str) -> bool:
    """
    Détecte si le document est explicitement un bilan initial / pré-thérapeutique.
    """
    return bool(PRE_TREATMENT_PATTERN.search(text))


def detect_neoadjuvant(text: str) -> bool:
    """Détecte un contexte néoadjuvant (chimio avant chirurgie)."""
    return bool(NEOADJUVANT_PATTERN.search(text))


# =============================================================================
# STAGE COMPUTATION
# =============================================================================

def normalize_tnm_component(value: str, axis: str) -> str:
    value = (value or "").lower().strip().replace(" ", "")
    if not value:
        return ""
    idx = value.find(axis)
    return value[idx:] if idx >= 0 else value


def normalize_explicit_stage(token: str) -> str:
    token = token.strip().upper()
    token = {"1": "I", "2": "II", "3": "III", "4": "IV"}.get(token, token)
    return f"Stage {token}"


def extract_explicit_stage(text: str) -> Optional[str]:
    for match in EXPLICIT_STAGE_PATTERN.finditer(text):
        around = text[max(0, match.start() - 120):min(len(text), match.end() + 120)]
        if EXPLICIT_STAGE_FALSE_POSITIVE_PATTERN.search(around):
            continue
        # Keep explicit stage even without context if AJCC is directly in match.
        if "ajcc" not in match.group(0).lower() and not EXPLICIT_STAGE_ONCO_CONTEXT_PATTERN.search(around):
            continue
        return normalize_explicit_stage(match.group(1))
    return None


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


def compute_stage(t_value: str, n_value: str, m_value: str) -> str:
    """Stadification générique TNM (logique commune à toutes localisations)."""
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
    if (
        (t in {"t0", "t1", "t1a", "t1b", "t1c", "t1mi"} and logic_n in {"n1", "n1mi"})
        or (t.startswith("t2") and logic_n == "n0")
    ) and logic_m == "m0":
        return "Stage IIA"
    if (
        (t.startswith("t2") and logic_n == "n1")
        or (t.startswith("t3") and logic_n == "n0")
    ) and logic_m == "m0":
        return "Stage IIB"
    if (
        (t.startswith(("t0", "t1", "t2")) and logic_n == "n2")
        or (t.startswith("t3") and logic_n in {"n1", "n1mi", "n2"})
    ) and logic_m == "m0":
        return "Stage IIIA"
    if t.startswith("t4") and logic_n in {"n0", "n1", "n2"} and logic_m == "m0":
        return "Stage IIIB"
    if logic_n == "n3" and logic_m == "m0":
        return "Stage IIIC"
    return "null"


def compute_melanoma_stage(t_value: str, n_value: str, m_value: str, ulcerated: bool) -> str:
    """
    Stadification mélanome AJCC 8e édition.

    Points clés :
      T4b (Breslow > 4mm + ulcération) + N0 + M0 = Stage IIC
      T4b + N+ + M0                               = Stage IIIB/IIIC
      Tout M1                                     = Stage IV
    """
    t = normalize_tnm_component(t_value, "t")
    n = normalize_tnm_component(n_value, "n")
    m = normalize_tnm_component(m_value, "m") or "mx"
    logic_n = n
    logic_m = m

    if logic_m == "mx":
        return "null"

    if logic_m.startswith("m1"):
        return "Stage IV"
    if logic_n == "nx":
        return "null"
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
            return "Stage IIC"          # ← CAS DE NOTRE PATIENT
        if logic_n in {"n1", "n1a", "n1b", "n2", "n2a", "n2b"}:
            return "Stage IIIB"
        if logic_n in {"n3", "n3a", "n3b", "n3c"}:
            return "Stage IIIC"
        return "Stage IIIB"
    return compute_stage(t_value, n_value, m_value)


def compute_breast_stage(t_value: str, n_value: str, m_value: str) -> str:
    """
    Stadification sein AJCC 8e édition (anatomique simplifiée).
    Utilisée pour le pTNM post-chirurgie primaire.

    Note : la stadification sein complète intègre grade, HER2, RH —
    ici on calcule le stade anatomique de base.
    """
    t = normalize_tnm_component(t_value, "t")
    n = normalize_tnm_component(n_value, "n")
    # Collapse breast N subcategories for anatomical stage mapping.
    if n.startswith("n3"):
        n = "n3"
    elif n.startswith("n2"):
        n = "n2"
    elif n.startswith("n1"):
        n = "n1"
    m = normalize_tnm_component(m_value, "m") or "mx"
    logic_n = n
    logic_m = m

    if logic_m.startswith("m1"):
        return "Stage IV"
    if logic_n in {"", "nx"} or logic_m in {"", "mx"}:
        return "null"
    if t == "tis" and logic_n == "n0":
        return "Stage 0"
    if t in {"t1", "t1a", "t1b", "t1c", "t1mi"} and logic_n == "n0":
        return "Stage IA"
    if t in {"t0", "t1", "t1a", "t1b", "t1c", "t1mi"} and logic_n in {"n1mi"}:
        return "Stage IB"
    if (
        t in {"t0", "t1", "t1a", "t1b", "t1c", "t1mi"} and logic_n == "n1"
    ) or (t.startswith("t2") and logic_n == "n0"):
        return "Stage IIA"
    if (t.startswith("t2") and logic_n == "n1") or (t.startswith("t3") and logic_n == "n0"):
        return "Stage IIB"
    if (
        t.startswith(("t0", "t1", "t2")) and logic_n == "n2"
    ) or (t.startswith("t3") and logic_n in {"n1", "n2"}):
        return "Stage IIIA"
    if t.startswith("t4") and logic_n in {"n0", "n1", "n2"}:
        return "Stage IIIB"
    if logic_n == "n3":
        return "Stage IIIC"
    return "null"


# =============================================================================
# BRESLOW / T-CATEGORY UTILITIES
# =============================================================================

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


def melanoma_ulceration_status(text: str) -> str:
    if ULCERATION_ABSENT_PATTERN.search(text):
        return "absent"
    if ULCERATION_PATTERN.search(text):
        return "present"
    return "unknown"


def breslow_t_category_with_ulceration(mm: float, ulceration_status: str) -> str:
    """
    Catégorie T mélanome selon Breslow + ulcération (AJCC 8e éd.).
    L'ulcération fait passer de T_a (sans) à T_b (avec).
    Ex : 7.5 mm + ulcéré → T4b → Stage IIC si N0
    """
    if ulceration_status == "unknown":
        if mm <= 1.0:
            return "t1"
        if mm <= 2.0:
            return "t2"
        if mm <= 4.0:
            return "t3"
        return "t4"
    if mm < 0.8:
        return "t1b" if ulceration_status == "present" else "t1a"
    if mm <= 1.0:
        return "t1b"
    if mm <= 2.0:
        return "t2b" if ulceration_status == "present" else "t2a"
    if mm <= 4.0:
        return "t3b" if ulceration_status == "present" else "t3a"
    return "t4b" if ulceration_status == "present" else "t4a"


def infer_n_from_nodal_context(text: str, has_imaging_evidence: bool = False) -> str:
    if detect_nodal_positive_signal(text) == "yes":
        return "n1"
    if detect_nodal_uncertain_signal(text) == "yes":
        return "nx"
    return "n0" if has_imaging_evidence else "nx"


def extract_melanoma_t_category_stage(
    text: str,
    metastasis_detected: str,
    has_imaging_evidence: bool = False,
) -> Optional[tuple[str, str, str, str, str]]:
    if not MELANOMA_CONTEXT_PATTERN.search(text):
        return None
    if metastasis_detected == "yes":
        return None

    t_values = [
        normalize_tnm_component(m.group(1), "t")
        for m in T_COMPONENT_PATTERN.finditer(text)
        if not has_post_treatment_tnm_prefix(m.group(1))
    ]
    t_values = [value for value in t_values if value and value not in {"tx"}]
    if not t_values:
        return None

    t_value = max(t_values, key=t_component_rank)
    n_value = infer_n_from_nodal_context(text, has_imaging_evidence)
    m_value = "m0" if has_imaging_evidence else "mx"
    stage = compute_melanoma_stage(t_value, n_value, m_value, ulcerated=False)
    return f"{t_value.upper()} (melanoma T category inferred with imaging-dependent N/M)", t_value, n_value, m_value, stage


# =============================================================================
# SORTING / RANKING UTILITIES
# =============================================================================

def parse_date_sort_key(value: str) -> str:
    return value if value and value != "null" else "99999999"


def parse_yyyymmdd(value: Optional[str]) -> Optional[datetime]:
    if not value or value == "null":
        return None
    try:
        return datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return None


def is_date_in_window(date_str: str, center_str: Optional[str], days: int = 62) -> bool:
    if center_str is None:
        return True
    doc_dt = parse_yyyymmdd(date_str)
    center_dt = parse_yyyymmdd(center_str)
    if doc_dt is None or center_dt is None:
        return False
    return center_dt - timedelta(days=days) <= doc_dt <= center_dt + timedelta(days=days)


def is_date_in_forward_window(date_str: str, start_str: Optional[str], days: int = 90) -> bool:
    if start_str is None:
        return False
    doc_dt = parse_yyyymmdd(date_str)
    start_dt = parse_yyyymmdd(start_str)
    if doc_dt is None or start_dt is None:
        return False
    return start_dt <= doc_dt <= start_dt + timedelta(days=days)


def stage_rank(stage: str) -> tuple[int, int]:
    if not stage or stage == "null":
        return (-1, -1)
    match = re.match(r"Stage\s+(IV|III|II|I|0)([A-D]?)", stage)
    if not match:
        return (-1, -1)
    major_order = {"0": 0, "I": 1, "II": 2, "III": 3, "IV": 4}
    letter_order = {"": 0, "A": 1, "B": 2, "C": 3, "D": 4}
    return major_order[match.group(1)], letter_order.get(match.group(2), 0)


def tnm_completeness_score(t: str, n: str, m: str) -> int:
    score = 0
    if t not in {"", "null", "tx"}:
        score += 1
    if n not in {"", "null", "nx"}:
        score += 1
    if m not in {"", "null", "mx"}:
        score += 1
    return score


def document_kind_priority(kind: str) -> int:
    """
    Priorité des types de documents pour la sélection du TNM initial.
    Plus petit = plus prioritaire.
    """
    return {
        "pathology": 0,      # Anapath = référence absolue (pTNM)
        "rcp":       1,      # RCP = TNM discuté en équipe
        "radiology": 2,      # Imagerie = cTNM (staging clinique)
        "consultation": 3,   # Consultation = TNM rapporté
        "hospitalization": 4,
        "other": 5,
    }.get(kind, 5)


def breast_document_kind_priority(kind: str) -> int:
    return {
        "pathology": 0,
        "rcp": 1,
        "consultation": 2,
        "radiology": 3,
        "hospitalization": 4,
        "other": 5,
    }.get(kind, 5)


# =============================================================================
# TNM ROW EXTRACTION
# =============================================================================

def tnm_rows(name: str, matches: list[re.Match]) -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    t_token_pattern = re.compile(
        r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))(?![A-Za-z0-9])",
        re.IGNORECASE,
    )
    t_irm_pattern = re.compile(
        r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
        r"(?:[\s,;:()\\/-]{0,12})irm\b",
        re.IGNORECASE,
    )
    for match in matches:
        raw = re.sub(r"\s+", " ", match.group(0)).strip()
        t = normalize_tnm_component(match.group(1) or "", "t")
        n = normalize_tnm_component(match.group(2) or "", "n")
        m = normalize_tnm_component(match.group(3) or "", "m") or "mx"
        if name == "TNM_LOOSE_PATTERN":
            irm_t_candidates = [normalize_tnm_component(i.group(1), "t") for i in t_irm_pattern.finditer(raw)]
            if irm_t_candidates:
                t = irm_t_candidates[-1]
            else:
                all_t = [normalize_tnm_component(i.group(1), "t") for i in t_token_pattern.finditer(raw)]
                if all_t:
                    t = all_t[-1]
        rows.append((name, raw, t, n, m))
    return rows


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


def is_forbidden_y_prefix_hit(row: dict) -> bool:
    return has_post_treatment_tnm_prefix(str(row.get("raw", "")))


# =============================================================================
# PROSTATE-SPECIFIC LOGIC
# =============================================================================

def t_component_rank(value: str) -> tuple[int, int]:
    value = normalize_tnm_component(value, "t")
    if not value:
        return (99, 99)
    if value == "tis":
        return (0, 0)
    match = re.match(r"t([1-4])(mi|a|b|c|d)?", value)
    if not match:
        return (99, 99) if value == "tx" else (98, 98)
    suffix_order = {"mi": 0, "a": 1, "b": 2, "c": 3, "d": 4, None: 5}
    return int(match.group(1)) + 1, suffix_order.get(match.group(2), 5)


def extract_prostate_t_only_stage(text: str, metastasis_detected: str) -> Optional[tuple[str, str, str, str, str]]:
    if not PROSTATE_CONTEXT_PATTERN.search(text):
        return None
    if metastasis_detected == "yes":
        return None
    has_nodal_positive = detect_nodal_positive_signal(text) == "yes"
    has_nodal_uncertain = detect_nodal_uncertain_signal(text) == "yes"
    if has_nodal_positive:
        return None
    t_irm_pattern = re.compile(
        r"(?<![A-Za-z0-9])((?:[cpyrai]{0,4})?t(?:is|x|0|1mi|1[abc]?|2[abc]?|3[abc]?|4[abcd]?))"
        r"(?:[\s,;:()\\/-]{0,12})irm\b",
        re.IGNORECASE,
    )
    irm_t = [
        normalize_tnm_component(m.group(1), "t")
        for m in t_irm_pattern.finditer(text)
        if not has_post_treatment_tnm_prefix(m.group(1))
    ]
    t_values = [v for v in irm_t if v and v not in {"tx"}]
    if not t_values:
        all_t = [
            normalize_tnm_component(m.group(1), "t")
            for m in T_COMPONENT_PATTERN.finditer(text)
            if not has_post_treatment_tnm_prefix(m.group(1))
        ]
        t_values = [v for v in all_t if v and v not in {"tx"}]
    if not t_values:
        return None
    t_value = max(t_values, key=t_component_rank)
    n_value = "nx" if has_nodal_uncertain else "n0"
    stage = "null" if n_value == "nx" else compute_stage(t_value, n_value, "m0")
    if stage == "null":
        return f"{t_value.upper()} (prostate inferred NxM0)", t_value, n_value, "m0", stage
    return f"{t_value.upper()} (prostate inferred N0M0)", t_value, n_value, "m0", stage


# =============================================================================
# BREAST-SPECIFIC LOGIC
# =============================================================================

def extract_breast_pathological_tnm(text: str) -> Optional[tuple[str, str, str, str]]:
    """
    Extrait le pTNM pathologique post-chirurgie sein.

    Contexte : pour le cancer du sein avec chirurgie première (sans néoadjuvant),
    il n'existe pas de TNM clinique formel pré-opératoire. Le pTNM de l'anapath
    est la référence pour le stade initial.

    Si néoadjuvant détecté → ypTNM → on le signale mais on le retourne quand même
    car c'est le seul TNM disponible (avec flag neoadjuvant=True).
    """
    is_neoadjuvant = detect_neoadjuvant(text)

    # Chercher pTNM explicite
    matches = list(BREAST_PATHOLOGICAL_TNM_PATTERN.finditer(text))
    if matches:
        # Prendre le match avec le T le plus précis (pas tx)
        best = None
        best_score = -1
        for match in matches:
            t = normalize_tnm_component(match.group(1) or "", "t")
            n = normalize_tnm_component(match.group(2) or "", "n")
            m = normalize_tnm_component(match.group(3) or "", "m") or "mx"
            score = tnm_completeness_score(t, n, m)
            if t not in {"", "tx"} and score > best_score:
                best = (match, t, n, m)
                best_score = score

        if best is not None:
            match, t, n, m = best
            raw = re.sub(r"\s+", " ", match.group(0)).strip()
            return raw, t, n, m

    t_only_matches = list(BREAST_PATHOLOGICAL_T_ONLY_PATTERN.finditer(text))
    if not t_only_matches:
        return None
    t_values = [
        (match, normalize_tnm_component(match.group(1) or "", "t"))
        for match in t_only_matches
    ]
    t_values = [(match, t) for match, t in t_values if t and t != "tx"]
    if not t_values:
        return None
    match, t = max(t_values, key=lambda item: t_component_rank(item[1]))
    raw = re.sub(r"\s+", " ", match.group(0)).strip()
    return f"{raw} (breast pT-only inferred N0M0)", t, "n0", "m0"


def debug_empty_breast_anapath_values() -> dict[str, str]:
    return {
        "histology_type": "null",
        "grade_sbr": "null",
        "sbr_tubule_score": "null",
        "sbr_nuclear_score": "null",
        "sbr_mitotic_score": "null",
        "er_percent": "null",
        "er_intensity": "null",
        "er_status": "null",
        "pr_percent": "null",
        "pr_intensity": "null",
        "pr_status": "null",
        "hormone_receptor_status_project": "null",
        "her2_ihc_score": "null",
        "her2_ish_result": "null",
        "her2_status": "null",
        "her2_qualification_project": "null",
        "pdl1_cps_value": "null",
        "pdl1_cps_status_project": "null",
        "breast_anapath_sources": "null",
    }


def debug_normalize_grade(value: str) -> Optional[int]:
    value = (value or "").strip().lower()
    if value in {"1", "i"}:
        return 1
    if value in {"2", "ii"}:
        return 2
    if value in {"3", "iii"}:
        return 3
    return None


def debug_normalize_intensity(value: str) -> str:
    value = (value or "").strip().lower()
    if value == "0":
        return "0"
    if value in {"+", "++", "+++"}:
        return value
    if value == "faible":
        return "+"
    if value.startswith("mod"):
        return "++"
    if value.startswith("fort") or value == "intense":
        return "+++"
    return "null"


def debug_intensity_rank(value: str) -> int:
    return {"0": 0, "+": 1, "++": 2, "+++": 3}.get(value, -1)


def debug_her2_score_rank(value: str) -> int:
    return {"0": 0, "1+": 1, "2+": 2, "3+": 3}.get(value, -1)


def debug_normalize_her2_score(value: str) -> str:
    value = (value or "").strip().replace(" ", "")
    if value in {"0", "0+"}:
        return "0"
    if value in {"1+", "2+", "3+"}:
        return value
    return "null"


def debug_extract_breast_anapath_values(text: str) -> dict[str, str]:
    values = debug_empty_breast_anapath_values()
    values.pop("breast_anapath_sources")
    if not DEBUG_BREAST_HISTOLOGY_EXCLUSION_PATTERN.search(text):
        histologies = [code for code, pattern in DEBUG_BREAST_HISTOLOGY_PATTERNS if pattern.search(text)]
        if "NST" in histologies and "LOBULAR" in histologies:
            values["histology_type"] = "MIXED_NST_LOBULAR"
        elif histologies:
            values["histology_type"] = histologies[0]
    if values["histology_type"] == "null" and DEBUG_BREAST_IN_SITU_PATTERN.search(text) and not DEBUG_BREAST_INFILTRATING_PATTERN.search(text):
        values["histology_type"] = "IN_SITU"
    grades = [debug_normalize_grade(match.group(1)) for match in DEBUG_BREAST_GRADE_PATTERN.finditer(text)]
    grades = [grade for grade in grades if grade is not None]
    if grades:
        values["grade_sbr"] = str(max(grades))
    details = [(int(m.group(1)), int(m.group(2)), int(m.group(3))) for m in DEBUG_BREAST_GRADE_DETAIL_PATTERN.finditer(text)]
    if details:
        tubule, nuclear, mitotic = max(details, key=lambda item: sum(item))
        values["sbr_tubule_score"] = str(tubule)
        values["sbr_nuclear_score"] = str(nuclear)
        values["sbr_mitotic_score"] = str(mitotic)
    er_percents = [int(match.group(1)) for match in DEBUG_BREAST_ER_PERCENT_PATTERN.finditer(text)]
    pr_percents = [int(match.group(1)) for match in DEBUG_BREAST_PR_PERCENT_PATTERN.finditer(text)]
    if er_percents:
        values["er_percent"] = str(max(er_percents))
        values["er_status"] = "POSITIVE" if max(er_percents) >= 10 else "NEGATIVE"
    elif DEBUG_BREAST_ER_POSITIVE_PATTERN.search(text):
        values["er_status"] = "POSITIVE"
    elif DEBUG_BREAST_ER_NEGATIVE_PATTERN.search(text):
        values["er_status"] = "NEGATIVE"
    if pr_percents:
        values["pr_percent"] = str(max(pr_percents))
        values["pr_status"] = "POSITIVE" if max(pr_percents) >= 10 else "NEGATIVE"
    elif DEBUG_BREAST_PR_POSITIVE_PATTERN.search(text):
        values["pr_status"] = "POSITIVE"
    elif DEBUG_BREAST_PR_NEGATIVE_PATTERN.search(text):
        values["pr_status"] = "NEGATIVE"
    er_intensities = [debug_normalize_intensity(match.group(1)) for match in DEBUG_BREAST_ER_INTENSITY_PATTERN.finditer(text)]
    er_intensities = [value for value in er_intensities if value != "null"]
    pr_intensities = [debug_normalize_intensity(match.group(1)) for match in DEBUG_BREAST_PR_INTENSITY_PATTERN.finditer(text)]
    pr_intensities = [value for value in pr_intensities if value != "null"]
    if er_intensities:
        values["er_intensity"] = max(er_intensities, key=debug_intensity_rank)
    if pr_intensities:
        values["pr_intensity"] = max(pr_intensities, key=debug_intensity_rank)
    if values["er_status"] == "POSITIVE" or values["pr_status"] == "POSITIVE":
        values["hormone_receptor_status_project"] = "POSITIVE"
    elif values["er_status"] == "NEGATIVE" and values["pr_status"] == "NEGATIVE":
        values["hormone_receptor_status_project"] = "NEGATIVE"
    elif DEBUG_BREAST_RH_POSITIVE_PATTERN.search(text):
        values["hormone_receptor_status_project"] = "POSITIVE"
    elif DEBUG_BREAST_RH_NEGATIVE_PATTERN.search(text) or DEBUG_BREAST_TRIPLE_NEGATIVE_PATTERN.search(text):
        values["hormone_receptor_status_project"] = "NEGATIVE"
    her2_scores = [debug_normalize_her2_score(match.group(1)) for match in DEBUG_BREAST_HER2_IHC_PATTERN.finditer(text)]
    her2_scores = [score for score in her2_scores if score != "null"]
    if her2_scores:
        values["her2_ihc_score"] = max(her2_scores, key=debug_her2_score_rank)
    if DEBUG_BREAST_HER2_ISH_NOT_AMPLIFIED_PATTERN.search(text):
        values["her2_ish_result"] = "NOT_AMPLIFIED"
    elif DEBUG_BREAST_HER2_ISH_AMPLIFIED_PATTERN.search(text) or DEBUG_BREAST_HER2_POSITIVE_PATTERN.search(text):
        values["her2_ish_result"] = "AMPLIFIED" if DEBUG_BREAST_HER2_ISH_AMPLIFIED_PATTERN.search(text) else values["her2_ish_result"]
        values["her2_status"] = "POSITIVE"
    elif DEBUG_BREAST_HER2_NEGATIVE_PATTERN.search(text):
        values["her2_status"] = "NEGATIVE"
    if values["her2_ihc_score"] == "3+" or values["her2_ish_result"] == "AMPLIFIED":
        values["her2_status"] = "POSITIVE"
        values["her2_qualification_project"] = "POSITIVE"
    elif values["her2_ihc_score"] == "2+" and values["her2_ish_result"] == "NOT_AMPLIFIED":
        values["her2_status"] = "NEGATIVE"
        values["her2_qualification_project"] = "LOW"
    elif values["her2_ihc_score"] == "1+" or DEBUG_BREAST_HER2_LOW_PATTERN.search(text):
        values["her2_status"] = "NEGATIVE" if values["her2_status"] == "null" else values["her2_status"]
        values["her2_qualification_project"] = "LOW"
    elif DEBUG_BREAST_HER2_ULTRALOW_PATTERN.search(text):
        values["her2_qualification_project"] = "ULTRALOW"
    elif values["her2_ihc_score"] == "0" or DEBUG_BREAST_HER2_NULL_PATTERN.search(text):
        values["her2_status"] = "NEGATIVE" if values["her2_status"] == "null" else values["her2_status"]
        values["her2_qualification_project"] = "HER2_NULL"
    elif DEBUG_BREAST_TRIPLE_NEGATIVE_PATTERN.search(text):
        values["her2_status"] = "NEGATIVE"
    cps_values = [int(match.group(1)) for match in DEBUG_BREAST_PDL1_CPS_PATTERN.finditer(text)]
    if cps_values:
        cps = max(cps_values)
        values["pdl1_cps_value"] = str(cps)
        values["pdl1_cps_status_project"] = "POSITIVE" if cps >= 10 else "NEGATIVE"
    return values


def debug_merge_breast_anapath_values(current: dict[str, str], incoming: dict[str, str]) -> dict[str, str]:
    merged = dict(current)
    for key in ("grade_sbr", "sbr_tubule_score", "sbr_nuclear_score", "sbr_mitotic_score", "er_percent", "pr_percent", "pdl1_cps_value"):
        if incoming.get(key, "null") != "null" and (merged.get(key, "null") == "null" or int(incoming[key]) > int(merged[key])):
            merged[key] = incoming[key]
    if incoming.get("histology_type", "null") != "null":
        if merged["histology_type"] == "null":
            merged["histology_type"] = incoming["histology_type"]
        elif merged["histology_type"] != incoming["histology_type"]:
            merged["histology_type"] = "MIXED_NST_LOBULAR" if {merged["histology_type"], incoming["histology_type"]} == {"NST", "LOBULAR"} else "OTHER_SPECIFIED"
    for key in ("er_intensity", "pr_intensity"):
        if incoming.get(key, "null") != "null" and debug_intensity_rank(incoming[key]) > debug_intensity_rank(merged[key]):
            merged[key] = incoming[key]
    for key in ("er_status", "pr_status", "hormone_receptor_status_project", "her2_status", "pdl1_cps_status_project"):
        if incoming.get(key) == "POSITIVE" or (merged.get(key, "null") == "null" and incoming.get(key, "null") != "null"):
            merged[key] = incoming[key]
    if incoming.get("her2_ish_result") == "AMPLIFIED" or (merged["her2_ish_result"] == "null" and incoming.get("her2_ish_result", "null") != "null"):
        merged["her2_ish_result"] = incoming["her2_ish_result"]
    if incoming.get("her2_ihc_score", "null") != "null" and debug_her2_score_rank(incoming["her2_ihc_score"]) > debug_her2_score_rank(merged["her2_ihc_score"]):
        merged["her2_ihc_score"] = incoming["her2_ihc_score"]
    qualification_rank = {"null": -1, "HER2_NULL": 0, "ULTRALOW": 1, "LOW": 2, "POSITIVE": 3}
    if qualification_rank.get(incoming.get("her2_qualification_project", "null"), -1) > qualification_rank.get(merged["her2_qualification_project"], -1):
        merged["her2_qualification_project"] = incoming["her2_qualification_project"]
    return merged


def debug_is_centered_date_window(date_str: str, pivot_date: Optional[str], days: int = 90) -> bool:
    if not pivot_date or not date_str or date_str == "null":
        return True
    try:
        date_value = datetime.strptime(date_str, "%Y%m%d")
        pivot_value = datetime.strptime(pivot_date, "%Y%m%d")
    except Exception:
        return True
    return abs((date_value - pivot_value).days) <= days


def debug_consolidate_breast_anapath_variables(selected: list[tuple[Path, dict, Path]], diagnosis_date: Optional[str]) -> dict[str, str]:
    consolidated = debug_empty_breast_anapath_values()
    sources: list[str] = []
    docs: list[tuple[str, str, Path, str]] = []
    for metadata_path, metadata, pdf_path in selected:
        date = extract_date_from_filename(pdf_path.name) or metadata_to_document_date(metadata)
        if not debug_is_centered_date_window(date, diagnosis_date, days=90):
            continue
        try:
            text = extract_pdf_text(pdf_path)
        except Exception as exc:
            LOGGER.warning(
                "Breast anapath debug skipped unreadable PDF | file=%s | error=%s",
                pdf_path,
                exc,
            )
            continue
        if not BREAST_CONTEXT_PATTERN.search(text):
            continue
        docs.append((detect_document_kind(metadata, metadata_path, pdf_path), date, pdf_path, text))
    pathology_docs = [doc for doc in docs if doc[0] == "pathology"]
    fallback_docs = [doc for doc in docs if doc[0] in {"consultation", "rcp", "radiology"}]
    found_keys: set[str] = set()
    for group in (pathology_docs, fallback_docs):
        prior_found_keys = set(found_keys)
        for document_kind, date, pdf_path, text in group:
            values = debug_extract_breast_anapath_values(text)
            filtered = {key: value if key not in prior_found_keys else "null" for key, value in values.items()}
            if any(value != "null" for value in filtered.values()):
                consolidated = debug_merge_breast_anapath_values(consolidated, filtered)
                sources.append(f"{pdf_path.name}:{date}:{document_kind}")
        found_keys.update(key for key, value in consolidated.items() if key != "breast_anapath_sources" and value != "null")
    if consolidated["er_percent"] != "null":
        consolidated["er_status"] = "POSITIVE" if int(consolidated["er_percent"]) >= 10 else "NEGATIVE"
    if consolidated["pr_percent"] != "null":
        consolidated["pr_status"] = "POSITIVE" if int(consolidated["pr_percent"]) >= 10 else "NEGATIVE"
    if consolidated["er_status"] == "POSITIVE" or consolidated["pr_status"] == "POSITIVE":
        consolidated["hormone_receptor_status_project"] = "POSITIVE"
    elif consolidated["er_status"] == "NEGATIVE" and consolidated["pr_status"] == "NEGATIVE":
        consolidated["hormone_receptor_status_project"] = "NEGATIVE"
    if consolidated["her2_status"] == "POSITIVE":
        consolidated["her2_qualification_project"] = "POSITIVE"
    elif consolidated["her2_ihc_score"] == "2+" and consolidated["her2_ish_result"] == "NOT_AMPLIFIED":
        consolidated["her2_status"] = "NEGATIVE"
        consolidated["her2_qualification_project"] = "LOW"
    elif consolidated["her2_ihc_score"] == "1+":
        consolidated["her2_status"] = "NEGATIVE"
        consolidated["her2_qualification_project"] = "LOW"
    elif consolidated["her2_ihc_score"] == "0" and consolidated["her2_qualification_project"] == "null":
        consolidated["her2_status"] = "NEGATIVE"
        consolidated["her2_qualification_project"] = "HER2_NULL"
    consolidated["breast_anapath_sources"] = ";".join(dict.fromkeys(sources)) if sources else "null"
    return consolidated


# =============================================================================
# DOCUMENT PROCESSING
# =============================================================================

def process_document(
    idx: int,
    total: int,
    metadata: dict,
    metadata_path: Path,
    pdf_path: Path,
    args: argparse.Namespace,
) -> list[dict]:
    """
    Traite un document et retourne des hits TNM/stade.

    Logique générale de sélection du TNM initial avant traitement :
    ───────────────────────────────────────────────────────────────

    MÉLANOME
      → Breslow + ulcération → T_avec_ulcération N M → stadification AJCC mélanome
      → Documents de surveillance/rémission ignorés pour Stage IV
      → Métastase confirmée (PET+) enregistrée avec sa date propre

    SEIN (chirurgie première)
      → pTNM de l'anapath post-chirurgie = stade initial de référence
      → Si néoadjuvant : ypTNM enregistré avec flag
      → TNM clinique pré-op extrait si disponible (RCP, consultation)

    PROSTATE
      → TNM IRM (cTNM) extrait en priorité
      → Si T seul disponible → N0M0 inféré

    GÉNÉRAL
      → TNM strict → TNM loose → explicit stage → Stage 0
      → Post-traitement détecté → flagué mais conservé
      → Pré-traitement détecté → priorité augmentée
    """
    doc_date = metadata_to_date(metadata, pdf_path)
    document_kind = detect_document_kind(metadata, metadata_path, pdf_path)
    hits: list[dict] = []

    if not pdf_path.exists():
        if not args.only_stage_hits:
            LOGGER.warning("PDF missing: %s", pdf_path)
        return hits

    try:
        text = extract_pdf_text(pdf_path)
    except Exception as exc:
        if not args.only_stage_hits:
            LOGGER.exception("PDF extraction failed for %s: %s", pdf_path, exc)
        return hits

    if args.show_text:
        LOGGER.info("FULL TEXT START\n%s\nFULL TEXT END", text)

    # Détection des contextes
    is_melanoma_doc  = bool(MELANOMA_CONTEXT_PATTERN.search(text))
    is_prostate_doc  = bool(PROSTATE_CONTEXT_PATTERN.search(text))
    has_breast_context = bool(BREAST_CONTEXT_PATTERN.search(text))
    has_strong_breast_context = bool(BREAST_STRONG_CONTEXT_PATTERN.search(text))
    has_non_clinical_breast_menu = bool(NON_CLINICAL_BREAST_MENU_PATTERN.search(text))
    is_breast_doc = has_breast_context and not (has_non_clinical_breast_menu and not has_strong_breast_context)
    # Avoid routing prostate files to breast branch when "sein" appears only in admin menus/footer.
    # Hard precedence: any prostate context disables breast routing for this document.
    if is_prostate_doc:
        is_breast_doc = False
    metastasis_detected = detect_metastasis_signal(text)
    if metastasis_detected == "yes" and is_breast_regional_nodal_only_metastasis(text):
        metastasis_detected = "no"
    if is_breast_doc and metastasis_detected == "yes" and not breast_has_distant_metastasis_signal(text):
        metastasis_detected = "no"
    is_post_treatment   = detect_post_treatment(text)
    is_pre_treatment    = detect_pre_treatment(text)
    ulceration_status   = melanoma_ulceration_status(text) if is_melanoma_doc else "unknown"

    def make_hit(mode, pattern, raw, t, n, m, stage, extra=None) -> dict:
        hit = {
            "idx": idx,
            "total": total,
            "pdf": pdf_path.name,
            "date": doc_date,
            "kind": document_kind,
            "mode": mode,
            "pattern": pattern,
            "raw": raw,
            "t": t,
            "n": n,
            "m": m,
            "stage": stage,
            "is_melanoma":       is_melanoma_doc,
            "is_breast":         is_breast_doc,
            "is_prostate":       is_prostate_doc,
            "is_post_treatment": is_post_treatment,
            "is_pre_treatment":  is_pre_treatment,
            "is_metastatic_event": False,
        }
        if extra:
            hit.update(extra)
        return hit

    # =========================================================================
    # BRANCHE MÉLANOME
    # =========================================================================
    if is_melanoma_doc:
        melanoma_meta_confirmed = detect_melanoma_metastasis_confirmed(text)
        nodal_signal = detect_melanoma_nodal_signal(text)
        imaging_evidence = detect_imaging_evidence(text, document_kind)
        default_m = "m0" if imaging_evidence else "mx"

        if nodal_signal == "positive":
            hits.append(make_hit(
                "melanoma_nodal_signal", "NODAL_POSITIVE_PATTERN",
                "melanoma_nodal_positive_signal", "null", "n1", "null", "null",
                extra={"nodal_signal": "positive"},
            ))
        elif nodal_signal == "negative":
            hits.append(make_hit(
                "melanoma_nodal_signal", "NODAL_NEGATIVE_PATTERN",
                "melanoma_nodal_negative_signal", "null", "n0", "null", "null",
                extra={"nodal_signal": "negative"},
            ))
        elif nodal_signal == "non_regional":
            hits.append(make_hit(
                "melanoma_nodal_signal", "MELANOMA_NON_REGIONAL_NODAL_PATTERN",
                "melanoma_non_regional_nodal_signal", "null", "nx", "m1a", "Stage IV",
                extra={"nodal_signal": "non_regional", "is_metastatic_event": True},
            ))

        if melanoma_meta_confirmed:
            LOGGER.info(
                "Mélanome métastase CONFIRMÉE | PDF=%s | date=%s → Stage IV enregistré",
                pdf_path.name, doc_date,
            )
            hits.append(make_hit(
                "melanoma_metastasis_confirmed", "MELANOMA_METASTASIS_CONFIRMED_PATTERN",
                "melanoma_metastatic_confirmed_signal", "null", "null", classify_melanoma_m_subtype(text), "Stage IV",
                extra={"is_metastatic_event": True, "ldh_status": classify_melanoma_ldh_status(text)},
            ))
            # On continue pour extraire un éventuel Breslow dans ce même document

        elif MELANOMA_SURVEILLANCE_PATTERN.search(text) and not melanoma_meta_confirmed:
            LOGGER.info(
                "Mélanome surveillance (rémission) | PDF=%s | date=%s — skip Stage IV",
                pdf_path.name, doc_date,
            )
        elif metastasis_detected == "yes":
            LOGGER.info(
                "Mélanome signal métastatique générique (non confirmé) | PDF=%s — pas de Stage IV automatique",
                pdf_path.name,
            )

        # Extraction Breslow (stade initial mélanome)
        breslow_matches = list(BRESLOW_PATTERN.finditer(text))
        if breslow_matches and not melanoma_meta_confirmed:
            for match in breslow_matches:
                raw = re.sub(r"\s+", " ", match.group(0)).strip()
                raw_value = extract_breslow_raw_value(match)
                if raw_value is None:
                    continue
                mm = parse_breslow_mm(raw_value)
                if mm is None:
                    continue
                t = breslow_t_category_with_ulceration(mm, ulceration_status)
                n = infer_n_from_nodal_context(text, imaging_evidence)
                stage = compute_melanoma_stage(t, n, default_m, ulceration_status == "present")
                LOGGER.info(
                    "Mélanome Breslow=%.1fmm ulcération=%s imagerie=%s → T=%s N=%s M=%s → %s | PDF=%s | date=%s",
                    mm, ulceration_status, imaging_evidence, t, n, default_m, stage, pdf_path.name, doc_date,
                )
                hits.append(make_hit(
                    "melanoma_breslow", "BRESLOW_PATTERN", raw, t, n, default_m, stage,
                    extra={
                        "breslow_mm": mm,
                        "ulcerated": ulceration_status == "present",
                        "ulceration_status": ulceration_status,
                        "imaging_evidence": imaging_evidence,
                    },
                ))
            return hits

        melanoma_t_stage = extract_melanoma_t_category_stage(text, metastasis_detected, imaging_evidence)
        if melanoma_t_stage is not None and not melanoma_meta_confirmed:
            raw, t, n, m, stage = melanoma_t_stage
            hits.append(make_hit(
                "melanoma_t_category", "T_COMPONENT_PATTERN", raw, t, n, m, stage,
            ))
            return hits

        # Fallback : stade explicite mélanome
        explicit_stage = extract_explicit_stage(text)
        if explicit_stage and not melanoma_meta_confirmed:
            hits.append(make_hit(
                "explicit_stage", "EXPLICIT_STAGE_PATTERN",
                "explicit_stage_mention", "null", "null", "null", explicit_stage,
            ))
            return hits

        return hits

    # =========================================================================
    # BRANCHE SEIN
    # =========================================================================
    if is_breast_doc:
        is_neoadjuvant = detect_neoadjuvant(text)

        # Cas 1 : métastase détectée → Stage IV
        if metastasis_detected == "yes":
            LOGGER.info("Sein métastase | PDF=%s | date=%s → Stage IV", pdf_path.name, doc_date)
            hits.append(make_hit(
                "metastatic_first", "METASTASIS_PATTERN",
                "metastatic_signal", "null", "null", "m1", "Stage IV",
                extra={"is_metastatic_event": True},
            ))
            return hits

        # Cas 2 : pTNM post-op sein, même si le type documentaire n'est pas "pathology"
        breast_tnm = extract_breast_pathological_tnm(text)
        if breast_tnm is not None:
            raw, t, n, m = breast_tnm
            stage = compute_breast_stage(t, n, m)
            mode = "breast_neoadjuvant_yptnm" if is_neoadjuvant else "breast_pathological_ptnm"
            LOGGER.info(
                "Sein pTNM détecté%s | T=%s N=%s M=%s → %s | PDF=%s | date=%s",
                " (néoadjuvant)" if is_neoadjuvant else "",
                t, n, m, stage, pdf_path.name, doc_date,
            )
            hits.append(make_hit(
                mode, "BREAST_PATHOLOGICAL_TNM_PATTERN", raw, t, n, m, stage,
                extra={"is_neoadjuvant": is_neoadjuvant},
            ))
            return hits

        # Cas 3 : TNM clinique (cTNM) pré-opératoire (RCP, consultation, bilan)
        # Prioritaire si document explicitement pré-thérapeutique
        tnm_matches = list(TNM_PATTERN.finditer(text))
        if not tnm_matches:
            tnm_matches = list(TNM_LOOSE_PATTERN.finditer(text))
            chosen_mode = "breast_clinical_tnm_loose"
        else:
            chosen_mode = "breast_clinical_tnm_strict"

        if tnm_matches:
            for pattern_name, raw, t, n, m in tnm_rows(chosen_mode, tnm_matches):
                if has_post_treatment_tnm_prefix(raw):
                    continue
                stage = compute_breast_stage(t, n, m)
                hits.append(make_hit(
                    chosen_mode, pattern_name, raw, t, n, m, stage,
                ))
            if hits:
                return hits

        # Cas 4 : stade explicite
        explicit_stage = extract_explicit_stage(text)
        if explicit_stage:
            hits.append(make_hit(
                "explicit_stage", "EXPLICIT_STAGE_PATTERN",
                "explicit_stage_mention", "null", "null", "null", explicit_stage,
            ))
        return hits

    # =========================================================================
    # BRANCHE GÉNÉRALE (prostate + tous autres cancers)
    # =========================================================================

    # TNM strict → loose (prioritaire pour capturer un TNM historique dans le même document)
    tnm_matches = list(TNM_PATTERN.finditer(text))
    if tnm_matches:
        hit_rows = tnm_rows("TNM_PATTERN", tnm_matches)
        chosen_mode = "strict"
    else:
        tnm_loose_matches = list(TNM_LOOSE_PATTERN.finditer(text))
        hit_rows = tnm_rows("TNM_LOOSE_PATTERN", tnm_loose_matches)
        chosen_mode = "loose_fallback"

    if not hit_rows:
        if is_prostate_doc:
            summary_match = PROSTATE_SUMMARY_TNM_PATTERN.search(text)
            if summary_match:
                t_value = normalize_tnm_component(summary_match.group(1), "t")
                n_value = normalize_tnm_component(summary_match.group(2).replace("o", "0").replace("O", "0"), "n")
                m_value = normalize_tnm_component(summary_match.group(3).replace("o", "0").replace("O", "0"), "m")
                stage = compute_stage(t_value, n_value, m_value)
                if stage != "null":
                    raw_tnm = re.sub(r"\s+", " ", summary_match.group(0)).strip()
                    hits.append(make_hit(
                        "prostate_summary_tnm_fallback", "PROSTATE_SUMMARY_TNM_PATTERN",
                        raw_tnm, t_value, n_value, m_value, stage,
                    ))
                    return hits
        # Prostate T-only
        prostate_t_only = extract_prostate_t_only_stage(text, metastasis_detected)
        if prostate_t_only is not None:
            raw_tnm, t_value, n_value, m_value, stage = prostate_t_only
            hits.append(make_hit(
                "prostate_t_only_assumed_n0m0", "T_COMPONENT_PATTERN",
                raw_tnm, t_value, n_value, m_value, stage,
            ))
            return hits
        # Stage IV métastatique (fallback s'il n'y a pas de TNM exploitable)
        if metastasis_detected == "yes":
            hits.append(make_hit(
                "metastatic_first", "METASTASIS_PATTERN",
                "metastatic_signal", "null", "null", "m1", "Stage IV",
                extra={"is_metastatic_event": True},
            ))
            return hits
        # Stage explicite
        explicit_stage = extract_explicit_stage(text)
        if explicit_stage is not None:
            hits.append(make_hit(
                "explicit_stage", "EXPLICIT_STAGE_PATTERN",
                "explicit_stage_mention", "null", "null", "null", explicit_stage,
            ))
            return hits
        # Stage 0 (DCIS / in situ)
        stage_zero = infer_stage_zero_from_pathology(text, document_kind)
        if stage_zero is not None:
            hits.append(make_hit(
                "pathology_stage_zero", "DCIS/IN_SITU_RULE",
                "dcis_stage_zero_rule", "tis", "null", "null", stage_zero,
            ))
            return hits
        elif not args.only_stage_hits:
            LOGGER.info("No TNM hit | PDF=%s", pdf_path.name)
        return hits

    for pattern_name, raw, t, n, m in hit_rows:
        stage = compute_stage(t, n, m)
        mode_used = chosen_mode
        n_used, m_used = n, m
        if (
            stage == "null" and document_kind == "pathology"
            and t not in {"", "null", "tx"}
            and n not in {"", "null", "nx"}
            and m in {"", "null", "mx"}
        ):
            stage = compute_stage(t, n, "m0")
            if stage != "null":
                m_used = "m0"
                mode_used = f"{chosen_mode}_assumed_m0"
        if (
            stage == "null"
            and t not in {"", "null", "tx"}
            and is_prostate_doc
        ):
            n_known = n not in {"", "null", "nx"}
            m_known = m in {"m0", "m1", "m1a", "m1b", "m1c", "m1d"}
            if (
                n_known
                or (
                    detect_nodal_positive_signal(text) != "yes"
                    and detect_nodal_uncertain_signal(text) != "yes"
                )
            ) and (
                m_known or metastasis_detected != "yes"
            ):
                n_used = n if n_known else "n0"
                m_used = m if m_known else "m0"
                derived = compute_stage(t, n_used, m_used)
                if derived != "null":
                    stage = derived
                    mode_used = f"{chosen_mode}_prostate_assumed_n0m0"
        hits.append(make_hit(mode_used, pattern_name, raw, t, n_used, m_used, stage))

    return hits


# =============================================================================
# STAGE SELECTION LOGIC
# =============================================================================

def select_initial_stage(document_hits: list[dict], diagnosis_date: Optional[str] = None) -> Optional[tuple[dict, str]]:
    """
    Sélectionne le stade INITIAL AU DIAGNOSTIC (avant tout traitement).

    Priorités communes à toutes localisations :
    ────────────────────────────────────────────
    1. Date chronologique réelle (extraite du nom de fichier)
    2. Document antérieur au premier traitement / première métastase
    3. Type de document : pathology > rcp > radiology > consultation
    4. Complétude TNM : T+N+M > T+N > T seul

    Spécificités par localisation :
    ────────────────────────────────
    MÉLANOME
      → Chercher le Breslow le plus ancien AVANT la première métastase confirmée
      → Appliquer ulcération pour obtenir T4b si applicable → Stage IIC

    SEIN (chirurgie première)
      → Préférer pTNM anapath (breast_pathological_ptnm) = gold standard
      → Si néoadjuvant : ypTNM après chimio (mentionné comme tel)
      → cTNM pré-op utilisé si pTNM non disponible

    PROSTATE
      → cTNM IRM prioritaire
      → N0M0 inféré si T seul disponible

    GÉNÉRAL
      → TNM pré-traitement > TNM post-traitement
      → TNM avant première métastase si dossier métastatique
    """
    valid_hits = [
        row for row in document_hits
        if row["stage"] not in {"null", ""}
        and not is_forbidden_y_prefix_hit(row)
    ]
    has_prostate_context = any(row.get("is_prostate") for row in document_hits)
    breast_candidate_hits = [
        row for row in document_hits
        if row.get("is_breast")
        and not row.get("is_prostate")
        and not has_post_treatment_tnm_prefix(str(row.get("raw", "")))
        and not is_forbidden_y_prefix_hit(row)
        and normalize_tnm_component(row.get("t", ""), "t") not in {"", "tx"}
        and normalize_tnm_component(row.get("n", ""), "n") not in {"", "nx"}
    ]
    if not valid_hits and not breast_candidate_hits:
        return None

    # ── MÉLANOME ─────────────────────────────────────────────────────────────
    melanoma_hits = [row for row in valid_hits if row.get("is_melanoma")]
    if melanoma_hits:
        metastatic_events = [row for row in melanoma_hits if row.get("is_metastatic_event")]
        first_metastatic_date = min(
            (row["date"] for row in metastatic_events),
            default=None,
        )
        breslow_hits = [
            row for row in melanoma_hits
            if row["mode"] == "melanoma_breslow" and not row.get("is_metastatic_event")
        ]
        if breslow_hits:
            if first_metastatic_date is not None:
                pre_meta = [
                    row for row in breslow_hits
                    if parse_date_sort_key(row["date"]) <= parse_date_sort_key(first_metastatic_date)
                ]
                if pre_meta:
                    chosen = min(pre_meta, key=lambda r: parse_date_sort_key(r["date"]))
                else:
                    chosen = min(breslow_hits, key=lambda r: parse_date_sort_key(r["date"]))
            else:
                chosen = min(breslow_hits, key=lambda r: parse_date_sort_key(r["date"]))

            # Consolidation nodale dans les 90 jours après Breslow initial
            melanoma_all_hits = [row for row in document_hits if row.get("is_melanoma")]
            window_hits = [
                row for row in melanoma_all_hits
                if is_date_in_forward_window(row.get("date", "null"), chosen.get("date"), days=90)
            ]
            nodal_positive = any(row.get("nodal_signal") == "positive" for row in window_hits)
            nodal_negative = any(row.get("nodal_signal") == "negative" for row in window_hits)
            nodal_non_regional = any(row.get("nodal_signal") == "non_regional" for row in window_hits)
            has_imaging_window = any(row.get("imaging_evidence") or normalize_tnm_component(row.get("m", ""), "m") == "m0" for row in window_hits)
            confirmed_metastatic_window = any(
                row.get("mode") == "melanoma_metastasis_confirmed"
                or (
                    row.get("is_metastatic_event")
                    and row.get("mode") in {"metastatic_first", "melanoma_metastasis_confirmed"}
                )
                for row in window_hits
            )

            selected = dict(chosen)
            if nodal_non_regional and confirmed_metastatic_window:
                selected["m"] = next(
                    (
                        normalize_tnm_component(row.get("m", "m1a"), "m")
                        for row in window_hits
                        if row.get("nodal_signal") == "non_regional"
                    ),
                    "m1a",
                )
                selected["n"] = "nx"
                selected["stage"] = "Stage IV"
                return selected, "melanoma_breslow_plus90d_non_regional_nodal_m1"
            if nodal_positive:
                selected["n"] = "n1"
                selected["m"] = "m0" if has_imaging_window and normalize_tnm_component(selected.get("m", ""), "m") in {"", "mx"} else selected.get("m", "mx")
                selected["stage"] = compute_melanoma_stage(selected["t"], "n1", selected.get("m", "mx"), bool(selected.get("ulcerated")))
                return selected, "melanoma_breslow_plus90d_nodal_positive"
            if nodal_negative:
                selected["n"] = "n0"
                selected["m"] = "m0" if has_imaging_window and normalize_tnm_component(selected.get("m", ""), "m") in {"", "mx"} else selected.get("m", "mx")
                selected["stage"] = compute_melanoma_stage(selected["t"], "n0", selected.get("m", "mx"), bool(selected.get("ulcerated")))
                return selected, "melanoma_breslow_plus90d_nodal_negative"
            selected["n"] = "n0" if has_imaging_window else "nx"
            selected["m"] = "m0" if has_imaging_window else "mx"
            selected["stage"] = compute_melanoma_stage(selected["t"], selected["n"], selected["m"], bool(selected.get("ulcerated")))
            return selected, "melanoma_breslow_plus90d_imaging_n0m0" if has_imaging_window else "melanoma_breslow_plus90d_nxmx_no_imaging"

        non_meta = [row for row in melanoma_hits if not row.get("is_metastatic_event")]
        if non_meta:
            chosen = min(non_meta, key=lambda r: parse_date_sort_key(r["date"]))
            return chosen, "melanoma_non_metastatic_fallback"

        non_iv = [row for row in melanoma_hits if row["stage"] != "Stage IV"]
        if non_iv:
            chosen = max(non_iv, key=lambda r: stage_rank(r["stage"]))
            return chosen, "melanoma_best_non_iv"

    # ── SEIN ──────────────────────────────────────────────────────────────────
    breast_hits = [
        row for row in breast_candidate_hits
        if row["stage"] not in {"null", ""}
        or row["mode"] in {"breast_pathological_ptnm", "breast_clinical_tnm_strict", "breast_clinical_tnm_loose"}
    ]
    if breast_candidate_hits and not breast_hits and not valid_hits:
        return None

    if breast_hits and not has_prostate_context:
        def with_breast_stage(row: dict, reason: str, forced_m_value: str = "m0") -> tuple[dict, str]:
            selected = dict(row)
            if normalize_tnm_component(selected.get("m", ""), "m") in {"", "mx"}:
                selected["m"] = forced_m_value
            selected["stage"] = compute_breast_stage(selected["t"], selected["n"], selected["m"])
            return selected, reason

        if diagnosis_date is None:
            early_pool = [
                row for row in breast_hits
                if not row.get("is_metastatic_event")
                and not row.get("is_post_treatment", False)
                and row["mode"] != "breast_neoadjuvant_yptnm"
            ]
            if early_pool:
                # No diagnosis anchor: keep the oldest valid baseline signal.
                chosen = min(
                    early_pool,
                    key=lambda r: (
                        parse_date_sort_key(r["date"]),
                        0 if r["mode"] == "breast_pathological_ptnm" else 1,
                        breast_document_kind_priority(r["kind"]),
                    ),
                )
                return with_breast_stage(chosen, "breast_earliest_valid_when_diag_missing")

        # Priorité métier SEIN: premier pTNM trouvé dans les 3 mois suivant la date_diag.
        if diagnosis_date is not None:
            breast_ptnm_3m = [
                row for row in breast_hits
                if row["mode"] == "breast_pathological_ptnm"
                and is_date_in_forward_window(row["date"], diagnosis_date, days=90)
            ]
            if breast_ptnm_3m:
                chosen = min(
                    breast_ptnm_3m,
                    key=lambda r: (
                        parse_date_sort_key(r["date"]),
                        breast_document_kind_priority(r["kind"]),
                    ),
                )
                return with_breast_stage(chosen, "breast_first_ptnm_within_3m_post_diag")

        breast_window = [row for row in breast_hits if is_date_in_window(row["date"], diagnosis_date, days=62)]
        breast_pool = breast_window if breast_window else breast_hits

        # Règle M (fenêtre ±2 mois): M0 explicite > M1 métastatique > défaut M0
        m0_hits = [
            row for row in breast_pool
            if normalize_tnm_component(row.get("m", ""), "m") == "m0"
        ]
        metastatic_events_in_window = [row for row in breast_pool if row.get("is_metastatic_event")]
        forced_m_value = "m0"
        if m0_hits:
            forced_m_value = "m0"
        elif metastatic_events_in_window:
            forced_m_value = "m1"

        # Si M1 trouvé dans fenêtre: stop et Stage IV immédiat
        if forced_m_value == "m1" and metastatic_events_in_window:
            chosen = min(metastatic_events_in_window, key=lambda r: parse_date_sort_key(r["date"]))
            return chosen, "breast_window_m1_priority"

        # TN: parcours du plus récent au plus ancien dans la fenêtre
        breast_pool_recent = sorted(
            breast_pool,
            key=lambda r: (
                parse_date_sort_key(r["date"]),
                -breast_document_kind_priority(r["kind"]),
                r["idx"],
            ),
            reverse=True,
        )

        # Priorité 1 : pTN anapath pièce opératoire
        for row in breast_pool_recent:
            if row["mode"] == "breast_pathological_ptnm":
                t = row["t"]
                n = row["n"]
                stage = compute_breast_stage(t, n, forced_m_value)
                selected = dict(row)
                selected["m"] = forced_m_value
                selected["stage"] = stage
                return selected, "breast_window_ptnm_recent"

        # Priorité 2 : TNM explicite en CS/RCP (prendre le plus récent)
        for row in breast_pool_recent:
            if row["mode"] in {"breast_clinical_tnm_strict", "breast_clinical_tnm_loose"} and row["kind"] in {"consultation", "rcp"}:
                t = row["t"]
                n = row["n"]
                stage = compute_breast_stage(t, n, forced_m_value)
                selected = dict(row)
                selected["m"] = forced_m_value
                selected["stage"] = stage
                return selected, "breast_window_recent_tnm_cs_rcp"

        metastatic_events = [row for row in breast_hits if row.get("is_metastatic_event")]
        first_metastatic_date = min(
            (row["date"] for row in metastatic_events),
            default=None,
        )

        # Priorité 1 : pTNM anapath chirurgie première (non néoadjuvant)
        ptnm_hits = [
            row for row in breast_hits
            if row["mode"] == "breast_pathological_ptnm"
            and not row.get("is_neoadjuvant", False)
        ]
        if ptnm_hits:
            if first_metastatic_date:
                pre = [
                    r for r in ptnm_hits
                    if parse_date_sort_key(r["date"]) <= parse_date_sort_key(first_metastatic_date)
                ]
                if pre:
                    chosen = max(pre, key=lambda r: parse_date_sort_key(r["date"]))
                    return with_breast_stage(chosen, "breast_ptnm_primary_surgery")
            chosen = max(ptnm_hits, key=lambda r: parse_date_sort_key(r["date"]))
            return with_breast_stage(chosen, "breast_ptnm_primary_surgery")

        # Priorité 2 : cTNM pré-op (RCP, consultation)
        ctnm_hits = [
            row for row in breast_hits
            if row["mode"] in {"breast_clinical_tnm_strict", "breast_clinical_tnm_loose"}
            and not row.get("is_post_treatment", False)
        ]
        if ctnm_hits:
            chosen = max(
                ctnm_hits,
                key=lambda r: (
                    parse_date_sort_key(r["date"]),
                    -breast_document_kind_priority(r["kind"]),
                ),
            )
            return with_breast_stage(chosen, "breast_clinical_tnm_pre_treatment")

        # Fallback sein
        non_meta_breast = [row for row in breast_hits if not row.get("is_metastatic_event")]
        if non_meta_breast:
            chosen = max(
                non_meta_breast,
                key=lambda r: (
                    parse_date_sort_key(r["date"]),
                    -breast_document_kind_priority(r["kind"]),
                ),
            )
            return with_breast_stage(chosen, "breast_fallback")

        if not valid_hits:
            return None

    # ── PROSTATE (T baseline + consolidation N/M à +90 jours) ───────────────
    prostate_hits = [
        row for row in valid_hits
        if row.get("is_prostate")
        and row.get("t") not in {"", "null", "tx"}
        and not has_post_treatment_tnm_prefix(str(row.get("raw", "")))
    ]
    if prostate_hits:
        baseline = min(prostate_hits, key=lambda r: parse_date_sort_key(r["date"]))
        window_hits = [
            row for row in valid_hits
            if row.get("is_prostate")
            and is_date_in_forward_window(row.get("date", "null"), baseline.get("date"), days=90)
        ]

        def n_rank(n_val: str) -> int:
            n = normalize_tnm_component(n_val or "", "n")
            if n == "n3":
                return 5
            if n == "n2":
                return 4
            if n in {"n1", "n1a", "n1b", "n1c", "n1mi"}:
                return 3
            if n == "n0":
                return 2
            if n == "nx" or not n:
                return 1
            return 1

        best_n = baseline.get("n", "nx")
        best_m = baseline.get("m", "mx")
        for row in window_hits:
            n_val = row.get("n", "nx")
            m_val = normalize_tnm_component(row.get("m", ""), "m") or "mx"
            if n_rank(n_val) > n_rank(best_n):
                best_n = n_val
            # Prefer explicit distant metastasis if present in consolidation window.
            if m_val.startswith("m1"):
                best_m = m_val
            elif (normalize_tnm_component(best_m, "m") in {"", "mx"}) and m_val == "m0":
                best_m = "m0"

        t_val = baseline.get("t", "null")
        n_norm = normalize_tnm_component(best_n, "n")
        m_norm = normalize_tnm_component(best_m, "m")
        if n_norm in {"", "nx"} and all(normalize_tnm_component(r.get("n", ""), "n") in {"", "nx"} for r in window_hits):
            n_norm = "n0"
        if m_norm in {"", "mx"}:
            m_norm = "m0"
        stage = compute_stage(t_val, n_norm, m_norm)

        selected = dict(baseline)
        selected["n"] = n_norm
        selected["m"] = m_norm
        selected["stage"] = stage
        return selected, "prostate_t_baseline_plus90d_nm_consolidation"

    # ── GÉNÉRAL (prostate + autres) ──────────────────────────────────────────
    first_metastatic_date = min(
        (
            row["date"] for row in valid_hits
            if row["stage"] == "Stage IV" or row.get("is_metastatic_event")
        ),
        default=None,
    )

    # Candidats pré-métastase
    if first_metastatic_date is not None:
        pre_metastatic = [
            row for row in valid_hits
            if row["stage"] != "Stage IV"
            and not row.get("is_metastatic_event")
            and parse_date_sort_key(row["date"]) <= parse_date_sort_key(first_metastatic_date)
        ]
        if pre_metastatic:
            # Parmi eux, préférer pré-traitement > post-traitement
            pre_treatment = [r for r in pre_metastatic if not r.get("is_post_treatment")]
            pool = pre_treatment if pre_treatment else pre_metastatic
            chosen = min(
                pool,
                key=lambda r: (
                    parse_date_sort_key(r["date"]),
                    document_kind_priority(r["kind"]),
                    -tnm_completeness_score(r["t"], r["n"], r["m"]),
                ),
            )
            return chosen, "pre_metastatic_baseline_priority"

    # Candidats structurés TNM complets
    structured = [
        row for row in valid_hits
        if row["stage"] != "null"
        and row["pattern"] in {"TNM_PATTERN", "TNM_LOOSE_PATTERN", "T_COMPONENT_PATTERN"}
        and row["t"] not in {"", "null", "tx"}
        and row["n"] not in {"", "null", "nx"}
    ]
    if structured:
        pre_treatment = [r for r in structured if not r.get("is_post_treatment")]
        pool = pre_treatment if pre_treatment else structured
        chosen = min(
            pool,
            key=lambda r: (
                parse_date_sort_key(r["date"]),
                document_kind_priority(r["kind"]),
                -tnm_completeness_score(r["t"], r["n"], r["m"]),
            ),
        )
        return chosen, "structured_tnm_first_chronological"

    # Candidats pathologie
    pathology = [row for row in valid_hits if row["kind"] == "pathology" and row["stage"] != "null"]
    if pathology:
        chosen = max(
            pathology,
            key=lambda r: (
                tnm_completeness_score(r["t"], r["n"], r["m"]),
                stage_rank(r["stage"])[0],
                stage_rank(r["stage"])[1],
            ),
        )
        return chosen, "pathology_best_tnm"

    # Dernier recours : meilleur stade disponible
    chosen = max(
        valid_hits,
        key=lambda r: (stage_rank(r["stage"])[0], stage_rank(r["stage"])[1]),
    )
    return chosen, "best_available_stage"


# =============================================================================
# MAIN
# =============================================================================

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
    diagnosis_date: Optional[str] = None
    for metadata_path in metadata_files:
        metadata = load_metadata(metadata_path)
        ipp = metadata_to_ipp(metadata, metadata_path)
        if ipp == target_ipp and not is_excluded_document(metadata):
            selected.append((metadata_path, metadata, metadata_to_pdf_path(metadata_path)))
            if diagnosis_date is None:
                diagnosis_date = metadata_to_diagnosis_date(metadata)

    if not selected:
        LOGGER.warning("No files found for IPP=%s", target_ipp)
        return 0

    # ── TRI CHRONOLOGIQUE PAR NOM DE FICHIER ──────────────────────────────
    # On trie d'abord par la date extraite du nom de fichier (date clinique réelle),
    # puis par le nom complet pour départager les fichiers de même date.
    # Cela garantit l'ordre chronologique des événements indépendamment
    # de la date d'intégration dans le DPI.
    def sort_key(row: tuple[Path, dict, Path]) -> tuple[str, str]:
        metadata_path, metadata, pdf_path = row
        fname_date = extract_date_from_filename(pdf_path.name) or "99999999"
        return fname_date, pdf_path.name

    selected.sort(key=sort_key)

    total = len(selected)
    # Keep only final concise output: suppress verbose INFO logs during processing.
    LOGGER.setLevel(logging.WARNING)
    print(f"INFO | IPP={target_ipp} | documents found={total}")

    document_hits: list[dict] = []
    for idx, (metadata_path, metadata, pdf_path) in enumerate(selected, start=1):
        hits = process_document(idx, total, metadata, metadata_path, pdf_path, args)
        document_hits.extend(hits)

    if not document_hits:
        LOGGER.info("Aucun stade trouvé pour IPP=%s", target_ipp)
        return 0

    result = select_initial_stage(document_hits, diagnosis_date=diagnosis_date)
    if result is None:
        LOGGER.info("Aucun stade valide sélectionné pour IPP=%s", target_ipp)
        return 0

    chosen, selection_reason = result

    print("INFO | ------------------------------------------------------------")
    print(
        f"INFO | SELECTED | reason={selection_reason} | doc={chosen['idx']}/{chosen['total']} | "
        f"PDF={chosen['pdf']} | date={chosen['date']} | kind={chosen['kind']} | mode={chosen['mode']}"
    )
    print(
        f"INFO | match={chosen['pattern']} | raw='{chosen['raw']}' | "
        f"T={chosen['t']} N={chosen['n']} M={chosen['m']} | stage={chosen['stage']}"
    )
    breast_values = debug_consolidate_breast_anapath_variables(selected, diagnosis_date)
    if breast_values.get("breast_anapath_sources", "null") != "null":
        visible_keys = [
            "histology_type",
            "grade_sbr",
            "sbr_tubule_score",
            "sbr_nuclear_score",
            "sbr_mitotic_score",
            "er_percent",
            "er_intensity",
            "er_status",
            "pr_percent",
            "pr_intensity",
            "pr_status",
            "hormone_receptor_status_project",
            "her2_ihc_score",
            "her2_ish_result",
            "her2_status",
            "her2_qualification_project",
            "pdl1_cps_value",
            "pdl1_cps_status_project",
        ]
        formatted_values = " | ".join(f"{key}={breast_values.get(key, 'null')}" for key in visible_keys)
        print(f"INFO | BREAST_ANAPATH | {formatted_values}")
        print(f"INFO | BREAST_ANAPATH_SOURCES | {breast_values.get('breast_anapath_sources', 'null')}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
