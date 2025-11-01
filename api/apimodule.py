import os, re, time, requests, pandas as pd
from urllib.parse import urlencode
from pathlib import Path

# HAL portal + filters
HAL_PORTAL   = "u-pec"          # UPEC portal
LANG_FILTER  = 'language_s:en'  # English only
NEED_N       = 100              # how many rows for the sample
PAGE_SIZE    = 100              # rows per API page (safe 200–500)

# Disciplines to keep (case-insensitive substring match)
DISCIPLINES = [
    "Chemical Engineering",
    "Civil Engineering",
    "Marketing",
    "Political Science",
    "Computer Science",
]
DISC_RE = re.compile("|".join([re.escape(x) for x in DISCIPLINES]), re.IGNORECASE)

# Output folder/files
OUT_DIR  = "data"
CSV_OUT  = os.path.join(OUT_DIR, "upec_sample200_keywords_domains.csv")
XLSX_OUT = os.path.join(OUT_DIR, "upec_sample200_keywords_domains.xlsx")
os.makedirs(OUT_DIR, exist_ok=True)

# Must-have keywords (author keywords live under several fields)
KEYWORD_FQ = '(' + ' OR '.join([
    'keyword_s:[* TO *]',
    'keyword_en_s:[* TO *]',
    'keyword_fr_s:[* TO *]',
    'keyword_t:[* TO *]'
]) + ')'

# Fields to request (match HAL docs; wildcards OK in fl=)
FIELDS = ",".join([
    "docid",
    "halId_s",
    "title_s",
    "abstract_s",
    # (authors intentionally NOT requested)
    "keyword*",            # any keyword field variant
    "domainAll_s",         # human-readable domain(s)
    "domainAllCode_s",     # domain code(s)
    "linkExtUrl_s",        # external link(s) if any
    "files_s",              # attached file URLs (fallback)
    #data for autors DANN
    'authFirstName_s',
    'authFirstName_sci',
    'authLastName_s',
    'authLastName_sci',
    'authQuality_s',
    #data for Organism DANN
    'authOrganismId_i',
    'authOrganism_s',
    'authorityInstitution_s'
    #data for docs DANN
    'keyword_s',
    'keyword_sci',
    'keyword_t'
])

BASE = f"https://api.archives-ouvertes.fr/search/{HAL_PORTAL}/"


def fetch_page(cursor="*"):
    """Fetch one page from HAL using cursorMark pagination."""
    params = {
        "q": "*:*",
        "fl": FIELDS,
        "wt": "json",
        "rows": PAGE_SIZE,
        "sort": "docid asc",
        "cursorMark": cursor,
        "fq": [LANG_FILTER, KEYWORD_FQ],   # English + must have keywords
    }
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def to_list(v):
    if isinstance(v, list): return [str(x) for x in v]
    return [str(v)] if v else []

def hal_record_url(hal_id):
    return f"https://hal.science/{hal_id}" if isinstance(hal_id, str) and hal_id.strip() else None

def choose_url(link_ext, hal_url, files_s):
    """Prefer external link; else HAL record; else first absolute file URL."""
    if isinstance(link_ext, list) and link_ext:
        return link_ext[0]
    if isinstance(link_ext, str) and link_ext.strip():
        return link_ext.strip()
    if hal_url:
        return hal_url
    flist = files_s if isinstance(files_s, list) else ([files_s] if files_s else [])
    for f in flist:
        if isinstance(f, str) and f.startswith("http"):
            return f
    return hal_url

def consolidate_keywords(doc: dict) -> str:
    kw = []
    for k, v in doc.items():
        if k.lower().startswith("keyword"):
            if isinstance(v, list):
                kw += [str(x) for x in v if str(x).strip()]
            elif v:
                kw.append(str(v))
    kw = sorted(set([x.strip() for x in kw if x.strip()]))
    return "; ".join(kw)

def consolidate_domains(doc: dict) -> str:
    labels = to_list(doc.get("domainAll_s"))
    codes  = to_list(doc.get("domainAllCode_s"))
    dom = sorted(set([*labels, *codes]))
    return "; ".join([x.strip() for x in dom if x.strip()])

def matches_disciplines(texts) -> bool:
    """True if any of the provided strings contains one of the target disciplines."""
    blob = " ; ".join([t for t in texts if isinstance(t, str)])
    return bool(DISC_RE.search(blob))


def probe(fqs):
    params = {"q": "*:*", "wt": "json", "rows": 0}
    params["fq"] = fqs
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("response", {}).get("numFound", 0)

print("UPEC English only:", probe([LANG_FILTER]))
print("UPEC English + keywords:", probe([LANG_FILTER, KEYWORD_FQ]))


# === Improved harvest: map domain codes to 5 disciplines and keep crawling until 200 ===
import re


records, cursor = [], "*"

# 1) mapping from HAL domain codes to  5 disciplines
#    (patterns are case-insensitive and match code prefixes)
MAP_PATTERNS = [
    # Computer Science
    (re.compile(r'^info(\.|$)', re.I), "Computer Science"),
    # Political Science (sciences politiques)
    (re.compile(r'^shs\.scipo$', re.I), "Political Science"),
    # Marketing (falls under "Gestion" = Management; also SHS "eco" may contain marketing in some records)
    (re.compile(r'^shs\.gestion$', re.I), "Marketing"),
    # Chemical Engineering (HAL codes vary; include chemistry + engineering sciences chemistry)
    (re.compile(r'^(chim|ens\.chim|sdeng\.chim)(\.|$)', re.I), "Chemical Engineering"),
    # Civil Engineering (typically engineering sciences / civil engineering, materials, structures)
    (re.compile(r'^(sdeng\.civi|sdeng\.mech|sdeng\.mat|sdeng\.genie-civi)(\.|$)', re.I), "Civil Engineering"),
]

DISC_WORDS = {
    "Computer Science":    re.compile(r'\bcomputer science|informatics|algorithm|machine learning|ai\b', re.I),
    "Political Science":   re.compile(r'\bpolitical science|politics|public policy|governance\b', re.I),
    "Marketing":           re.compile(r'\bmarketing|consumer|branding|advertising|retail\b', re.I),
    "Chemical Engineering":re.compile(r'\bchemical engineering|process engineering|reactor|catalyst|polymer\b', re.I),
    "Civil Engineering":   re.compile(r'\bcivil engineering|geotechnical|structural|transportation engineering|concrete\b', re.I),
}

def map_codes_to_discipline(codes:list, labels:list) -> str|None:
    """Try code-based mapping first; return one of the 5 disciplines or None."""
    codes = [c.strip() for c in codes if isinstance(c, str)]
    for code in codes:
        for pat, name in MAP_PATTERNS:
            if pat.search(code):
                return name
    # sometimes labels carry English category names; try a light label hint
    lbl_blob = " ; ".join([l for l in labels if isinstance(l, str)])
    if re.search(r'\b(science|engineering|management|political)\b', lbl_blob, re.I):
        # we still prefer word-based fallback below to avoid wrong guesses
        return None
    return None

def fallback_text_match_for_discipline(texts:list) -> str|None:
    """If code mapping failed, use title/abstract/keywords text to infer a bucket."""
    blob = " ; ".join([t for t in texts if isinstance(t, str)])
    for name, rx in DISC_WORDS.items():
        if rx.search(blob):
            return name
    return None

def consolidate_keywords(doc: dict) -> str:
    kw = []
    for k, v in doc.items():
        if k.lower().startswith("keyword"):
            if isinstance(v, list):
                kw += [str(x) for x in v if str(x).strip()]
            elif v:
                kw.append(str(v))
    kw = sorted(set([x.strip() for x in kw if x.strip()]))
    return "; ".join(kw)

def consolidate_domains(doc: dict):
    labels = doc.get("domainAll_s")
    codes  = doc.get("domainAllCode_s")
    labels = labels if isinstance(labels, list) else ([labels] if labels else [])
    codes  = codes  if isinstance(codes,  list) else ([codes]  if codes  else [])
    # clean
    labels = [str(x).strip() for x in labels if str(x).strip()]
    codes  = [str(x).strip() for x in codes  if str(x).strip()]
    return labels, codes

def choose_url(link_ext, hal_url, files_s):
    if isinstance(link_ext, list) and link_ext:
        return link_ext[0]
    if isinstance(link_ext, str) and link_ext.strip():
        return link_ext.strip()
    if hal_url:
        return hal_url
    flist = files_s if isinstance(files_s, list) else ([files_s] if files_s else [])
    for f in flist:
        if isinstance(f, str) and f.startswith("http"):
            return f
    return hal_url

def hal_record_url(hal_id):
    return f"https://hal.science/{hal_id}" if isinstance(hal_id, str) and hal_id.strip() else None


# how to save files csv ad json
def saveto_csv_and_excel(df_sample):
    """Save dataframe to CSV and Excel with adjusted column widths."""
    df_sample.to_csv(CSV_OUT, index=False, encoding="utf-8")
    print("Saved CSV:", os.path.abspath(CSV_OUT))

    # Excel with widths
    with pd.ExcelWriter(XLSX_OUT, engine="xlsxwriter") as writer:
        df_sample.to_excel(writer, index=False, sheet_name="sample")
        ws = writer.sheets["sample"]
        widths = {"A":12,"B":20,"C":60,"D":90,"E":50,"F":25,"G":40,"H":28,"I":60} # adjust as needed
        for col, w in widths.items():
            ws.set_column(f"{col}:{col}", w)
    print("Saved Excel:", os.path.abspath(XLSX_OUT))
    

def savetojson(df, output_filename="output.json"):
    """
    Save a pandas DataFrame as a JSON file inside /api/data/
    Automatically creates the folder if it doesn't exist.
    """
    # Define the base folder (/api/data)
    base_dir = Path(__file__).resolve().parent / "data"
    base_dir.mkdir(parents=True, exist_ok=True)

    # Build full path inside /api/data/
    output_path = base_dir / output_filename

    # Save the DataFrame to JSON
    df.to_json(output_path, orient="records", indent=2, force_ascii=False)
    print(f"JSON file saved successfully at: {output_path}")



