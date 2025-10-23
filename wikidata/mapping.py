# Enhanced Wikidata Mapping (local paths + UA + gentle rate limiting)
import json, time, os
from pathlib import Path
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON
from concurrent.futures import ThreadPoolExecutor, as_completed
from rapidfuzz import fuzz, process

# --- Paths (relative to repo root) ---
REPO_ROOT = Path(__file__).resolve().parents[1]  # .../hall-api-test-db-mysql
INPUT_JSON = REPO_ROOT / "api" / "data" / "upec_sample200_keywords_domains.json"
OUTPUT_DIR = REPO_ROOT / "wikidata" / "hal_field_audit_out"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_CSV = OUTPUT_DIR / "Upec_Wikidata_Enriched_Improved.csv"

# --- Load HAL documents ---
with open(INPUT_JSON, "r", encoding="utf-8") as f:
    documents = json.load(f)

# The file may use different keys; normalize here
def get_title(d):    return d.get("title") or d.get("title_s") or ""
def get_abstract(d):return d.get("abstract") or d.get("abstract_s") or ""
def get_keywords(d):
    # common HAL shapes: list under 'keywords' or 'keyword_s'
    ks = d.get("keywords")
    if ks is None: ks = d.get("keyword_s")
    if isinstance(ks, str):  # sometimes comma-separated
        ks = [k.strip() for k in ks.split(",") if k.strip()]
    return ks or []

# Flatten keywords with context
keyword_entries = []
for doc in documents:
    title = get_title(doc)
    abstract = get_abstract(doc)
    for kw in get_keywords(doc):
        keyword_entries.append({"keyword": kw, "title": title, "abstract": abstract})

# --- SPARQL setup (Wikidata requires a descriptive User-Agent) ---
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setReturnFormat(SPARQL_JSON)
sparql.addCustomHttpHeader("User-Agent",
    "SciKey-Wikidata-Mapping/1.0 (Carmen Sandate; academic use)")

# Simple cache to avoid repeating requests
dependency_cache = {}

def search_candidates(term, limit=8, sleep=0.35):
    clean = term.strip().lower().replace('"','').replace('\\','')
    query = f"""
    SELECT ?item ?itemLabel ?bnfID WHERE {{
      ?item rdfs:label ?label .
      FILTER(CONTAINS(LCASE(?label), "{clean}"))
      MINUS {{ ?item wdt:P31 wd:Q4167410 }}  # fuera páginas de desambiguación
      OPTIONAL {{ ?item wdt:P268 ?bnfID. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,fr". }}
    }}
    LIMIT {limit}
    """
    sparql.setQuery(query)
    try:
        time.sleep(sleep)
        return sparql.query().convert()["results"]["bindings"]
    except Exception:
        return []



def is_disambiguation(qid):
    query = (
        "ASK { "
        f"  wd:{qid} wdt:P31 wd:Q4167410. "
        "}"
    )
    sparql.setQuery(query)
    try:
        time.sleep(0.2)
        return sparql.query().convert()["boolean"]
    except Exception:
        return False

def score_candidate(label, keyword, title, abstract):
    context = f"{keyword} {title} {abstract}".lower()
    return fuzz.partial_ratio(label.lower(), context)

def query_dependencies(qid):
    query = (
        "SELECT ?property ?value ?valueLabel WHERE { "
        "  VALUES ?property { wdt:P279 wdt:P31 wdt:P361 } "
        f"  wd:{qid} ?property ?value. "
        '  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". } '
        "}"
    )
    sparql.setQuery(query)
    try:
        time.sleep(0.25)
        result = sparql.query().convert()["results"]["bindings"]
        dependency_cache[qid] = result
        return result
    except Exception:
        return []

def build_lineage(qid, max_nodes=40):
    visited, lineage = set(), []
    def traverse(q):
        if q in visited or len(lineage) >= max_nodes:
            return
        visited.add(q)
        for dep in query_dependencies(q):
            dep_label = dep["valueLabel"]["value"]
            dep_qid = dep["value"]["value"].split("/")[-1]
            lineage.append(dep_label)
            traverse(dep_qid)
    traverse(qid)
    return " → ".join(lineage)

def enrich_keyword(entry):
    kw, title, abstract = entry["keyword"], entry["title"], entry["abstract"]

    # Step 1: context-aware search
    candidates = search_candidates(f"{kw} {title} {abstract}", limit=10, sleep=0.6)
    scored = []
    for c in candidates:
        label = c["itemLabel"]["value"]
        qid = c["item"]["value"].split("/")[-1]
        if is_disambiguation(qid):
            continue
        scored.append((score_candidate(label, kw, title, abstract), c))
    scored.sort(reverse=True, key=lambda x: x[0])
    match_source = "context"

    # Step 2: fallback to keyword-only
    if not scored:
        candidates = search_candidates(kw, limit=10, sleep=0.6)
        scored = []
        for c in candidates:
            label = c["itemLabel"]["value"]
            qid = c["item"]["value"].split("/")[-1]
            if is_disambiguation(qid):
                continue
            scored.append((score_candidate(label, kw, "", ""), c))
        scored.sort(reverse=True, key=lambda x: x[0])
        match_source = "fallback"

    if scored:
        top = scored[0][1]
        label = top["itemLabel"]["value"]
        qid = top["item"]["value"].split("/")[-1]
        bnf_id = top.get("bnfID", {}).get("value")
        lineage_path = build_lineage_3levels(qid)
    else:
        label = qid = bnf_id = lineage_path = None
        match_source = "none"

    return {
        "keyword": kw,
        "title": title,
        "wikidata_label": label,
        "wikidata_qid": qid,
        "bnf_id": bnf_id,
        "lineage_path": lineage_path,
        "match_source": match_source
    }
# --- NUEVO: lineage de 3 niveles, 1 sola consulta, sin DFS ---
from functools import lru_cache

@lru_cache(maxsize=4096)
def build_lineage_3levels(qid: str) -> str:
    """
    Construye un lineage hasta 3 niveles usando una sola consulta SPARQL.
    Niveles a través de P279 (subclass of), P31 (instance of) y P361 (part of).
    Devuelve un string con formato: "Nivel1_1 / Nivel1_2 → Nivel2_1 / ... → Nivel3_1 / ..."
    """
    query = f"""
    SELECT ?l1 ?l2 ?l3 WHERE {{
      wd:{qid} (wdt:P279|wdt:P31|wdt:P361) ?n1 .
      OPTIONAL {{ ?n1 (wdt:P279|wdt:P31|wdt:P361) ?n2 . }}
      OPTIONAL {{ ?n2 (wdt:P279|wdt:P31|wdt:P361) ?n3 . }}
      SERVICE wikibase:label {{
        bd:serviceParam wikibase:language "en,fr" .
        ?n1 rdfs:label ?l1 .
        OPTIONAL {{ ?n2 rdfs:label ?l2 . }}
        OPTIONAL {{ ?n3 rdfs:label ?l3 . }}
      }}
    }}
    LIMIT 200
    """
    try:
        # usa el mismo objeto `sparql` que ya tienes configurado
        sparql.setQuery(query)
        time.sleep(0.25)  # rate limiting suave
        rows = sparql.query().convert()["results"]["bindings"]

        # Agrupa etiquetas por nivel y quita duplicados manteniendo orden alfabético estable
        L1 = sorted({r["l1"]["value"] for r in rows if "l1" in r})
        L2 = sorted({r["l2"]["value"] for r in rows if "l2" in r})
        L3 = sorted({r["l3"]["value"] for r in rows if "l3" in r})

        parts = []
        if L1: parts.append(" / ".join(L1))
        if L2: parts.append(" / ".join(L2))
        if L3: parts.append(" / ".join(L3))
        return " → ".join(parts)
    except Exception:
        return ""


# --- Run (keep concurrency modest to respect the API) ---
results = []
with ThreadPoolExecutor(max_workers=3) as ex:
    futures = [ex.submit(enrich_keyword, e) for e in keyword_entries]
    for fut in as_completed(futures):
        results.append(fut.result())

# --- Save ---
df = pd.DataFrame(results)
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
print(f"✅ Saved: {OUTPUT_CSV}")
