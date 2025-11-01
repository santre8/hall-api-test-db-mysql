#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import re
import time
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import requests
from rapidfuzz import fuzz
# Importación corregida
from neo4j import GraphDatabase, Driver, WRITE_ACCESS 

# =============== CONFIG & CONSTANTS =================
INPUT_JSON = Path(r"C:\Users\sanda\Documents\Langara_College\DANA-4850-001-Capstone_Project\hall-api-test-db-mysql\api\data\upec_chemical_20_5.json")
OUTPUT_CSV = Path(r"C:\Users\sanda\Documents\Langara_College\DANA-4850-001-Capstone_Project\hall-api-test-db-mysql\wikidata\hal_field_audit_out\Wikidata_upec_chemical_20_5.csv")

# Neo4j CONFIG
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your_password" # ¡ACTUALIZA ESTA LÍNEA!

WIKIDATA_API = "https://www.wikidata.org/w/api.php"
HEADERS = {"User-Agent": "Keyword2Wikidata/1.2 (contact: your-email@example.com)"}

# Propiedades de Wikidata
P_INSTANCE_OF = "P31"
P_SUBCLASS_OF = "P279"
P_BNF_ID = "P268"
Q_DISAMBIGUATION = "Q4167410"

# Constantes de tu proyecto
LANGS = ["en", "fr"]
MIN_LABEL_SIM = 70
MIN_TOTAL_SCORE = 30
MAX_LEVELS_LINEAGE = 5
SEARCH_LIMIT = 50 
DISALLOWED_P31 = { "Q13442814", "Q571", "Q1002697", "Q737498", "Q732577", "Q47461344" }
PREFERRED_P31 = { "Q486972", "Q618123", "Q82794", "Q16889133", "Q151885", "Q11173", "Q11862829", "Q7187", "Q16521" }

# =============== NEO4J CONNECTOR =================
class Neo4jConnector:
    def __init__(self, uri, user, password):
        self.driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()

    def run_query(self, query: str, parameters: Optional[Dict] = None):
        # Usando WRITE_ACCESS para transacciones de escritura
        with self.driver.session(default_access_mode=WRITE_ACCESS) as session:
            try:
                result = session.execute_write(self._execute_query, query, parameters)
                return result
            except Exception as e:
                print(f"Error al ejecutar Cypher: {e}\nConsulta: {query}\nParámetros: {parameters}")
                return None

    @staticmethod
    def _execute_query(tx, query, parameters):
        return tx.run(query, parameters).consume()

# =============== NEO4J INGESTION LOGIC =================

# 1. Función para guardar la entidad QID y su jerarquía P279
# Acepta un mapa de etiquetas para los ancestros
def ingest_p279_hierarchy(connector: Neo4jConnector, entity_qid: str, entity_label: str, 
                         qid_paths: List[List[str]], labels_map: Dict[str, str]):
    """Guarda la entidad principal (Item) y todos los caminos P279 hacia sus ancestros, incluyendo sus etiquetas."""
    
    # 1. Garantizar que el nodo entidad exista con su label inicial
    connector.run_query("""
        MERGE (e:Item {qid: $qid})
        SET e.label = $label
    """, {"qid": entity_qid, "label": entity_label})

    # 2. Iterar sobre cada camino y crear las relaciones con labels en los nodos ancestros
    for path in qid_paths:
        current_child_qid = entity_qid
        
        for parent_qid in path:
            if parent_qid == current_child_qid:
                continue

            # Obtener el label para el nodo padre (ancestral)
            parent_label = labels_map.get(parent_qid, parent_qid)
            
            # Cypher MERGE: Crea los nodos si no existen y establece la propiedad 'label' en el padre.
            connector.run_query("""
                MERGE (child:Item {qid: $child_qid})
                MERGE (parent:Item {qid: $parent_qid})
                SET parent.label = $parent_label 
                MERGE (child)-[:SUBCLASS_OF]->(parent)
            """, {
                "child_qid": current_child_qid,
                "parent_qid": parent_qid,
                "parent_label": parent_label # Pasa el label del nodo padre
            })
            
            current_child_qid = parent_qid

    print(f"   -> [Neo4j] Item {entity_qid} ingresado con {len(qid_paths)} rutas P279 (y sus etiquetas).")


# 2. Función para guardar el documento, keyword y la relación de mapeo (Se mantiene igual)
def ingest_document_map(connector: Neo4jConnector, docid: str, keyword: str, qid: str):
    connector.run_query("""
        // 1. Nodos base
        MERGE (d:Document {id: $docid})
        MERGE (k:Keyword {name: $keyword})
        MERGE (q:Item {qid: $qid})

        // 2. Relación Documento -> Keyword
        MERGE (d)-[:CONTAINS_KEYWORD]->(k)

        // 3. Relación Keyword -> Item
        MERGE (k)-[:MAPS_TO]->(q)
    """, {
        "docid": docid,
        "keyword": keyword,
        "qid": qid
    })

# 3. Función para guardar las relaciones P31 (Instancia de) (Se mantiene igual)
def ingest_p31_types(connector: Neo4jConnector, entity_qid: str, p31_ids: set, p31_labels: Dict[str, str]):
    for p31_qid in p31_ids:
        label = p31_labels.get(p31_qid, p31_qid)
        
        connector.run_query("""
            MERGE (item:Item {qid: $item_qid})
            MERGE (type:Class {qid: $type_qid})
            SET type.label = $type_label
            MERGE (item)-[:INSTANCE_OF]->(type)
        """, {
            "item_qid": entity_qid,
            "type_qid": p31_qid,
            "type_label": label
        })


# =============== Funciones Helper (sin cambios) =================

_ws_re = re.compile(r"\s+", re.UNICODE)
_token_re = re.compile(r"[^\w\-]+")

def normalize_kw(s: str) -> str:
    if not s: return ""
    s = s.replace("\u00A0", " ").replace("\ufeff", "")
    s = _ws_re.sub(" ", s.strip())
    s = s.strip(";, ")
    return s

def tokenize(text: str) -> List[str]:
    return [t for t in _token_re.split((text or "").lower()) if t]

def singularize_en(word: str) -> str:
    w = normalize_kw(word)
    wl = w.lower()
    if len(w) > 3 and wl.endswith("ies"): return w[:-3] + "y"
    if len(w) > 3 and wl.endswith("ses"): return w[:-2]
    if len(w) > 2 and wl.endswith("s") and not wl.endswith("ss"): return w[:-1]
    return w

def _get(params: Dict, sleep_sec: float = 0.1) -> Dict:
    params = {**params, "format": "json"}
    for attempt in range(5):
        try:
            r = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            data = r.json()
            if "error" in data: raise RuntimeError(data["error"])
            time.sleep(sleep_sec)
            return data
        except Exception:
            if attempt == 4: raise
            time.sleep(0.5 * (attempt + 1))
    return {}

def wbsearchentities(search: str, language: str = "en", limit: int = SEARCH_LIMIT) -> List[Dict]:
    search = normalize_kw(search)
    return _get({"action": "wbsearchentities", "search": search, "language": language, "uselang": language, "type": "item", "limit": limit, "strictlanguage": 0}).get("search", [])

def wbsearch_label_only(search: str, language: str = "en", limit: int = SEARCH_LIMIT) -> List[Dict]:
    search = normalize_kw(search)
    return _get({"action": "wbsearchentities", "search": f"label:{search}", "language": language, "uselang": language, "type": "item", "limit": limit, "strictlanguage": 0}).get("search", [])

def chunked(seq, size):
    for i in range(0, len(seq), size): yield seq[i:i + size]

def wbgetentities(ids: List[str], languages: List[str] = LANGS) -> Dict:
    combined = {}
    for batch in chunked(list(ids), 50):
        data = _get({"action": "wbgetentities", "ids": "|".join(batch), "props": "labels|descriptions|aliases|claims", "languages": "|".join(languages), "languagefallback": 1}, sleep_sec=0.05)
        combined.update(data.get("entities", {}))
    return combined

def get_labels_for(qids: List[str], languages: List[str] = LANGS) -> Dict[str, str]:
    entities = wbgetentities(qids, languages)
    labels = {}
    for q, ent in entities.items():
        lab = None
        for lg in languages:
            if "labels" in ent and lg in ent["labels"]:
                lab = ent["labels"][lg]["value"]
                break
        labels[q] = lab or q
    return labels

def best_label_and_aliases_str(ent_like: Dict) -> str:
    label = ent_like.get("label") or ""
    aliases = " ".join(ent_like.get("aliases") or [])
    return f"{label} {aliases}".strip()

def label_similarity(keyword: str, ent_like: Dict) -> float:
    target = best_label_and_aliases_str(ent_like)
    return float(fuzz.token_sort_ratio(normalize_kw(keyword), normalize_kw(target)))

def context_overlap(keyword: str, context: str, ent_like: Dict) -> int:
    ctx_tokens = set(tokenize(normalize_kw(context)))
    label = (ent_like.get("label") or "")
    desc = (ent_like.get("description") or "")
    
    # MODIFICACIÓN CLAVE: Obtener las aliases originales.
    raw_aliases = " ".join(ent_like.get("aliases") or []) 
    
    # Crear una única cadena de contexto del candidato (Label, Desc, Aliases)
    candidate_context_str = " ".join([label, desc, raw_aliases])
    
    # Obtener todos los tokens del contexto del candidato
    all_candidate_tokens = tokenize(candidate_context_str)

    # Filtrar tokens que son el propio keyword (o sus variantes) para evitar sesgos
    keyword_tokens = set(tokenize(normalize_kw(keyword)))
    
    # Crear el set final de tokens del candidato que no sean parte del keyword
    cand_tokens = set(token for token in all_candidate_tokens if token not in keyword_tokens) 
    
    # Devolver la superposición
    return len(ctx_tokens & cand_tokens)

def total_score(keyword: str, context: str, ent_like: Dict, allow_exact_bonus: bool = True) -> float:
    lbl = normalize_kw(ent_like.get("label") or "").lower()
    kw_norm = normalize_kw(keyword).lower()
    kw_sing = singularize_en(kw_norm).lower()
    exact = (lbl == kw_norm) or (lbl == kw_sing)
    exact_bonus = 50.0 if (allow_exact_bonus and exact) else 0.0
    return exact_bonus + context_overlap(keyword, context, ent_like) + 0.6 * label_similarity(keyword, ent_like)

def _claim_ids(entity: Dict, pid: str) -> List[str]:
    out = []
    for cl in entity.get("claims", {}).get(pid, []):
        dv = cl.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        if isinstance(dv, dict) and dv.get("id"): out.append(dv["id"])
    return out

def get_p31_ids(entity: Dict) -> set:
    return set(_claim_ids(entity, P_INSTANCE_OF))

def type_bonus_or_block(p31s: set) -> Tuple[bool, float]:
    if p31s & DISALLOWED_P31: return True, 0.0
    bonus = 30.0 if (p31s & PREFERRED_P31) else 0.0
    return False, bonus

def expand_p279_paths(start_parents: List[str], max_levels: int, languages: List[str]) -> List[List[str]]:
    if not start_parents: return []
    paths = []
    frontier = [[p] for p in start_parents]
    for _ in range(max_levels - 1):
        new_frontier = []
        for path in frontier:
            current = path[-1]
            ent = wbgetentities([current], languages).get(current, {})
            parents = _claim_ids(ent, P_SUBCLASS_OF)
            if not parents:
                paths.append(path); continue
            for par in parents: new_frontier.append(path + [par])
        frontier = new_frontier or frontier
    for p in frontier:
        if p not in paths: paths.append(p)
    return paths

def is_disambiguation(qid: str, entity: Dict) -> bool:
    for cl in entity.get("claims", {}).get(P_INSTANCE_OF, []):
        v = cl.get("mainsnak", {}).get("datavalue", {}).get("value", {})
        if v.get("id") == Q_DISAMBIGUATION: return True
    return False

def extract_bnf_id(entity: Dict) -> Optional[str]:
    for cl in entity.get("claims", {}).get(P_BNF_ID, []):
        dv = cl.get("mainsnak", {}).get("datavalue", {})
        if dv.get("value"): return str(dv["value"])
    return None

def extract_label(entity: Dict, languages: List[str] = LANGS) -> str:
    for lg in languages:
        if "labels" in entity and lg in entity["labels"]: return entity["labels"][lg]["value"]
    labs = entity.get("labels", {})
    if labs: return list(labs.values())[0]["value"]
    return ""

def pick_exact_label_only(keyword: str) -> Optional[Dict]:
    kw_norm = normalize_kw(keyword).lower()
    kw_sing = singularize_en(kw_norm).lower()
    targets = {kw_norm, kw_sing}
    for lg in LANGS:
        # CORREGIDO: Usando 'limit=5'
        hits = wbsearch_label_only(kw_sing, language=lg, limit=5) or \
               wbsearchentities(kw_sing, language=lg, limit=5)
        for h in hits:
            lbl = normalize_kw(h.get("label") or "").lower()
            if lbl in targets:
                qid = h.get("id")
                ent = wbgetentities([qid]).get(qid, {})
                p31s = get_p31_ids(ent)
                block, type_bonus = type_bonus_or_block(p31s)
                if block: continue
                return {
                    "id": qid, "label": h.get("label"), "description": h.get("description"),
                    "aliases": h.get("aliases") or [], "language": lg, "label_similarity": 100.0,
                    "match_score": 50.0 + type_bonus, "__p31s": p31s, "__stage": "exact_label",
                }
    return None

def pick_with_context_then_exact(keyword: str, context: str) -> Optional[Dict]:
    keyword = normalize_kw(keyword); context = normalize_kw(context)
    terms = [keyword]; kw_sing = singularize_en(keyword)
    if kw_sing != keyword: terms.append(kw_sing)
    raw, seen = [], set()
    for term in terms:
        for lg in LANGS:
            for hit in wbsearchentities(term, language=lg, limit=SEARCH_LIMIT) or wbsearch_label_only(term, language=lg, limit=SEARCH_LIMIT):
                qid = hit.get("id")
                if not qid or qid in seen: continue
                seen.add(qid)
                raw.append({"id": qid, "label": hit.get("label"), "description": hit.get("description"), "aliases": hit.get("aliases") or [], "language": lg})
    if raw:
        ents = wbgetentities([c["id"] for c in raw]); candidates = []
        for c in raw:
            ent = ents.get(c["id"], {}); p31s = get_p31_ids(ent)
            block, type_bonus = type_bonus_or_block(p31s)
            if block: continue
            sim = label_similarity(keyword, c); score = total_score(keyword, context, c, allow_exact_bonus=True) + type_bonus
            c["label_similarity"] = sim; c["match_score"] = score; c["__p31s"] = p31s; candidates.append(c)
        if candidates:
            candidates.sort(key=lambda c: (c["match_score"], c["label_similarity"], -LANGS.index(c.get("language", "en")) if c.get("language", "en") in LANGS else -99,), reverse=True,)
            top = candidates[0]
            if top["label_similarity"] >= MIN_LABEL_SIM and top["match_score"] >= MIN_TOTAL_SCORE:
                top["__stage"] = "context"; return top
    return pick_exact_label_only(keyword)


# =============== Pipeline con Neo4j =================
def map_keywords(records: List[Dict], neo4j_conn: Neo4jConnector) -> List[Dict]:
    rows = []
    seen_pairs = set()

    for rec in records:
        title = rec.get("title_s") or ""
        abstract = rec.get("abstract_s") or ""
        context = f"{title}. {abstract}"
        docid = rec.get("docid") or rec.get("halId_s") or ""
        
        keywords = rec.get("keyword_s") or []
        if not keywords and rec.get("keywords_joined"):
            raw = rec["keywords_joined"]
            keywords = [k.strip() for k in re.split(r"[;,]", raw) if k.strip()]

        print(f"\n--- Procesando Documento {docid} con {len(keywords)} keywords ---")

        for kw in keywords:
            if (docid, kw) in seen_pairs: continue
            seen_pairs.add((docid, kw))

            # ... (Inicialización de variables para CSV)
            qid = label = bnf = ""
            disambig = False
            match_stage = "none"
            best_sim = 0.0
            best_score = 0.0
            p31s_out = set()
            p31_labels_out = ""
            p279_paths_labels: List[str] = []

            cand = pick_with_context_then_exact(kw, context)
            
            if cand:
                ent = wbgetentities([cand["id"]]).get(cand["id"], {})
                if ent:
                    disambig = is_disambiguation(cand["id"], ent)
                    if not disambig:
                        qid = cand["id"]
                        label = extract_label(ent)
                        bnf = extract_bnf_id(ent) or ""
                        match_stage = cand.get("__stage", "context_or_exact")
                        best_sim = cand.get("label_similarity", 0.0)
                        best_score = cand.get("match_score", 0.0)

                        # P31 (instancia de)
                        p31s_out = get_p31_ids(ent)
                        p31_labels = get_labels_for(list(p31s_out)) if p31s_out else {}
                        p31_labels_out = ";".join(p31_labels.get(x, x) for x in p31s_out)
                        
                        # Neo4j: Guardar P31 (Instancia de)
                        ingest_p31_types(neo4j_conn, qid, p31s_out, p31_labels)


                        # P279 (subclase de)
                        direct_p279 = _claim_ids(ent, P_SUBCLASS_OF)
                        if direct_p279:
                            qid_paths = expand_p279_paths(direct_p279, MAX_LEVELS_LINEAGE, LANGS)
                            
                            # 🎯 CAMBIO CLAVE: Obtener todas las etiquetas P279 para todos los QIDs
                            all_p279_qids = set()
                            for qpath in qid_paths:
                                all_p279_qids.update(qpath)
                            
                            p279_labels_map = get_labels_for(list(all_p279_qids), LANGS)
                            
                            # Neo4j: Guardar P279 (Jerarquía) - ¡AÑADIR EL MAPA DE ETIQUETAS!
                            ingest_p279_hierarchy(neo4j_conn, qid, label, qid_paths, p279_labels_map)

                            # CSV: Convertir QID paths a etiquetas para la columna (usando el nuevo mapa)
                            for qpath in qid_paths:
                                # Usamos p279_labels_map en lugar de llamar a get_labels_for
                                p279_paths_labels.append(" > ".join(p279_labels_map.get(q, q) for q in qpath))
                        
                        # Neo4j: Guardar el mapeo Documento-Keyword-Item (QID)
                        ingest_document_map(neo4j_conn, docid, kw, qid)

            # --- Escritura del CSV (se mantiene igual) ---
            paths = p279_paths_labels or [""] if qid else [""]
            
            for path_text in paths:
                rows.append({
                    "docid": docid, "title": title, "keyword": kw,
                    "wikidata_label": label, "wikidata_qid": qid,
                    "bnf_id": bnf, "p279_path": path_text, 
                    "retry_source": match_stage, "match_stage": match_stage, 
                    "is_disambiguation": "yes" if (cand and disambig) else "no",
                    "label_similarity": round(best_sim, 1), "match_score": round(best_score, 1), 
                    "p31_types": ";".join(sorted(p31s_out)) if p31s_out else "", 
                    "p31_label": p31_labels_out,
                })
                
    return rows

# =============== Main (sin cambios) =================
def main():
    print(f"🔗 Intentando conectar a Neo4j en {NEO4J_URI}...")
    try:
        # Usamos 127.0.0.1 como fallback si localhost sigue dando problemas
        uri_to_connect = NEO4J_URI.replace("localhost", "127.0.0.1")
        neo4j_conn = Neo4jConnector(uri_to_connect, NEO4J_USER, NEO4J_PASSWORD)
        neo4j_conn.driver.verify_connectivity()
        print("✅ Conexión con Neo4j exitosa.")
    except Exception as e:
        print(f"❌ Error al conectar a Neo4j. Verifica tus credenciales y si el servicio está corriendo. Detalle: {e}")
        return

    print(f"📥 Leyendo JSON de: {INPUT_JSON}")
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"🔍 Procesando {len(records)} records e ingresando en Neo4j y CSV...")
    
    rows = map_keywords(records, neo4j_conn)

    fieldnames = [
        "docid", "title", "keyword", "wikidata_label", "wikidata_qid",
        "bnf_id", "p279_path", "retry_source", "match_stage", "is_disambiguation",
        "label_similarity", "match_score", "p31_types", "p31_label"
    ]
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    print(f"\n💾 Guardando resultados en CSV: {OUTPUT_CSV}")
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    neo4j_conn.close()
    print("✅ Proceso finalizado. Conexión a Neo4j cerrada.")

if __name__ == "__main__":
    main()