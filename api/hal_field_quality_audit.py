import os
import time
import math
import json
import re
from typing import Any, Dict, List, Tuple, Optional
import requests
import pandas as pd

# =========================
# CONFIGURACIÓN RÁPIDA
# =========================
PORTAL = "u-pec"         # p.ej. "u-pec" o "hal" (portal general)
ROWS_PER_PAGE = 200      # seguro entre 200–500
MAX_DOCS = 10         # cuántos docs quieres auditar (sube si quieres más robustez)
SLEEP_SEC = 0.12         # respeta API

# Filtros (agrega/quita; se envían como lista fq=)
FQS = [
    "language_s:en",     # solo en inglés (opcional)
    # "status_i:200",    # si quieres SOLO publicados; si ves que no filtra, coméntalo
    # "docType_s:ART",   # ejemplo: solo 'article'; ajusta según tus necesidades
    # "keyword_s:[* TO *]"  # exigir que tenga keywords
]

# Dónde guardar
OUT_DIR = "hal_field_audit_out"
os.makedirs(OUT_DIR, exist_ok=True)

# =========================
# FUNCIONES AUXILIARES
# =========================
BASE = f"https://api.archives-ouvertes.fr/search/{PORTAL}/"

def fetch_page(cursor: str = "*", rows: int = ROWS_PER_PAGE, fqs: Optional[List[str]] = None) -> Dict[str, Any]:
    """Trae una página usando cursorMark y pidiendo TODOS los campos (fl=*)."""
    params = {
        "q": "*:*",
        "fl": "*",             # <-- TODOS los campos disponibles
        "wt": "json",
        "rows": rows,
        "sort": "docid asc",   # cursorMark requiere un orden estable
        "cursorMark": cursor,
    }
    if fqs:
        params["fq"] = fqs
    r = requests.get(BASE, params=params, timeout=40)
    r.raise_for_status()
    return r.json()

def normalize_value(v: Any) -> Any:
    """Normaliza para DataFrame: deja listas como listas; objetos como JSON string."""
    # HAL devuelve mezclas: strings, listas, y a veces objetos.
    if isinstance(v, (str, int, float)) or v is None:
        return v
    if isinstance(v, list):
        return v  # mantenemos lista para poder detectar 'tipo lista'
    # si es dict u otro, serialize
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return str(v)

def harvest(max_docs: int, fqs: Optional[List[str]]) -> List[Dict[str, Any]]:
    records, cursor = [], "*"
    while len(records) < max_docs:
        data = fetch_page(cursor=cursor, rows=ROWS_PER_PAGE, fqs=fqs)
        docs = data.get("response", {}).get("docs", [])
        if not docs:
            break
        for d in docs:
            # normalizamos valores por clave
            norm = {k: normalize_value(v) for k, v in d.items()}
            records.append(norm)
            if len(records) >= max_docs:
                break
        next_c = data.get("nextCursorMark")
        if not next_c or next_c == cursor:
            break
        cursor = next_c
        time.sleep(SLEEP_SEC)
    return records

def build_rectangular_df(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Une el universo de claves y regresa un DataFrame rectangular."""
    # pandas ya alinea por columnas; nos aseguramos de no perder listas
    df = pd.DataFrame.from_records(records)
    return df

def is_empty_cell(x: Any) -> bool:
    """Considera vacío: None, '', [], {}."""
    if x is None:
        return True
    if isinstance(x, str) and x.strip() == "":
        return True
    if isinstance(x, list) and len(x) == 0:
        return True
    if isinstance(x, dict) and len(x) == 0:
        return True
    return False

def cell_type(x: Any) -> str:
    if isinstance(x, list):
        return "list"
    if isinstance(x, (str, int, float)) or x is None:
        return "scalar"
    return "json"

def avg_len_nonempty(series: pd.Series) -> float:
    """Longitud media (en caracteres) solo en celdas no vacías (scalar o json stringify)."""
    lens = []
    for v in series:
        if is_empty_cell(v):
            continue
        if isinstance(v, list):
            s = "; ".join([str(t) for t in v])
        else:
            s = str(v)
        lens.append(len(s))
    return float(sum(lens)) / len(lens) if lens else 0.0

def example_nonempty(series: pd.Series) -> Any:
    for v in series:
        if not is_empty_cell(v):
            return v
    return None

def predominant_cell_type(series: pd.Series) -> str:
    counts = {"scalar":0, "list":0, "json":0}
    for v in series:
        t = cell_type(v)
        counts[t] = counts.get(t, 0) + 1
    return max(counts, key=counts.get)

def quality_report(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve un DF con métricas por campo."""
    rows = []
    n = len(df)
    for col in df.columns:
        s = df[col]
        nonempty_mask = ~s.apply(is_empty_cell)
        nonempty = int(nonempty_mask.sum())
        coverage = nonempty / n if n else 0.0
        nunique = s[nonempty_mask].astype(str).nunique() if nonempty else 0
        ex = example_nonempty(s)
        avglen = avg_len_nonempty(s)
        ptype = predominant_cell_type(s)
        rows.append({
            "field": col,
            "nonempty": nonempty,
            "total_rows": n,
            "coverage_pct": round(100*coverage, 2),
            "unique_nonempty": nunique,
            "predominant_type": ptype,
            "avg_len_nonempty": round(avglen, 1),
            "example_value": ex if (isinstance(ex, (str,int,float)) or ex is None) else json.dumps(ex, ensure_ascii=False)  # para Excel
        })
    rep = pd.DataFrame(rows).sort_values(["coverage_pct","field"], ascending=[False, True])
    return rep

def save_outputs(df_docs: pd.DataFrame, df_quality: pd.DataFrame):
    csv_docs = os.path.join(OUT_DIR, "sample_docs_rectangular.csv")
    csv_quality = os.path.join(OUT_DIR, "field_quality_report.csv")
    xlsx_path = os.path.join(OUT_DIR, "hal_field_quality.xlsx")

    df_docs.to_csv(csv_docs, index=False, encoding="utf-8")
    df_quality.to_csv(csv_quality, index=False, encoding="utf-8")

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        df_quality.to_excel(writer, index=False, sheet_name="field_quality")
        df_docs.head(200).to_excel(writer, index=False, sheet_name="sample_200")  # muestra para inspección
        # ancho de columnas para calidad
        ws_q = writer.sheets["field_quality"]
        ws_q.set_column("A:A", 36)  # field
        ws_q.set_column("B:D", 12)  # counts
        ws_q.set_column("E:E", 16)  # unique
        ws_q.set_column("F:F", 18)  # predominant_type
        ws_q.set_column("G:G", 18)  # avg_len
        ws_q.set_column("H:H", 80)  # example_value

    print(f"Guardado CSV docs: {os.path.abspath(csv_docs)}")
    print(f"Guardado CSV quality: {os.path.abspath(csv_quality)}")
    print(f"Guardado Excel: {os.path.abspath(xlsx_path)}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print(f"Portal: {PORTAL} | Filtros: {FQS} | Máx. docs: {MAX_DOCS}")
    records = harvest(MAX_DOCS, FQS)
    print(f"Docs recolectados: {len(records)}")

    if not records:
        print("No se recuperaron documentos con esos filtros. Intenta quitar/ajustar FQS.")
        exit(0)

    df_docs = build_rectangular_df(records)
    # opcional: reordenar primeras columnas “clásicas” si existen
    front_cols = [c for c in ["docid","halId_s","title_s","abstract_s","domainAll_s","domainAllCode_s","keyword_s"] if c in df_docs.columns]
    other_cols = [c for c in df_docs.columns if c not in front_cols]
    df_docs = df_docs[front_cols + other_cols]

    df_quality = quality_report(df_docs)

    # Recomendación “naive” para KEEP en DB (ajusta umbral según tu proyecto)
    COV_THRESHOLD = 30.0  # % mínimo
    df_quality["KEEP_candidate"] = df_quality["coverage_pct"].apply(lambda x: "YES" if x >= COV_THRESHOLD else "NO")

    save_outputs(df_docs, df_quality)

    # Mini resumen por consola
    print("\nTOP 25 campos por cobertura:")
    print(df_quality.head(25)[["field","coverage_pct","predominant_type","unique_nonempty"]].to_string(index=False))

    # Sugerencia: imprime algunas claves interesantes si existen
    for k in ["status_i","docType_s","producedDate_tdate","submittedDate_tdate","authorityInstitution_s","authOrganismId_i"]:
        if k in df_docs.columns:
            filled = df_docs[k].notna().sum()
            print(f"Campo {k}: {filled}/{len(df_docs)} no vacíos")
