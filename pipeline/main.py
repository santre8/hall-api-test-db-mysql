# create a command-line runnable pipeline
# from etl.extract import extract_data
# from etl.transform import clean_data, transform_data
# import etl.load as load

import yaml
import os
import time
import pandas as pd
from sqlalchemy import create_engine

# import pipeline configuration
# with open('config.yaml', 'r') as file:
#     config_data = yaml.safe_load(file)


# ====== Normalizers to your schema ======
from pipeline.load import load_data


def normalize_documents(df: pd.DataFrame) -> pd.DataFrame:
    # Your table wants: id (auto), doc_id, title, abstract
    # Map df fields: docid -> doc_id, title_s -> title, abstract_s -> abstract
    out = pd.DataFrame({
        "doc_id": pd.to_numeric(df.get("docid"), errors="coerce"),
        "title": df.get("title_s"),
        "abstract": df.get("abstract_s").astype("string")
    })
    out = out.dropna(subset=["doc_id"])
    return out

def normalize_authors(df: pd.DataFrame) -> pd.DataFrame:
    # authors table: doc_id, authFirstName_s, authFirstName_sci, authLastName_s,
    #                authLastName_sci, authQuality_s, organismId_i
    base_doc = pd.to_numeric(df.get("docid"), errors="coerce")
    # Ensure lists; explode each author attribute
    fn = df.get("authFirstName_s")
    ln = df.get("authLastName_s")
    qual = df.get("authQuality_s")
    fn = fn.apply(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [v]))
    ln = ln.apply(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [v]))
    qual = qual.apply(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [v]))

    rows = []
    print(df.get("organismId_i"))
    for doc, fns, lns, quals, org_id in zip(base_doc, fn, ln, qual, df.get("organismId_i")):


        n = max(len(fns), len(lns), len(quals))
        if n == 0:
            continue
        # pad to same length
        fns += [""] * (n - len(fns))
        lns += [""] * (n - len(lns))
        quals += [""] * (n - len(quals))
        for i in range(n):
            rows.append({
                "doc_id": doc,
                "authFirstName_s": fns[i] or None,
                "authFirstName_sci": None,  # you can map if you have these
                "authLastName_s": lns[i] or None,
                "authLastName_sci": None,
                "authQuality_s": quals[i] or None,
                # "organismId_i": org_id if pd.notna(org_id) else None TODO several
            })
    return pd.DataFrame(rows)

def normalize_keywords(df: pd.DataFrame) -> pd.DataFrame:
    # keywords table: doc_id, keyword_s, keyword_sci, keyword_t
    base_doc = pd.to_numeric(df.get("docid"), errors="coerce")
    kw = df.get("keyword_s")
    kw = kw.apply(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [v]))
    rows = []
    for doc, kws, kw_sci, kw_t in zip(base_doc, kw, df.get("keyword_sci"), df.get("keyword_t")):
        if not kws:
            continue
        for k in kws:
            rows.append({
                "doc_id": doc,
                "keyword_s": k,
                "keyword_sci": None if pd.isna(kw_sci) else kw_sci,
                "keyword_t": None if pd.isna(kw_t) else kw_t
            })
    return pd.DataFrame(rows)

def normalize_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    # identifiers: doc_id, doi_s, halId_s, isbn
    out = pd.DataFrame({
        "doc_id": pd.to_numeric(df.get("docid"), errors="coerce"),
        "doi_s": df.get("doi_s"),
        "halId_s": df.get("halId_s"),
        "isbn": df.get("isbn")
    })
    out = out.dropna(subset=["doc_id"])
    return out

# ====== Your crawler glue: build df_sample (using your existing code) ======

def crawl_to_df_sample():
    # ---- paste your crawling loop here ----
    # For demo, we assume you already produced df_sample
    # from your script with `records` and `pd.DataFrame.from_records(records)`
    # If you're reading your sample JSON file:
    # df_sample = pd.read_json("sample.json")
    raise NotImplementedError("Replace with your existing crawl code that returns df_sample")

def run_pipeline(df_sample: pd.DataFrame):
    # Minimal cleanup
    df_sample = df_sample.copy()
    # If you accidentally duplicated 'organismId_i' key with different meaning, keep the right one
    # df_sample.rename(columns={"authOrganismId_i": "organismId_i"}, inplace=True)  # uncomment if needed

    # Normalize into tables
    docs_df = normalize_documents(df_sample)
    auth_df = normalize_authors(df_sample)
    kw_df = normalize_keywords(df_sample)
    id_df = normalize_identifiers(df_sample)

    # Load
    load_data(docs_df, "documents", if_exists="append")
    load_data(auth_df, "authors", if_exists="append")
    load_data(kw_df, "keywords", if_exists="append")
    load_data(id_df, "identifiers", if_exists="append")

if __name__ == "__main__":
    # If you’re using your in-memory 'records' from the code you pasted:
    # Just import them or read from the JSON you saved with savetojson(...)
    # Example using the JSON you showed in the message:
    import json
    data_path= os.path.join(os.path.dirname(__file__), '..', 'api', 'data','upec_sample200_keywords_domains.json')
    with open(data_path, "r", encoding="utf-8") as f:
        sample_list = json.load(f)
    df_sample = pd.DataFrame(sample_list)

    print("Rows in sample:", len(df_sample))
    run_pipeline(df_sample)