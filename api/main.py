import os, re, time, requests, pandas as pd
from urllib.parse import urlencode

from api.apimodule import NEED_N, choose_url, consolidate_domains, consolidate_keywords, \
    fallback_text_match_for_discipline, fetch_page, hal_record_url, map_codes_to_discipline, savetojson

records, cursor = [], "*"

if __name__ == '__main__':
    """call api module"""
    # Crawl
    print("pepeas")
    while len(records) < NEED_N/3:
        data = fetch_page(cursor)
        docs = data.get("response", {}).get("docs", [])
        if not docs: #si nohay documentos no hago nada
            break

        for d in docs: # cargo documentos del api
            # title/abstract/halId sometimes come as single-item lists
            for k in ["title_s", "abstract_s", "halId_s"]:
                v = d.get(k)
                if isinstance(v, list) and v:
                    d[k] = v[0]

            # consolidate metadata
            d["keywords_joined"] = consolidate_keywords(d)
            labels, codes = consolidate_domains(d)
            d["domain_labels"] = "; ".join(labels) if labels else ""
            d["domain_codes"] = "; ".join(codes) if codes else ""

            # build URL
            d["record_url"] = hal_record_url(d.get("halId_s"))
            d["url_primary"] = choose_url(d.get("linkExtUrl_s"), d.get("record_url"), d.get("files_s"))

            # must have keywords
            if not d["keywords_joined"]:
                continue

            # infer discipline (codes first, then text fallback)
            discipline = map_codes_to_discipline(codes, labels)
            if discipline is None:
                discipline = fallback_text_match_for_discipline(
                    [d.get("title_s"), d.get("abstract_s"), d.get("keywords_joined")]
                )
            if discipline is None:
                continue  # not one of your 5 buckets

            d["discipline"] = discipline

            # keep only requested output columns
            records.append({
                "docid": d.get("docid"),
                "halId_s": d.get("halId_s"),
                "title_s": d.get("title_s"),
                "abstract_s": d.get("abstract_s"),
                "keywords_joined": d.get("keywords_joined"),
                "domain_codes": d.get("domain_codes"),  # raw HAL codes (for audit)
                "domain_labels": d.get("domain_labels"),  # any labels if present
                "discipline": d.get("discipline"),  # your 5 buckets (clean)
                "url_primary": d.get("url_primary"),
                "authOrganismId_i": d.get("authOrganismId_i"),
                #data for autors DANN
                'authFirstName_s': d.get("authFirstName_s"),
                'authFirstName_sci': d.get("authFirstName_sci"),
                'authLastName_s': d.get("authLastName_s"),
                'authLastName_sci': d.get("authLastName_sci"),
                'authQuality_s': d.get("authQuality_s"),
                #data for Organism DANN
                'authOrganismId_i': d.get("authOrganismId_i"),
                'authOrganism_s': d.get("authOrganism_s"),
                'authorityInstitution_s': d.get("authorityInstitution_s"),
                #data for keywords DANN
                'keyword_s': d.get("keyword_s"),
                'keyword_sci': d.get("keyword_sci"),
                'keyword_t': d.get("keyword_t")
    })

            if len(records) >= NEED_N:
                break

        next_c = data.get("nextCursorMark")
        if not next_c or next_c == cursor:
            break
        cursor = next_c
        time.sleep(0.12)

    # Build dataframe
    df_sample = pd.DataFrame.from_records(records)
    print("Rows in sample:", len(df_sample))
    #df_sample.head(3)
    df_sample = df_sample.drop(columns=["domain_labels"], errors="ignore")
    savetojson(df_sample)
