# 🧠 Wikidata Enrichment Script

This project performs **keyword-to-Wikidata enrichment** using the HAL dataset.  
It retrieves candidate Wikidata entities based on keyword, title, and abstract similarity, applies fuzzy matching, and builds lineage paths for each matched QID.

---

## 📁 Project Structure

```
HALL-API-TEST-DB-MYSQL/
│
├── api/
│   └── data/
│       └── upec_sample200_keywords_domains.json
│
├── wikidata/
│   ├── mapping.py
│   ├── venv/
│   └── hal_field_audit_out/
│       └── Upec_Wikidata_Enriched_Improved.csv   # output file
│
└── ...
```

---

## ⚙️ 1. Environment Setup

### Option A – Create a new virtual environment (recommended)
Open a terminal in the `wikidata` folder and run:

```powershell
#Install Python 3.11
winget install Python.Python.3.11

# Create the virtual environment
py -3.11 -m venv venv

# Activate it
venv\Scripts\Activate.ps1
```

You should now see `(venv)` at the start of your terminal prompt.

## 📦 2. Install Required Libraries

With the environment activated, install the dependencies:

```powershell
python.exe -m pip install --upgrade pip
```

Confirm Python version
```powershell
python --version
pip --version
```
You should now see:
Python 3.11.9
pip 25.2 from ...\wikidata\venv\Lib\site-packages\pip

```powershell
pip install bertopic
```

```powershell
pip install pandas SPARQLWrapper rapidfuzz tqdm
```

> If you plan to edit or extend the script, you can save these dependencies:
> ```powershell
> pip freeze > requirements.txt
> ```

---

## 🚀 3. Run the Script

Once everything is set up, execute the enrichment pipeline:

```powershell
python mapping.py
```

The script will:
1. Load the HAL data from  
   `api/data/upec_sample200_keywords_domains.json`
2. Query the Wikidata SPARQL endpoint.
3. Score and filter candidate entities.
4. Save the enriched output to  
   `wikidata/hal_field_audit_out/Upec_Wikidata_Enriched_Improved.csv`

---

## 🧩 Notes and Recommendations

- **User-Agent:**  
  The script includes a descriptive User-Agent header to comply with [Wikidata Query Service policies](https://www.wikidata.org/wiki/Wikidata:SPARQL_query_service/User_Agent_policy).

- **Concurrency:**  
  The number of parallel requests is limited (`max_workers=3`) to avoid overloading the Wikidata servers.

- **API Rate Limiting:**  
  Each SPARQL request includes small `time.sleep()` delays between queries to stay within fair-use limits.

- **Output Columns:**
  | Column | Description |
  |---------|-------------|
  | `keyword` | Original HAL keyword |
  | `title` | Title of the associated document |
  | `wikidata_label` | Best matched Wikidata label |
  | `wikidata_qid` | Wikidata QID (identifier) |
  | `bnf_id` | French National Library (BNF) ID, if available |
  | `lineage_path` | Hierarchical lineage from the matched entity |
  | `match_source` | Indicates whether the match came from context, fallback, or none |

---

## 🧠 Example Output

```
keyword,title,wikidata_label,wikidata_qid,bnf_id,lineage_path,match_source
Photosynthesis,Light energy conversion,Photosynthesis,Q212743,None,Biological process → Metabolism,context
Artificial Intelligence,Computational modeling,Artificial intelligence,Q11660,None,Information processing → Computer science,context
...
```

---

## 🔧 Troubleshooting

| Issue | Cause | Fix |
|-------|--------|-----|
| `SyntaxError: f-string expression part cannot include a backslash` | f-string parsing SPARQL braces | Use concatenated strings instead of triple f-strings (already fixed in latest script). |
| `SPARQLWrapper: 429 Too Many Requests` | Too many concurrent queries | Lower `max_workers` or increase sleep delay. |
| `FileNotFoundError` | Wrong input path | Verify that the JSON file is in `api/data/`. |
| `SSL error / timeout` | Internet or endpoint issue | Re-run after a few minutes. |

---

## 👩‍💻 Author
For academic and data enrichment purposes within the *SciKey* and *Wikidata mapping* projects.

---
