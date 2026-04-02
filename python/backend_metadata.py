"""
Text-to-SQL Chatbot Backend
- Reads ALL table metadata from workorder_metadata.xlsx
- Dynamically discovers DB tables
- Picks relevant tables per question using metadata keywords
- Injects rich schema context into LLM prompt
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from langchain_ollama import OllamaLLM
from sqlalchemy import text, create_engine, inspect
from urllib.parse import quote_plus
import pandas as pd
import re, json, time, uvicorn, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5436,
    "database": "Rock_Prod040226",
    "username": "postgres",
    "password": "sa@123",
}
METADATA_FILE = "table_metadata.xlsx"

EXCLUDE_TABLES = {
    "__EFMigrationsHistory",
    "WO_BCK_ALL_0926", "wo_bck_0926",
    "SampleSpecimens_BCK", "Samples_BCK",
    "FormBillingLabors_BCK",
    "view_backup", "view_backup_audit",
    "v_all_ids", "totalprojects",
}

MAX_TABLES_IN_PROMPT = 6

DB_URI = (
    f"postgresql://{DB_CONFIG['username']}:{quote_plus(DB_CONFIG['password'])}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ─── APP ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="DB Chatbot API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

engine       = None
llm          = None
llm_fast     = None
_all_tables  = []
_meta        = {}
_schema_cache = {}
_eco_map     = []
_triggers    = {}
_table_keywords = {}
_suggest_cache  = {}



# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL METADATA READER
# ═══════════════════════════════════════════════════════════════════════════════
def load_excel_metadata(path: str) -> tuple[dict, list, dict]:
    p = Path(path)
    if not p.exists():
        logger.warning(f"Metadata file not found: {path}")
        return {}, [], {}

    try:
        xl = pd.ExcelFile(p)

        # ── Tables Sheet ──
        t_df = xl.parse("Tables", dtype=str).fillna("")
        meta = {}
        for _, row in t_df.iterrows():
            tname = str(row.get("TableName", "")).strip()
            if not tname:
                continue
            meta[tname] = {
                "description": str(row.get("Description", "")),
                "name_column": str(row.get("NameColumn", "")),
                "notes": str(row.get("ImportantNotes", "")),
                "columns": {},
                "relationships": [],
            }

        # ── Columns Sheet ──
        c_df = xl.parse("Columns", dtype=str).fillna("")
        for _, row in c_df.iterrows():
            tname = str(row.get("TableName", "")).strip()
            cname = str(row.get("ColumnName", "")).strip()
            if not tname or not cname:
                continue
            if tname not in meta:
                meta[tname] = {"description": "", "name_column": "", "notes": "", "columns": {}, "relationships": []}
            meta[tname]["columns"][cname] = {
                "desc": str(row.get("Description", "")),
                "rel": str(row.get("Relationship", "")),
            }

        # ── Relationships Sheet ──
        r_df = xl.parse("Relationships", dtype=str).fillna("")
        for _, row in r_df.iterrows():
            ft = str(row.get("FromTable", "")).strip()
            if not ft:
                continue
            if ft not in meta:
                meta[ft] = {"description": "", "name_column": "", "notes": "", "columns": {}, "relationships": []}
            meta[ft]["relationships"].append({
                "from_col": str(row.get("FromColumn", "")),
                "to_table": str(row.get("ToTable", "")),
                "to_col": str(row.get("ToColumn", "")),
                "note": str(row.get("Notes", "")),
            })

        # ── Triggers Sheet ──
        _trg = {}
        if "Triggers" in xl.sheet_names:
            trg_df = xl.parse("Triggers", dtype=str).fillna("")
            for _, row in trg_df.iterrows():
                tname = str(row.get("TableName", "")).strip()
                words = {w.strip().lower() for w in str(row.get("TriggerWords", "")).split(",") if w.strip()}
                priority = str(row.get("Priority", "optional")).strip().lower()
                if tname:
                    _trg[tname] = {"words": words, "priority": priority}
            logger.info(f"Triggers loaded for {len(_trg)} tables")

        eco_map = []  # Currently not loading from Excel

        logger.info(f"Metadata loaded: {len(meta)} tables from Excel")
        return meta, eco_map, _trg

    except Exception as ex:
        logger.error(f"Failed to load metadata: {ex}", exc_info=True)
        return {}, [], {}


def build_table_keywords(meta: dict) -> dict:
    kw = {}
    for tname, info in meta.items():
        words = set()
        for w in re.findall(r'[A-Z][a-z]+|[A-Z]{2,}(?=[A-Z]|$)', tname):
            words.add(w.lower())
        for w in re.findall(r'[a-z]{4,}', info.get("description", "").lower()):
            words.add(w)
        for col in info.get("columns", {}):
            for w in re.findall(r'[A-Z][a-z]+', col):
                words.add(w.lower())
        for w in re.findall(r'[a-z]{4,}', info.get("notes", "").lower()):
            words.add(w)
        kw[tname] = words
    return kw


def build_schema_from_metadata(tname: str, db_cols: list, meta: dict) -> str:
    info = meta.get(tname, {})
    col_meta = info.get("columns", {})
    rels = info.get("relationships", [])
    notes = info.get("notes", "")
    desc = info.get("description", "")
    name_col = info.get("name_column", "")

    lines = []

    if desc:
        lines.append(f"-- TABLE: {tname}")
        lines.append(f"-- {desc}")
    else:
        lines.append(f"-- TABLE: {tname}")

    if notes:
        note_parts = [n.strip() for n in notes.split(".") if len(n.strip()) > 10]
        for np in note_parts[:3]:
            lines.append(f"-- NOTE: {np}")

    if name_col and name_col in db_cols:
        lines.append(f'-- NAME COLUMN: Use "{tname}"."{name_col}" for display and output')

    col_parts = []
    for col in db_cols:
        cm = col_meta.get(col, {})
        cdesc = cm.get("desc", "")
        crel = cm.get("rel", "")
        annotation = ""
        if cdesc:
            annotation = f"  -- {cdesc}"
            if crel and "->" in crel:
                annotation += f" {crel}"
        elif crel and "->" in crel:
            annotation = f"  -- {crel}"
        col_parts.append(f'  "{col}" TEXT{annotation}')

    lines.append(f'CREATE TABLE "{tname}" (')
    lines.append(",\n".join(col_parts))
    lines.append(");")

    for r in rels:
        to = r.get("to_table", "").strip()
        tc = r.get("to_col", "").strip()
        fc = r.get("from_col", "").strip()
        note_ = r.get("note", "").strip()
        comment = f"  -- {note_}" if note_ else ""
        if to and fc:
            lines.append(f'-- FK: "{tname}"."{fc}" -> "{to}"."{tc}"{comment}')

    return "\n".join(lines)


def build_slim_schema(selected_tables: list) -> str:
    """Light schema without comments for faster suggestion generation"""
    lines = []
    for t in selected_tables:
        if t in _schema_cache:
            # Remove comments for slim version
            slim = re.sub(r'--.*$', '', _schema_cache[t], flags=re.MULTILINE)
            slim = re.sub(r'\n\s*\n', '\n', slim)
            lines.append(slim.strip())
    return "\n\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════
def discover_tables(eng) -> list:
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        ))
        tables = [r[0] for r in rows if r[0] not in EXCLUDE_TABLES]
    logger.info(f"Discovered {len(tables)} tables")
    return tables


def build_schema_cache(eng, tables: list, meta: dict) -> dict:
    insp = inspect(eng)
    cache = {}
    for t in tables:
        try:
            db_cols = [c["name"] for c in insp.get_columns(t)]
            if t in meta:
                cache[t] = build_schema_from_metadata(t, db_cols, meta)
            else:
                col_defs = ",\n  ".join(f'"{c}" TEXT' for c in db_cols)
                cache[t] = f'CREATE TABLE "{t}" (\n  {col_defs}\n);'
        except Exception as ex:
            logger.warning(f"Could not inspect {t}: {ex}")
    logger.info(f"Schema cache built for {len(cache)} tables")
    return cache


# ═══════════════════════════════════════════════════════════════════════════════
# TABLE SELECTION
# ═══════════════════════════════════════════════════════════════════════════════
def pick_relevant_tables(question: str, all_tables: list, limit: int) -> list:
    q_lower = question.lower()
    q_words = set(re.findall(r'[a-z]+', q_lower))

    forced = []
    optional = []
    noisy = set()

    for t, info in _triggers.items():
        if t not in all_tables:
            continue
        priority = info.get("priority", "optional")
        matched = any(w in q_words for w in info["words"])

        if priority == "noisy":
            noisy.add(t)
            if t.lower() in q_lower:
                forced.append(t)
        elif priority == "force" and matched:
            forced.append(t)
        elif priority == "optional" and matched:
            optional.append(t)

    extra = []
    for t in all_tables:
        if t in forced or t in optional or t in noisy:
            continue
        if t.lower() in q_lower:
            extra.append(t)

    selected = list(dict.fromkeys(forced + optional + extra))
    return selected[:limit]


def get_eco_hint(question: str) -> str:
    q = question.lower()
    matches = []
    for entry in _eco_map:
        qt = entry.get("question_type", "").lower()
        score = sum(1 for w in qt.split() if len(w) > 3 and w in q)
        if score > 0:
            matches.append((score, entry))

    if not matches:
        return ""

    matches.sort(key=lambda x: -x[0])
    best = matches[0][1]
    hint = (
        f"-- ECOSYSTEM HINT: For '{best['question_type']}' queries:\n"
        f"--   Tables: {best['join_path']}\n"
        f"--   Example: {best['filter_example']}"
    )
    return hint


# ═══════════════════════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════════════════════
try:
    # Load metadata
    _meta, _eco_map, _triggers = load_excel_metadata(METADATA_FILE)
    _table_keywords = build_table_keywords(_meta)

    # Connect to DB
    engine = create_engine(DB_URI)
    _all_tables = discover_tables(engine)

    # Build schema cache
    _schema_cache = build_schema_cache(engine, _all_tables, _meta)

    # Main LLM
    llm = OllamaLLM(
        model="qwen2.5-coder:7b",
        temperature=0.0,
        num_ctx=8192,
        num_predict=400,
    )

    # Faster LLM for suggestions
    llm_fast = OllamaLLM(
        model="qwen2.5-coder:7b",
        temperature=0.0,
        num_ctx=4096,
        num_predict=150,
    )

    logger.info("Backend initialized successfully.")
    logger.info(f"Tables loaded: {len(_all_tables)} | Metadata tables: {len(_meta)}")

except Exception as e:
    logger.error(f"Startup error: {e}", exc_info=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PROMPT
# ═══════════════════════════════════════════════════════════════════════════════
SQL_PROMPT = """\
### Instructions:
Convert the question into a single valid PostgreSQL SELECT query.

Rules:
- Only use tables and columns listed in the schema below
- Always double-quote table and column names: "TableName"."ColumnName"
- Read ALL comments (-- lines) carefully
- Use ILIKE for case-insensitive text search
- Boolean columns use actual booleans: "IsDeleted" = false
- Return ONLY the SQL query, no explanation, no markdown

{eco_hint}

### Question:
{question}

### Schema:
{schema}

### SQL:
SELECT"""


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def get_schema_for_question(question: str) -> tuple[str, list]:
    selected = pick_relevant_tables(question, _all_tables, MAX_TABLES_IN_PROMPT)
    lines = [_schema_cache[t] for t in selected if t in _schema_cache]
    return "\n\n".join(lines), selected


def extract_sql(raw: str) -> str:
    raw = re.sub(r"```sql|```", "", raw, flags=re.IGNORECASE)
    raw = raw.strip()
    if not raw.upper().startswith(("SELECT", "WITH")):
        raw = "SELECT " + raw
    match = re.search(r"(SELECT|WITH)\b.*", raw, flags=re.IGNORECASE | re.DOTALL)
    if match:
        sql = match.group(0).strip()
        if ";" in sql:
            sql = sql[:sql.index(";") + 1]
        return sql
    return raw


def run_query(sql: str):
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            cols = list(result.keys())
            raw_rows = result.fetchmany(50)

        def safe(v):
            if v is None: return None
            if isinstance(v, (int, float, bool)): return v
            if isinstance(v, str): return v
            return str(v)

        rows = [[safe(v) for v in row] for row in raw_rows]
        return rows, cols, None
    except Exception as ex:
        return [], [], str(ex)


def sse(t: str, data: dict) -> str:
    return f"data: {json.dumps({'type': t, **data})}\n\n"


def generate_metadata(question: str, cols: list, rows: list) -> dict:
    q = question.lower()
    chart_type = None
    if "pie" in q:   chart_type = "pie"
    elif "bar" in q: chart_type = "bar"
    elif "line" in q: chart_type = "line"
    elif len(cols) == 2 and rows and len(rows) > 1:
        chart_type = "bar"

    title = question.strip().capitalize()[:60]
    if rows and len(cols) == 1 and len(rows) == 1:
        title = f"Result: {rows[0][0]}"

    return {"title": title, "chart_type": chart_type}


# ═══════════════════════════════════════════════════════════════════════════════
# STREAM HANDLER
# ═══════════════════════════════════════════════════════════════════════════════
async def chat_stream(question: str):
    try:
        schema, selected = get_schema_for_question(question)
        eco_hint = get_eco_hint(question)

        prompt = SQL_PROMPT.format(
            schema=schema,
            question=question,
            eco_hint=eco_hint if eco_hint else "",
        )

        yield sse("status", {"text": "Generating SQL…"})
        yield sse("tables_used", {"tables": selected})

        t0 = time.time()
        raw = llm.invoke(prompt)
        t1 = time.time()

        sql = extract_sql(str(raw))

        if not sql or len(sql) < 7:
            yield sse("error", {"text": "Could not generate SQL. Try rephrasing."})
            return

        yield sse("sql", {"text": sql, "time": round(t1 - t0, 1)})
        yield sse("status", {"text": "Running query…"})

        t2 = time.time()
        rows, cols, err = run_query(sql)
        t3 = time.time()

        if err:
            yield sse("error", {"text": f"SQL error: {err}"})
            return

        meta = generate_metadata(question, cols, rows)

        yield sse("result", {
            "columns": cols,
            "rows": rows,
            "count": len(rows),
            "title": meta["title"],
            "chart_type": meta["chart_type"],
            "chart_title": meta["title"] if meta["chart_type"] else None,
            "timings": {
                "sql_gen": round(t1 - t0, 1),
                "db_query": round(t3 - t2, 1),
                "total": round(t3 - t0, 1),
            }
        })

    except Exception as ex:
        logger.error(f"Stream error: {ex}", exc_info=True)
        yield sse("error", {"text": str(ex)})


# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════
class AskRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat(req: AskRequest):
    if not llm or not engine:
        raise HTTPException(503, "Backend not ready.")
    return StreamingResponse(
        chat_stream(req.message),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


class SuggestRequest(BaseModel):
    query: str

@app.post("/suggest")
def suggest(req: SuggestRequest):
    global _suggest_cache

    if not llm_fast or not engine:
        raise HTTPException(503, "Backend not ready.")

    query = req.query.strip()
    if len(query) < 3:
        return {"suggestions": [], "tables_used": []}

    cache_key = query.lower()
    if cache_key in _suggest_cache:
        return _suggest_cache[cache_key]

    selected = pick_relevant_tables(query, _all_tables, MAX_TABLES_IN_PROMPT)
    if not selected:
        selected = list(_schema_cache.keys())[:MAX_TABLES_IN_PROMPT]

    schema = build_slim_schema(selected)
    if not schema.strip():
        return {"suggestions": [], "tables_used": selected}

    suggest_prompt = f"""Database schema:
{schema}

User typed: "{query}"

Return ONLY a JSON array of 5 question suggestions answerable from this schema.
No markdown, no explanation. Example: ["Q1?", "Q2?", "Q3?", "Q4?", "Q5?"]"""

    try:
        raw = llm_fast.invoke(suggest_prompt).strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if not match:
            raise ValueError("No JSON array found")

        suggestions = json.loads(match.group(0))
        suggestions = [str(s).strip() for s in suggestions if s][:5]

        result = {"suggestions": suggestions, "tables_used": selected}
        _suggest_cache[cache_key] = result
        return result

    except Exception as ex:
        logger.error(f"Suggest error: {ex}")
        return {
            "suggestions": [
                f"Show all work orders related to {query}",
                f"How many records match '{query}'?",
                f"List active entries involving {query}",
                f"Show recent activity for {query}",
                f"Count results grouped by status for {query}",
            ],
            "tables_used": selected,
        }


@app.get("/health")
def health():
    return {
        "status": "ok" if (engine and llm) else "error",
        "model": "qwen2.5-coder:7b",
        "total_tables": len(_all_tables),
        "metadata_tables": len(_meta),
    }


if __name__ == "__main__":
    uvicorn.run("backend_metadata:app", host="0.0.0.0", port=8000, reload=True)