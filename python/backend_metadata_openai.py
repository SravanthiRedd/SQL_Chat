"""
Text-to-SQL Chatbot Backend
- Reads ALL table metadata from table_metadata.xlsx
- Dynamically discovers DB tables
- Uses OpenAI GPT-4o-mini for accurate SQL generation
- Picks relevant tables per question using Excel Triggers sheet
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from openai import OpenAI
from sqlalchemy import text, create_engine, inspect
from urllib.parse import quote_plus
import pandas as pd
import re, json, time, uvicorn, logging
from pathlib import Path
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "urock",   # <-- change this
    "username": "postgres",         # <-- change this
    "password": "sa123",         # <-- change this
}

OPENAI_API_KEY = ""        # <-- paste your OpenAI API key here
OPENAI_MODEL   = "gpt-4o-mini"  # cheap + smart. Use "gpt-4o" for best accuracy

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

engine          = None
openai_client   = None
_all_tables     = []
_meta           = {}
_schema_cache   = {}
_triggers       = {}
_table_keywords = {}
_suggest_cache  = {}
_eco_map        = []


# ===============================================================================
# OPENAI LLM CALL
# ===============================================================================
def call_llm(system: str, user: str, max_tokens: int = 500) -> str:
    response = openai_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user",   "content": user},
        ],
        temperature=0,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content.strip()


# ===============================================================================
# EXCEL METADATA READER
# ===============================================================================
def load_excel_metadata(path: str) -> tuple[dict, list, dict]:
    p = Path(path)
    if not p.exists():
        logger.warning(f"Metadata file not found: {path}")
        return {}, [], {}

    try:
        xl = pd.ExcelFile(p)

        # Tables Sheet
        t_df = xl.parse("Tables", dtype=str).fillna("")
        meta = {}
        for _, row in t_df.iterrows():
            tname = str(row.get("TableName", "")).strip()
            if not tname:
                continue
            meta[tname] = {
                "description": str(row.get("Description", "")),
                "name_column": str(row.get("NameColumn", "")),
                "notes":       str(row.get("ImportantNotes", "")),
                "columns":     {},
                "relationships": [],
            }

        # Columns Sheet
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
                "rel":  str(row.get("Relationship", "")),
            }

        # Relationships Sheet
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
                "to_col":   str(row.get("ToColumn", "")),
                "note":     str(row.get("Notes", "")),
            })

        # Triggers Sheet
        _trg = {}
        if "Triggers" in xl.sheet_names:
            trg_df = xl.parse("Triggers", dtype=str).fillna("")
            for _, row in trg_df.iterrows():
                tname    = str(row.get("TableName", "")).strip()
                words    = {w.strip().lower() for w in str(row.get("TriggerWords", "")).split(",") if w.strip()}
                priority = str(row.get("Priority", "optional")).strip().lower()
                if tname:
                    _trg[tname] = {"words": words, "priority": priority}
            logger.info(f"Triggers loaded for {len(_trg)} tables")

        logger.info(f"Metadata loaded: {len(meta)} tables from Excel")
        return meta, [], _trg

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
    info     = meta.get(tname, {})
    col_meta = info.get("columns", {})
    rels     = info.get("relationships", [])
    notes    = info.get("notes", "")
    desc     = info.get("description", "")
    name_col = info.get("name_column", "")

    lines = []
    lines.append(f"-- TABLE: {tname}")
    if desc:
        lines.append(f"-- {desc}")

    if notes:
        note_parts = [n.strip() for n in notes.split(".") if len(n.strip()) > 10]
        for np in note_parts[:4]:
            lines.append(f"-- NOTE: {np}")

    if name_col and name_col in db_cols:
        lines.append(f'-- NAME COLUMN: Use "{tname}"."{name_col}" for display and output')

    col_parts = []
    for col in db_cols:
        cm    = col_meta.get(col, {})
        cdesc = cm.get("desc", "")
        crel  = cm.get("rel", "")
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
        to    = r.get("to_table", "").strip()
        tc    = r.get("to_col", "").strip()
        fc    = r.get("from_col", "").strip()
        note_ = r.get("note", "").strip()
        comment = f"  -- {note_}" if note_ else ""
        if to and fc:
            lines.append(f'-- FK: "{tname}"."{fc}" -> "{to}"."{tc}"{comment}')

    return "\n".join(lines)


# ===============================================================================
# TABLE DISCOVERY
# ===============================================================================
def discover_tables(eng) -> list:
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        ))
        tables = [r[0] for r in rows if r[0] not in EXCLUDE_TABLES]
    logger.info(f"Discovered {len(tables)} tables")
    return tables

def build_compact_metadata(tname: str, meta: dict) -> str:
    info     = meta.get(tname, {})
    desc     = info.get("description", "")
    name_col = info.get("name_column", "")
    notes    = info.get("notes", "")
    col_meta = info.get("columns", {})
    rels     = info.get("relationships", [])

    # Only documented columns
    key_cols = [c for c, m in col_meta.items() if m.get("desc") or m.get("rel")]
    
    # Always include critical columns
    for c in ["Id", "IsDeleted", "CreationTime", "TenantId"]:
        if c not in key_cols:
            key_cols.insert(0, c)

    fk_parts = [
        f'{r["from_col"]}->{r["to_table"]}.{r["to_col"]}'
        for r in rels if r.get("from_col") and r.get("to_table")
    ]

    lines = [f"TABLE: {tname}"]
    if desc:     lines.append(f"DESC: {desc}")
    if name_col: lines.append(f"NAME_COL: {name_col}")
    if key_cols: lines.append(f"KEY_COLS: {', '.join(key_cols)}")
    if fk_parts: lines.append(f"FK: {' | '.join(fk_parts)}")
    if notes:    lines.append(f"NOTES: {notes}")

    return "\n".join(lines)

def build_schema_cache(tables: list, meta: dict) -> dict:
    cache = {}
    for t in tables:
        if t in meta:
            # Use compact metadata instead of full schema
            cache[t] = build_compact_metadata(t, meta)
        else:
            # Unknown tables — just pass table name, no schema
            cache[t] = f"TABLE: {t}"
    logger.info(f"Schema cache built for {len(cache)} tables")
    return cache


# ===============================================================================
# TABLE SELECTION
# ===============================================================================
def pick_relevant_tables(question: str, all_tables: list, limit: int) -> list:
    q_lower = question.lower()
    q_words = set(re.findall(r'[a-z]+', q_lower))

    forced   = []
    optional = []
    noisy    = set()

    for t, info in _triggers.items():
        if t not in all_tables:
            continue
        priority = info.get("priority", "optional")
        matched  = any(w in q_words for w in info["words"])

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

    if not selected and "WorkOrders" in all_tables:
        selected = ["WorkOrders"]

    return selected[:limit]


# ===============================================================================
# STARTUP
# ===============================================================================
try:
    _meta, _eco_map, _triggers = load_excel_metadata(METADATA_FILE)
    _table_keywords = build_table_keywords(_meta)
    logger.info(f"Metadata tables: {list(_meta.keys())}")

    engine      = create_engine(DB_URI)
    _all_tables = discover_tables(engine)
    _schema_cache = build_schema_cache(_all_tables, _meta)

    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    logger.info(f"OpenAI ready. Model: {OPENAI_MODEL}")

except Exception as e:
    logger.error(f"Startup error: {e}", exc_info=True)


# ===============================================================================
# PROMPTS
# ===============================================================================
SYSTEM_PROMPT = """\
You are a PostgreSQL expert. Convert natural language questions into accurate SQL queries.

Rules:
- Only use tables and columns from the schema provided
- Always double-quote table and column names: "TableName"."ColumnName"
- Always prefix every column with table name in WHERE, GROUP BY, ORDER BY
- Use ILIKE for case-insensitive text search
- Boolean columns use actual booleans: "IsDeleted" = false
- For FK columns (CreatorUserId, TechUserId, RequestedBy etc) always JOIN the related table
- For date ranges use >= and < not BETWEEN
- For day-wise counts cast timestamp to date using ::date and GROUP BY date
- For "count by X" questions use COUNT() with GROUP BY
- Read ALL schema comments carefully
- Return ONLY the SQL query, no explanation, no markdown
- If the question has typos, interpret it intelligently"""

USER_PROMPT = """\
### Schema:
{schema}

### Question:
{question}

### SQL:"""

SUGGEST_SYSTEM = """\
You are a helpful assistant. Given a partial query and database schema,
suggest 5 complete questions the user might want to ask.
Return ONLY a JSON array of 5 strings. No markdown, no explanation."""


# ===============================================================================
# HELPERS
# ===============================================================================
def get_schema_for_question(question: str) -> tuple[str, list]:
    selected = pick_relevant_tables(question, _all_tables, MAX_TABLES_IN_PROMPT)
    lines    = [_schema_cache[t] for t in selected if t in _schema_cache]
    return "\n\n".join(lines), selected


def extract_sql(raw: str) -> str:
    raw = re.sub(r"```sql|```", "", raw, flags=re.IGNORECASE).strip()
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
            result   = conn.execute(text(sql))
            cols     = list(result.keys())
            raw_rows = result.fetchall()

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
    if "pie" in q:    chart_type = "pie"
    elif "bar" in q:  chart_type = "bar"
    elif "line" in q: chart_type = "line"
    elif len(cols) == 2 and rows and len(rows) > 1:
        chart_type = "bar"

    title = question.strip().capitalize()[:60]
    if rows and len(cols) == 1 and len(rows) == 1:
        title = f"Result: {rows[0][0]}"

    return {"title": title, "chart_type": chart_type}



def detect_format_from_sql(sql: str, question: str) -> str:
    s = sql.upper()
    q = question.lower()

    # Only return a chart format when the user explicitly asked for one
    user_wants_chart = any(kw in q for kw in [
        "chart", "graph", "plot", "visualize", "visualization",
        "pie", "bar chart", "bar graph", "line chart", "donut", "doughnut"
    ])

    if user_wants_chart:
        if "pie" in q or "donut" in q or "doughnut" in q:  return "pie"
        if "bar" in q:                                       return "bar"
        if "line" in q or "trend" in q:                     return "line"
        # User asked for a chart but didn't specify type — auto-pick from SQL
        has_group_by  = "GROUP BY" in s
        has_date_cast = "::DATE" in s or "DATE_TRUNC" in s
        if has_date_cast and has_group_by:                   return "line"
        return "bar"

    # User did NOT ask for a chart — use text/table only
    has_count    = "COUNT(" in s
    has_sum      = "SUM(" in s
    has_group_by = "GROUP BY" in s

    if has_count and not has_group_by:  return "text"   # single count
    if has_sum   and not has_group_by:  return "text"   # single sum

    return "table"   # default for all grouped/multi-row results

# ===============================================================================
# STREAM HANDLER
# ===============================================================================
async def chat_stream(question: str):
    try:
        schema, selected = get_schema_for_question(question)
        logger.info(f"Tables selected ({len(selected)}): {selected}")
        logger.info(f"Schema sent to LLM:\n{schema}")

        user_prompt = USER_PROMPT.format(schema=schema, question=question)

        yield sse("status",      {"text": "Generating SQL..."})
        yield sse("tables_used", {"tables": selected})

        t0  = time.time()
        raw = await asyncio.get_event_loop().run_in_executor(
            None, lambda: call_llm(SYSTEM_PROMPT, user_prompt, max_tokens=500)
        )
        t1  = time.time()

        logger.info(f"OpenAI {t1-t0:.1f}s | Raw: {raw[:300]}")

        sql = extract_sql(raw)
        fmt = detect_format_from_sql(sql, question)
        yield sse("format", {"format": fmt})
        logger.info(f"SQL: {sql}")

        if not sql or len(sql) < 7:
            yield sse("error", {"text": "Could not generate SQL. Try rephrasing."})
            return

        yield sse("sql",    {"text": sql, "time": round(t1 - t0, 1)})
        yield sse("status", {"text": "Running query..."})

        t2 = time.time()
        rows, cols, err = run_query(sql)
        t3 = time.time()

        if err:
            yield sse("error", {"text": f"SQL error: {err}"})
            return

        meta = generate_metadata(question, cols, rows)

        yield sse("result", {
            "columns":     cols,
            "rows":        rows,
            "count":       len(rows),
            "title":       meta["title"],
            "chart_type":  meta["chart_type"],
            "chart_title": meta["title"] if meta["chart_type"] else None,
            "timings": {
                "sql_gen":  round(t1 - t0, 1),
                "db_query": round(t3 - t2, 1),
                "total":    round(t3 - t0, 1),
            }
        })

    except Exception as ex:
        logger.error(f"Stream error: {ex}", exc_info=True)
        yield sse("error", {"text": str(ex)})


# ===============================================================================
# ENDPOINTS
# ===============================================================================
class AskRequest(BaseModel):
    message: str

@app.post("/chat")
async def chat(req: AskRequest):
    if not openai_client or not engine:
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
    if not openai_client or not engine:
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

    slim_lines = []
    for t in selected:
        if t in _schema_cache:
            slim = re.sub(r'--.*$', '', _schema_cache[t], flags=re.MULTILINE)
            slim = re.sub(r'\n\s*\n', '\n', slim)
            slim_lines.append(slim.strip())
    schema = "\n\n".join(slim_lines)

    user_msg = f'Schema:\n{schema}\n\nUser typed: "{query}"\n\nSuggest 5 questions as JSON array:'

    try:
        raw         = call_llm(SUGGEST_SYSTEM, user_msg, max_tokens=300)
        raw         = re.sub(r"```json|```", "", raw).strip()
        match       = re.search(r'\[.*?\]', raw, re.DOTALL)
        suggestions = json.loads(match.group(0)) if match else []
        suggestions = [str(s).strip() for s in suggestions if s][:5]

        result = {"suggestions": suggestions, "tables_used": selected}
        _suggest_cache[cache_key] = result
        return result

    except Exception as ex:
        logger.error(f"Suggest error: {ex}")
        return {
            "suggestions": [
                f"How many work orders are related to {query}?",
                f"Show all active records for {query}",
                f"Count {query} by organization",
                f"List {query} created this month",
                f"Show {query} by status",
            ],
            "tables_used": selected,
        }


@app.get("/health")
def health():
    return {
        "status":          "ok" if (openai_client and engine) else "error",
        "model":           OPENAI_MODEL,
        "total_tables":    len(_all_tables),
        "metadata_tables": len(_meta),
    }

@app.get("/tables")
def list_tables():
    return {"total": len(_all_tables), "tables": _all_tables}

@app.get("/debug/schema")
def debug_schema(question: str = "work orders from Fort Myers"):
    schema, selected = get_schema_for_question(question)
    return {"tables_selected": selected, "schema": schema, "chars": len(schema)}

@app.get("/debug/prompt")
def debug_prompt(question: str = "work orders from Fort Myers"):
    schema, selected = get_schema_for_question(question)
    prompt = USER_PROMPT.format(schema=schema, question=question)
    return {"tables_selected": selected, "system": SYSTEM_PROMPT, "user": prompt}

@app.get("/debug/metadata")
def debug_metadata(table: str = "WorkOrders"):
    return _meta.get(table, {"error": f"{table} not found in metadata"})

@app.get("/test/speed")
def test_speed():
    t0  = time.time()
    out = call_llm("You are a SQL expert.", "Return only: SELECT 1", max_tokens=10)
    return {"elapsed_seconds": round(time.time() - t0, 2), "output": out}


if __name__ == "__main__":
    uvicorn.run("backend_metadata_openai:app", host="0.0.0.0", port=8000, reload=True)
