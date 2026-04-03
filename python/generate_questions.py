"""
generate_questions.py
─────────────────────
Generates 1000 diverse natural-language questions from your PostgreSQL schema
using Qwen2.5-Coder:7b (via Ollama).

Table filtering rules (both must pass):
  1. Table must exist in table_metadata.xlsx  (Excel-only tables used)
  2. Table name must NOT start with "DT_"     (DT_ tables are skipped)

Place this file next to backend_metadata.py and table_metadata.xlsx.
Run:  python generate_questions.py

Output:  generated_questions.json   – deduplicated list of 1000 questions
         generation_log.txt         – per-batch details for debugging
"""

import re, json, random, time, logging
from pathlib import Path
from urllib.parse import quote_plus

from langchain_ollama import OllamaLLM
from sqlalchemy import text, create_engine, inspect
import pandas as pd

# ── Copy these from backend_metadata.py (or import if you prefer) ─────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "urock",
    "username": "postgres",
    "password": "sa123",
}
METADATA_FILE   = "table_metadata.xlsx"
EXCLUDE_TABLES  = {
    "__EFMigrationsHistory",
    "WO_BCK_ALL_0926", "wo_bck_0926",
    "SampleSpecimens_BCK", "Samples_BCK",
    "FormBillingLabors_BCK",
    "view_backup", "view_backup_audit",
    "v_all_ids", "totalprojects",
}

# ── Table filter rules ─────────────────────────────────────────────────────────
# Rule 1: Skip any table whose name starts with these prefixes (case-insensitive)
EXCLUDE_PREFIXES = ("DT_", "dt_")

# Rule 2: ONLY generate from tables that exist in table_metadata.xlsx
#         DB tables not present in the Excel sheet are ignored entirely.
METADATA_TABLES_ONLY = True   # set False to allow all discovered DB tables

# ── Generation settings ────────────────────────────────────────────────────────
TARGET_QUESTIONS   = 1000   # how many unique questions to collect
QUESTIONS_PER_CALL = 20     # questions requested per LLM call
TABLES_PER_BATCH   = 5      # number of tables sent in each LLM call
MAX_RETRIES        = 3      # retries on parse failure
OUTPUT_FILE        = "generated_questions.json"
LOG_FILE           = "generation_log.txt"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# PROMPT TEMPLATE
# We ask the model to vary question styles across batches via a "style_hint".
# ─────────────────────────────────────────────────────────────────────────────
QUESTION_PROMPT = """\
You are a data analyst creating a training dataset for a Text-to-SQL model.

### Task
Generate exactly {count} distinct natural-language business questions that can be \
answered by querying the PostgreSQL tables in the schema below.

### Style hint for THIS batch
{style_hint}

### Rules
- Write clear, varied English questions ONLY — no SQL, no code, no markdown.
- Use a mix of: counts, sums, averages, filters, date ranges, grouping, joins, \
  top-N rankings, existence checks, and comparisons.
- Reference specific column names or table concepts naturally (e.g. "work orders", \
  "samples", "projects", "technicians").
- Each question must be answerable with a single SELECT query on the given schema.
- Return ONLY a valid JSON array of strings. No preamble, no trailing text.

### Schema
{schema}

### Output (JSON array only)
"""

# Question style hints — rotated across batches for diversity
STYLE_HINTS = [
    "Focus on COUNT and GROUP BY questions (how many, how often, totals per category).",
    "Focus on date/time filters — questions about 'this week', 'last month', 'after date X'.",
    "Focus on TOP-N and ranking questions — 'top 5 projects by cost', 'most active technicians'.",
    "Focus on existence and status checks — 'are there any open …', 'which … are pending'.",
    "Focus on aggregation across joins — questions that naturally span two or more tables.",
    "Focus on average and min/max calculations — 'average turnaround', 'maximum cost'.",
    "Focus on listing/details questions — 'list all samples for project X', 'show details of …'.",
    "Mix all styles freely — create the most diverse set you can.",
]


# ─────────────────────────────────────────────────────────────────────────────
# REUSE FUNCTIONS FROM backend_metadata.py  (copied verbatim — no import needed)
# ─────────────────────────────────────────────────────────────────────────────
def load_excel_metadata(path: str):
    p = Path(path)
    if not p.exists():
        logger.warning(f"Metadata file not found: {path}")
        return {}, []

    xl = pd.ExcelFile(p)
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

    logger.info(f"Metadata loaded: {len(meta)} tables")
    return meta, []


def discover_tables(eng) -> list:
    with eng.connect() as conn:
        rows = conn.execute(text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public' ORDER BY tablename"
        ))
        tables = [r[0] for r in rows if r[0] not in EXCLUDE_TABLES]
    logger.info(f"Discovered {len(tables)} tables")
    return tables


def build_schema_from_metadata(tname, db_cols, meta) -> str:
    info    = meta.get(tname, {})
    col_meta = info.get("columns", {})
    rels    = info.get("relationships", [])
    lines   = []

    desc = info.get("description", "")
    lines.append(f"-- TABLE: {tname}")
    if desc:
        lines.append(f"-- {desc}")

    notes = info.get("notes", "")
    if notes:
        for np in [n.strip() for n in notes.split(".") if len(n.strip()) > 10][:2]:
            lines.append(f"-- NOTE: {np}")

    col_parts = []
    for col in db_cols:
        cm   = col_meta.get(col, {})
        cdesc = cm.get("desc", "")
        crel  = cm.get("rel", "")
        ann  = ""
        if cdesc:
            ann = f"  -- {cdesc}"
            if crel and "->" in crel:
                ann += f" {crel}"
        elif crel and "->" in crel:
            ann = f"  -- {crel}"
        col_parts.append(f'  "{col}" TEXT{ann}')

    lines.append(f'CREATE TABLE "{tname}" (')
    lines.append(",\n".join(col_parts))
    lines.append(");")

    for r in rels:
        to = r.get("to_table", "").strip()
        tc = r.get("to_col", "").strip()
        fc = r.get("from_col", "").strip()
        note_ = r.get("note", "").strip()
        if to and fc:
            comment = f"  -- {note_}" if note_ else ""
            lines.append(f'-- FK: "{tname}"."{fc}" -> "{to}"."{tc}"{comment}')

    return "\n".join(lines)


def build_schema_cache(eng, tables, meta) -> dict:
    insp  = inspect(eng)
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


# ─────────────────────────────────────────────────────────────────────────────
# QUESTION GENERATION LOGIC
# ─────────────────────────────────────────────────────────────────────────────
def call_llm_for_questions(llm, schema: str, style_hint: str, count: int) -> list[str]:
    """Ask Qwen to generate `count` questions for the given schema. Returns a list."""
    prompt = QUESTION_PROMPT.format(schema=schema, style_hint=style_hint, count=count)
    raw = llm.invoke(prompt)

    # Strip markdown fences if model wraps output
    raw = re.sub(r"```json|```", "", raw, flags=re.IGNORECASE).strip()

    # Extract the JSON array
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if not match:
        raise ValueError(f"No JSON array found in response:\n{raw[:300]}")

    parsed = json.loads(match.group(0))
    return [str(q).strip() for q in parsed if isinstance(q, str) and len(q.strip()) > 10]


def generate_questions(schema_cache: dict, llm, target: int) -> list[str]:
    all_tables  = list(schema_cache.keys())
    questions   = []
    seen        = set()
    batch_num   = 0
    style_cycle = 0

    log_lines = []

    while len(questions) < target:
        batch_num   += 1
        style_hint   = STYLE_HINTS[style_cycle % len(STYLE_HINTS)]
        style_cycle += 1

        # Pick TABLES_PER_BATCH random tables; ensure variety by shuffling
        sample_tables = random.sample(all_tables, min(TABLES_PER_BATCH, len(all_tables)))
        schema = "\n\n".join(schema_cache[t] for t in sample_tables if t in schema_cache)

        # How many more do we need?
        remaining   = target - len(questions)
        request_n   = min(QUESTIONS_PER_CALL, remaining + 5)  # ask a few extra to fill dedup gaps

        logger.info(f"Batch {batch_num:03d} | tables: {sample_tables} | style: '{style_hint[:40]}…'")

        batch_qs = []
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                batch_qs = call_llm_for_questions(llm, schema, style_hint, request_n)
                break
            except Exception as ex:
                logger.warning(f"  Attempt {attempt} failed: {ex}")
                time.sleep(1)

        # Deduplicate (case-insensitive)
        new_qs = []
        for q in batch_qs:
            key = q.lower().strip()
            if key not in seen:
                seen.add(key)
                questions.append(q)
                new_qs.append(q)

        log_entry = (
            f"Batch {batch_num:03d} | tables: {sample_tables}\n"
            f"  Style: {style_hint}\n"
            f"  Generated: {len(batch_qs)} | New unique: {len(new_qs)} | Total: {len(questions)}\n"
            f"  Sample: {new_qs[:3]}\n"
        )
        log_lines.append(log_entry)
        logger.info(f"  → +{len(new_qs)} unique | total so far: {len(questions)}")

        # Safety valve: stop if we've generated way more batches than expected
        if batch_num > (target // QUESTIONS_PER_CALL) * 3:
            logger.warning("Too many batches — stopping early to avoid infinite loop.")
            break

    # Write log
    Path(LOG_FILE).write_text("\n".join(log_lines), encoding="utf-8")
    logger.info(f"Log written to {LOG_FILE}")

    return questions[:target]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    logger.info("═" * 60)
    logger.info(" Question Generator — Qwen2.5-Coder:7b via Ollama")
    logger.info("═" * 60)

    # 1. Load metadata — Excel sheet is the source of truth for allowed tables
    meta, _ = load_excel_metadata(METADATA_FILE)
    metadata_table_names = set(meta.keys())
    logger.info(f"Tables in metadata Excel: {len(metadata_table_names)}")

    # 2. Connect to DB
    db_uri = (
        f"postgresql://{DB_CONFIG['username']}:{quote_plus(DB_CONFIG['password'])}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(db_uri)
    all_db_tables = discover_tables(engine)

    # 3. Apply filters
    #    Filter A — metadata-only: keep only tables present in the Excel sheet
    #    Filter B — prefix exclusion: drop any table starting with DT_
    filtered_tables = []
    skipped_dt      = []
    skipped_no_meta = []

    for t in all_db_tables:
        if t.startswith(EXCLUDE_PREFIXES):
            skipped_dt.append(t)
            continue
        if METADATA_TABLES_ONLY and t not in metadata_table_names:
            skipped_no_meta.append(t)
            continue
        filtered_tables.append(t)

    logger.info("── Table filter summary ──────────────────────────────")
    logger.info(f"  DB tables discovered    : {len(all_db_tables)}")
    logger.info(f"  Skipped  (DT_ prefix)   : {len(skipped_dt)}  → {skipped_dt}")
    logger.info(f"  Skipped  (not in Excel) : {len(skipped_no_meta)}")
    logger.info(f"  ✅ Tables for generation : {len(filtered_tables)}  → {filtered_tables}")
    logger.info("──────────────────────────────────────────────────────")

    if not filtered_tables:
        logger.error("No tables left after filtering — check your Excel metadata file.")
        return

    # 4. Build schema cache ONLY for the filtered tables
    schema_cache = build_schema_cache(engine, filtered_tables, meta)
    logger.info(f"Schema cache ready: {len(schema_cache)} tables")

    # 5. Init LLM — higher temperature for creative diversity
    llm = OllamaLLM(
        model="qwen2.5-coder:7b",
        temperature=0.7,       # more variety than the SQL model's 0.0
        num_ctx=8192,
        num_predict=1000,      # enough room for 20 questions
    )
    logger.info("LLM ready.")

    # 6. Generate
    t_start = time.time()
    questions = generate_questions(schema_cache, llm, TARGET_QUESTIONS)
    elapsed  = round(time.time() - t_start, 1)

    # 7. Save
    out = Path(OUTPUT_FILE)
    out.write_text(json.dumps(questions, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info("═" * 60)
    logger.info(f"✅  Done!  {len(questions)} questions saved → {OUTPUT_FILE}")
    logger.info(f"⏱  Total time: {elapsed}s  (~{elapsed/60:.1f} min)")
    logger.info("═" * 60)

    # Print a preview
    print("\n── Sample questions ──")
    for i, q in enumerate(random.sample(questions, min(10, len(questions))), 1):
        print(f"  {i:>3}. {q}")


if __name__ == "__main__":
    main()
