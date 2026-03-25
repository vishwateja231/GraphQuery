"""
query.py — Production-grade LLM query pipeline (all 13 steps)

Flow:
1) Dynamic schema loaded at startup from PostgreSQL
2) Guardrails — reject non-dataset queries
3) Groq generates SQL from live schema (10s timeout)
4) SQL validation — allow only SELECT/WITH + LIMIT
5) Async PostgreSQL execution (safe wrapped)
6) Graph builder — {nodes, edges} from result rows
7) Gemini summarizes if >10 rows (10s timeout)
8) Consistent response: {type, summary, total, data, graph}
"""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import re
from collections import OrderedDict
from decimal import Decimal
from datetime import date, datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Request  # pyre-ignore[21]
from fastapi.responses import StreamingResponse, JSONResponse  # pyre-ignore[21]
from pydantic import BaseModel  # pyre-ignore[21]

from config import GEMINI_API_KEY, GROQ_API_KEY  # pyre-ignore[21]
from database import fetch_all  # pyre-ignore[21]
import graph_builder  # pyre-ignore[21]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# DEBUG: Add file handler
fh = logging.FileHandler("debug_query.log")
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)
import traceback


def _trunc(s: str, n: int = 100) -> str:
    """Truncate string safely."""
    if not s:
        return ""
    return (s[:n] + "..") if len(s) > n else s


router = APIRouter()

# ── Global dynamic schema ─────────────────────────────────────────────────────
LIVE_SCHEMA: Dict[str, Any] = {}   # populated immediately below


def load_schema_from_db() -> Dict[str, Any]:
    """Extract tables, columns and FKs using a direct connection (no pool needed)."""
    import os
    schema: Dict[str, Any] = {"tables": {}, "foreign_keys": []}
    try:
        import psycopg  # pyre-ignore[21]
        from psycopg.rows import dict_row as _dict_row  # pyre-ignore[21]
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            raise RuntimeError("DATABASE_URL not set")
        with psycopg.connect(db_url, row_factory=_dict_row, prepare_threshold=None) as conn:  # pyre-ignore[16]
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema='public' AND table_type='BASE TABLE'
                    ORDER BY table_name
                """)
                for row in cur.fetchall():
                    name = dict(row).get("table_name", "")
                    if name:
                        schema["tables"][name] = []

                cur.execute("""
                    SELECT table_name, column_name FROM information_schema.columns
                    WHERE table_schema='public'
                    ORDER BY table_name, ordinal_position
                """)
                for row in cur.fetchall():
                    r = dict(row)
                    t, c = r.get("table_name", ""), r.get("column_name", "")
                    if t in schema["tables"] and c:
                        schema["tables"][t].append(c)

                cur.execute("""
                    SELECT tc.table_name, kcu.column_name,
                           ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                """)
                for row in cur.fetchall():
                    r = dict(row)
                    schema["foreign_keys"].append({
                        "from_table": r.get("table_name", ""),
                        "from_col":   r.get("column_name", ""),
                        "to_table":   r.get("foreign_table", ""),
                        "to_col":     r.get("foreign_column", ""),
                    })
        logger.info("[SCHEMA] Loaded %d tables from PostgreSQL", len(schema["tables"]))
    except Exception as exc:
        logger.error("[SCHEMA] Failed to load schema: %s", exc)
        schema["tables"] = {
            "customers":   ["customer_id", "name", "grouping", "is_blocked", "created_date"],
            "orders":      ["order_id", "customer_id", "order_date", "total_amount", "delivery_status"],
            "order_items": ["order_id", "line_no", "product_id", "quantity", "net_amount"],
            "deliveries":  ["delivery_id", "order_id", "ship_date", "picking_status", "goods_status"],
            "invoices":    ["invoice_id", "order_id", "customer_id", "invoice_date", "total_amount"],
            "payments":    ["payment_id", "customer_id", "clearing_date", "amount", "is_incoming"],
            "products":    ["product_id", "product_name", "product_type", "product_group"],
        }
    return schema


# FIX 1: Populate LIVE_SCHEMA at module load time — never empty.
LIVE_SCHEMA = load_schema_from_db()


def format_schema_for_prompt(schema: Dict[str, Any]) -> str:
    lines: List[str] = []
    for table, cols in schema.get("tables", {}).items():
        lines.append(f"TABLE {table} ({', '.join(cols)})")
    fks = schema.get("foreign_keys", [])
    if fks:
        lines.append("\nFOREIGN KEYS:")
        for fk in fks:
            lines.append(f"  {fk['from_table']}.{fk['from_col']} -> {fk['to_table']}.{fk['to_col']}")
    return "\n".join(lines)


# ── JSON helpers ──────────────────────────────────────────────────────────────

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:  # pyre-ignore[14]
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def safe_serialize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [safe_serialize(i) for i in obj]
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


# ── Request model ─────────────────────────────────────────────────────────────

class QuestionRequest(BaseModel):
    question: str


# ── Caching ───────────────────────────────────────────────────────────────────

# FIX 3: Sentinel object so we can cache None without confusing it with a miss.
_CACHE_MISS = object()


class LRUCache:
    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self._store: OrderedDict = OrderedDict()  # pyre-ignore[24]

    def get(self, key: str) -> Any:
        if key not in self._store:
            return _CACHE_MISS
        self._store.move_to_end(key)
        return self._store[key]

    def set(self, key: str, value: Any) -> None:
        self._store[key] = value
        self._store.move_to_end(key)
        while len(self._store) > self.max_size:
            self._store.popitem(last=False)


_sql_gen_cache: LRUCache = LRUCache(200)
_result_cache: LRUCache = LRUCache(200)

# ── Constants ─────────────────────────────────────────────────────────────────

DATASET_KEYWORDS = {
    "order", "orders", "customer", "customers",
    "delivery", "deliveries", "invoice", "invoices",
    "payment", "payments", "product", "products",
    "revenue", "data", "sales", "billing",
    "trace", "flow", "highest", "top", "show", "all",
}

# FIX 2: Prompt now instructs the model to return JSON {"sql": "..."}
# which matches response_format={"type": "json_object"}.
SQL_PROMPT_TEMPLATE = """You are a PostgreSQL expert.

Database schema:
{schema}

Rules:
- Use ONLY the tables and columns listed above. Do NOT hallucinate.
- Always use proper JOINs for relationships.
- No SELECT * — always name columns explicitly.
- Always alias IDs: o.order_id AS order_id, c.customer_id AS customer_id,
  d.delivery_id AS delivery_id, i.invoice_id AS invoice_id,
  p.payment_id AS payment_id, pr.product_id AS product_id
- Always include LIMIT 20.
- Return ONLY a JSON object with a single key "sql" containing the SQL query.
  Example: {{"sql": "SELECT c.customer_id AS customer_id FROM customers c LIMIT 20"}}
- No explanation, no markdown, no extra keys.
"""

LLM_RATE_SIGNALS = [
    "rate_limit", "rate limit", "quota", "429", "401", "403",
    "api_key", "exceeded", "limit exceeded", "insufficient_quota",
]

# ── Helpers ───────────────────────────────────────────────────────────────────

def is_dataset_question(question: str) -> bool:
    return any(k in question.lower() for k in DATASET_KEYWORDS)


def is_llm_rate_error(error_str: str) -> bool:
    return any(sig in error_str.lower() for sig in LLM_RATE_SIGNALS)


def sanitize_and_validate_sql(sql: str) -> Optional[str]:
    if not sql or not sql.strip():
        return None
    raw = sql.strip().rstrip(";")
    lower = raw.lower()
    if not (lower.startswith("select") or lower.startswith("with")):
        logger.warning("[SQL] Rejected non-SELECT")
        return None
    disallowed = ["drop", "delete", "update", "insert", "alter", "truncate", "grant", "revoke", "create"]
    if any(re.search(r"\b" + w + r"\b", lower) for w in disallowed):
        logger.warning("[SQL] Rejected unsafe keyword")
        return None
    if "limit" not in lower:
        raw = raw + " LIMIT 20"
    logger.info("[SQL] Validated: %s", _trunc(raw))
    return raw


def format_small_result(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return "No matching data found."
    lines: List[str] = [f"Found {len(rows)} result(s):"]
    for row in itertools.islice(rows, 10):
        pairs = [f"{k}: {v}" for k, v in row.items()]
        lines.append(f"- {', '.join(pairs)}")
    return "\n".join(lines)


def _error_response(msg: str, detail: str = "") -> Dict[str, Any]:
    return {
        "type": "error",
        "message": detail or msg,
    }


def _empty_response(message: str = "No data found") -> Dict[str, Any]:
    return {"type": "empty", "message": message}


def normalize_question(question: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract query IDs and normalize the prompt for stronger SQL generation.
    Example:
    'show customer 320000083 orders' -> includes guardrail: use orders.customer.
    """
    cleaned = re.sub(r"\s+", " ", question).strip()
    hints: Dict[str, str] = {}

    customer_match = re.search(r"\bcustomer\s+([A-Za-z0-9_-]+)\b", cleaned, flags=re.IGNORECASE)
    if customer_match:
        hints["customer"] = customer_match.group(1)

    order_match = re.search(r"\border\s+([A-Za-z0-9_-]+)\b", cleaned, flags=re.IGNORECASE)
    if order_match:
        hints["order"] = order_match.group(1)

    if hints.get("customer"):
        cleaned += (
            f"\n\nSQL constraint: when filtering orders by customer, "
            f"use `orders.customer = '{hints['customer']}'` (not customer_id)."
        )
    return cleaned, hints


def _extract_known_columns(sql: str) -> Set[str]:
    tokens = re.findall(r"\b([a-z_][a-z0-9_]*)\b", sql.lower())
    reserved = {
        "select", "from", "join", "left", "right", "inner", "outer", "on", "where", "and", "or", "as",
        "with", "limit", "offset", "group", "by", "order", "desc", "asc", "count", "sum", "avg", "min",
        "max", "distinct", "case", "when", "then", "else", "end", "not", "null", "is", "having", "true",
        "false", "in", "like", "ilike", "coalesce",
    }
    return {t for t in tokens if t not in reserved and not t.isdigit()}


def validate_sql_against_schema(sql: str, schema: Dict[str, Any]) -> Optional[str]:
    """
    Reject invalid table/column references and duplicate aliases for *_id columns.
    """
    tables = schema.get("tables", {})
    valid_tables = set(tables.keys())
    valid_columns = {c for cols in tables.values() for c in cols}
    tokens = _extract_known_columns(sql)

    # Unknown tokens are tolerated if they are aliases/functions, but unknown identifier ratio
    # should remain low to catch hallucinations.
    unknown = [t for t in tokens if t not in valid_tables and t not in valid_columns]
    if len(unknown) > max(5, int(len(tokens) * 0.45)):
        logger.warning("[SQL] Too many unknown identifiers: %s", unknown[:8])
        return None

    aliases = re.findall(r"\bas\s+([a-z_][a-z0-9_]*)\b", sql.lower())
    id_aliases = [a for a in aliases if a.endswith("_id")]
    if len(id_aliases) != len(set(id_aliases)):
        logger.warning("[SQL] Duplicate *_id aliases are not allowed")
        return None

    required_aliases = {"customer_id", "order_id", "delivery_id", "invoice_id", "payment_id", "product_id"}
    bad_aliases = [a for a in id_aliases if a not in required_aliases]
    if bad_aliases:
        logger.warning("[SQL] Invalid ID aliases found: %s", bad_aliases)
        return None
    return sql


# ── LLM calls (all blocking — run in thread pool) ────────────────────────────

def _groq_call(question: str, schema_str: str) -> Dict[str, Any]:
    from groq import Groq  # pyre-ignore[21]
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set")
    client = Groq(api_key=GROQ_API_KEY)
    prompt = SQL_PROMPT_TEMPLATE.format(schema=schema_str)
    resp = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        response_format={"type": "json_object"},
        temperature=0.0,
        timeout=10,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": question},
        ],
    )
    content = resp.choices[0].message.content or "{}"
    return json.loads(content)  # type: ignore[return-value]


def _gemini_call(question: str, sql: str, rows: List[Dict[str, Any]]) -> str:
    import google.generativeai as genai  # pyre-ignore[21]
    if not GEMINI_API_KEY:
        return format_small_result(list(itertools.islice(rows, 10)))
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        model_name="gemini-2.0-flash",
        generation_config=genai.types.GenerationConfig(temperature=0.1, max_output_tokens=260),  # pyre-ignore[16]
    )
    sample = safe_serialize(list(itertools.islice(rows, 20)))
    prompt = (
        f"User: {question}\nSQL: {sql}\nRows: {len(rows)}\n"
        f"Sample:\n{json.dumps(sample)}\n\nWrite a concise business summary."
    )
    result = model.generate_content(["Summarize SQL results as a business analyst. Plain text only.", prompt])
    return (result.text or "").strip() or format_small_result(list(itertools.islice(rows, 10)))


def _groq_fallback_summary(question: str, rows: List[Dict[str, Any]]) -> str:
    """Blocking Groq summary — always called via asyncio.to_thread."""
    try:
        from groq import Groq  # pyre-ignore[21]
        client = Groq(api_key=GROQ_API_KEY)
        sample = safe_serialize(list(itertools.islice(rows, 15)))
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            temperature=0.1,
            max_tokens=200,
            timeout=10,
            messages=[
                {"role": "system", "content": "Summarize SQL results concisely as a business analyst."},
                {"role": "user", "content": f"Question: {question}\nData: {json.dumps(sample)}"},
            ],
        )
        return (resp.choices[0].message.content or "").strip() or format_small_result(list(itertools.islice(rows, 10)))
    except Exception as exc:
        logger.warning("[WARN] Groq fallback summary failed: %s", exc)
        return format_small_result(list(itertools.islice(rows, 10)))


# ── Core pipeline ─────────────────────────────────────────────────────────────

async def generate_sql_from_llm(question: str) -> Optional[str]:
    cached = _sql_gen_cache.get(question)
    # FIX 3: Use _CACHE_MISS so None (invalid SQL) is cached and returned correctly.
    if cached is not _CACHE_MISS:
        logger.info("[CACHE] SQL cache hit")
        return cached  # type: ignore[return-value]

    normalized_question, _ = normalize_question(question)
    schema_str = format_schema_for_prompt(LIVE_SCHEMA)
    try:
        logger.info("[GROQ] Generating SQL for: %s", _trunc(question, 60))
        result = await asyncio.wait_for(
            asyncio.to_thread(_groq_call, normalized_question, schema_str),  # pyre-ignore[6]
            timeout=12.0,
        )
        sql_raw: str = str(result.get("sql", "")).strip()
        logger.info("[GROQ] Raw SQL: %s", _trunc(sql_raw))
    except asyncio.TimeoutError:
        raise ValueError("LLM timeout — Groq did not respond within 10 seconds")
    except Exception as exc:
        if is_llm_rate_error(str(exc)):
            raise ValueError("LLM limit exceeded")
        logger.warning("[GROQ] Error: %s", exc)
        return None

    sql = sanitize_and_validate_sql(sql_raw)
    if sql:
        sql = validate_sql_against_schema(sql, LIVE_SCHEMA)
    _sql_gen_cache.set(question, sql)
    return sql


async def run_sql(sql: str) -> Any:
    cached = _result_cache.get(sql)
    if cached is not _CACHE_MISS:
        logger.info("[CACHE] Result cache hit")
        return cached

    try:
        logger.info("[DB] Executing SQL…")
        rows = await asyncio.to_thread(fetch_all, sql)  # pyre-ignore[6]
        logger.info("[DB] Got %d rows", len(rows))  # pyre-ignore[6]
        result = safe_serialize(list(rows))
        _result_cache.set(sql, result)
        return result
    except Exception as exc:
        logger.error("[DB] Query failed: %s", exc)
        return {"error": "Database error", "details": str(exc)}


async def summarize(question: str, sql: str, rows: List[Dict[str, Any]]) -> str:
    if len(rows) < 10:
        return format_small_result(rows)
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(_gemini_call, question, sql, rows),  # pyre-ignore[6]
            timeout=12.0,
        )
    except asyncio.TimeoutError:
        logger.warning("[GEMINI] Timeout — Groq fallback")
        # FIX 5: Run blocking fallback in thread pool.
        return await asyncio.to_thread(_groq_fallback_summary, question, rows)  # pyre-ignore[6]
    except Exception as exc:
        if is_llm_rate_error(str(exc)):
            logger.info("[GEMINI] Quota exceeded — Groq fallback")
            return await asyncio.to_thread(_groq_fallback_summary, question, rows)  # pyre-ignore[6]
        logger.warning("[GEMINI] Error: %s", exc)
        return format_small_result(rows)


async def build_query_response(question: str) -> Dict[str, Any]:
    """Steps 1-13: Full pipeline."""
    try:
        logger.info("[DEBUG] build_query_response started for: %s", question)
        # Check if question is valid for dataset
        if not is_dataset_question(question):
            return _error_response("This system only answers dataset-related queries.")
        
        sql = await generate_sql_from_llm(question)
        logger.info("[DEBUG] SQL generated: %s", sql)
        
        if not sql:
            return _error_response("Could not generate valid SQL for this query.")
        
        db_result = await run_sql(sql)
        logger.info("[DEBUG] DB result received")
        
        if isinstance(db_result, dict) and "error" in db_result:
            return _error_response(db_result["error"], db_result.get("details", ""))
        
        rows: List[Dict[str, Any]] = db_result  # type: ignore[assignment]
        if not rows:
            return _empty_response("No data found")
        
        graph_data: Dict[str, Any] = graph_builder.build_graph(rows)
        logger.info("[DEBUG] Graph built: %d nodes, %d edges", 
                    len(graph_data.get("nodes", [])), len(graph_data.get("edges", [])))
        
        summary_text = await summarize(question, sql, rows)
        logger.info("[DEBUG] Summary generated")
        
        return {
            "type": "graph",
            "summary": summary_text,
            "data": list(itertools.islice(rows, 50)),
            "graph": graph_data,
        }
    except Exception as e:
        logger.error("[DEBUG] CRASH in build_query_response: %s", e, exc_info=True)
        with open("debug_query.log", "a") as f:
            f.write(f"\n[CRASH] {str(e)}\n{traceback.format_exc()}\n")
        raise


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/")
async def natural_language_query(request: Request) -> Any:
    try:
        body = await request.json()
        question = body.get("question", "").strip()
        print(f"DEBUG: Processing raw question: {question}")
        if not question:
            return JSONResponse(content=_error_response("Question is empty"), status_code=200)
        
        result = await build_query_response(question)
        return JSONResponse(content=result, status_code=200)
    except Exception as exc:
        with open("debug_query.log", "a") as f:
            f.write(f"\n[ENDPOINT CRASH] {str(exc)}\n{traceback.format_exc()}\n")
        logger.error("[API] Unhandled error: %s", exc, exc_info=True)
        return JSONResponse(content=_error_response("Internal error", str(exc)), status_code=200)


@router.post("/stream/")
async def stream_query(body: QuestionRequest) -> Any:
    question = body.question.strip()

    async def event_stream() -> AsyncGenerator[str, None]:
        yield "event: status\ndata: Processing...\n\n"
        try:
            logger.info("[STREAM] Starting pipeline for: %s", _trunc(question))
            payload = await build_query_response(question)
            logger.info("[STREAM] Payload ready, sending...")
            yield f"event: result\ndata: {json.dumps(payload, cls=CustomJSONEncoder)}\n\n"
        except Exception as exc:
            logger.error("[STREAM] Error in event_stream: %s", exc, exc_info=True)
            yield f"event: result\ndata: {json.dumps(_error_response('Internal error', str(exc)))}\n\n"
        finally:
            logger.info("[STREAM] Stream closed")

    return StreamingResponse(event_stream(), media_type="text/event-stream")
