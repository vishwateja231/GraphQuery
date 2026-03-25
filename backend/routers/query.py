"""
query.py — Production-grade LLM query pipeline with strict schema enforcement.

Flow:
1) Schema loaded from schema_enforcer (schema.json / live DB fallback)
2) Guardrails — reject non-dataset queries
3) Groq generates SQL from strict SAP schema prompt
4) SQL validation — allow only SELECT/WITH + LIMIT
5) Strict column validation against real SAP schema
7) Async PostgreSQL execution
8) On DB error → one retry with error context sent back to LLM
9) Graph builder — {nodes, edges} from result rows
10) Gemini summarizes if >10 rows
11) Consistent response: {type, summary, total, data, graph}
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
import schema_enforcer  # pyre-ignore[21]

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
    return (s[:n] + "..") if len(s) > n else s  # type: ignore


router = APIRouter()

# ── Global schema (SAP-only, loaded from schema_enforcer) ─────────────────────
# schema_enforcer.load_schema() returns {table: [col, ...]} using schema.json.
# It filters OUT legacy phantom tables (orders, customers, invoices, etc.).
LIVE_SCHEMA: Dict[str, Any] = {"tables": {}, "foreign_keys": []}


def _bootstrap_live_schema() -> None:
    """Populate LIVE_SCHEMA at startup using schema_enforcer."""
    try:
        sap_tables = schema_enforcer.load_schema()
        LIVE_SCHEMA["tables"] = sap_tables
        LIVE_SCHEMA["foreign_keys"] = []  # join hints are in schema_enforcer.JOIN_HINTS
        logger.info("[SCHEMA] Bootstrapped with %d SAP tables via schema_enforcer", len(sap_tables))
    except Exception as exc:
        logger.error("[SCHEMA] Bootstrap failed — using empty schema: %s", exc)


_bootstrap_live_schema()


def format_schema_for_prompt(schema: Dict[str, Any]) -> str:
    """Delegates to schema_enforcer for a rich schema+join-hints prompt block."""
    return schema_enforcer.build_schema_prompt()


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

# ── SQL Prompt Templates (schema injected at call time) ───────────────────────

SQL_PROMPT_TEMPLATE = """You are a PostgreSQL expert working with an SAP Order-to-Cash database.

STRICT RULES — VIOLATION WILL CAUSE QUERY FAILURE:
1. Use ONLY the tables and columns from the SCHEMA below. DO NOT invent column names.
2. Do NOT invent aliases for missing columns. Use exact schema names only.
3. Always use explicit column names. No SELECT *.
4. All ID/text values in WHERE clauses MUST be wrapped in single quotes:
   e.g. sold_to_party = '320000085'  NOT  sold_to_party = 320000085
5. Use the JOIN HINTS in the schema to form correct relationships.
6. Always include LIMIT 20.
7. Return ONLY a JSON object: {{"sql": "SELECT ..."}}
   No markdown, no explanation, no extra keys.
8. For payments/paid status, ALWAYS JOIN payments_accounts_receivable par ON soh.sold_to_party = par.customer and check par.clearing_date IS NOT NULL.

SCHEMA:
{schema}

EXAMPLE OUTPUT:
{{"sql": "SELECT soh.sales_order, soh.sold_to_party, soh.total_net_amount FROM sales_order_headers soh LIMIT 20"}}
"""

SQL_VALIDATOR_PROMPT_TEMPLATE = """You are a strict PostgreSQL SQL validator for a SAP database.

Validate and fix the SQL below so it correctly answers: "{question}"

SCHEMA:
{schema}

SQL TO VALIDATE:
{sql}

VALIDATION CHECKLIST:
1. Every table name MUST exist in the SCHEMA.
2. Every column name MUST exist in the SCHEMA.
3. Do NOT invent columns or tables and do NOT use any mapping.
4. All literal values in WHERE clauses must be single-quoted.
5. Apply LIMIT 20 if missing.
6. Use proper LEFT JOINs via the JOIN HINTS for rich results.

Return ONLY: {{"sql": "corrected SQL here"}}
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
        "message": msg if msg == "Invalid column in query" else (detail or msg),
    }


def _empty_response(message: str = "No data found") -> Dict[str, Any]:
    return {"type": "empty", "message": message, "graph": {"nodes": [], "edges": []}}


def normalize_question(question: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract query IDs and normalize the prompt using REAL SAP column names.
    Adds SQL constraints referencing actual table/column names.
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
        cid = hints["customer"]
        cleaned += (
            f"\n\nSQL CONSTRAINT: Filter by customer using "
            f"`sales_order_headers.sold_to_party = '{cid}'` "
            f"OR `business_partners.customer = '{cid}'`. "
            f"Do NOT use customer_id — that column does not exist."
        )
    if hints.get("order"):
        oid = hints["order"]
        cleaned += (
            f"\n\nSQL CONSTRAINT: Filter by order using "
            f"`sales_order_headers.sales_order = '{oid}'`. "
            f"Do NOT use order_id — that column does not exist."
        )
    return cleaned, hints


def validate_sql_against_schema(sql: str, schema: Dict[str, Any]) -> Optional[str]:
    """
    Validate SQL column/table names against the real SAP schema.
    Delegates to schema_enforcer.validate_sql_columns().
    """
    is_valid, bad_tokens = schema_enforcer.validate_sql_columns(sql)
    if not is_valid:
        logger.warning("[SQL-VALIDATE] Rejected — unknown identifiers: %s", bad_tokens[:8])
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


def _groq_validate_sql_call(question: str, sql: str, schema_str: str) -> Dict[str, Any]:
    from groq import Groq  # pyre-ignore[21]
    client = Groq(api_key=GROQ_API_KEY)
    prompt = SQL_VALIDATOR_PROMPT_TEMPLATE.format(
        schema=schema_str, question=question, sql=sql
    )
    resp = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        response_format={"type": "json_object"},
        temperature=0.0,
        timeout=10,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": "Validate and fix the SQL query."},
        ],
    )
    content = resp.choices[0].message.content or "{}"
    return json.loads(content)  # type: ignore[return-value]


def _groq_retry_call(question: str, failed_sql: str, error_msg: str) -> Dict[str, Any]:
    """Retry SQL generation after a DB execution error, sending error context to LLM."""
    from groq import Groq  # pyre-ignore[21]
    client = Groq(api_key=GROQ_API_KEY)
    retry_prompt = schema_enforcer.build_retry_prompt(question, failed_sql, error_msg)
    resp = client.chat.completions.create(
        model="llama-3.1-8b-instant",
        response_format={"type": "json_object"},
        temperature=0.0,
        timeout=12,
        messages=[
            {"role": "system", "content": retry_prompt},
            {"role": "user", "content": "Fix the SQL query based on the error above."},
        ],
    )
    content = resp.choices[0].message.content or "{}"
    return json.loads(content)  # type: ignore[return-value]


def _gemini_sql_call(question: str, schema_str: str) -> Dict[str, Any]:
    import google.generativeai as genai  # pyre-ignore[21]
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set")
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        model_name="gemini-2.0-flash",
        generation_config=genai.types.GenerationConfig(temperature=0.0, max_output_tokens=400),  # pyre-ignore[16]
    )
    prompt = SQL_PROMPT_TEMPLATE.format(schema=schema_str)
    result = model.generate_content([prompt, f"Question: {question}"])
    text = (result.text or "{}").strip()
    return json.loads(text)  # type: ignore[return-value]


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

async def generate_sql_from_llm(question: str, retry_context: Optional[Tuple[str, str]] = None) -> Optional[str]:
    """
    Generate SQL from LLM with schema enforcement.
    retry_context: (failed_sql, error_message) — triggers a single retry call.
    """
    cache_key = question + ("::retry" if retry_context else "")
    if not retry_context:
        cached = _sql_gen_cache.get(cache_key)
        if cached is not _CACHE_MISS:
            logger.info("[CACHE] SQL cache hit")
            return cached  # type: ignore[return-value]

    normalized_question, _ = normalize_question(question)
    schema_str = schema_enforcer.build_schema_prompt()

    try:
        if retry_context:
            # ── RETRY PATH: send error + schema back to LLM ──────────────────
            failed_sql, error_msg = retry_context
            logger.info("[GROQ] Retry SQL generation after error: %s", _trunc(error_msg, 80))
            result = await asyncio.wait_for(
                asyncio.to_thread(_groq_retry_call, normalized_question, failed_sql, error_msg),  # pyre-ignore[6]
                timeout=14.0,
            )
            sql_raw: str = str(result.get("sql", "")).strip()
            logger.info("[GROQ] Retry SQL: %s", _trunc(sql_raw))
            sql_validated = sql_raw  # skip second validation pass on retry
        else:
            # ── NORMAL PATH ──────────────────────────────────────────────────
            logger.info("[GROQ] Generating SQL for: %s", _trunc(question, 60))
            result = await asyncio.wait_for(
                asyncio.to_thread(_groq_call, normalized_question, schema_str),  # pyre-ignore[6]
                timeout=12.0,
            )
            sql_raw = str(result.get("sql", "")).strip()
            logger.info("[GROQ] Initial SQL: %s", _trunc(sql_raw))

            logger.info("[GROQ] Validating SQL via LLM Reviewer...")
            val_result = await asyncio.wait_for(
                asyncio.to_thread(_groq_validate_sql_call, normalized_question, sql_raw, schema_str),  # pyre-ignore[6]
                timeout=12.0,
            )
            sql_validated = str(val_result.get("sql", sql_raw)).strip()
            logger.info("[GROQ] Validated SQL: %s", _trunc(sql_validated))

    except asyncio.TimeoutError:
        raise ValueError("LLM timeout — Groq did not respond within 14 seconds")
    except Exception as exc:
        if is_llm_rate_error(str(exc)) and not retry_context:
            logger.warning("[GROQ] Rate-limited. Falling back to Gemini SQL generation.")
            await asyncio.sleep(1.0)
            try:
                result = await asyncio.wait_for(
                    asyncio.to_thread(_gemini_sql_call, normalized_question, schema_str),  # pyre-ignore[6]
                    timeout=14.0,
                )
                sql_validated = str(result.get("sql", "")).strip()
                logger.info("[GEMINI] Fallback SQL: %s", _trunc(sql_validated))
            except Exception as gemini_exc:
                logger.warning("[GEMINI] SQL fallback failed: %s", gemini_exc)
                raise ValueError("LLM limit exceeded")
        elif is_llm_rate_error(str(exc)):
            raise ValueError("LLM limit exceeded")
        logger.warning("[GROQ] Error: %s", exc)
        if "sql_validated" not in locals():
            return None

    # ── Step 4 (syntax): Ensure SELECT/WITH + LIMIT ───────────────────────────
    sql_clean = sanitize_and_validate_sql(sql_validated)

    # ── Step 5: Column validation against real SAP schema ─────────────────────
    if sql_clean:
        sql_final = validate_sql_against_schema(sql_clean, LIVE_SCHEMA)
    else:
        sql_final = None

    if not retry_context:
        _sql_gen_cache.set(cache_key, sql_final)
    return sql_final


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
        # Return structured error so build_query_response can attempt a retry
        return {"error": "Database error", "details": str(exc), "failed_sql": sql}


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
    """Full pipeline with strict schema enforcement and single retry-on-error."""
    try:
        logger.info("[DEBUG] build_query_response started for: %s", question)

        if not is_dataset_question(question):
            return _error_response("This system only answers SAP Order-to-Cash dataset queries.")

        # ── Step 3+6: Generate SQL (with auto-correct inside) ─────────────────
        sql = await generate_sql_from_llm(question)
        logger.info("[DEBUG] SQL generated: %s", sql)

        if not sql:
            return _error_response("Invalid column in query")

        # ── Step 7: Execute SQL ───────────────────────────────────────────────
        db_result = await run_sql(sql)
        logger.info("[DEBUG] DB result received")

        if isinstance(db_result, dict) and "error" in db_result:
            err_dict = db_result  # type: ignore[assignment]
            error_msg_raw = err_dict.get("details") or err_dict.get("error", "unknown error")
            failed_sql_raw = err_dict.get("failed_sql", sql)
            
            error_msg: str = str(error_msg_raw)
            failed_sql: str = str(failed_sql_raw)

            logger.warning("[RETRY] DB error '%s'. Retrying SQL generation...", _trunc(error_msg, 80))
            await asyncio.sleep(1.0)
            sql_retry = await generate_sql_from_llm(question, retry_context=(failed_sql, error_msg))
            if sql_retry and sql_retry != sql:
                logger.info("[RETRY] Retrying with corrected SQL: %s", _trunc(sql_retry))
                await asyncio.sleep(1.0)
                db_result = await run_sql(sql_retry)
                sql = sql_retry

            if isinstance(db_result, dict) and "error" in db_result:
                return _error_response(
                    db_result["error"],
                    f"{db_result.get('details', '')} | Retry also failed."
                )

        rows: List[Dict[str, Any]] = db_result  # type: ignore[assignment]
        if not rows:
            return _empty_response("No data found")

        graph_data: Dict[str, Any] = graph_builder.build_graph(rows)
        logger.info(
            "[DEBUG] Graph built: %d nodes, %d edges",
            len(graph_data.get("nodes", [])),
            len(graph_data.get("edges", [])),
        )

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
