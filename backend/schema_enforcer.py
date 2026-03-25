"""
schema_enforcer.py — Strict schema enforcement for LLM SQL generation.

Responsibilities:
  1. Load & cache schema from schema.json (or live DB at startup)
  2. Validate every table/column used in a SQL query
  3. Build a strict schema prompt string for the LLM
  4. Provide retry context when SQL execution fails

The AUTHORITATIVE table list is SAP_TABLES — phantom legacy tables
(orders, customers, invoices, payments, deliveries, order_items) that
still exist in the DB from earlier modelling are excluded from the LLM
prompt so the model never touches them.
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# ── Authoritative SAP table names ─────────────────────────────────────────────
# Only these tables are exposed to the LLM.  Legacy tables that remain in the
# database (orders, customers, invoices, payments, deliveries, order_items) are
# deliberately excluded.
SAP_TABLES: Set[str] = {
    "business_partners",
    "business_partner_addresses",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "sales_order_headers",
    "sales_order_items",
    "sales_order_schedule_lines",
    "outbound_delivery_headers",
    "outbound_delivery_items",
    "billing_document_headers",
    "billing_document_items",
    "billing_document_cancellations",
    "payments_accounts_receivable",
    "journal_entry_items_accounts_receivable",
    "products",
    "product_descriptions",
    "product_plants",
    "product_storage_locations",
    "plants",
}

# ── Table join hints (to help LLM form correct JOINs) ─────────────────────────
JOIN_HINTS: List[str] = [
    "sales_order_headers.sold_to_party = business_partners.customer",
    "sales_order_headers.sales_order = sales_order_items.sales_order",
    "sales_order_items.reference_sd_document (via billing_document_items) → billing_document_headers.billing_document",
    "billing_document_headers.accounting_document = payments_accounts_receivable.invoice_reference",
    "outbound_delivery_items.reference_sd_document = sales_order_headers.sales_order",
    "product_descriptions.product = products.product (JOIN ON language='EN' for English names)",
]

# ── In-memory schema cache ─────────────────────────────────────────────────────
_SCHEMA_CACHE: Optional[Dict[str, List[str]]] = None

_SCHEMA_JSON_PATH = os.path.join(os.path.dirname(__file__), "schema.json")


def load_schema() -> Dict[str, List[str]]:
    """Return SAP-only schema dict {table: [col, ...]}. Loads once then caches."""
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is not None:
        return _SCHEMA_CACHE

    raw: Dict[str, List[str]] = {}
    if os.path.exists(_SCHEMA_JSON_PATH):
        try:
            with open(_SCHEMA_JSON_PATH, "r") as f:
                raw = json.load(f)
            logger.info("[SCHEMA] Loaded schema.json with %d tables", len(raw))
        except Exception as exc:
            logger.error("[SCHEMA] Failed to read schema.json: %s", exc)

    # Filter to SAP tables only — explicit str() cast for pyre type inference
    filtered: Dict[str, List[str]] = {str(t): list(cols) for t, cols in raw.items() if t in SAP_TABLES}
    _SCHEMA_CACHE = filtered
    if not _SCHEMA_CACHE:
        logger.warning("[SCHEMA] schema.json missing or empty — using hardcoded fallback")
        _SCHEMA_CACHE = _hardcoded_fallback()

    logger.info("[SCHEMA] Active schema: %d SAP tables", len(_SCHEMA_CACHE))
    return _SCHEMA_CACHE


def refresh_schema_from_db() -> Dict[str, List[str]]:
    """Re-query the live DB and refresh schema.json + in-memory cache."""
    global _SCHEMA_CACHE
    try:
        import psycopg  # pyre-ignore[21]
        from psycopg.rows import dict_row  # pyre-ignore[21]
        db_url = os.environ.get("DATABASE_URL", "").strip()
        if not db_url:
            raise RuntimeError("DATABASE_URL not set")

        schema: Dict[str, List[str]] = {}
        with psycopg.connect(db_url, row_factory=dict_row, prepare_threshold=None) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position
                """)
                for row in cur.fetchall():
                    r = dict(row)
                    t, c = r.get("table_name", ""), r.get("column_name", "")
                    if t in SAP_TABLES and t and c:
                        schema.setdefault(t, []).append(c)

        # Persist
        with open(_SCHEMA_JSON_PATH, "w") as f:
            json.dump(schema, f, indent=2)

        _SCHEMA_CACHE = schema
        logger.info("[SCHEMA] Refreshed %d SAP tables from live DB", len(schema))
        return schema
    except Exception as exc:
        logger.error("[SCHEMA] Live refresh failed: %s", exc)
        return load_schema()


# ── Schema prompt builder ──────────────────────────────────────────────────────

def build_schema_prompt() -> str:
    """Build the schema block injected into the LLM system prompt."""
    schema = load_schema()
    lines: List[str] = []
    for table, cols in sorted(schema.items()):
        lines.append(f"TABLE {table} ({', '.join(cols)})")
    lines.append("\nFOREIGN KEY / JOIN HINTS:")
    for hint in JOIN_HINTS:
        lines.append(f"  {hint}")
    return "\n".join(lines)


def build_column_mapping_prompt() -> str:
    """Mapping is intentionally disabled to prevent invalid auto-corrections."""
    return (
        "COLUMN NAME MAPPING: DISABLED.\n"
        "Use exact table and column names from schema only."
    )


# ── Auto-correct ───────────────────────────────────────────────────────────────

def autocorrect_sql(sql: str) -> Tuple[str, List[str]]:
    """Auto-correction is intentionally disabled. Return SQL unchanged."""
    return sql, []


# ── Validation ─────────────────────────────────────────────────────────────────

_SQL_RESERVED = {
    "select", "from", "join", "left", "right", "inner", "outer", "full", "cross",
    "on", "where", "and", "or", "not", "as", "with", "limit", "offset", "group",
    "by", "order", "desc", "asc", "having", "union", "all", "distinct", "case",
    "when", "then", "else", "end", "null", "is", "in", "like", "ilike", "between",
    "exists", "true", "false", "count", "sum", "avg", "min", "max", "coalesce",
    "cast", "to", "over", "partition", "row_number", "rank", "date", "now",
    "extract", "interval", "varchar", "text", "integer", "numeric", "boolean",
}

def validate_sql_columns(sql: str) -> Tuple[bool, List[str]]:
    """
    Strictly validate table and column identifiers in SQL.
    Returns (is_valid, list_of_bad_identifiers).
    """
    schema = load_schema()
    valid_tables = {t.lower() for t in schema.keys()}
    table_columns = {t.lower(): {c.lower() for c in cols} for t, cols in schema.items()}
    valid_columns: Set[str] = set().union(*table_columns.values()) if table_columns else set()

    cleaned = re.sub(r"'(?:''|[^'])*'", "''", sql.lower())
    cleaned = re.sub(r'"(?:[^"]|"")*"', '""', cleaned)

    table_aliases: Dict[str, str] = {}
    table_refs = re.findall(
        r"\b(from|join)\s+([a-z_][a-z0-9_]*)(?:\s+(?:as\s+)?([a-z_][a-z0-9_]*))?",
        cleaned,
    )
    bad: List[str] = []
    for _, table, alias in table_refs:
        if table not in valid_tables:
            bad.append(table)
            continue
        table_aliases[table] = table
        if alias and alias not in _SQL_RESERVED:
            table_aliases[alias] = table

    qualified_refs = re.findall(r"\b([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)\b", cleaned)
    for qualifier, column in qualified_refs:
        mapped_table = table_aliases.get(qualifier)
        if not mapped_table:
            bad.append(f"{qualifier}.{column}")
            continue
        if column not in table_columns.get(mapped_table, set()):
            bad.append(f"{qualifier}.{column}")

    cleaned_unqualified = re.sub(r"\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b", " ", cleaned)
    cleaned_unqualified = re.sub(r"\b[0-9]+(?:\.[0-9]+)?\b", " ", cleaned_unqualified)
    tokens = re.findall(r"\b([a-z_][a-z0-9_]*)\b", cleaned_unqualified)
    seen_tokens = set(tokens)
    table_alias_names = set(table_aliases.keys())
    for tok in seen_tokens:
        if tok in _SQL_RESERVED:
            continue
        if tok in valid_tables or tok in valid_columns or tok in table_alias_names:
            continue
        bad.append(tok)

    dedup_bad = sorted(set(bad))
    return (len(dedup_bad) == 0), dedup_bad


def build_retry_prompt(original_question: str, failed_sql: str, error_msg: str) -> str:
    """Build the retry prompt sent back to the LLM after a SQL execution failure."""
    schema_block = build_schema_prompt()
    mapping_block = build_column_mapping_prompt()
    return f"""The following SQL query failed with this error:

ERROR: {error_msg}

ORIGINAL QUESTION: {original_question}

FAILED SQL:
{failed_sql}

You MUST fix the SQL using ONLY the exact column and table names from the schema below.
Do NOT invent or guess any column names.

{mapping_block}

{schema_block}

Return ONLY a JSON object: {{"sql": "..."}}"""


# ── Hardcoded fallback (last resort only) ─────────────────────────────────────

def _hardcoded_fallback() -> Dict[str, List[str]]:
    return {
        "business_partners": [
            "business_partner", "customer", "business_partner_category",
            "business_partner_full_name", "business_partner_grouping",
            "business_partner_name", "creation_date", "business_partner_is_blocked",
            "is_marked_for_archiving",
        ],
        "sales_order_headers": [
            "sales_order", "sales_order_type", "sales_organization",
            "distribution_channel", "sold_to_party", "creation_date",
            "total_net_amount", "overall_delivery_status",
            "overall_ord_reltd_billg_status", "transaction_currency",
            "requested_delivery_date", "delivery_block_reason",
        ],
        "sales_order_items": [
            "sales_order", "sales_order_item", "material",
            "requested_quantity", "net_amount", "transaction_currency",
            "material_group", "production_plant",
        ],
        "outbound_delivery_headers": [
            "delivery_document", "actual_goods_movement_date",
            "creation_date", "overall_goods_movement_status",
            "overall_picking_status", "shipping_point",
        ],
        "outbound_delivery_items": [
            "delivery_document", "delivery_document_item",
            "actual_delivery_quantity", "plant",
            "reference_sd_document", "reference_sd_document_item",
        ],
        "billing_document_headers": [
            "billing_document", "billing_document_type", "creation_date",
            "billing_document_date", "billing_document_is_cancelled",
            "total_net_amount", "transaction_currency",
            "accounting_document", "sold_to_party",
        ],
        "billing_document_items": [
            "billing_document", "billing_document_item", "material",
            "billing_quantity", "net_amount", "transaction_currency",
            "reference_sd_document",
        ],
        "payments_accounts_receivable": [
            "company_code", "fiscal_year", "accounting_document",
            "accounting_document_item", "clearing_date",
            "clearing_accounting_document", "amount_in_transaction_currency",
            "transaction_currency", "customer", "invoice_reference",
            "sales_document", "posting_date",
        ],
        "products": [
            "product", "product_type", "creation_date", "product_group",
            "base_unit", "division", "industry_sector",
        ],
        "product_descriptions": [
            "product", "language", "product_description",
        ],
    }
