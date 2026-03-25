"""
schema_enforcer.py — Strict schema enforcement for LLM SQL generation.

Responsibilities:
  1. Load & cache schema from schema.json (or live DB at startup)
  2. Auto-correct common wrong column names before validation
  3. Validate every table/column used in a SQL query
  4. Build a strict schema prompt string for the LLM
  5. Provide retry context when SQL execution fails

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

# ── Semantic column alias mapping (what LLMs like to hallucinate → real col) ──
COLUMN_ALIAS_MAP: Dict[str, str] = {
    # Customer / partner
    "customer_id":          "customer",          # business_partners.customer
    "partner_id":           "business_partner",
    "partner":              "business_partner",
    "customer_name":        "business_partner_full_name",
    "name":                 "business_partner_full_name",
    "full_name":            "business_partner_full_name",

    # Orders
    "order_id":             "sales_order",
    "order_number":         "sales_order",
    "sales_order_number":   "sales_order",
    "order_date":           "creation_date",
    "total_amount":         "total_net_amount",
    "currency":             "transaction_currency",
    "delivery_status":      "overall_delivery_status",
    "billing_status":       "overall_ord_reltd_billg_status",
    "customer_ref":         "sold_to_party",

    # Deliveries
    "delivery_id":          "delivery_document",
    "delivery_number":      "delivery_document",
    "ship_date":            "actual_goods_movement_date",
    "picking_status":       "overall_picking_status",
    "goods_status":         "overall_goods_movement_status",

    # Billing / Invoices
    "invoice_id":           "billing_document",
    "invoice_number":       "billing_document",
    "invoice_date":         "billing_document_date",
    "invoice_amount":       "total_net_amount",
    "invoice_type":         "billing_document_type",
    "is_cancelled":         "billing_document_is_cancelled",
    "accounting_doc":       "accounting_document",

    # Payments
    "payment_id":           "accounting_document",
    "payment_date":         "clearing_date",
    "payment_amount":       "amount_in_transaction_currency",
    "amount":               "amount_in_transaction_currency",
    "payment_doc":          "clearing_accounting_document",

    # Products
    "product_id":           "product",
    "product_name":         "product_description",  # join product_descriptions
    "material_id":          "material",

    # Misc
    "created_date":         "creation_date",
    "last_modified":        "last_change_date",
}

# ── Table alias map (wrong table name → correct SAP table) ────────────────────
TABLE_ALIAS_MAP: Dict[str, str] = {
    "customers":     "business_partners",
    "orders":        "sales_order_headers",
    "order_items":   "sales_order_items",
    "invoices":      "billing_document_headers",
    "payments":      "payments_accounts_receivable",
    "deliveries":    "outbound_delivery_headers",
    "delivery_items":"outbound_delivery_items",
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
    """Build the MAPPING section injected into the LLM prompt."""
    lines = ["COLUMN NAME MAPPING (use the RIGHT side only):"]
    for wrong, right in sorted(COLUMN_ALIAS_MAP.items()):
        lines.append(f"  {wrong} → {right}")
    return "\n".join(lines)


# ── Auto-correct ───────────────────────────────────────────────────────────────

def autocorrect_sql(sql: str) -> Tuple[str, List[str]]:
    """
    Replace known wrong table and column names with their SAP equivalents.
    Returns (corrected_sql, list_of_changes_made).
    Uses whole-word regex to avoid partial replacements.
    """
    changes: List[str] = []

    # 1. Table corrections (do tables first so column rename targets are right)
    for wrong_table, right_table in TABLE_ALIAS_MAP.items():
        pattern = re.compile(r"\b" + re.escape(wrong_table) + r"\b", re.IGNORECASE)
        if pattern.search(sql):
            sql = pattern.sub(right_table, sql)
            changes.append(f"table: {wrong_table} → {right_table}")

    # 2. Column corrections
    for wrong_col, right_col in COLUMN_ALIAS_MAP.items():
        pattern = re.compile(r"\b" + re.escape(wrong_col) + r"\b", re.IGNORECASE)
        if pattern.search(sql):
            sql = pattern.sub(right_col, sql)
            changes.append(f"col: {wrong_col} → {right_col}")

    if changes:
        logger.info("[AUTOCORRECT] Applied %d corrections: %s", len(changes), ", ".join(changes[:6]))

    return sql, changes


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

_ALLOWED_SHORT_TOKENS = {"bp", "soh", "soi", "bdh", "bdi", "par", "od", "odi", "pr"}


def validate_sql_columns(sql: str) -> Tuple[bool, List[str]]:
    """
    Check every non-reserved identifier in `sql` against the loaded schema.
    Returns (is_valid, list_of_bad_identifiers).

    Validation is intentionally lenient for aliases and table-qualified refs
    (t.column notation) — we only fail if the identifier is completely unknown
    across ALL tables AND is longer than 3 chars (avoids alias false-positives).
    """
    schema = load_schema()
    valid_tables = set(schema.keys())
    valid_columns: Set[str] = set()
    for cols in schema.values():
        valid_columns.update(cols)

    # Strip string literals so we don't check values
    cleaned = re.sub(r"'[^']*'", "''", sql)
    # Strip table.column qualified refs — we trust the table part separately
    cleaned = re.sub(r"\b[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\b", "qualified_ref", cleaned.lower())

    tokens = set(re.findall(r"\b([a-z_][a-z0-9_]{2,})\b", cleaned))
    tokens -= _SQL_RESERVED
    tokens -= _ALLOWED_SHORT_TOKENS

    bad: List[str] = []
    for tok in tokens:
        if tok in valid_tables or tok in valid_columns or tok == "qualified_ref":
            continue
        # Allow tokens that look like aliases (short, non-column-like)
        if len(tok) <= 4:
            continue
        bad.append(tok)

    return (len(bad) == 0), list(bad)


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
