"""
database.py — PostgreSQL connection helper with logging
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional

import psycopg  # pyre-ignore[21]
from psycopg.rows import dict_row  # pyre-ignore[21]
from psycopg_pool import ConnectionPool  # pyre-ignore[21]

from config import DATABASE_URL  # pyre-ignore[21]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

_pool: Optional[ConnectionPool] = None  # pyre-ignore[9]


def _to_postgres_placeholders(sql: str) -> str:
    """Allow legacy SQLite-style '?' placeholders."""
    return sql.replace("?", "%s")


def init_db_pool() -> None:
    """Initialize PostgreSQL connection pool once."""
    global _pool
    if _pool is not None:
        logger.info("[OK] Database pool already initialized")
        return
    if not DATABASE_URL:
        logger.error("[ERROR] DATABASE_URL is not set")
        raise RuntimeError("DATABASE_URL is required")

    logger.info("[INIT] Initializing PostgreSQL connection pool...")
    _pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=2,
        max_size=20,
        kwargs={"row_factory": dict_row, "prepare_threshold": None},
        open=False,
    )
    _pool.open()
    logger.info("[OK] PostgreSQL connection pool initialized successfully")
    ensure_core_indexes()


def close_db_pool() -> None:
    global _pool
    if _pool is not None:
        logger.info("[CLOSE] Closing PostgreSQL connection pool...")
        _pool.close()
        _pool = None
        logger.info("[OK] Connection pool closed")


def get_pool() -> ConnectionPool:  # pyre-ignore[11]
    if _pool is None:
        init_db_pool()
    assert _pool is not None
    return _pool


def fetch_all(sql: str, params: Any = None) -> List[Any]:
    """Execute SQL and return all results as plain Python dicts."""
    try:
        pool = get_pool()
        pg_sql = _to_postgres_placeholders(sql)
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(pg_sql, params or [])
                rows = cur.fetchall()
                logger.info("[DB] Query returned %d rows", len(rows))
                # Convert psycopg DictRow objects to plain dicts
                return [dict(r) for r in rows]
    except Exception as exc:
        logger.error("[DB] SQL execution error: %s", exc, exc_info=True)
        raise


def ensure_core_indexes() -> None:
    """Create critical indexes used by chat query patterns."""
    if _pool is None:
        return
    statements = [
        "CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_deliveries_order_id ON deliveries(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_invoices_order_id ON invoices(order_id)",
        "CREATE INDEX IF NOT EXISTS idx_invoices_customer_id ON invoices(customer_id)",
        "CREATE INDEX IF NOT EXISTS idx_payments_customer_id ON payments(customer_id)",
    ]
    try:
        with _pool.connection() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    try:
                        cur.execute(stmt)
                    except Exception:
                        conn.rollback()
            conn.commit()
    except Exception as exc:
        logger.warning("[WARN] Could not create indexes: %s", exc)


def get_conn() -> psycopg.Connection:  # pyre-ignore[11]
    return psycopg.connect(DATABASE_URL, row_factory=dict_row, prepare_threshold=None)


def query(sql: str, params: Any = None) -> List[Any]:
    conn = get_conn()
    result: List[Any] = []
    try:
        with conn.cursor() as cur:
            cur.execute(_to_postgres_placeholders(sql), params or [])
            result = [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
    return result