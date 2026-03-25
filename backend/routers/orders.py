from fastapi import APIRouter, Query
from database import query as db_query

router = APIRouter()


@router.get("/")
def list_orders(limit: int = Query(50), offset: int = Query(0)):
    sql = """
    SELECT o.order_id, o.customer_id, c.name AS customer_name,
           o.order_date, o.total_amount, o.currency,
           o.delivery_status, o.process_status
    FROM orders o
    LEFT JOIN customers c ON c.customer_id = o.customer_id
    ORDER BY o.order_date DESC
    LIMIT ? OFFSET ?
    """
    return db_query(sql, [limit, offset])


@router.get("/incomplete")
def incomplete_orders(
    stage: str = Query("delivery",
                       description="Stage to check: delivery | billing | payment")
):
    """Find orders that are stuck at a specific stage."""
    if stage == "delivery":
        sql = """
        SELECT o.order_id, c.name AS customer_name,
               o.order_date, o.total_amount, o.delivery_status
        FROM orders o
        LEFT JOIN customers  c ON c.customer_id = o.customer_id
        LEFT JOIN deliveries d ON d.order_id    = o.order_id
        WHERE d.delivery_id IS NULL
        ORDER BY o.order_date
        """
        params = []
    elif stage == "billing":
        sql = """
        SELECT o.order_id, o.total_amount, d.delivery_id, d.ship_date
        FROM orders o
        JOIN  deliveries d ON d.order_id   = o.order_id AND d.goods_status = 'C'
        LEFT JOIN invoices i ON i.order_id = o.order_id
        WHERE i.invoice_id IS NULL
        """
        params = []
    elif stage == "payment":
        sql = """
        SELECT i.invoice_id, c.name AS customer_name,
               i.invoice_date, i.total_amount
        FROM invoices i
        JOIN  customers c ON c.customer_id = i.customer_id
        LEFT JOIN payments p ON p.customer_id = i.customer_id
                            AND p.is_incoming = 1
        WHERE p.payment_id IS NULL AND i.is_cancelled = 0
        ORDER BY i.invoice_date
        """
        params = []
    else:
        return {"error": "stage must be: delivery | billing | payment"}

    return db_query(sql, params)


# NOTE: specific sub-routes /{order_id}/flow and /{order_id}/items must come
# BEFORE the generic /{order_id} catch-all, otherwise FastAPI routes them wrong.

@router.get("/{order_id}/flow/")
def order_flow(order_id: str):
    """Full O2C trace for one order."""
    sql = """
    SELECT
        o.order_id,
        o.customer_id,
        c.name                  AS customer_name,
        o.total_amount          AS order_amount,
        o.delivery_status,
        o.requested_delivery_date,
        d.delivery_id,
        d.ship_date,
        d.goods_status,
        d.picking_status,
        i.invoice_id,
        i.invoice_date,
        i.total_amount          AS invoice_amount,
        i.is_cancelled,
        p.payment_id,
        p.clearing_date,
        p.amount                AS payment_amount
    FROM orders o
    LEFT JOIN customers  c ON c.customer_id = o.customer_id
    LEFT JOIN deliveries d ON d.order_id    = o.order_id
    LEFT JOIN invoices   i ON i.order_id    = o.order_id
    LEFT JOIN payments   p ON p.customer_id = o.customer_id
                          AND p.is_incoming = 1
    WHERE o.order_id = ?
    """
    return db_query(sql, [order_id])


@router.get("/{order_id}/items/")
def order_items(order_id: str):
    sql = """
    SELECT oi.*, pr.product_name, pr.product_type, pr.old_sku
    FROM order_items oi
    LEFT JOIN products pr ON pr.product_id = oi.product_id
    WHERE oi.order_id = ?
    ORDER BY CAST(oi.line_no AS INTEGER)
    """
    return db_query(sql, [order_id])


@router.get("/{order_id}/")
def get_order(order_id: str):
    rows = db_query("SELECT * FROM orders WHERE order_id = ?", [order_id])
    return rows[0] if rows else {"error": "Not found"}
