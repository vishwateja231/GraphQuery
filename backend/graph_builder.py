"""Row-true graph builder: builds nodes/edges strictly from each row's own values."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple


def _norm(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _merge_context(existing: Dict[str, Any], row: Dict[str, Any]) -> Dict[str, Any]:
    """Merge only missing/empty fields; never overwrite non-empty existing values."""
    merged = dict(existing)
    for key, value in row.items():
        if key not in merged or merged[key] in (None, "", []):
            merged[key] = value
    return merged


def _upsert_node(
    nodes_map: Dict[str, Dict[str, Any]],
    entity_type: str,
    entity_id: str,
    row: Dict[str, Any],
) -> str:
    node_id = f"{entity_type}_{entity_id}"
    new_data = {
        "id": entity_id,
        "type": entity_type,
        "label": f"{entity_type.title()} {entity_id}",
        "context": row,
    }

    if node_id not in nodes_map:
        nodes_map[node_id] = {
            "id": node_id,
            "type": entity_type,
            "data": new_data,
        }
    else:
        existing_data = nodes_map[node_id].get("data", {})
        existing_context = existing_data.get("context", {})
        merged_context = _merge_context(existing_context if isinstance(existing_context, dict) else {}, row)
        nodes_map[node_id]["data"] = {
            **existing_data,
            "context": merged_context,
            "id": existing_data.get("id") or entity_id,
            "type": existing_data.get("type") or entity_type,
            "label": existing_data.get("label") or f"{entity_type.title()} {entity_id}",
        }

    return node_id


def _add_edge(
    edges_set: Set[Tuple[str, str]],
    edges_map: Dict[str, Dict[str, Any]],
    source_id: Optional[str],
    target_id: Optional[str],
) -> None:
    if not source_id or not target_id or source_id == target_id:
        return
    pair = (source_id, target_id)
    if pair in edges_set:
        return
    edges_set.add(pair)
    edge_id = f"e_{source_id}_{target_id}"
    edges_map[edge_id] = {
        "id": edge_id,
        "source": source_id,
        "target": target_id,
        "label": "related",
    }
    print("CREATED EDGE:", source_id, "→", target_id)


def build_graph(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    if not rows:
        return {"nodes": [], "edges": []}

    nodes_map: Dict[str, Dict[str, Any]] = {}
    edges_map: Dict[str, Dict[str, Any]] = {}
    edges_set: Set[Tuple[str, str]] = set()

    for row in rows:
        print("ROW:", row)

        # STEP 1: Strict row-level extraction (this row only).
        customer_id = _norm(row.get("customer_id")) or _norm(row.get("business_partner")) or _norm(row.get("customer"))
        order_id = _norm(row.get("order_id")) or _norm(row.get("sales_order"))
        invoice_id = _norm(row.get("invoice_id")) or _norm(row.get("billing_document"))
        delivery_id = _norm(row.get("delivery_id")) or _norm(row.get("delivery_document"))
        product_id = _norm(row.get("product_id")) or _norm(row.get("product")) or _norm(row.get("material"))

        customer_node_id = _upsert_node(nodes_map, "customer", customer_id, row) if customer_id else None
        order_node_id = _upsert_node(nodes_map, "order", order_id, row) if order_id else None
        invoice_node_id = _upsert_node(nodes_map, "invoice", invoice_id, row) if invoice_id else None
        delivery_node_id = _upsert_node(nodes_map, "delivery", delivery_id, row) if delivery_id else None
        product_node_id = _upsert_node(nodes_map, "product", product_id, row) if product_id else None

        # STEP 3: Edges only from same row.
        _add_edge(edges_set, edges_map, customer_node_id, order_node_id)
        _add_edge(edges_set, edges_map, order_node_id, invoice_node_id)
        _add_edge(edges_set, edges_map, order_node_id, delivery_node_id)
        _add_edge(edges_set, edges_map, order_node_id, product_node_id)

    if not nodes_map and rows:
        first_row = rows[0]
        fallback_id = "record_0"
        nodes_map[fallback_id] = {
            "id": fallback_id,
            "type": "record",
            "data": {
                "id": fallback_id,
                "type": "record",
                "label": "Result Row",
                "context": first_row,
            },
        }

    return {"nodes": list(nodes_map.values()), "edges": list(edges_map.values())}
