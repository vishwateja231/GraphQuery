"""Builds a data-driven graph payload from SQL rows: {nodes: [], edges: []}."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

ENTITY_COLUMN_MAP: Dict[str, Tuple[str, ...]] = {
    "customer": ("customer_id", "customer", "sold_to_party", "business_partner"),
    "order": ("order_id", "sales_order"),
    "delivery": ("delivery_id", "delivery_document"),
    "invoice": ("invoice_id", "billing_document", "invoice_reference"),
    "payment": ("payment_id", "accounting_document", "clearing_accounting_document"),
    "product": ("product_id", "product", "material"),
}


def _normalize_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _make_node(entity_type: str, entity_value: str, row: Dict[str, Any]) -> Dict[str, Any]:
    node_id = f"{entity_type}_{entity_value}"
    return {
        "id": node_id,
        "type": entity_type,
        "data": {
            "id": entity_value,
            "type": entity_type,
            "label": f"{entity_type.title()} {entity_value}",
            "context": row,
        },
    }


def _extract_entities_in_row(row: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Extract entities in row column order. Returns [(entity_type, entity_value), ...]."""
    column_to_entity: Dict[str, str] = {}
    for entity_type, columns in ENTITY_COLUMN_MAP.items():
        for col in columns:
            column_to_entity[col] = entity_type

    entities: List[Tuple[str, str]] = []
    seen_in_row = set()

    for col_name, raw_value in row.items():
        col_lower = str(col_name).lower()
        entity_type = column_to_entity.get(col_lower)
        if not entity_type:
            continue
        entity_value = _normalize_value(raw_value)
        if not entity_value:
            continue
        entity_id = f"{entity_type}_{entity_value}"
        if entity_id in seen_in_row:
            continue
        seen_in_row.add(entity_id)
        entities.append((entity_type, entity_value))

    return entities


def build_graph(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    if not rows:
        return {"nodes": [], "edges": []}

    unique_nodes: Dict[str, Dict[str, Any]] = {}
    unique_edges: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        entities = _extract_entities_in_row(row)

        for entity_type, entity_value in entities:
            node_id = f"{entity_type}_{entity_value}"
            if node_id not in unique_nodes:
                unique_nodes[node_id] = _make_node(entity_type, entity_value, row)

        # Build relationships strictly from same-row entities (in row order).
        if len(entities) >= 2:
            for (source_type, source_value), (target_type, target_value) in zip(entities, entities[1:]):
                source_id = f"{source_type}_{source_value}"
                target_id = f"{target_type}_{target_value}"
                if source_id == target_id:
                    continue
                edge_id = f"e_{source_id}_{target_id}"
                if edge_id in unique_edges:
                    continue
                unique_edges[edge_id] = {
                    "id": edge_id,
                    "source": source_id,
                    "target": target_id,
                    "label": "related",
                }

    if not unique_nodes and rows:
        first_row = rows[0]
        fallback_id = "row_0"
        unique_nodes[fallback_id] = {
            "id": fallback_id,
            "type": "record",
            "data": {
                "id": fallback_id,
                "type": "record",
                "label": "Result Row",
                "context": first_row,
            },
        }

    return {
        "nodes": list(unique_nodes.values()),
        "edges": list(unique_edges.values()),
    }
