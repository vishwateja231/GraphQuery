"""Converts SQL rows into a stable graph payload: {nodes: [], edges: []}."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

SUPPORTED_TYPES: Tuple[str, ...] = (
    "customer",
    "order",
    "delivery",
    "invoice",
    "payment",
    "product",
)

RELATION_LABELS: Dict[Tuple[str, str], str] = {
    ("customer", "order"): "placed",
    ("order", "delivery"): "fulfilled by",
    ("delivery", "invoice"): "billed via",
    ("order", "invoice"): "billed via",
    ("invoice", "payment"): "paid by",
    ("order", "product"): "contains",
}


def _node_id(entity_type: str, entity_value: Any) -> str:
    return f"{entity_type}_{entity_value}"


def build_graph(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    graph: Dict[str, List[Dict[str, Any]]] = {"nodes": [], "edges": []}
    if not rows:
        return graph

    unique_nodes: Dict[str, Dict[str, Any]] = {}
    unique_edges: Dict[str, Dict[str, Any]] = {}

    def add_node(entity_type: str, entity_value: Any, row: Dict[str, Any]) -> None:
        if entity_value in (None, ""):
            return
        node_id = _node_id(entity_type, entity_value)
        if node_id in unique_nodes:
            return
        label = f"{entity_type.title()} {entity_value}"
        unique_nodes[node_id] = {
            "id": node_id,
            "type": entity_type,
            "data": {
                "id": entity_value,
                "label": label,
                "type": entity_type,
                **row,
            },
        }

    def add_edge(source_type: str, source_value: Any, target_type: str, target_value: Any) -> None:
        if source_value in (None, "") or target_value in (None, ""):
            return
        source_id = _node_id(source_type, source_value)
        target_id = _node_id(target_type, target_value)
        if source_id not in unique_nodes or target_id not in unique_nodes:
            return
        label = RELATION_LABELS.get((source_type, target_type), "related")
        edge_id = f"e_{source_id}_{target_id}"
        if edge_id in unique_edges:
            return
        unique_edges[edge_id] = {
            "id": edge_id,
            "source": source_id,
            "target": target_id,
            "label": label,
        }

    # Map of generic node types to the possible SAP column names that contain their IDs
    ENTITY_COLUMNS: Dict[str, List[str]] = {
        "customer": ["customer", "business_partner", "sold_to_party"],
        "order":    ["sales_order"],
        "delivery": ["delivery_document"],
        "invoice":  ["billing_document", "invoice_reference"],
        "payment":  ["accounting_document"],
        "product":  ["product", "material"],
    }

    for row in rows:
        entity_values: Dict[str, Any] = {}
        
        # 1. First extract generic entity IDs from the row using SAP column names
        for entity_type, possible_cols in ENTITY_COLUMNS.items():
            # also check the legacy *_id name just in case Aliases were used
            possible_cols.append(f"{entity_type}_id") 
            
            for col in possible_cols:
                if col in row and row[col] not in (None, ""):
                    entity_values[entity_type] = str(row[col])
                    break # Found the ID for this entity type in this row

        # 2. Add nodes for any entities found in this row
        for entity_type, entity_id in entity_values.items():
            add_node(entity_type, entity_id, row)

        # 3. Canonical O2C chain edges when IDs are present.
        add_edge("customer", entity_values.get("customer"), "order", entity_values.get("order"))
        add_edge("order", entity_values.get("order"), "delivery", entity_values.get("delivery"))
        add_edge("delivery", entity_values.get("delivery"), "invoice", entity_values.get("invoice"))
        add_edge("order", entity_values.get("order"), "invoice", entity_values.get("invoice"))
        add_edge("invoice", entity_values.get("invoice"), "payment", entity_values.get("payment"))
        add_edge("order", entity_values.get("order"), "product", entity_values.get("product"))

    graph["nodes"] = [node for node in unique_nodes.values()]
    graph["edges"] = [edge for edge in unique_edges.values()]
    return graph
