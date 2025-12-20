"""
Hierarchy abstraction using closure tables.

This provides O(1) hierarchy queries for ancestor/descendant lookups
by pre-computing all ancestor-descendant relationships.

Closure table pattern:
- Each node has a self-reference entry (ancestor=self, descendant=self, depth=0)
- Each parent-child relationship creates entries for all ancestors
- Depth indicates the distance between ancestor and descendant
"""

from typing import Any


def generate_closure_table(
    table_name: str,
    pk_column: str = "id",
) -> dict[str, Any]:
    """
    Generate closure table schema for hierarchical data.

    Args:
        table_name: Name of the main table (e.g., "categories")
        pk_column: Primary key column name (default: "id")

    Returns:
        Schema dict with closure table definition.
    """
    closure_name = f"{table_name}_closure"

    return {
        closure_name: {
            "columns": {
                "ancestor_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": f"{table_name}.{pk_column}",
                    "on_delete": "cascade",
                },
                "descendant_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": f"{table_name}.{pk_column}",
                    "on_delete": "cascade",
                },
                "depth": {
                    "type": "integer",
                    "nullable": False,
                    "default": 0,
                },
            },
            "primary_key": ["ancestor_id", "descendant_id"],
            "indexes": {
                f"idx_{closure_name}_ancestor": {
                    "columns": ["ancestor_id", "depth"],
                },
                f"idx_{closure_name}_descendant": {
                    "columns": ["descendant_id", "depth"],
                },
            },
        }
    }


def closure_insert_sql(table_name: str) -> str:
    """
    Generate SQL to insert a node with a parent.

    This creates:
    - Self-reference entry (node is its own ancestor at depth 0)
    - All ancestor entries (copying from parent's ancestors with depth + 1)

    Args:
        table_name: Name of the main table

    Returns:
        INSERT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""INSERT INTO {closure_name} (ancestor_id, descendant_id, depth)
SELECT ancestor_id, :node_id, depth + 1
FROM {closure_name}
WHERE descendant_id = :parent_id
UNION ALL
SELECT :node_id, :node_id, 0"""


def closure_insert_root_sql(table_name: str) -> str:
    """
    Generate SQL to insert a root node (no parent).

    Root nodes only have a self-reference entry.

    Args:
        table_name: Name of the main table

    Returns:
        INSERT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""INSERT INTO {closure_name} (ancestor_id, descendant_id, depth)
VALUES (:node_id, :node_id, 0)"""


def closure_update_parent_sql(table_name: str) -> str:
    """
    Generate SQL to move a node to a new parent.

    This requires:
    1. Delete old closure entries (except self-reference)
    2. Insert new closure entries from new parent

    Args:
        table_name: Name of the main table

    Returns:
        SQL statements for move operation.
    """
    closure_name = f"{table_name}_closure"

    # This is a multi-statement operation
    # First delete old paths (keeping self-reference)
    delete_sql = f"""DELETE FROM {closure_name}
WHERE descendant_id IN (
    SELECT descendant_id FROM {closure_name} WHERE ancestor_id = :node_id
)
AND ancestor_id IN (
    SELECT ancestor_id FROM {closure_name}
    WHERE descendant_id = :node_id AND ancestor_id != :node_id
)"""

    # Then insert new paths from new parent
    insert_sql = f"""INSERT INTO {closure_name} (ancestor_id, descendant_id, depth)
SELECT supertree.ancestor_id, subtree.descendant_id, supertree.depth + subtree.depth + 1
FROM {closure_name} AS supertree
CROSS JOIN {closure_name} AS subtree
WHERE supertree.descendant_id = :new_parent_id
AND subtree.ancestor_id = :node_id"""

    return f"{delete_sql};\n{insert_sql}"


def closure_delete_subtree_sql(table_name: str) -> str:
    """
    Generate SQL to delete a node and all descendants.

    The actual row deletions will cascade; this just handles closure entries.

    Args:
        table_name: Name of the main table

    Returns:
        DELETE SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""DELETE FROM {closure_name}
WHERE descendant_id IN (
    SELECT descendant_id FROM {closure_name} WHERE ancestor_id = :node_id
)"""


def descendants_query_sql(table_name: str) -> str:
    """
    Generate SQL to get all descendants of a node.

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT t.*
FROM {table_name} t
JOIN {closure_name} c ON t.id = c.descendant_id
WHERE c.ancestor_id = :node_id AND c.depth > 0"""


def descendants_at_depth_sql(table_name: str) -> str:
    """
    Generate SQL to get descendants at a specific depth.

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT t.*
FROM {table_name} t
JOIN {closure_name} c ON t.id = c.descendant_id
WHERE c.ancestor_id = :node_id AND c.depth = :depth"""


def ancestors_query_sql(table_name: str) -> str:
    """
    Generate SQL to get all ancestors of a node.

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT t.*
FROM {table_name} t
JOIN {closure_name} c ON t.id = c.ancestor_id
WHERE c.descendant_id = :node_id AND c.depth > 0"""


def path_query_sql(table_name: str) -> str:
    """
    Generate SQL to get path from root to node.

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement ordered by depth (root first).
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT t.*, c.depth
FROM {table_name} t
JOIN {closure_name} c ON t.id = c.ancestor_id
WHERE c.descendant_id = :node_id
ORDER BY c.depth DESC"""


def children_query_sql(table_name: str) -> str:
    """
    Generate SQL to get direct children (depth=1).

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT t.*
FROM {table_name} t
JOIN {closure_name} c ON t.id = c.descendant_id
WHERE c.ancestor_id = :node_id AND c.depth = 1"""


def parent_query_sql(table_name: str) -> str:
    """
    Generate SQL to get direct parent.

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT t.*
FROM {table_name} t
JOIN {closure_name} c ON t.id = c.ancestor_id
WHERE c.descendant_id = :node_id AND c.depth = 1"""


def is_descendant_sql(table_name: str) -> str:
    """
    Generate SQL to check if a node is a descendant of another.

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement returning boolean.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT EXISTS (
    SELECT 1 FROM {closure_name}
    WHERE ancestor_id = :ancestor_id
    AND descendant_id = :descendant_id
    AND depth > 0
) AS is_descendant"""


def subtree_count_sql(table_name: str) -> str:
    """
    Generate SQL to count descendants (including self).

    Args:
        table_name: Name of the main table

    Returns:
        SELECT SQL statement with COUNT.
    """
    closure_name = f"{table_name}_closure"

    return f"""SELECT COUNT(*) AS descendant_count
FROM {closure_name}
WHERE ancestor_id = :node_id"""


def build_tree(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    """
    Build tree structure from closure table data.

    Args:
        rows: List of dicts with id, ancestor_id, descendant_id, depth keys

    Returns:
        Dict mapping node IDs to node data with 'children' lists.
    """
    if not rows:
        return {}

    # Extract unique nodes
    nodes: dict[int, dict[str, Any]] = {}

    for row in rows:
        node_id = row.get("id") or row.get("descendant_id")
        if node_id is not None and node_id not in nodes:
            nodes[node_id] = {
                "id": node_id,
                "children": [],
                **{
                    k: v
                    for k, v in row.items()
                    if k not in ("ancestor_id", "descendant_id", "depth")
                },
            }

    # Build parent-child relationships from depth=1 entries
    for row in rows:
        if row.get("depth") == 1:
            parent_id = row.get("ancestor_id")
            child_id = row.get("descendant_id")
            if parent_id in nodes and child_id in nodes:
                nodes[parent_id]["children"].append(nodes[child_id])

    return nodes
