"""
FK dependency ordering for DML operations.

Pure functions that analyze a schema's foreign key relationships and
return table orderings that respect referential integrity:

- Insert order: parents before children
- Delete order: children before parents

Also provides:
- strip_foreign_keys(): remove FKs from schema for cloud tables
- sort_operations(): reorder a batch of DML ops by FK dependencies
- execute_fk_ordered(): explicit batch execution in FK-safe order
"""

import logging
from typing import Any

from declaro_persistum.types import Schema

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure functions — no I/O, no state
# ---------------------------------------------------------------------------


def _build_fk_graph(schema: Schema) -> dict[str, set[str]]:
    """Extract FK dependency graph from schema (pure).

    Returns dict mapping each table to the set of tables it depends on
    (i.e., tables it references via foreign keys).
    """
    deps: dict[str, set[str]] = {table: set() for table in schema}

    for table_name, table_def in schema.items():
        for col_def in table_def.get("columns", {}).values():
            ref = col_def.get("references")
            if ref and "." in ref:
                ref_table = ref.split(".")[0]
                if ref_table in schema and ref_table != table_name:
                    deps[table_name].add(ref_table)

    return deps


def _toposort(deps: dict[str, set[str]]) -> list[str]:
    """Kahn's algorithm for topological sort (pure).

    Returns table names in dependency order (no-deps first).
    Raises ValueError on circular FK dependencies.
    """
    in_degree = {t: len(parents) for t, parents in deps.items()}

    reverse: dict[str, list[str]] = {t: [] for t in deps}
    for table, parents in deps.items():
        for parent in parents:
            if parent in reverse:
                reverse[parent].append(table)

    queue = sorted(t for t, deg in in_degree.items() if deg == 0)
    result: list[str] = []

    while queue:
        table = queue.pop(0)
        result.append(table)
        for child in sorted(reverse.get(table, [])):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
        queue.sort()

    if len(result) != len(deps):
        missing = set(deps) - set(result)
        raise ValueError(f"Circular FK dependencies: {missing}")

    return result


def fk_insert_order(schema: Schema) -> list[str]:
    """Return tables in FK-safe insert order (parents first).

    Uses Kahn's algorithm.  Tables with no FK dependencies come first;
    tables that reference other tables come after their parents.
    """
    return _toposort(_build_fk_graph(schema))


def fk_delete_order(schema: Schema) -> list[str]:
    """Return tables in FK-safe delete order (children first)."""
    return list(reversed(fk_insert_order(schema)))


def strip_foreign_keys(schema: Schema) -> Schema:
    """Return a copy of the schema with all FK constraints removed.

    Used by migrate-remote --no-fks to create cloud tables without
    FK constraints, avoiding sync engine replay-order violations.
    """
    stripped: Schema = {}
    for table_name, table_def in schema.items():
        new_columns = {}
        for col_name, col_def in table_def.get("columns", {}).items():
            new_col = {k: v for k, v in col_def.items()
                       if k not in ("references", "on_delete", "on_update")}
            new_columns[col_name] = new_col  # type: ignore
        new_table = {**table_def, "columns": new_columns}
        stripped[table_name] = new_table  # type: ignore
    return stripped


# Type for batch operations: (table_name, "insert"|"delete", data_dict)
DMLOp = tuple[str, str, dict[str, Any]]


def sort_operations(schema: Schema, ops: list[DMLOp]) -> list[DMLOp]:
    """Sort DML operations by FK dependencies (pure).

    Inserts are ordered parents-first.  Deletes are ordered children-first.
    Mixed batches: all deletes (children-first), then all inserts (parents-first).

    Operations on the same table preserve their original relative order.
    Tables not in the schema are placed at the end.
    """
    insert_order = fk_insert_order(schema)
    delete_order = fk_delete_order(schema)

    # Build priority lookup: table_name -> position
    insert_priority = {t: i for i, t in enumerate(insert_order)}
    delete_priority = {t: i for i, t in enumerate(delete_order)}
    fallback = len(insert_order)

    deletes = [(i, op) for i, op in enumerate(ops) if op[1] == "delete"]
    inserts = [(i, op) for i, op in enumerate(ops) if op[1] != "delete"]

    # Stable sort: by FK priority, then by original index (preserves order within table)
    sorted_deletes = sorted(deletes, key=lambda x: (delete_priority.get(x[1][0], fallback), x[0]))
    sorted_inserts = sorted(inserts, key=lambda x: (insert_priority.get(x[1][0], fallback), x[0]))

    return [op for _, op in sorted_deletes] + [op for _, op in sorted_inserts]


# ---------------------------------------------------------------------------
# I/O boundary — execute_fk_ordered
# ---------------------------------------------------------------------------


async def execute_fk_ordered(
    pool: Any,
    schema: Schema,
    ops: list[DMLOp],
) -> list[int]:
    """Execute DML operations in FK-safe order with push after each.

    Sorts ops by FK dependencies, executes each via the pool's write
    connection, and pushes to cloud after each operation.

    Args:
        pool: TursoPool (or any pool with acquire_write)
        schema: Schema with FK relationships
        ops: List of (table_name, "insert"|"delete"|"update", data) tuples

    Returns:
        List of rowcounts for each operation (in sorted order)
    """
    sorted_ops = sort_operations(schema, ops)
    rowcounts: list[int] = []

    for table_name, op_type, data in sorted_ops:
        async with pool.acquire_write() as conn:
            if op_type == "insert":
                cols = ", ".join(f'"{k}"' for k in data)
                placeholders = ", ".join("?" for _ in data)
                sql = f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})'
                params = tuple(data.values())
            elif op_type == "delete":
                wheres = " AND ".join(f'"{k}" = ?' for k in data)
                sql = f'DELETE FROM "{table_name}" WHERE {wheres}'
                params = tuple(data.values())
            elif op_type == "update":
                # data must have "_where" key with filter dict
                where = data.get("_where", {})
                updates = {k: v for k, v in data.items() if k != "_where"}
                set_clause = ", ".join(f'"{k}" = ?' for k in updates)
                where_clause = " AND ".join(f'"{k}" = ?' for k in where)
                sql = f'UPDATE "{table_name}" SET {set_clause} WHERE {where_clause}'
                params = tuple(updates.values()) + tuple(where.values())
            else:
                raise ValueError(f"Unknown op_type: {op_type}")

            cursor = await conn.execute(sql, params)
            rowcounts.append(cursor.rowcount)
            logger.debug("FK-ordered %s on %s: %d rows", op_type, table_name, cursor.rowcount)

    return rowcounts
