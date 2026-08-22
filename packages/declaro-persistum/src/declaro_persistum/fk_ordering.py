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
from collections.abc import Callable
from typing import Any

from declaro_persistum.database import Database, writing
from declaro_persistum.types import Column, Schema, Table

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure functions — no I/O, no state
# ---------------------------------------------------------------------------


def _build_fk_graph(schema: Schema) -> dict[str, set[str]]:
    """Extract FK dependency graph from schema (pure).

    Returns dict mapping each table to the set of tables it depends on
    (i.e., tables it references via foreign keys).
    """
    # A SELF-REFERENCE IS NOT A DEPENDENCY: a tree whose parent_id points at
    # its own table does not wait for itself.
    return {
        table_name: {
            ref.split(".")[0]
            for col_def in table_def.get("columns", {}).values()
            for ref in [col_def.get("references") or ""]
            if "." in ref
            and ref.split(".")[0] in schema
            and ref.split(".")[0] != table_name
        }
        for table_name, table_def in schema.items()
    }


def _refuse_cycle(remaining: set[str]) -> set[str]:
    """Nothing is ready and tables remain, so they wait on each other.

    Everything still remaining is either in a cycle or queued behind one,
    which includes a table naming a parent the graph does not hold: nothing
    ever satisfies it. A table that sorted cleanly has already gone, so it is
    not named here.
    """
    raise ValueError(f"Circular FK dependencies: {remaining}")


def _the_ready_tables(remaining: set[str]) -> set[str]:
    """Something is ready, so the remaining set is handed back unchanged."""
    return remaining


_WHEN_READY: dict[bool, Callable[[set[str]], set[str]]] = {
    True: _the_ready_tables,
    False: _refuse_cycle,
}


def _toposort(deps: dict[str, set[str]]) -> list[str]:
    """Kahn's algorithm, written as set algebra (pure).

    Returns table names in dependency order (no-deps first).
    Raises ValueError on circular FK dependencies.

    Each round takes the ready set — every remaining table whose parents have
    all been placed — and places the alphabetically first member of it. That
    is the same choice the old queue made, which re-sorted after every pop.

    A PARENT THE GRAPH DOES NOT HOLD IS NEVER PLACED, so a table naming one
    can never be ready and the batch is refused. The old in-degree count said
    the same thing by never reaching zero.
    """
    remaining = set(deps)
    order: list[str] = []

    while remaining:
        placed = set(deps) - remaining
        ready = sorted(t for t in remaining if deps[t] <= placed)
        _WHEN_READY[bool(ready)](remaining)
        order.append(ready[0])
        remaining.discard(ready[0])

    return order


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
    FK constraints, avoiding replication engine replay-order violations.
    """
    stripped: Schema = {}
    for table_name, table_def in schema.items():
        new_columns: dict[str, Column] = {}
        for col_name, col_def in table_def.get("columns", {}).items():
            # NAMED, NOT FILTERED. This was a comprehension testing each key
            # against a tuple, which produced a plain dict[str, object] that
            # only a `# type: ignore` would let into a Column. Naming the three
            # FK keys says the same thing to the reader and to mypy, and the
            # escape goes with it.
            new_col: Column = {**col_def}
            new_col.pop("references", None)
            new_col.pop("on_delete", None)
            new_col.pop("on_update", None)
            new_columns[col_name] = new_col
        new_table: Table = {**table_def}
        new_table["columns"] = new_columns
        stripped[table_name] = new_table
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


def insert_statement(table: str, data: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """Every key becomes a column; every value binds positionally."""
    cols = ", ".join(f'"{k}"' for k in data)
    placeholders = ", ".join("?" for _ in data)
    return (
        f'INSERT INTO "{table}" ({cols}) VALUES ({placeholders})',
        tuple(data.values()),
    )


def delete_statement(table: str, data: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """Every key is a filter, ANDed together."""
    wheres = " AND ".join(f'"{k}" = ?' for k in data)
    return f'DELETE FROM "{table}" WHERE {wheres}', tuple(data.values())


def update_statement(table: str, data: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
    """`_where` is the filter; everything else is the assignment.

    Set values come before where values because the placeholders bind by
    position, and SET is written before WHERE.
    """
    where = data.get("_where", {})
    updates = {k: v for k, v in data.items() if k != "_where"}
    set_clause = ", ".join(f'"{k}" = ?' for k in updates)
    where_clause = " AND ".join(f'"{k}" = ?' for k in where)
    return (
        f'UPDATE "{table}" SET {set_clause} WHERE {where_clause}',
        tuple(updates.values()) + tuple(where.values()),
    )


# The SQL half of `execute_fk_ordered`, lifted out of it.
#
# It was an if/elif/else on op_type inside the write loop (Rule 1), which meant
# the only way to assert on the SQL was to drive the whole function through a
# recording connection and read what it issued. That mock was the cost of the
# mixing, not a fact about the code (Rule 4, then Rule 10). These take data and
# return data, so the tests are `assert f(input) == expected`.
STATEMENTS: dict[str, Callable[[str, dict[str, Any]], tuple[str, tuple[Any, ...]]]] = {
    "insert": insert_statement,
    "delete": delete_statement,
    "update": update_statement,
}


def _refuse_unknown_ops(unknown: list[str], _ops: list[DMLOp]) -> list[DMLOp]:
    """`STATEMENTS` holds no SQL for these, so the batch is refused."""
    raise ValueError(f"Unknown op_type: {', '.join(unknown)}")


def _the_whole_batch(_unknown: list[str], ops: list[DMLOp]) -> list[DMLOp]:
    """Every op_type has SQL, so the batch is handed back unchanged."""
    return ops


_WHEN_UNKNOWN_OPS: dict[bool, Callable[[list[str], list[DMLOp]], list[DMLOp]]] = {
    True: _refuse_unknown_ops,
    False: _the_whole_batch,
}


async def _execute_one(db: Database, table_name: str, op_type: str, data: dict[str, Any]) -> int:
    """One statement, on its own write connection, committed before returning."""
    sql, params = STATEMENTS[op_type](table_name, data)
    async with writing(db) as conn:
        cursor = await conn.execute(sql, params)
        # COMMIT, WHICH THIS DID NOT DO. `writing(db)` closes the connection
        # when the block ends and does not commit, so every operation here was
        # rolled back on the way out — while `cursor.rowcount` still reported
        # 1 and this function still returned a list of successes. Measured
        # 2026-08-13 against a real SQLite file: rowcount 1, zero rows in the
        # table.
        #
        # The old tests could not see it. They drove this through a recording
        # connection that had no transaction to discard.
        await conn.commit()
        rowcount: int = cursor.rowcount
    logger.debug("FK-ordered %s on %s: %d rows", op_type, table_name, rowcount)
    return rowcount


async def execute_fk_ordered(
    db: Database,
    schema: Schema,
    ops: list[DMLOp],
) -> list[int]:
    """Execute DML operations in FK-safe order with push after each.

    Sorts ops by FK dependencies, executes each through the write
    connection, and pushes to cloud after each operation.

    Args:
        db: The database
        schema: Schema with FK relationships
        ops: List of (table_name, "insert"|"delete"|"update", data) tuples

    Returns:
        List of rowcounts for each operation (in sorted order)

    Raises:
        ValueError: when any op names a type `STATEMENTS` has no SQL for. The
            whole batch is read first, so an unknown type at the end refuses
            the batch before anything is written. The old check sat inside the
            write loop and raised on reaching the bad op, which left every
            operation before it committed.
    """
    unknown = sorted({op_type for _, op_type, _ in ops if op_type not in STATEMENTS})
    _WHEN_UNKNOWN_OPS[bool(unknown)](unknown, ops)

    return [
        await _execute_one(db, table_name, op_type, data)
        for table_name, op_type, data in sort_operations(schema, ops)
    ]
