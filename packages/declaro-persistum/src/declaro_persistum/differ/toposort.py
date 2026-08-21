"""
Topological sorting for operation dependency ordering.

Ensures operations are executed in an order that respects dependencies.
For example, DROP FOREIGN KEY must happen before DROP TABLE.
"""

from collections.abc import Callable

from declaro_persistum.exceptions import CycleError
from declaro_persistum.types import Operation


def build_dependency_graph(operations: list[Operation]) -> dict[int, list[int]]:
    """
    Build a dependency graph for operations.

    Dependency rules:
    - DROP FOREIGN KEY: No dependencies (can always run first)
    - DROP INDEX: No dependencies
    - DROP COLUMN: Depends on DROP FOREIGN KEY if column is referenced
    - DROP COLUMN: Depends on DROP INDEX if column is in index
    - DROP TABLE: Depends on all DROP FOREIGN KEY that reference it
    - DROP TABLE: Depends on DROP VIEW for views that reference it
    - CREATE TABLE: Depends on CREATE TABLE for referenced tables
    - ADD COLUMN: Depends on CREATE TABLE
    - ADD FOREIGN KEY: Depends on CREATE TABLE and ADD COLUMN for referenced
    - ADD INDEX: Depends on ADD COLUMN for indexed columns
    - ALTER COLUMN: Depends on DROP INDEX/FK if type change
    - CREATE VIEW: Depends on CREATE TABLE and CREATE VIEW for referenced objects
    - DROP VIEW: Depends on DROP VIEW for views that reference it

    Args:
        operations: List of operations to order

    Returns:
        Dict mapping operation index to list of dependency indices
    """
    dependencies: dict[int, list[int]] = {i: [] for i in range(len(operations))}

    # Build indexes for quick lookup
    op_by_type: dict[str, list[int]] = {}
    for i, op in enumerate(operations):
        op_type = op["op"]
        if op_type not in op_by_type:
            op_by_type[op_type] = []
        op_by_type[op_type].append(i)

    # Tables being created
    created_tables: dict[str, int] = {}
    for i in op_by_type.get("create_table", []):
        created_tables[operations[i]["table"]] = i

    # Tables being dropped
    dropped_tables: dict[str, int] = {}
    for i in op_by_type.get("drop_table", []):
        dropped_tables[operations[i]["table"]] = i

    # Views being created
    created_views: dict[str, int] = {}
    for i in op_by_type.get("create_view", []):
        created_views[operations[i]["details"]["name"]] = i

    # Views being dropped
    dropped_views: dict[str, int] = {}
    for i in op_by_type.get("drop_view", []):
        dropped_views[operations[i]["details"]["name"]] = i

    # Foreign keys being dropped
    dropped_fks: list[tuple[int, str, str]] = []  # (op_idx, table, column)
    for i in op_by_type.get("drop_foreign_key", []):
        op = operations[i]
        dropped_fks.append((i, op["table"], op["details"].get("column", "")))

    # Indexes being dropped
    dropped_indexes: list[tuple[int, str, str]] = []  # (op_idx, table, index_name)
    for i in op_by_type.get("drop_index", []):
        op = operations[i]
        dropped_indexes.append((i, op["table"], op["details"].get("index", "")))

    # Process each operation type
    for i, op in enumerate(operations):
        deps = _get_operation_dependencies(
            i,
            op,
            operations,
            created_tables,
            created_views,
            dropped_fks,
            dropped_indexes,
        )
        dependencies[i] = deps

    return dependencies


def _handle_drop_table(
    op: Operation,
    op_idx: int,
    all_operations: list[Operation],
    dropped_fks: list[tuple[int, str, str]],
) -> list[int]:
    """Handle DROP TABLE dependencies."""
    table_name = op["table"]

    # Keys pointing at this table go before it.
    keys = [
        fk_idx
        for fk_idx, _fk_table, _ in dropped_fks
        if all_operations[fk_idx]["details"]
        .get("references", "")
        .startswith(f"{table_name}.")
    ]
    # Views selecting from it go before it.
    views = [
        idx
        for idx, other in enumerate(all_operations)
        if other["op"] == "drop_view"
        and table_name in other["details"].get("depends_on", [])
    ]
    # A TABLE THAT REFERENCES THIS ONE MUST GO FIRST. PostgreSQL refuses to
    # drop a table another still points at, so a plan that dropped the parent
    # first failed halfway. There is no drop_foreign_key operation to hang the
    # edge on when both tables are going: the key leaves with the table.
    parents = [
        idx
        for idx, other in enumerate(all_operations)
        if other["op"] == "drop_table"
        and idx != op_idx
        and table_name in (other["details"].get("references") or [])
    ]
    return keys + views + parents


def _handle_drop_column(
    op: Operation,
    all_operations: list[Operation],
    dropped_fks: list[tuple[int, str, str]],
    dropped_indexes: list[tuple[int, str, str]],
) -> list[int]:
    """Handle DROP COLUMN dependencies."""
    table_name = op["table"]
    col_name = op["details"].get("column", "")

    keys = [
        fk_idx
        for fk_idx, fk_table, fk_col in dropped_fks
        if (fk_table, fk_col) == (table_name, col_name)
    ]
    indexes = [
        idx_idx
        for idx_idx, idx_table, _ in dropped_indexes
        if idx_table == table_name
        and col_name
        in all_operations[idx_idx]["details"].get("definition", {}).get("columns", [])
    ]
    return keys + indexes


def _handle_create_table(
    op: Operation,
    created_tables: dict[str, int],
) -> list[int]:
    """Handle CREATE TABLE dependencies."""
    table_name = op["table"]

    # A SELF-EDGE IS NOT AN EDGE. `- {table_name}` is the whole of it: a tree
    # whose parent_id points at its own table does not wait for itself, and
    # without this the sort reports a cycle in a schema that has none.
    referenced = {
        col.get("references", "").split(".")[0]
        for col in op["details"].get("columns", {}).values()
        if col.get("references")
    } & set(created_tables) - {table_name}

    return [created_tables[name] for name in sorted(referenced)]


def _handle_add_column(
    op: Operation,
    created_tables: dict[str, int],
) -> list[int]:
    """Handle ADD COLUMN dependencies."""
    table_name = op["table"]

    return [created_tables[name] for name in [table_name] if name in created_tables]


def _handle_add_foreign_key(
    op: Operation,
    all_operations: list[Operation],
    created_tables: dict[str, int],
) -> list[int]:
    """Handle ADD FOREIGN KEY dependencies."""
    ref = op["details"].get("references", "")
    parts = ref.split(".")
    ref_table = parts[0]

    # The table it points at, when this migration is the one creating it.
    table = [
        created_tables[name]
        for name in [ref_table]
        if ref and name in created_tables
    ]
    # And the COLUMN it points at, when that arrives here too. A key emitted
    # against a column one statement early is refused by every engine.
    columns = [
        idx
        for idx, other in enumerate(all_operations)
        if len(parts) == 2
        and other["op"] == "add_column"
        and other["table"] == parts[0]
        and other["details"].get("column") == parts[1]
    ]
    return table + columns


def _handle_add_index(
    op: Operation,
    all_operations: list[Operation],
    created_tables: dict[str, int],
) -> list[int]:
    """Handle ADD INDEX dependencies."""
    table_name = op["table"]
    idx_cols = op["details"].get("definition", {}).get("columns", [])

    table = [created_tables[name] for name in [table_name] if name in created_tables]
    columns = [
        idx
        for idx, other in enumerate(all_operations)
        if other["op"] == "add_column"
        and other["table"] == table_name
        and other["details"].get("column") in idx_cols
    ]
    return table + columns


def _handle_alter_column(
    op: Operation,
    all_operations: list[Operation],
    dropped_indexes: list[tuple[int, str, str]],
) -> list[int]:
    """Handle ALTER COLUMN dependencies."""
    table_name = op["table"]
    col_name = op["details"].get("column", "")
    # ONLY A TYPE CHANGE WAITS. Setting a default rewrites no data and
    # invalidates no index, so ordering it behind a DROP INDEX would be a
    # dependency the schema never asked for.
    retyped = "type" in op["details"].get("changes", {})

    return [
        idx_idx
        for idx_idx, idx_table, _ in dropped_indexes
        if retyped
        and idx_table == table_name
        and col_name
        in all_operations[idx_idx]["details"].get("definition", {}).get("columns", [])
    ]


def _handle_create_view(
    op: Operation,
    created_tables: dict[str, int],
    created_views: dict[str, int],
) -> list[int]:
    """Handle CREATE VIEW dependencies."""
    named = op["details"].get("depends_on", [])

    # A view waits for whatever it selects from, whether that is a table this
    # migration creates or another view it creates.
    return [
        index
        for name in named
        for source in (created_tables, created_views)
        for index in [source.get(name)]
        if index is not None
    ]


def _handle_drop_view(
    op: Operation,
    op_idx: int,
    all_operations: list[Operation],
) -> list[int]:
    """Handle DROP VIEW dependencies."""
    view_name = op["details"]["name"]

    # A view selecting from this one goes first.
    return [
        idx
        for idx, other in enumerate(all_operations)
        if other["op"] == "drop_view"
        and idx != op_idx
        and view_name in other["details"].get("depends_on", [])
    ]


# Dict-based dispatch table for operation types.
#
# EVERY LAMBDA TAKES THE SAME FOUR THINGS whether it reads them or not, which
# is what lets the dispatch be a lookup instead of an if/elif chain (Rule 1).
# The ones a given handler ignores carry a leading underscore, the same
# convention `_details` uses in the applier and `_dialect` in the condition
# renderer, so an argument that is deliberately unread reads differently from
# one that was forgotten.
OP_DEPENDENCY_HANDLERS: dict = {
    "drop_table": lambda op, op_idx, all_ops, **kw: _handle_drop_table(op, op_idx, all_ops, kw["dropped_fks"]),
    "drop_column": lambda op, _op_idx, all_ops, **kw: _handle_drop_column(op, all_ops, kw["dropped_fks"], kw["dropped_indexes"]),
    "create_table": lambda op, _op_idx, _all_ops, **kw: _handle_create_table(op, kw["created_tables"]),
    "add_column": lambda op, _op_idx, _all_ops, **kw: _handle_add_column(op, kw["created_tables"]),
    "add_foreign_key": lambda op, _op_idx, all_ops, **kw: _handle_add_foreign_key(op, all_ops, kw["created_tables"]),
    "add_index": lambda op, _op_idx, all_ops, **kw: _handle_add_index(op, all_ops, kw["created_tables"]),
    "alter_column": lambda op, _op_idx, all_ops, **kw: _handle_alter_column(op, all_ops, kw["dropped_indexes"]),
    "create_view": lambda op, _op_idx, _all_ops, **kw: _handle_create_view(op, kw["created_tables"], kw["created_views"]),
    "drop_view": lambda op, op_idx, all_ops, **_kw: _handle_drop_view(op, op_idx, all_ops),
}


def _get_operation_dependencies(
    op_idx: int,
    op: Operation,
    all_operations: list[Operation],
    created_tables: dict[str, int],
    created_views: dict[str, int],
    dropped_fks: list[tuple[int, str, str]],
    dropped_indexes: list[tuple[int, str, str]],
) -> list[int]:
    """Get dependencies for a single operation using dict-based dispatch."""
    op_type = op["op"]
    handler = OP_DEPENDENCY_HANDLERS.get(op_type)
    if handler:
        return handler(
            op,
            op_idx,
            all_operations,
            created_tables=created_tables,
            created_views=created_views,
            dropped_fks=dropped_fks,
            dropped_indexes=dropped_indexes,
        )
    return []


def _refuse_cycle(operations: list[Operation], stuck: set[int]) -> None:
    """Nothing is ready and operations remain, so they wait on each other.

    THE STUCK SET IS THE ANSWER, so no search is needed. `_find_cycle` used to
    walk the graph depth-first to recover a path for the message; everything
    that never became ready is either in a cycle or waiting behind one, and
    naming all of it tells an operator more than naming one arbitrary loop.
    """
    tables = {operations[i]["table"] for i in stuck}
    cycle_ops = [f"{operations[i]['op']} on {operations[i]['table']}" for i in sorted(stuck)]
    raise CycleError(cycle_ops, tables)


# WHETHER ANYTHING IS READY, AS A TABLE RATHER THAN A BRANCH. One key, two
# entries, and the refusal is a value in the table rather than a path through
# an `if`.
def _proceed(_operations: list[Operation], _stuck: set[int]) -> None:
    """Something is ready, so there is nothing to refuse."""


_WHEN_READY: dict[bool, Callable[[list[Operation], set[int]], None]] = {
    True: _proceed,
    False: _refuse_cycle,
}


def topological_sort(
    operations: list[Operation],
    dependencies: dict[int, list[int]],
) -> list[int]:
    """Operations in an order that runs every dependency first.

    Kahn's algorithm, written with set algebra. Each round takes the ready
    set, which is every remaining operation whose dependencies have all run,
    and executes the highest-priority member of it.

    ONE AT A TIME RATHER THAN A LAYER AT A TIME, and the difference matters.
    The old loop re-sorted its queue on every pop, so it always took the
    highest-priority READY operation. Emitting a whole layer would order a
    drop that becomes ready in the second round after a create from the
    first, and `test_the_sort_holds_every_dependency.py` fails on exactly
    that.

    A SELF-EDGE IS NOT STRIPPED HERE, and it was in a first draft. An
    operation that waits for itself is unsatisfiable and must be refused;
    quietly dropping the edge made `{0: [0]}` sort cleanly. The exclusion
    belongs where the graph is BUILT, and `_handle_create_table` does it with
    `- {table_name}` so that a table whose key points at itself does not wait
    for itself. That is the case worth handling, and handling it at the source
    means the sort can stay honest about a genuine self-dependency.

    Raises:
        CycleError: when operations remain and none of them can run.
    """
    remaining = set(range(len(operations)))
    deps = {i: frozenset(dependencies.get(i, ())) for i in remaining}
    order: list[int] = []

    while remaining:
        ready = [i for i in remaining if not (deps[i] & remaining)]
        _WHEN_READY[bool(ready)](operations, remaining)
        chosen = min(ready, key=lambda i: (_operation_priority(operations[i]), i))
        order.append(chosen)
        remaining.discard(chosen)

    return order


def _operation_priority(op: Operation) -> int:
    """
    Get priority for an operation type.

    Lower number = higher priority (executed first).
    """
    priorities = {
        # Drops first (reverse dependency order)
        "drop_foreign_key": 0,
        "drop_index": 1,
        "drop_constraint": 2,
        "drop_view": 3,  # Drop views before tables they might reference
        "drop_column": 4,
        "drop_table": 5,
        # Renames
        "rename_table": 6,
        "rename_column": 7,
        # Creates
        "create_table": 8,
        "add_column": 9,
        # Modifications
        "alter_column": 10,
        # Additions last
        "add_constraint": 11,
        "add_index": 12,
        "add_foreign_key": 13,
        "create_view": 14,  # Create views after all tables and indexes
    }
    return priorities.get(op["op"], 99)
