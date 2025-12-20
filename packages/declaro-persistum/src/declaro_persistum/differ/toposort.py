"""
Topological sorting for operation dependency ordering.

Ensures operations are executed in an order that respects dependencies.
For example, DROP FOREIGN KEY must happen before DROP TABLE.
"""

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


def _get_operation_dependencies(
    op_idx: int,
    op: Operation,
    all_operations: list[Operation],
    created_tables: dict[str, int],
    created_views: dict[str, int],
    dropped_fks: list[tuple[int, str, str]],
    dropped_indexes: list[tuple[int, str, str]],
) -> list[int]:
    """Get dependencies for a single operation."""
    deps: list[int] = []
    op_type = op["op"]
    table_name = op["table"]

    if op_type == "drop_table":
        # DROP TABLE depends on all DROP FOREIGN KEY that reference this table
        for fk_idx, _fk_table, _ in dropped_fks:
            # Check if the FK references this table
            fk_op = all_operations[fk_idx]
            ref = fk_op["details"].get("references", "")
            if ref.startswith(f"{table_name}."):
                deps.append(fk_idx)

        # DROP TABLE depends on DROP VIEW for views that depend on this table
        for view_idx, view_op in enumerate(all_operations):
            if view_op["op"] == "drop_view":
                view_deps = view_op["details"].get("depends_on", [])
                if table_name in view_deps:
                    deps.append(view_idx)

    elif op_type == "drop_column":
        # DROP COLUMN depends on DROP FK if column is referenced
        col_name = op["details"].get("column", "")
        for fk_idx, fk_table, fk_col in dropped_fks:
            if fk_table == table_name and fk_col == col_name:
                deps.append(fk_idx)

        # DROP COLUMN depends on DROP INDEX if column is in index
        for idx_idx, idx_table, _ in dropped_indexes:
            if idx_table == table_name:
                idx_op = all_operations[idx_idx]
                idx_def = idx_op["details"].get("definition", {})
                if col_name in idx_def.get("columns", []):
                    deps.append(idx_idx)

    elif op_type == "create_table":
        # CREATE TABLE depends on CREATE of referenced tables
        columns = op["details"].get("columns", {})
        for col_def in columns.values():
            ref = col_def.get("references", "")
            if ref:
                ref_table = ref.split(".")[0]
                if ref_table in created_tables and ref_table != table_name:
                    deps.append(created_tables[ref_table])

    elif op_type == "add_column":
        # ADD COLUMN depends on CREATE TABLE
        if table_name in created_tables:
            deps.append(created_tables[table_name])

    elif op_type == "add_foreign_key":
        # ADD FK depends on CREATE TABLE for referenced table
        ref = op["details"].get("references", "")
        if ref:
            ref_table = ref.split(".")[0]
            if ref_table in created_tables:
                deps.append(created_tables[ref_table])

        # Also depends on ADD COLUMN for the referenced column
        for j, other_op in enumerate(all_operations):
            if other_op["op"] == "add_column":
                ref_parts = ref.split(".")
                if len(ref_parts) == 2:
                    ref_table, ref_col = ref_parts
                    if (
                        other_op["table"] == ref_table
                        and other_op["details"].get("column") == ref_col
                    ):
                        deps.append(j)

    elif op_type == "add_index":
        # ADD INDEX depends on ADD COLUMN for indexed columns
        if table_name in created_tables:
            deps.append(created_tables[table_name])

        idx_def = op["details"].get("definition", {})
        idx_cols = idx_def.get("columns", [])

        for j, other_op in enumerate(all_operations):
            if (
                other_op["op"] == "add_column"
                and other_op["table"] == table_name
                and other_op["details"].get("column") in idx_cols
            ):
                deps.append(j)

    elif op_type == "alter_column":
        # ALTER COLUMN may depend on DROP INDEX if type changes
        changes = op["details"].get("changes", {})
        if "type" in changes:
            col_name = op["details"].get("column", "")
            for idx_idx, idx_table, _ in dropped_indexes:
                if idx_table == table_name:
                    idx_op = all_operations[idx_idx]
                    idx_def = idx_op["details"].get("definition", {})
                    if col_name in idx_def.get("columns", []):
                        deps.append(idx_idx)

    elif op_type == "create_view":
        # CREATE VIEW depends on CREATE TABLE/VIEW for referenced objects
        view_deps = op["details"].get("depends_on", [])
        for dep_name in view_deps:
            # Check if it's a table being created
            if dep_name in created_tables:
                deps.append(created_tables[dep_name])
            # Check if it's another view being created
            if dep_name in created_views:
                deps.append(created_views[dep_name])

    elif op_type == "drop_view":
        # DROP VIEW depends on DROP VIEW for views that depend on this view
        view_name = op["details"]["name"]
        for other_idx, other_op in enumerate(all_operations):
            if other_op["op"] == "drop_view" and other_idx != op_idx:
                other_deps = other_op["details"].get("depends_on", [])
                if view_name in other_deps:
                    deps.append(other_idx)

    return deps


def topological_sort(
    operations: list[Operation],
    dependencies: dict[int, list[int]],
) -> list[int]:
    """
    Topologically sort operations based on dependencies.

    Uses Kahn's algorithm for cycle detection.

    Args:
        operations: List of operations
        dependencies: Dependency graph (op index -> dependency indices)

    Returns:
        List of operation indices in execution order

    Raises:
        CycleError: If dependencies form a cycle
    """
    n = len(operations)
    if n == 0:
        return []

    # Build in-degree count and reverse adjacency
    in_degree = [0] * n
    reverse_deps: dict[int, list[int]] = {i: [] for i in range(n)}

    for op_idx, deps in dependencies.items():
        in_degree[op_idx] = len(deps)
        for dep_idx in deps:
            reverse_deps[dep_idx].append(op_idx)

    # Start with operations that have no dependencies
    queue = [i for i in range(n) if in_degree[i] == 0]
    result: list[int] = []

    while queue:
        # Sort queue for deterministic ordering (by operation type priority)
        queue.sort(key=lambda i: _operation_priority(operations[i]))

        current = queue.pop(0)
        result.append(current)

        # Reduce in-degree of dependent operations
        for dependent in reverse_deps[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(result) != n:
        # Cycle detected - find it for error message
        cycle = _find_cycle(dependencies, in_degree)
        tables = {operations[i]["table"] for i in cycle}
        cycle_ops = [f"{operations[i]['op']} on {operations[i]['table']}" for i in cycle]
        raise CycleError(cycle_ops, tables)

    return result


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


def _find_cycle(dependencies: dict[int, list[int]], in_degree: list[int]) -> list[int]:
    """Find a cycle in the dependency graph for error reporting."""
    # Start from a node with non-zero in-degree
    start = next((i for i, d in enumerate(in_degree) if d > 0), 0)

    visited: set[int] = set()
    path: list[int] = []

    def dfs(node: int) -> list[int] | None:
        if node in visited:
            # Found cycle
            cycle_start = path.index(node)
            return path[cycle_start:]

        visited.add(node)
        path.append(node)

        for dep in dependencies.get(node, []):
            result = dfs(dep)
            if result:
                return result

        path.pop()
        return None

    result = dfs(start)
    return result or [start]
