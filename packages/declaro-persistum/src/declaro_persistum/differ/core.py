"""
Core diff logic for schema comparison.

Implements the main diff() function that computes operations needed
to transform current schema to target schema.
"""

from typing import Any

from declaro_persistum.differ.ambiguity import detect_ambiguities
from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort
from declaro_persistum.types import (
    Column,
    Decision,
    DiffResult,
    Operation,
    Schema,
    Table,
)


def diff(
    current: Schema,
    target: Schema,
    *,
    decisions: dict[str, Decision] | None = None,
) -> DiffResult:
    """
    Compute operations needed to transform current schema to target schema.

    This is a pure function: same inputs always produce same outputs.
    No side effects, no hidden state, no I/O.

    Set Theory Operations:
        Let C = set of current table names
        Let T = set of target table names

        dropped = C - T (tables to drop)
        added = T - C (tables to create)
        modified = C ∩ T (tables to compare for changes)

    Args:
        current: Current database schema state
        target: Desired schema state
        decisions: Pre-made decisions for ambiguous changes (from pending.toml)

    Returns:
        DiffResult with operations, dependencies, and execution order

    Raises:
        CycleError: If operation dependencies form a cycle

    Example:
        >>> current = {"users": {"columns": {"id": {"type": "integer"}}}}
        >>> target = {"users": {"columns": {"id": {"type": "integer"}, "email": {"type": "text"}}}}
        >>> result = diff(current, target)
        >>> print(result["operations"][0]["op"])
        'add_column'
    """
    decisions = decisions or {}
    operations: list[Operation] = []

    # Compute table set differences
    current_tables = set(current.keys())
    target_tables = set(target.keys())

    dropped_tables = current_tables - target_tables  # C - T
    added_tables = target_tables - current_tables  # T - C
    modified_tables = current_tables & target_tables  # C ∩ T

    # Handle table renames (check decisions and renamed_from hints)
    actual_drops: set[str] = set()
    actual_adds: set[str] = set()
    renames: list[tuple[str, str]] = []  # (from, to)

    for added_table in added_tables:
        target_table = target[added_table]
        renamed_from = target_table.get("renamed_from")

        if renamed_from and renamed_from in dropped_tables:
            # Explicit rename hint
            renames.append((renamed_from, added_table))
        else:
            actual_adds.add(added_table)

    for dropped_table in dropped_tables:
        # Check if it was renamed
        if not any(r[0] == dropped_table for r in renames):
            actual_drops.add(dropped_table)

    # Generate DROP TABLE operations
    for table_name in actual_drops:
        operations.append(
            {
                "op": "drop_table",
                "table": table_name,
                "details": {},
            }
        )

    # Generate RENAME TABLE operations
    for from_name, to_name in renames:
        operations.append(
            {
                "op": "rename_table",
                "table": from_name,
                "details": {"new_name": to_name},
            }
        )

    # Generate CREATE TABLE operations
    for table_name in actual_adds:
        target_table = target[table_name]
        operations.append(
            {
                "op": "create_table",
                "table": table_name,
                "details": {
                    "columns": target_table.get("columns", {}),
                    "primary_key": target_table.get("primary_key"),
                    "indexes": target_table.get("indexes", {}),
                    "constraints": target_table.get("constraints", {}),
                },
            }
        )

    # Process modified tables
    for table_name in modified_tables:
        current_table = current[table_name]
        target_table = target[table_name]

        # Also check if this table was renamed to (update the name reference)
        effective_table_name = table_name
        for from_name, to_name in renames:
            if from_name == table_name:
                effective_table_name = to_name
                break

        table_ops = _diff_table(
            effective_table_name,
            current_table,
            target_table,
        )
        operations.extend(table_ops)

    # Detect ambiguities
    ambiguities = detect_ambiguities(current, target, decisions)

    # Build dependency graph and sort
    dependencies = build_dependency_graph(operations)
    execution_order = topological_sort(operations, dependencies)

    return {
        "operations": operations,
        "dependencies": dependencies,
        "execution_order": execution_order,
        "ambiguities": ambiguities,
    }


def _diff_table(
    table_name: str,
    current: Table,
    target: Table,
) -> list[Operation]:
    """
    Compute operations to transform a single table.

    Args:
        table_name: Name of the table
        current: Current table definition
        target: Target table definition
        decisions: Pre-made decisions for ambiguous changes

    Returns:
        List of operations for this table
    """
    operations: list[Operation] = []

    # Diff columns
    column_ops = _diff_columns(table_name, current, target)
    operations.extend(column_ops)

    # Diff indexes
    index_ops = _diff_indexes(table_name, current, target)
    operations.extend(index_ops)

    # Diff constraints
    constraint_ops = _diff_constraints(table_name, current, target)
    operations.extend(constraint_ops)

    return operations


def _diff_columns(
    table_name: str,
    current: Table,
    target: Table,
) -> list[Operation]:
    """
    Compute column-level operations.

    Set Theory:
        Let CC = current column names
        Let TC = target column names

        dropped = CC - TC
        added = TC - CC
        modified = CC ∩ TC
    """
    operations: list[Operation] = []

    current_columns = current.get("columns", {})
    target_columns = target.get("columns", {})

    current_col_names = set(current_columns.keys())
    target_col_names = set(target_columns.keys())

    dropped_cols = current_col_names - target_col_names
    added_cols = target_col_names - current_col_names
    common_cols = current_col_names & target_col_names

    # Handle column renames (check renamed_from hints)
    actual_drops: set[str] = set()
    actual_adds: set[str] = set()
    renames: list[tuple[str, str]] = []

    for added_col in added_cols:
        target_col = target_columns[added_col]
        renamed_from = target_col.get("renamed_from")

        if renamed_from and renamed_from in dropped_cols:
            renames.append((renamed_from, added_col))
        else:
            actual_adds.add(added_col)

    for dropped_col in dropped_cols:
        if not any(r[0] == dropped_col for r in renames):
            actual_drops.add(dropped_col)

    # Generate DROP COLUMN operations
    for col_name in actual_drops:
        operations.append(
            {
                "op": "drop_column",
                "table": table_name,
                "details": {"column": col_name},
            }
        )

    # Generate RENAME COLUMN operations
    for from_name, to_name in renames:
        operations.append(
            {
                "op": "rename_column",
                "table": table_name,
                "details": {"from_column": from_name, "to_column": to_name},
            }
        )

    # Generate ADD COLUMN operations
    for col_name in actual_adds:
        target_col = target_columns[col_name]
        operations.append(
            {
                "op": "add_column",
                "table": table_name,
                "details": {"column": col_name, "definition": target_col},
            }
        )

    # Check for column alterations
    for col_name in common_cols:
        current_col = current_columns[col_name]
        target_col = target_columns[col_name]

        alter_details = _compute_column_alterations(current_col, target_col)
        if alter_details:
            operations.append(
                {
                    "op": "alter_column",
                    "table": table_name,
                    "details": {"column": col_name, "changes": alter_details},
                }
            )

    # Handle foreign key changes
    fk_ops = _diff_foreign_keys(table_name, current_columns, target_columns)
    operations.extend(fk_ops)

    return operations


def _compute_column_alterations(
    current: Column,
    target: Column,
) -> dict[str, Any] | None:
    """
    Compute what alterations are needed for a column.

    Returns None if no changes needed.
    """
    changes: dict[str, Any] = {}

    # Type change
    if current.get("type") != target.get("type"):
        changes["type"] = {"from": current.get("type"), "to": target.get("type")}

    # Nullability change
    current_nullable = current.get("nullable", True)
    target_nullable = target.get("nullable", True)
    if current_nullable != target_nullable:
        changes["nullable"] = {"from": current_nullable, "to": target_nullable}

    # Default change
    current_default = current.get("default")
    target_default = target.get("default")
    if current_default != target_default:
        changes["default"] = {"from": current_default, "to": target_default}

    return changes if changes else None


def _diff_foreign_keys(
    table_name: str,
    current_columns: dict[str, Column],
    target_columns: dict[str, Column],
) -> list[Operation]:
    """Diff foreign key constraints on columns."""
    operations: list[Operation] = []

    for col_name in set(current_columns.keys()) | set(target_columns.keys()):
        current_col = current_columns.get(col_name, {})
        target_col = target_columns.get(col_name, {})

        current_ref = current_col.get("references")
        target_ref = target_col.get("references")

        if current_ref and not target_ref:
            # FK removed
            operations.append(
                {
                    "op": "drop_foreign_key",
                    "table": table_name,
                    "details": {"column": col_name, "references": current_ref},
                }
            )
        elif target_ref and not current_ref:
            # FK added
            operations.append(
                {
                    "op": "add_foreign_key",
                    "table": table_name,
                    "details": {
                        "column": col_name,
                        "references": target_ref,
                        "on_delete": target_col.get("on_delete"),
                        "on_update": target_col.get("on_update"),
                    },
                }
            )
        elif current_ref != target_ref:
            # FK changed - drop and add
            operations.append(
                {
                    "op": "drop_foreign_key",
                    "table": table_name,
                    "details": {"column": col_name, "references": current_ref},
                }
            )
            operations.append(
                {
                    "op": "add_foreign_key",
                    "table": table_name,
                    "details": {
                        "column": col_name,
                        "references": target_ref,
                        "on_delete": target_col.get("on_delete"),
                        "on_update": target_col.get("on_update"),
                    },
                }
            )

    return operations


def _diff_indexes(
    table_name: str,
    current: Table,
    target: Table,
) -> list[Operation]:
    """
    Diff index definitions.

    Set Theory:
        Let CI = current index names
        Let TI = target index names

        dropped = CI - TI
        added = TI - CI
        modified = CI ∩ TI (compare definitions)
    """
    operations: list[Operation] = []

    current_indexes = current.get("indexes", {})
    target_indexes = target.get("indexes", {})

    current_names = set(current_indexes.keys())
    target_names = set(target_indexes.keys())

    # Drop removed indexes
    for idx_name in current_names - target_names:
        operations.append(
            {
                "op": "drop_index",
                "table": table_name,
                "details": {"index": idx_name},
            }
        )

    # Add new indexes
    for idx_name in target_names - current_names:
        operations.append(
            {
                "op": "add_index",
                "table": table_name,
                "details": {"index": idx_name, "definition": target_indexes[idx_name]},
            }
        )

    # Check for modified indexes (drop + add)
    for idx_name in current_names & target_names:
        current_idx = current_indexes[idx_name]
        target_idx = target_indexes[idx_name]

        if current_idx != target_idx:
            operations.append(
                {
                    "op": "drop_index",
                    "table": table_name,
                    "details": {"index": idx_name},
                }
            )
            operations.append(
                {
                    "op": "add_index",
                    "table": table_name,
                    "details": {"index": idx_name, "definition": target_idx},
                }
            )

    return operations


def _diff_constraints(
    table_name: str,
    current: Table,
    target: Table,
) -> list[Operation]:
    """Diff named constraints."""
    operations: list[Operation] = []

    current_constraints = current.get("constraints", {})
    target_constraints = target.get("constraints", {})

    current_names = set(current_constraints.keys())
    target_names = set(target_constraints.keys())

    # Drop removed constraints
    for const_name in current_names - target_names:
        operations.append(
            {
                "op": "drop_constraint",
                "table": table_name,
                "details": {"constraint": const_name},
            }
        )

    # Add new constraints
    for const_name in target_names - current_names:
        operations.append(
            {
                "op": "add_constraint",
                "table": table_name,
                "details": {"constraint": const_name, "definition": target_constraints[const_name]},
            }
        )

    # Check for modified constraints
    for const_name in current_names & target_names:
        if current_constraints[const_name] != target_constraints[const_name]:
            operations.append(
                {
                    "op": "drop_constraint",
                    "table": table_name,
                    "details": {"constraint": const_name},
                }
            )
            operations.append(
                {
                    "op": "add_constraint",
                    "table": table_name,
                    "details": {
                        "constraint": const_name,
                        "definition": target_constraints[const_name],
                    },
                }
            )

    return operations
