"""
Ambiguity detection for schema changes.

Detects changes that could be interpreted multiple ways:
- Column/table removed + added with different name (rename vs drop+add?)
- Type changes that might lose data
"""

from declaro_persistum.applier.shared import same_type
from declaro_persistum.types import (
    Ambiguity,
    Column,
    Decision,
    Dialect,
    Nothing,
    Schema,
    Table,
    resolved,
)


def detect_ambiguities(
    current: Schema,
    target: Schema,
    dialect: Dialect,
    decisions: dict[str, Decision] | None | Nothing,
) -> list[Ambiguity]:
    """
    Detect ambiguous changes that require human decision.

    Args:
        current: Current database schema
        target: Target schema
        decisions: Already-made decisions (won't report ambiguities for these)

    Returns:
        List of ambiguous changes requiring resolution

    Example:
        >>> current = {"users": {"columns": {"name": {"type": "text"}}}}
        >>> target = {"users": {"columns": {"full_name": {"type": "text"}}}}
        >>> ambiguities = detect_ambiguities(current, target, dialect="postgresql", decisions=NOTHING)
        >>> print(ambiguities[0]["type"])
        'possible_rename'
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    decisions = resolved(decisions, None)
    decisions = decisions or {}
    ambiguities: list[Ambiguity] = []

    # Get table sets
    current_tables = set(current.keys())
    target_tables = set(target.keys())

    dropped_tables = current_tables - target_tables
    added_tables = target_tables - current_tables
    common_tables = current_tables & target_tables

    # Check for possible table renames
    # `current` is gone from this call. The only thing that read it was the
    # column-overlap score, and a score is not entitled to decide whether the
    # operator hears about a drop.
    table_ambiguities = _detect_table_rename_ambiguities(
        dropped_tables, added_tables, target, decisions
    )
    ambiguities.extend(table_ambiguities)

    # Check columns in common tables
    for table_name in common_tables:
        current_table = current[table_name]
        target_table = target[table_name]

        column_ambiguities = _detect_column_ambiguities(
            table_name, current_table, target_table, decisions, dialect
        )
        ambiguities.extend(column_ambiguities)

    return ambiguities


def _detect_table_rename_ambiguities(
    dropped: set[str],
    added: set[str],
    target: Schema,
    decisions: dict[str, Decision],
) -> list[Ambiguity]:
    """Every dropped table beside an added one, with no scoring.

    THIS USED TO MEASURE THE PAIR AND STAY SILENT BELOW A THRESHOLD. Edit
    distance on the names, averaged with the fraction of columns shared, gated
    at 50% overlap. Renaming a table while also reshaping it scored under the
    gate, so no ambiguity was raised, the drop ran, and the rows were gone
    without the operator being asked. Measured 2026-08-21.

    A similarity score is not entitled to decide whether data loss gets
    announced. A drop beside an add is ambiguous by construction: the differ
    holds two schemas and cannot know whether the author moved a thing or
    replaced it. Nothing about the names or the columns changes that.

    `renamed_from` is the answer when the author gives it, and a `decisions`
    entry is the answer when they gave it at the prompt. Absent both, ask.
    """
    resolved = frozenset(t for t in dropped if f"table_{t}" in decisions)
    declared = frozenset(
        target[a]["renamed_from"] for a in added if target[a].get("renamed_from")
    )
    named = frozenset(a for a in added if not target[a].get("renamed_from"))

    return [
        {
            "type": "possible_rename",
            "table": dropped_table,
            "from_column": None,
            "to_column": None,
            "column": None,
            "message": (
                f"Table '{dropped_table}' removed and '{added_table}' added in "
                f"the same migration. Declare `renamed_from` on "
                f"'{added_table}' if this is a rename; the drop destroys the "
                f"rows if it is not."
            ),
        }
        for dropped_table in sorted(dropped - resolved - declared)
        for added_table in sorted(named)
    ]


def _detect_column_ambiguities(
    table_name: str,
    current: Table,
    target: Table,
    decisions: dict[str, Decision],
    dialect: Dialect,
) -> list[Ambiguity]:
    """Detect possible column renames and destructive changes."""
    ambiguities: list[Ambiguity] = []

    current_columns = current.get("columns", {})
    target_columns = target.get("columns", {})

    current_col_names = set(current_columns.keys())
    target_col_names = set(target_columns.keys())

    dropped_cols = current_col_names - target_col_names
    added_cols = target_col_names - current_col_names

    # Filter out columns with explicit hints
    for added_col in list(added_cols):
        col_def = target_columns[added_col]
        if col_def.get("renamed_from") or col_def.get("is_new"):
            added_cols.discard(added_col)
            if col_def.get("renamed_from"):
                dropped_cols.discard(col_def["renamed_from"])

    # Detect possible renames
    column_ambiguities = _detect_column_rename_ambiguities(
        table_name, dropped_cols, added_cols, target_columns, decisions
    )
    ambiguities.extend(column_ambiguities)

    # Detect destructive type changes
    type_ambiguities = _detect_type_change_ambiguities(
        table_name, current_columns, target_columns, decisions, dialect
    )
    ambiguities.extend(type_ambiguities)

    return ambiguities


def _detect_column_rename_ambiguities(
    table_name: str,
    dropped: set[str],
    added: set[str],
    target_columns: dict[str, Column],
    decisions: dict[str, Decision],
) -> list[Ambiguity]:
    """Every dropped column beside an added one, with no scoring.

    THIS USED TO REQUIRE A MATCHING TYPE AND A MATCHING NULLABILITY before it
    would ask, and it scored the names with edit distance on top. A column
    renamed and retyped in one migration therefore passed unnoticed, and its
    values went with the drop.

    The same rule as the table half: the differ holds two schemas and cannot
    know whether a column moved or was replaced. `renamed_from` on the column
    is the declarative answer and reaches `rename_column` in the differ, so
    the author who says what they did is never asked.
    """
    resolved = frozenset(c for c in dropped if f"{table_name}_{c}" in decisions)
    declared = frozenset(
        target_columns[a]["renamed_from"]
        for a in added
        if target_columns[a].get("renamed_from")
    )
    named = frozenset(a for a in added if not target_columns[a].get("renamed_from"))

    return [
        {
            "type": "possible_rename",
            "table": table_name,
            "from_column": dropped_col,
            "to_column": added_col,
            "column": None,
            "message": (
                f"Column '{dropped_col}' removed and '{added_col}' added on "
                f"'{table_name}' in the same migration. Declare `renamed_from` "
                f"on '{added_col}' if this is a rename; the drop destroys the "
                f"values if it is not."
            ),
        }
        for dropped_col in sorted(dropped - resolved - declared)
        for added_col in sorted(named)
    ]


def _detect_type_change_ambiguities(
    table_name: str,
    current_columns: dict[str, Column],
    target_columns: dict[str, Column],
    decisions: dict[str, Decision],
    dialect: Dialect,
) -> list[Ambiguity]:
    """Detect potentially destructive type changes.

    THE COMPARISON IS PER ENGINE. `uuid` and `text` are one column on SQLite,
    so "text -> uuid" there is not a change at all, let alone a risky one.
    Reading it as one blocked every migration after the first with an
    ambiguity about a column nobody had touched, and an ambiguity stops the
    run. On PostgreSQL the two are genuinely different and still trip.
    """
    ambiguities: list[Ambiguity] = []

    # Types that might lose data when changed
    risky_changes = {
        ("text", "integer"),
        ("text", "boolean"),
        ("text", "uuid"),
        ("jsonb", "text"),
        ("float8", "integer"),
        ("numeric", "integer"),
        ("timestamptz", "date"),
    }

    common_cols = set(current_columns.keys()) & set(target_columns.keys())

    for col_name in common_cols:
        decision_key = f"{table_name}_{col_name}_type"
        if decision_key in decisions:
            continue

        current_type = current_columns[col_name].get("type", "").lower()
        target_type = target_columns[col_name].get("type", "").lower()

        if not same_type(current_type, target_type, dialect):
            # Normalize types for comparison
            current_base = current_type.split("(")[0]
            target_base = target_type.split("(")[0]

            if (current_base, target_base) in risky_changes:
                ambiguities.append(
                    {
                        "type": "type_change",
                        "table": table_name,
                        "from_column": None,
                        "to_column": None,
                        "column": col_name,
                        "confidence": 0.0,
                        "message": (
                            f"Column '{col_name}' type change from {current_type} to {target_type} "
                            f"may cause data loss. Confirm this change?"
                        ),
                    }
                )

    return ambiguities
