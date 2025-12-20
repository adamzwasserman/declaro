"""
Ambiguity detection for schema changes.

Detects changes that could be interpreted multiple ways:
- Column/table removed + added with different name (rename vs drop+add?)
- Type changes that might lose data
"""

from declaro_persistum.types import Ambiguity, Column, Decision, Schema, Table


def detect_ambiguities(
    current: Schema,
    target: Schema,
    decisions: dict[str, Decision] | None = None,
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
        >>> ambiguities = detect_ambiguities(current, target)
        >>> print(ambiguities[0]["type"])
        'possible_rename'
    """
    decisions = decisions or {}
    ambiguities: list[Ambiguity] = []

    # Get table sets
    current_tables = set(current.keys())
    target_tables = set(target.keys())

    dropped_tables = current_tables - target_tables
    added_tables = target_tables - current_tables
    common_tables = current_tables & target_tables

    # Check for possible table renames
    table_ambiguities = _detect_table_rename_ambiguities(
        dropped_tables, added_tables, current, target, decisions
    )
    ambiguities.extend(table_ambiguities)

    # Check columns in common tables
    for table_name in common_tables:
        current_table = current[table_name]
        target_table = target[table_name]

        column_ambiguities = _detect_column_ambiguities(
            table_name, current_table, target_table, decisions
        )
        ambiguities.extend(column_ambiguities)

    return ambiguities


def _detect_table_rename_ambiguities(
    dropped: set[str],
    added: set[str],
    current: Schema,
    target: Schema,
    decisions: dict[str, Decision],
) -> list[Ambiguity]:
    """Detect possible table renames."""
    ambiguities: list[Ambiguity] = []

    for dropped_table in dropped:
        # Skip if already has a decision
        decision_key = f"table_{dropped_table}"
        if decision_key in decisions:
            continue

        # Check if any added table has renamed_from hint
        for added_table in added:
            target_def = target[added_table]
            if target_def.get("renamed_from") == dropped_table:
                # Explicit rename, no ambiguity
                continue

        # Check for possible renames based on structure similarity
        current_def = current[dropped_table]
        current_cols = set(current_def.get("columns", {}).keys())

        for added_table in added:
            target_def = target[added_table]

            # Skip if has explicit renamed_from
            if target_def.get("renamed_from"):
                continue

            target_cols = set(target_def.get("columns", {}).keys())

            # Check column overlap
            if current_cols and target_cols:
                overlap = len(current_cols & target_cols) / max(len(current_cols), len(target_cols))

                if overlap >= 0.5:  # At least 50% column overlap
                    confidence = calculate_rename_confidence(dropped_table, added_table)
                    confidence = (confidence + overlap) / 2  # Average with structural similarity

                    ambiguities.append(
                        {
                            "type": "possible_rename",
                            "table": dropped_table,
                            "from_column": None,
                            "to_column": None,
                            "column": None,
                            "confidence": confidence,
                            "message": (
                                f"Table '{dropped_table}' removed, '{added_table}' added with "
                                f"{overlap * 100:.0f}% column overlap. Is this a rename?"
                            ),
                        }
                    )

    return ambiguities


def _detect_column_ambiguities(
    table_name: str,
    current: Table,
    target: Table,
    decisions: dict[str, Decision],
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
        table_name, dropped_cols, added_cols, current_columns, target_columns, decisions
    )
    ambiguities.extend(column_ambiguities)

    # Detect destructive type changes
    type_ambiguities = _detect_type_change_ambiguities(
        table_name, current_columns, target_columns, decisions
    )
    ambiguities.extend(type_ambiguities)

    return ambiguities


def _detect_column_rename_ambiguities(
    table_name: str,
    dropped: set[str],
    added: set[str],
    current_columns: dict[str, Column],
    target_columns: dict[str, Column],
    decisions: dict[str, Decision],
) -> list[Ambiguity]:
    """
    Detect potential column renames vs drop+add.

    Heuristics for likely rename:
    - Same type
    - Same nullability
    - Similar name (edit distance)
    """
    ambiguities: list[Ambiguity] = []

    for dropped_col in dropped:
        # Skip if already has a decision
        decision_key = f"{table_name}_{dropped_col}"
        if decision_key in decisions:
            continue

        dropped_def = current_columns[dropped_col]

        for added_col in added:
            added_def = target_columns[added_col]

            # Check if types match
            if dropped_def.get("type") != added_def.get("type"):
                continue

            # Check nullability match
            dropped_nullable = dropped_def.get("nullable", True)
            added_nullable = added_def.get("nullable", True)
            if dropped_nullable != added_nullable:
                continue

            # Potential rename detected
            confidence = calculate_rename_confidence(dropped_col, added_col)

            ambiguities.append(
                {
                    "type": "possible_rename",
                    "table": table_name,
                    "from_column": dropped_col,
                    "to_column": added_col,
                    "column": None,
                    "confidence": confidence,
                    "message": (
                        f"Column '{dropped_col}' removed, '{added_col}' added with same type. "
                        f"Is this a rename (preserves data) or drop+add (loses data)?"
                    ),
                }
            )

    return ambiguities


def _detect_type_change_ambiguities(
    table_name: str,
    current_columns: dict[str, Column],
    target_columns: dict[str, Column],
    decisions: dict[str, Decision],
) -> list[Ambiguity]:
    """Detect potentially destructive type changes."""
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

        if current_type != target_type:
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


def calculate_rename_confidence(from_name: str, to_name: str) -> float:
    """
    Calculate confidence score that two names represent a rename.

    Uses Levenshtein-like similarity plus common patterns.

    Args:
        from_name: Original name
        to_name: New name

    Returns:
        Confidence score between 0.0 and 1.0
    """
    from_lower = from_name.lower()
    to_lower = to_name.lower()

    # Exact match (case change only)
    if from_lower == to_lower:
        return 1.0

    # One is prefix/suffix of other
    if from_lower in to_lower or to_lower in from_lower:
        longer = max(len(from_lower), len(to_lower))
        shorter = min(len(from_lower), len(to_lower))
        return shorter / longer

    # Common rename patterns
    patterns = [
        ("name", "full_name"),
        ("name", "display_name"),
        ("user", "user_id"),
        ("id", "user_id"),
        ("date", "created_at"),
        ("date", "updated_at"),
        ("time", "timestamp"),
    ]

    for pattern_from, pattern_to in patterns:
        if (pattern_from in from_lower and pattern_to in to_lower) or (
            pattern_to in from_lower and pattern_from in to_lower
        ):
            return 0.7

    # Levenshtein distance based similarity
    distance = _levenshtein_distance(from_lower, to_lower)
    max_len = max(len(from_lower), len(to_lower))

    if max_len == 0:
        return 0.0

    similarity = 1.0 - (distance / max_len)

    # Only consider it a possible rename if reasonably similar
    return similarity if similarity >= 0.4 else 0.0


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Compute Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row: list[int] = list(range(len(s2) + 1))

    for i, c1 in enumerate(s1):
        current_row: list[int] = [i + 1]

        for j, c2 in enumerate(s2):
            # Cost is 0 if characters match, 1 otherwise
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))

        previous_row = current_row

    return previous_row[-1]
