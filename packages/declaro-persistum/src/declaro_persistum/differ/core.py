"""
Core diff logic for schema comparison.

Implements the main diff(, current=NOTHING, target=NOTHING, dialect=NOTHING, decisions=NOTHING) function that computes operations needed
to transform current schema to target schema.
"""

import re
from collections.abc import Callable
from typing import Any

from declaro_persistum.applier.shared import same_type
from declaro_persistum.boundary import helper
from declaro_persistum.differ.ambiguity import detect_ambiguities
from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort
from declaro_persistum.types import (
    Column,
    Decision,
    Dialect,
    DiffResult,
    Index,
    Nothing,
    Operation,
    Schema,
    Table,
    applied_to_text,
    is_nullable,
    is_our_table,
    resolved,
    spelling_of,
)


@helper
def diff(
    current: Schema,
    target: Schema,
    *,
    dialect: Dialect,
    decisions: dict[str, Decision] | None | Nothing,
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

    THE DIALECT IS REQUIRED, AND WITHOUT IT THIS WAS NOT IDEMPOTENT.
    A declared type and an introspected type are the same fact in two
    spellings: `uuid` is stored as TEXT on SQLite and Turso, so introspection
    returns "text" and a plain string comparison saw text -> uuid on every
    run. Measured 2026-08-14, same models file, nothing edited:

        first : True, 1 operation
        second: False, "Column 'id' type change from text to uuid may cause
                data loss. Confirm this change?"

    Every uuid, timestamptz, jsonb, numeric and boolean column did it. Both
    sides are now put through `map_type` before being compared, so they are
    compared as the engine spells them. On PostgreSQL that changes nothing and
    a real text -> uuid change still trips, which is the point.

    Args:
        current: Current database schema state
        target: Desired schema state
        dialect: The engine, because a type comparison is meaningless without
            one. Required for the same reason `map_type` requires it (Rule 14)
        decisions: Pre-made decisions for ambiguous changes (from pending.toml)

    Returns:
        DiffResult with operations, dependencies, and execution order

    Raises:
        CycleError: If operation dependencies form a cycle

    Example:
        >>> current = {"users": {"columns": {"id": {"type": "integer"}}}}
        >>> target = {"users": {"columns": {"id": {"type": "integer"}, "email": {"type": "text"}}}}
        >>> result = diff(current, target, dialect="postgresql", decisions=NOTHING)
        >>> print(result["operations"][0]["op"])
        'add_column'
    """
    # THE VISIBLE RESOLVE. A default would hide this inside the
    # signature, where no reader sees it happen and no test reaches it.
    decisions = resolved(decisions, None)
    decisions = decisions or {}
    operations: list[Operation] = []

    # OUR OWN BOOKKEEPING TABLES ARE NOT PART OF THE COMPARISON. The differ
    # reads an introspected table nobody declared as a table the author
    # dropped, and that has always been true of `_declaro_meta`,
    # `_declaro_tmp_*`, `_dp_materialized_views` and `_dp_transfer_state`.
    # Dropping them here rather than exempting them from the ambiguity check
    # is the difference between the phantom `drop_table` not existing and it
    # existing but going unmentioned.
    current = {n: t for n, t in current.items() if not is_our_table(n)}

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
    #
    # THE REFERENCES GO WITH IT, because the sort cannot order two drops
    # without them. `create_table` has always carried its columns, so
    # `_handle_create_table` can put a parent before its child. `drop_table`
    # carried an empty dict, so dropping two related tables produced no edge
    # between them and the order was whatever Kahn's algorithm happened to
    # emit. Measured 2026-08-18:
    #
    #     sqlite      parent first  ok        child first  ok
    #     postgresql  parent first  FAILS     child first  ok
    #
    # So removing two related tables from the models broke the migration on
    # PostgreSQL, or did not, depending on an order nothing decided. A test
    # asserting the order passed alone and failed in the suite for that
    # reason, which is how this was found.
    for table_name in actual_drops:
        operations.append(
            {
                "op": "drop_table",
                "table": table_name,
                "details": {
                    "references": sorted(
                        {
                            str(column["references"]).split(".")[0]
                            for column in (
                                (current.get(table_name) or {}).get("columns") or {}
                            ).values()
                            if column.get("references")
                        }
                    )
                },
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

        # AN INDEX DECLARED ON A NEW TABLE IS AN OPERATION OF ITS OWN.
        # `create_table`'s details carry `indexes` and `create_table_sql` never
        # looks at them, so a fresh migration created the table and none of its
        # indexes. Measured 2026-08-17: two declared, zero created, and
        # `declaro apply` reported success.
        #
        # It self-healed on the next deploy, which is what kept it quiet: the
        # second diff noticed and emitted `add_index`. A deployment ran without
        # the indexes it declared until somebody migrated again.
        #
        # Same fix as `insert_enum_values` got this morning, for the same
        # reason: data the operation needed was sitting in the schema where the
        # applier could not reach it. `add_index` already exists in the
        # vocabulary and every applier already runs it.
        for index_name, definition in (target_table.get("indexes") or {}).items():
            operations.append(
                {
                    "op": "add_index",
                    "table": table_name,
                    "details": {"index": index_name, "definition": definition},
                }
            )

        # A lookup table that nothing fills rejects every value its column
        # declares. The values are in the schema and the applier only sees
        # operations, so the differ -- which holds both -- puts them in one.
        enum_values = target_table.get("_enum_values")
        if enum_values:
            operations.append(
                {
                    "op": "insert_enum_values",
                    "table": table_name,
                    "details": {"values": list(enum_values)},
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
            dialect,
        )
        operations.extend(table_ops)

    # Detect ambiguities
    ambiguities = detect_ambiguities(current, target, dialect, decisions)

    # Build dependency graph and sort
    dependencies = build_dependency_graph(operations)
    execution_order = topological_sort(operations, dependencies)

    return {
        "operations": operations,
        "dependencies": dependencies,
        "execution_order": execution_order,
        "ambiguities": ambiguities,
    }


@helper
def _diff_table(
    table_name: str,
    current: Table,
    target: Table,
    dialect: Dialect,
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
    column_ops = _diff_columns(table_name, current, target, dialect)
    operations.extend(column_ops)

    # Diff indexes
    index_ops = _diff_indexes(table_name, current, target)
    operations.extend(index_ops)

    # Diff constraints
    constraint_ops = _diff_constraints(table_name, current, target, dialect)
    operations.extend(constraint_ops)

    return operations


@helper
def _with_unique_folded(table: dict[str, Any]) -> dict[str, Column]:
    """A table's columns, with single-column unique indexes folded onto them.

    The same fold `inspector.shared.apply_unique_columns` performs on the way
    out of a database, applied to both sides of a diff so they are asking one
    question.
    """
    columns = {name: dict(col) for name, col in (table.get("columns") or {}).items()}
    for definition in (table.get("indexes") or {}).values():
        if not definition.get("unique"):
            continue
        targets = definition.get("columns") or []
        if len(targets) != 1:
            continue
        name = targets[0]
        if name in columns and not columns[name].get("primary_key"):
            columns[name]["unique"] = True

    # A COMPOSITE PRIMARY KEY IS DECLARED ON THE TABLE AND FELT BY THE COLUMN.
    # `is_nullable` answers from the column alone, so it could see
    # `primary_key: True` on a single-column key and nothing at all on a
    # composite one. The database makes every key column NOT NULL either way,
    # so introspection returned `nullable: False` against a declaration that
    # said nothing, and the differ proposed making a primary key nullable on
    # every deploy. PostgreSQL refuses that, so the migration could never
    # succeed. Measured 2026-08-18:
    #
    #     declared     primary_key: ['left', 'right']
    #     introspected primary_key: ['left', 'right'], both nullable: False
    #     every deploy alter_column left  nullable False -> True
    #                  alter_column right nullable False -> True
    #
    # `is_nullable` records that this same mistake was made twice before, in
    # the differ and the validator, and was fixed by asking the question in
    # one place. This is the third: the question needs the TABLE when the key
    # is composite, and the function is handed a column. Folding here gives it
    # what it needs without every caller learning about composite keys.
    for name in table.get("primary_key") or []:
        if name in columns:
            columns[name]["primary_key"] = True

    return columns


@helper
def _diff_columns(
    table_name: str,
    current: Table,
    target: Table,
    dialect: Dialect,
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

    # ONE SPELLING FOR UNIQUENESS ON BOTH SIDES.
    #
    # A schema may say a column is unique in two ways: `unique: True` on the
    # column, or a single-column unique index in `indexes`. Introspection folds
    # the second into the first -- `apply_unique_columns` does it -- and a
    # declared schema usually writes whichever the author prefers.
    #
    # Comparing the flag without folding both sides made a table that declared
    # a unique INDEX look like a column that had lost its uniqueness, and the
    # differ proposed an alter_column on every deploy. The same shape as
    # comparing a translated default against its declared spelling, one screen
    # up: two ways of writing one fact, compared as though they were two facts.
    current_columns = _with_unique_folded(current)
    target_columns = _with_unique_folded(target)

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

        alter_details = _compute_column_alterations(current_col, target_col, dialect)
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


@helper
def _compute_column_alterations(
    current: Column,
    target: Column,
    dialect: Dialect,
) -> dict[str, Any] | None:
    """
    Compute what alterations are needed for a column.

    Returns None if no changes needed.
    """
    changes: dict[str, Any] = {}

    # Type change, compared AS THE ENGINE SPELLS IT. "uuid" and "text" are the
    # same column on SQLite, and comparing the two strings made every
    # migration after the first one propose a type change nobody asked for.
    if not same_type(current.get("type"), target.get("type"), dialect):
        changes["type"] = {"from": current.get("type"), "to": target.get("type")}

    # Nullability change
    current_nullable = is_nullable(current)
    target_nullable = is_nullable(target)
    if current_nullable != target_nullable:
        changes["nullable"] = {"from": current_nullable, "to": target_nullable}

    # Default change
    current_default = current.get("default")
    target_default = target.get("default")
    if not same_default(current_default, target_default, dialect):
        changes["default"] = {"from": current_default, "to": target_default}

    # EVERY OTHER DECLARED CONSTRAINT, which were compared by nothing.
    #
    # Measured 2026-08-17 at the table level: adding or removing `unique`,
    # and changing `check`, emitted NO operations, and `apply` then reported
    # success with the DDL unchanged. A user declares a constraint, deploys,
    # is told the migration succeeded, and it is not there.
    #
    # It also closed the only route out of a related failure. SQLite refuses
    # to drop a UNIQUE column, so the way through is to drop the constraint
    # first and the column second -- and step one did nothing, so step two
    # went on failing forever.
    # `check` IS BACK, AND ONLY BECAUSE ALL THREE INSPECTORS NOW READ ONE.
    #
    # It was added, shipped and removed within an hour earlier today. The
    # constraint was created and enforced and invisible to introspection, so
    # the differ saw None against the declared value on every deploy, and
    # `alter_column` rebuilds the table on SQLite -- a loop that copied,
    # dropped and renamed a table forever to apply a constraint already there.
    #
    # What made it safe was not a better comparison. It was teaching SQLite,
    # PostgreSQL and Turso to report a CHECK, and MEASURING the round trip on
    # each before turning this back on. Turso was the one that would have
    # looped: SQLite had just learned to read one and Turso had not, and
    # enabling this after checking only SQLite would have repeated the same
    # mistake on a different engine.
    # `check` ONLY WHERE THE STORED TEXT IS THE DECLARED TEXT.
    #
    # SQLite and Turso keep the CREATE TABLE verbatim, so what comes back is
    # what was written. PostgreSQL parses the expression and stores its own
    # canonical form: measured over seven expressions, five differ --
    # `n > 0 AND n < 100` becomes `(n > 0) AND (n < 100)`, `s <> ''` gains a
    # `::text` cast, and `s IN ('x','y')` becomes
    # `s = ANY (ARRAY['x'::text, 'y'::text])`.
    #
    # Comparing the declared text against that proposes an alter_column on
    # every deploy. I enabled this comparison after measuring the round trip
    # on all three engines and still shipped that loop, because I measured
    # every engine with ONE expression and `n > 0` is one of the two
    # PostgreSQL leaves alone.
    #
    # Matching it would mean reimplementing PostgreSQL's parser and printer,
    # and this function is pure so it cannot ask the server. So a CHECK change
    # is not noticed on PostgreSQL, which is where it stood before today.
    # SQLITE ONLY. Turso was in this list, and it rewrites a stored expression
    # too -- measured 2026-08-17 by the round-trip gate:
    #
    #     c <> ''         stored as  c != ''
    #     length(c) > 2   stored as  length (c) > 2
    #
    # The operator change is the one that settles it: `<>` and `!=` mean the
    # same thing and no amount of whitespace folding turns one into the other
    # without an expression parser. PostgreSQL is excluded for the same
    # reason. SQLite keeps the CREATE TABLE text verbatim, which is what makes
    # the comparison sound there and nowhere else.
    compared = ("unique", "check") if dialect == "sqlite" else ("unique",)
    for prop in compared:
        if not _same_constraint(prop, current.get(prop), target.get(prop)):
            changes[prop] = {"from": current.get(prop), "to": target.get(prop)}

    return changes if changes else None


@helper
def _canonical_spacing(expression: str) -> str:
    """An expression with its cosmetic whitespace and outer parens removed.

    Turso stores `datetime('now')` as `datetime ('now')`. The space carries no
    meaning, and a string literal is left alone because a space inside quotes
    is a character.
    """
    out: list[str] = []
    quote: str | None = None
    for char in expression.strip():
        if quote:
            out.append(char)
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
            out.append(char)
            continue
        if char == "(" and out and out[-1] == " ":
            out.pop()
        out.append(char)

    text = "".join(out)
    while text.startswith("(") and text.endswith(")"):
        depth = 0
        for position, char in enumerate(text):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and position != len(text) - 1:
                    return text
        text = text[1:-1].strip()
    return text


@helper
def _same_constraint(prop: str, current: Any, target: Any) -> bool:
    """Do these two spellings declare the same constraint?

    `unique: False` AND NO `unique` KEY ARE THE SAME FACT. Introspection omits
    the key for a column that is not unique, and a schema may write it out, so
    comparing them directly proposed an alter_column on every deploy:

        alter_column {"unique": {"from": None, "to": False}}

    Measured 2026-08-17 on SQLite and PostgreSQL, by a gate that tries every
    compared property against a real database rather than by anybody noticing.

    A CHECK is different: absent means no constraint and a string means one,
    so there is nothing to fold.
    """
    if prop == "unique":
        return bool(current) == bool(target)
    return bool(current == target)


# WHAT A NUMBER LOOKS LIKE WRITTEN DOWN, so that recognising one is a declared
# pattern rather than an exception caught from `float()`. Optional sign, digits
# either side of an optional point, optional exponent.
_NUMERIC = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?\Z")


@helper
def _as_number(value: Any) -> float | None:
    """The number this default denotes, or None if it does not denote one.

    A bare `0` is a number. `'0'` WITH QUOTES is the one-character string and
    must not fold into it, which is why the quotes are not stripped first.

    A BOOL IS NOT A NUMBER HERE even though Python says `True == 1`. A default
    of `true` and a default of `1` are different declarations, and folding
    them would report a changed column as unchanged.
    """
    kind = type(value)  # honest: ignore HC-P005: the exact type, so a bool is not an int
    if kind is bool:
        return None
    if kind is int or kind is float:
        return float(value)
    if kind is not str:
        return None
    return float(value) if _NUMERIC.match(value.strip()) else None


@helper
def _comparable(value: Any, spelled: dict[str, str]) -> Any:
    """One default, in the form both sides can be compared in.

    A default is a value on one side of the comparison and an expression on
    the other, so each is put into one form before they meet. Only text needs
    it: a number or a bool is already what it is.

    THE ENGINE'S OWN SPELLING FIRST. A default declared `now()` is WRITTEN to
    SQLite as `(datetime('now'))`, because SQLite cannot parse the first, and
    read back as what was written. Comparing the stored spelling against the
    declared one without knowing the translation happened proposed an
    alter_column on every deploy.

    THEN THE WHITESPACE. Turso inserts a space after a function name when it
    stores DDL, so `(datetime('now'))` came back as `((datetime ('now')))`
    and migrated on every deploy. Whitespace and redundant parentheses only:
    an operator rewrite is not foldable, which is why the CHECK comparison is
    SQLite-only.

    WRITTEN ONCE AND APPLIED TO BOTH SIDES. This was four inline guards, two
    per rule, one per argument. `same_default` has been fixed three times, for
    booleans, then numbers, then `now()`, and each fix added a guard. A rule
    that reaches one argument and not the other is the shape all three had.
    """
    return applied_to_text(
        lambda text: _canonical_spacing(
            spelled.get(text.strip().lower().replace(" ", ""), text)
        ),
        value,
    )


@helper
def _lowered(text: str) -> str:
    """A bare word, compared without regard to case."""
    return text.lower()


@helper
def _as_written(text: str) -> str:
    """A quoted literal: `'Pending'` and `'pending'` are two different values."""
    return text


# WHETHER THE TEXT IS A BARE WORD, as a table rather than a guard.
_FOLD_KEYWORD: dict[bool, Callable[[str], str]] = {
    True: _lowered,
    False: _as_written,
}


@helper
def _folded_keyword(text: str) -> str:
    """A bare keyword folded to lower case, a quoted literal left alone.

    A BARE KEYWORD IS CASE-INSENSITIVE, and this folded only booleans.
    PostgreSQL stores `current_timestamp` as `CURRENT_TIMESTAMP`, so a schema
    declaring the lowercase form migrated on every deploy:

        alter_column {"default": {"from": "CURRENT_TIMESTAMP",
                                  "to": "current_timestamp"}}
    """
    stripped = text.strip()
    return _FOLD_KEYWORD[stripped.replace("_", "").isalnum()](stripped)



@helper
def same_default(current: Any, target: Any, dialect: Dialect) -> bool:
    """Do these two spellings mean the same default?

    ONE QUESTION, RATHER THAN A LIST OF THE LITERALS SOMEBODY HAS BEEN BITTEN
    BY. This began as `_normalize_default`, which folded boolean literals
    because the loader emits "TRUE" for a Python bool and PostgreSQL hands back
    "false", so every migration re-emitted an alter_column that changed
    nothing. On 2026-08-17 the identical symptom arrived for numbers:

        default: {"from": "0", "to": 0}

    measured on a round trip against a real server. The schema declares the
    integer, the database returns the expression as text, and comparing them by
    identity proposes a change forever.

    Two instances of one bug in one place is the architecture talking. The
    cause is not that booleans and numbers were forgotten; it is that a default
    is a VALUE on one side and an EXPRESSION on the other, and identity is the
    wrong question to ask across that boundary. So this asks whether they
    denote the same thing, and falls back to verbatim comparison for the
    opaque case, which is where an expression like `now()` belongs.

    `dialect` IS REQUIRED, AND ITS ABSENCE COST TWO MIGRATION LOOPS IN A DAY.
    A default declared `now()` is WRITTEN to SQLite as `(datetime('now'))`,
    because SQLite cannot parse the first, and read back as what was written.
    Comparing the stored spelling against the declared one without knowing the
    translation happened proposes an `alter_column` on every deploy:

        alter_column {"default": {"from": "(datetime('now'))", "to": "now()"}}

    `same_type` one screen away has taken a dialect from the beginning and
    asks `map_type` whether two declared types name the same column ON THIS
    ENGINE. This is the same question about defaults, and the asymmetry
    between two functions doing one job is what should have been noticed the
    first time.

    WHAT IT DOES NOT DECIDE, stated so its silence is not read as agreement:
    whether `true` and `1` are the same default on SQLite, which stores
    booleans as integers. Not measured, so not folded.
    """
    if current is None or target is None:
        return current is target

    # The declared default, spelled the way this engine stores it. The applier
    # uses the same table on the way in, so the two sides are asking one
    # question rather than two.
    from declaro_persistum.applier.shared import EXPRESSION_DEFAULTS

    spelled = EXPRESSION_DEFAULTS.get(dialect, {})
    current = _comparable(current, spelled)
    target = _comparable(target, spelled)

    current_number = _as_number(current)
    target_number = _as_number(target)
    if current_number is not None and target_number is not None:
        return current_number == target_number

    # EACH SIDE FOLDED ON ITS OWN, THEN COMPARED. This asked whether both were
    # text and then whether both were bare words, so a rule that reached one
    # argument and not the other was the shape of all three bugs this function
    # has been fixed for.
    return bool(
        applied_to_text(_folded_keyword, current)
        == applied_to_text(_folded_keyword, target)
    )


@helper
def _same_fk_action(current: Any, target: Any) -> bool:
    """Whether two referential actions are the same constraint.

    NO ACTION IS WHAT SQL DOES WHEN NO CLAUSE IS WRITTEN, so `no action` and
    absence are one constraint spelled two ways. The database can only ever
    report the second: `PRAGMA foreign_key_list` gives `NO ACTION` for a plain
    `REFERENCES author(id)` and for an explicit `ON DELETE NO ACTION` alike,
    and the inspector drops it precisely because it cannot tell them apart.

    Comparing them with `!=` meant a model that declared `on_delete="no
    action"` proposed dropping and rebuilding its foreign key on every run,
    forever, applying a change that left the schema exactly as it found it.
    """
    absent = (None, "no action")
    return (current in absent and target in absent) or current == target


@helper
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

        # THE CASCADE IS PART OF THE FOREIGN KEY, and changing it was
        # invisible: `on_delete` cascade to restrict emitted nothing. Those are
        # opposite answers to "what happens to the children when the parent
        # goes", so a schema that changed its mind was told it had migrated.
        if current_ref and target_ref and current_ref == target_ref:
            for prop in ("on_delete", "on_update"):
                if not _same_fk_action(current_col.get(prop), target_col.get(prop)):
                    operations.append(
                        {
                            "op": "drop_foreign_key",
                            "table": table_name,
                            "details": {"column": col_name},
                        }
                    )
                    operations.append(
                        {
                            "op": "add_foreign_key",
                            "table": table_name,
                            "details": {
                                "column": col_name,
                                "references": target_ref,
                                **{
                                    k: target_col[k]
                                    for k in ("on_delete", "on_update")
                                    if k in target_col
                                },
                            },
                        }
                    )
                    break

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


_BARE_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


@helper
def _fold_predicate(expression: str) -> str:
    """One SQL predicate, in the spelling every engine would agree on.

    PostgreSQL does not keep the text an author wrote. It parses the clause and
    renders it back through `pg_get_expr`, which wraps the whole thing in
    parentheses and drops the quoting from any identifier that does not need
    it, so `WHERE "deleted_at" IS NULL` reads back as `(deleted_at IS NULL)`.
    Comparing those as strings proposed rebuilding the index on every run.

    QUOTES COME OFF ONLY WHERE THEY CARRY NOTHING. A quoted name that is
    already lower case and holds nothing but word characters means the same
    column unquoted, and PostgreSQL will render it that way. `"Email"` and
    `"my col"` keep theirs, because there the quoting is what makes the name
    mean that column: unquoted, `Email` folds to `email` and is a different
    column. Folding those would report a real difference as no change.

    `_canonical_spacing` cannot do this job. It was written for a default like
    `datetime ('now')` and deletes the space before an opening paren, which
    turns `(a > 0) AND (b > 0)` into something no engine emits.
    """
    text = " ".join(expression.split())

    out: list[str] = []
    position = 0
    while position < len(text):
        char = text[position]
        if char == '"':
            closing = text.find('"', position + 1)
            if closing == -1:
                out.append(text[position:])
                break
            name = text[position + 1 : closing]
            out.append(name if _BARE_IDENTIFIER.match(name) else f'"{name}"')
            position = closing + 1
            continue
        if char == "'":
            closing = text.find("'", position + 1)
            if closing == -1:
                out.append(text[position:])
                break
            out.append(text[position : closing + 1])
            position = closing + 1
            continue
        out.append(char)
        position += 1

    folded = "".join(out).strip()
    while folded.startswith("(") and folded.endswith(")"):
        depth = 0
        for index, char in enumerate(folded):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and index != len(folded) - 1:
                    return folded
        folded = folded[1:-1].strip()
    return folded


@helper
def _same_index(current: Index, target: Index) -> bool:
    """Do these two definitions declare the same index?

    The whole dict was compared with `!=`, which made the WHERE clause of a
    partial index a string comparison against whatever the engine chose to
    render. Columns and uniqueness compare directly; the predicate folds.
    """
    if list(current.get("columns", [])) != list(target.get("columns", [])):
        return False
    if bool(current.get("unique")) != bool(target.get("unique")):
        return False
    current_where = current.get("where")
    target_where = target.get("where")
    if current_where is None or target_where is None:
        return current_where == target_where
    return _fold_predicate(current_where) == _fold_predicate(target_where)


@helper
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

        if not _same_index(current_idx, target_idx):
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


@helper
def _same_constraint_definition(current: Any, target: Any) -> bool:
    """Two constraint definitions, compared as the engine stores them.

    A RAW DICT COMPARISON LOOPED ON TURSO. That engine rewrites `<>` to `!=`
    in the DDL it stores, so a declared `lo <> hi` came back as `lo != hi`,
    the two dicts differed, and every deploy emitted drop_constraint and
    add_constraint -- which Turso then refuses, so every deploy failed.
    Measured 2026-08-19 across four expression shapes; that was the only one
    that differed, and finding it took running all four rather than one.
    #
    The same normalisation the column-level CHECK comparison uses, plus the
    inequality fold. `_canonical_spacing` already strips the space-before-paren
    Turso adds and the redundant outer parens PostgreSQL adds.
    """
    # A DEFINITION THAT IS NOT A MAPPING HAS NO FIELDS TO COMPARE, so the two
    # are the same only if they are equal outright.
    both = {spelling_of(current), spelling_of(target)}
    if both != {dict}:
        return bool(current == target)
    if current.get("type") != target.get("type"):
        return False

    keys = set(current) | set(target)
    for key in keys - {"expression"}:
        if current.get(key) != target.get(key):
            return False

    return bool(
        _canonical_expression(current.get("expression"))
        == _canonical_expression(target.get("expression"))
    )


@helper
def _canonical_expression(expression: Any) -> Any:
    """One spelling for expressions the engines write differently."""
    return applied_to_text(
        lambda text: _canonical_spacing(text).replace("<>", "!="), expression
    )


@helper
def _diff_constraints(
    table_name: str,
    current: Table,
    target: Table,
    dialect: Dialect,
) -> list[Operation]:
    """Diff named constraints.

    COMPARED ON THE ENGINES THAT STORE THE EXPRESSION AS IT WAS WRITTEN, which
    is the same restriction `_compute_column_alterations` already puts on a
    column-level CHECK, for the same reason and after the same mistake.

    Measured 2026-08-19, declaring a constraint and reading it back:

        sqlite      lo < hi, lo <> hi, lo > 0 AND hi > 0, lo IN (1,2,3)  all verbatim
        turso       the same, except `<>` stored as `!=`
        postgresql  lo > 0 AND hi > 0  ->  (lo > 0) AND (hi > 0)
                    lo IN (1, 2, 3)    ->  not reported as a table constraint

    Turso's one rewriting is folded by `_canonical_expression`. PostgreSQL
    parenthesises each operand of an AND, rewrites IN into `= ANY (ARRAY[...])`
    on some shapes, and classifies a single-column named constraint as a
    column check, so comparing there proposed dropping and re-adding a
    constraint that had not changed.

    This package has shipped two CHECK migration loops already, both by
    enabling a comparison after checking one engine. The constraints are read
    back on all three so a rebuild can carry them; they are compared where the
    comparison is known to converge, and a changed constraint migrates nothing
    on PostgreSQL, which is the same standing limitation a changed column CHECK
    has there.
    """
    operations: list[Operation] = []

    if dialect not in ("sqlite", "turso"):
        return operations

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
        if not _same_constraint_definition(
            current_constraints[const_name], target_constraints[const_name]
        ):
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
