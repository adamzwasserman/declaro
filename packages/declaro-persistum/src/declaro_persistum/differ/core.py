"""
Core diff logic for schema comparison.

Implements the main diff(, current=NOTHING, target=NOTHING, dialect=NOTHING, decisions=NOTHING) function that computes operations needed
to transform current schema to target schema.
"""

import re
from collections.abc import Callable
from itertools import accumulate
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
    type_of,
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
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
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
    #
    # A rename is a table the target declares `renamed_from` on, naming a table
    # the current schema has and the target does not. What is left over on each
    # side is a real add and a real drop, which is set subtraction rather than
    # a search through the pairs.
    renames: list[tuple[str, str]] = [
        (renamed_from, added_table)
        for added_table in added_tables
        for renamed_from in [target[added_table].get("renamed_from")]
        if renamed_from and renamed_from in dropped_tables
    ]
    actual_adds = added_tables - {to_name for _, to_name in renames}
    actual_drops = dropped_tables - {from_name for from_name, _ in renames}

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
    operations.extend(
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
        for table_name in actual_drops
    )

    # Generate RENAME TABLE operations
    operations.extend(
        {
            "op": "rename_table",
            "table": from_name,
            "details": {"new_name": to_name},
        }
        for from_name, to_name in renames
    )

    # Generate CREATE TABLE operations
    operations.extend(
        operation
        for table_name in actual_adds
        for operation in _created_table_operations(table_name, target[table_name])
    )

    # Process modified tables
    #
    # A RENAMED TABLE IS NEVER ONE OF THESE. `effective_table_name` walked the
    # renames here looking for one whose source was this table, and no rename's
    # source can be: a source is in `current` and not in `target`, and every
    # name here is in both. The walk never found anything and the name it
    # produced was always the name it started with.
    operations.extend(
        operation
        for table_name in modified_tables
        for operation in _diff_table(
            table_name,
            current[table_name],
            target[table_name],
            dialect,
        )
    )

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
def _created_table_operations(table_name: str, target_table: Table) -> list[Operation]:
    """Everything a table declared for the first time needs, in order.

    AN INDEX DECLARED ON A NEW TABLE IS AN OPERATION OF ITS OWN.
    `create_table`'s details carry `indexes` and `create_table_sql` never
    looks at them, so a fresh migration created the table and none of its
    indexes. Measured 2026-08-17: two declared, zero created, and
    `declaro apply` reported success.

    It self-healed on the next deploy, which is what kept it quiet: the
    second diff noticed and emitted `add_index`. A deployment ran without
    the indexes it declared until somebody migrated again.

    Same fix as `insert_enum_values` got that morning, for the same reason:
    data the operation needed was sitting in the schema where the applier
    could not reach it. `add_index` already exists in the vocabulary and every
    applier already runs it.

    A lookup table that nothing fills rejects every value its column declares.
    The values are in the schema and the applier only sees operations, so the
    differ -- which holds both -- puts them in one.
    """
    return [
        {
            "op": "create_table",
            "table": table_name,
            "details": {
                "columns": target_table.get("columns", {}),
                "primary_key": target_table.get("primary_key"),
                "indexes": target_table.get("indexes", {}),
                "constraints": target_table.get("constraints", {}),
            },
        },
        *[
            {
                "op": "add_index",
                "table": table_name,
                "details": {"index": index_name, "definition": definition},
            }
            for index_name, definition in (target_table.get("indexes") or {}).items()
        ],
        *[
            {
                "op": "insert_enum_values",
                "table": table_name,
                "details": {"values": list(enum_values)},
            }
            for enum_values in [target_table.get("_enum_values")]
            if enum_values
        ],
    ]


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

    # The columns a single-column unique index makes unique. An index over two
    # columns says nothing about either one on its own, so it is left out.
    uniquely = {
        targets[0]
        for definition in (table.get("indexes") or {}).values()
        for targets in [list(definition.get("columns") or [])]
        if definition.get("unique") and len(targets) == 1
    }
    folded = {
        **columns,
        **{
            name: {**columns[name], "unique": True}
            for name in uniquely & set(columns)
            if not columns[name].get("primary_key")
        },
    }

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
    return {
        **folded,
        **{
            name: {**folded[name], "primary_key": True}
            for name in (table.get("primary_key") or [])
            if name in folded
        },
    }


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

    # Handle column renames (check renamed_from hints), the same set algebra
    # the table-level rename uses one screen up.
    renames: list[tuple[str, str]] = [
        (renamed_from, added_col)
        for added_col in added_cols
        for renamed_from in [target_columns[added_col].get("renamed_from")]
        if renamed_from and renamed_from in dropped_cols
    ]
    actual_adds = added_cols - {to_name for _, to_name in renames}
    actual_drops = dropped_cols - {from_name for from_name, _ in renames}

    # Generate DROP COLUMN operations
    operations.extend(
        {
            "op": "drop_column",
            "table": table_name,
            "details": {"column": col_name},
        }
        for col_name in actual_drops
    )

    # Generate RENAME COLUMN operations
    operations.extend(
        {
            "op": "rename_column",
            "table": table_name,
            "details": {"from_column": from_name, "to_column": to_name},
        }
        for from_name, to_name in renames
    )

    # Generate ADD COLUMN operations
    operations.extend(
        {
            "op": "add_column",
            "table": table_name,
            "details": {"column": col_name, "definition": target_columns[col_name]},
        }
        for col_name in actual_adds
    )

    # Check for column alterations
    operations.extend(
        {
            "op": "alter_column",
            "table": table_name,
            "details": {"column": col_name, "changes": alter_details},
        }
        for col_name in common_cols
        for alter_details in [
            _compute_column_alterations(
                current_columns[col_name], target_columns[col_name], dialect
            )
        ]
        if alter_details
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
    # Type change, compared AS THE ENGINE SPELLS IT. "uuid" and "text" are the
    # same column on SQLite, and comparing the two strings made every
    # migration after the first one propose a type change nobody asked for.
    current_nullable = is_nullable(current)
    target_nullable = is_nullable(target)
    current_default = current.get("default")
    target_default = target.get("default")

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

    # WHETHER EACH PROPERTY AGREES, AND WHAT WOULD BE WRITTEN IF IT DID NOT,
    # kept apart so the change record is one comprehension over the answers
    # rather than a branch per property.
    agreed: dict[str, bool] = {
        "type": same_type(current.get("type"), target.get("type"), dialect),
        "nullable": current_nullable == target_nullable,
        "default": same_default(current_default, target_default, dialect),
        **{
            prop: _same_constraint(prop, current.get(prop), target.get(prop))
            for prop in compared
        },
    }
    recorded: dict[str, Any] = {
        "type": {"from": current.get("type"), "to": target.get("type")},
        "nullable": {"from": current_nullable, "to": target_nullable},
        "default": {"from": current_default, "to": target_default},
        **{
            prop: {"from": current.get(prop), "to": target.get(prop)}
            for prop in compared
        },
    }
    changes: dict[str, Any] = {
        prop: recorded[prop] for prop, settled in agreed.items() if not settled
    }

    return changes if changes else None


# A STRING LITERAL, CLOSED OR NOT. The capture group makes `re.split` hand back
# the literals alongside what sits between them, so a rule that must not reach
# inside quotes is applied to the pieces at even positions only. The trailing
# `?` covers a quote nobody closed: the character scanner this replaced copied
# the rest of the text verbatim in that case, and so does this.
_STRING_LITERAL = re.compile(r"('[^']*'?|\"[^\"]*\"?)")

# The space Turso inserts in front of an opening paren when it stores DDL.
_SPACE_BEFORE_PAREN = re.compile(r" \(")


@helper
def _first_balanced(text: str) -> int:
    """Where the leading paren is closed, or -1 when nothing closes it."""
    closed = [
        position
        for position, depth in enumerate(
            accumulate(int(char == "(") - int(char == ")") for char in text)
        )
        if depth == 0
    ]
    return [*closed, -1][0]


@helper
def _wraps_the_whole(text: str) -> bool:
    """Whether one pair of parens encloses everything between them.

    `(a > 0) AND (b > 0)` does not: its leading paren closes at the seventh
    character rather than at the last one, so the parens are part of the
    expression and taking them off would change what it says.
    """
    return (
        text.startswith("(")
        and text.endswith(")")
        and _first_balanced(text) in (-1, len(text) - 1)
    )


@helper
def _peeled(text: str) -> str:
    """The parens carry nothing, so they come off and the rest is asked again."""
    return _without_outer_parens(text[1:-1].strip())


@helper
def _as_it_stands(text: str) -> str:
    """The parens belong to the expression, so the text is already right."""
    return text


_UNWRAP: dict[bool, Callable[[str], str]] = {True: _peeled, False: _as_it_stands}


@helper
def _without_outer_parens(text: str) -> str:
    """An expression with parentheses enclosing the whole of it removed.

    PostgreSQL renders an expression back through `pg_get_expr`, which wraps
    the whole thing, so the declared text and the stored text differ by a pair
    of parens that mean nothing.
    """
    return _UNWRAP[_wraps_the_whole(text)](text)


@helper
def _canonical_spacing(expression: str) -> str:
    """An expression with its cosmetic whitespace and outer parens removed.

    Turso stores `datetime('now')` as `datetime ('now')`. The space carries no
    meaning, and a string literal is left alone because a space inside quotes
    is a character.
    """
    spaced = "".join(
        piece if index % 2 else _SPACE_BEFORE_PAREN.sub("(", piece)
        for index, piece in enumerate(_STRING_LITERAL.split(expression.strip()))
    )
    return _without_outer_parens(spaced)


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
    return _CONSTRAINT_COMPARISON[prop](current, target)


@helper
def _same_flag(current: Any, target: Any) -> bool:
    """A flag, where absent and False say the same thing."""
    return bool(current) == bool(target)


@helper
def _same_text(current: Any, target: Any) -> bool:
    """An expression, where absent and empty are different declarations."""
    return bool(current == target)


# EACH COMPARED PROPERTY NAMES ITS OWN COMPARISON. Read with `[]`, so a
# property that joins the list without one is refused rather than quietly
# compared the way `unique` is.
_CONSTRAINT_COMPARISON: dict[str, Callable[[Any, Any], bool]] = {
    "unique": _same_flag,
    "check": _same_text,
}


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
    reading = [_NUMBER_IN[kind] for kind in [type_of(value)] if kind in _NUMBER_IN]
    return [*reading, _not_a_number][0](value)


@helper
def _not_a_number(_value: Any) -> float | None:
    """Nothing here denotes a number, and saying so is the answer."""
    return 0


@helper
def _the_number(value: Any) -> float | None:
    """An int or a float already is one."""
    return float(value)


@helper
def _the_number_in_text(value: Any) -> float | None:
    """Text denotes a number when it is written like one, and not otherwise."""
    return float(value) if _NUMERIC.match(value.strip()) else None


# THE EXACT TYPE, so a bool is not an int. `translated_by` would walk the class
# hierarchy and read `True` as 1.
_NUMBER_IN: dict[type, Callable[[Any], float | None]] = {
    bool: _not_a_number,
    int: _the_number,
    float: _the_number,
    str: _the_number_in_text,
}


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
    return _WHEN_EITHER_IS_ABSENT[current is None or target is None](
        current, target, dialect
    )


@helper
def _both_absent(current: Any, target: Any, _dialect: Dialect) -> bool:
    """One side declares no default, so they agree only if neither does."""
    return current is target


@helper
def _same_spelling(current: Any, target: Any, dialect: Dialect) -> bool:
    """Both sides declare a default, so the two spellings are compared."""
    # The declared default, spelled the way this engine stores it. The applier
    # uses the same table on the way in, so the two sides are asking one
    # question rather than two.
    from declaro_persistum.applier.shared import EXPRESSION_DEFAULTS

    spelled = EXPRESSION_DEFAULTS.get(dialect, {})
    current = _comparable(current, spelled)
    target = _comparable(target, spelled)

    current_number = _as_number(current)
    target_number = _as_number(target)

    return _WHEN_BOTH_ARE_NUMBERS[
        current_number is not None and target_number is not None
    ](current, target, current_number, target_number)


@helper
def _same_number(
    _current: Any, _target: Any, current_number: Any, target_number: Any
) -> bool:
    """Both denote a number, so the numbers decide it."""
    return bool(current_number == target_number)


@helper
def _same_keyword(
    current: Any, target: Any, _current_number: Any, _target_number: Any
) -> bool:
    """At least one is opaque, so the two spellings are compared verbatim.

    EACH SIDE FOLDED ON ITS OWN, THEN COMPARED. This asked whether both were
    text and then whether both were bare words, so a rule that reached one
    argument and not the other was the shape of all three bugs `same_default`
    has been fixed for.
    """
    return bool(
        applied_to_text(_folded_keyword, current)
        == applied_to_text(_folded_keyword, target)
    )


_WHEN_EITHER_IS_ABSENT: dict[bool, Callable[[Any, Any, Dialect], bool]] = {
    True: _both_absent,
    False: _same_spelling,
}

_WHEN_BOTH_ARE_NUMBERS: dict[bool, Callable[[Any, Any, Any, Any], bool]] = {
    True: _same_number,
    False: _same_keyword,
}


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
    columns = set(current_columns.keys()) | set(target_columns.keys())

    # THE CASCADE IS PART OF THE FOREIGN KEY, and changing it was invisible:
    # `on_delete` cascade to restrict emitted nothing. Those are opposite
    # answers to "what happens to the children when the parent goes", so a
    # schema that changed its mind was told it had migrated.
    #
    # A `for` STATEMENT RATHER THAN A COMPREHENSION, and it is the one loop
    # left here. `test_every_compared_property_round_trips` reads this function
    # with `ast` to learn which referential properties the differ compares, and
    # what it reads is a `for` over a tuple of their names. A comprehension is
    # a different node and would leave that gate reading nothing.
    changed_actions: set[str] = set()
    for prop in ("on_delete", "on_update"):
        changed_actions |= {
            col_name
            for col_name in columns
            if not _same_fk_action(
                current_columns.get(col_name, {}).get(prop),
                target_columns.get(col_name, {}).get(prop),
            )
        }

    return [
        operation
        for col_name in columns
        for operation in _column_key_operations(
            table_name,
            col_name,
            current_columns.get(col_name, {}),
            target_columns.get(col_name, {}),
            col_name in changed_actions,
        )
    ]


@helper
def _no_key_change(
    _table_name: str,
    _col_name: str,
    _current_col: Column,
    _target_col: Column,
    _changed_action: bool,
) -> list[Operation]:
    """Neither side declares a key, so nothing migrates."""
    return []


@helper
def _dropped_key(
    table_name: str,
    col_name: str,
    current_col: Column,
    _target_col: Column,
    _changed_action: bool,
) -> list[Operation]:
    """The key was removed from the models, so it leaves the database."""
    return [
        {
            "op": "drop_foreign_key",
            "table": table_name,
            "details": {
                "column": col_name,
                "references": current_col.get("references"),
            },
        }
    ]


@helper
def _added_key(
    table_name: str,
    col_name: str,
    _current_col: Column,
    target_col: Column,
    _changed_action: bool,
) -> list[Operation]:
    """The models declare a key the database does not have."""
    return [
        {
            "op": "add_foreign_key",
            "table": table_name,
            "details": {
                "column": col_name,
                "references": target_col.get("references"),
                "on_delete": target_col.get("on_delete"),
                "on_update": target_col.get("on_update"),
            },
        }
    ]


@helper
def _replaced_key(
    table_name: str,
    col_name: str,
    current_col: Column,
    target_col: Column,
    changed_action: bool,
) -> list[Operation]:
    """The key points somewhere else now, so the old one goes and a new one comes."""
    return _dropped_key(
        table_name, col_name, current_col, target_col, changed_action
    ) + _added_key(table_name, col_name, current_col, target_col, changed_action)


@helper
def _restated_key(
    table_name: str,
    col_name: str,
    _current_col: Column,
    target_col: Column,
) -> list[Operation]:
    """The key points where it did and does something else on delete or update.

    The declared referential actions are carried across only where the models
    write them, so a key declared without an `ON DELETE` clause is restated
    without one rather than with an explicit NULL.
    """
    return [
        {
            "op": "drop_foreign_key",
            "table": table_name,
            "details": {"column": col_name},
        },
        {
            "op": "add_foreign_key",
            "table": table_name,
            "details": {
                "column": col_name,
                "references": target_col.get("references"),
                **{
                    key: target_col[key]
                    for key in ("on_delete", "on_update")
                    if key in target_col
                },
            },
        },
    ]


@helper
def _kept_key(
    _table_name: str,
    _col_name: str,
    _current_col: Column,
    _target_col: Column,
) -> list[Operation]:
    """Both sides declare the same key doing the same thing."""
    return []


_WHEN_ACTION_CHANGED: dict[
    bool, Callable[[str, str, Column, Column], list[Operation]]
] = {
    True: _restated_key,
    False: _kept_key,
}


@helper
def _same_reference(
    table_name: str,
    col_name: str,
    current_col: Column,
    target_col: Column,
    changed_action: bool,
) -> list[Operation]:
    """The key points where it did, so only its actions can have changed."""
    return _WHEN_ACTION_CHANGED[changed_action](
        table_name, col_name, current_col, target_col
    )


# WHAT THE TWO SIDES SAY ABOUT ONE COLUMN'S FOREIGN KEY, as the three facts
# that decide it: whether each side declares one, and whether the two name the
# same target. Every combination those three can take is a key here, so an
# unlisted one is a state nobody has thought about rather than a silent no-op.
_COLUMN_KEY: dict[
    tuple[bool, bool, bool], Callable[[str, str, Column, Column, bool], list[Operation]]
] = {
    (True, True, True): _same_reference,
    (True, True, False): _replaced_key,
    (True, False, False): _dropped_key,
    (False, True, False): _added_key,
    (False, False, True): _no_key_change,
    (False, False, False): _replaced_key,
}


@helper
def _column_key_operations(
    table_name: str,
    col_name: str,
    current_col: Column,
    target_col: Column,
    changed_action: bool,
) -> list[Operation]:
    """The operations one column's foreign key needs, if any."""
    current_ref = current_col.get("references")
    target_ref = target_col.get("references")
    return _COLUMN_KEY[
        (bool(current_ref), bool(target_ref), current_ref == target_ref)
    ](table_name, col_name, current_col, target_col, changed_action)


# A QUOTED NAME THAT NEEDS NO QUOTING: lower case, word characters, and not
# opening with a digit. PostgreSQL renders such a name with the quotes off.
_REDUNDANTLY_QUOTED = re.compile(r'^"[a-z_][a-z0-9_]*"$')


@helper
def _unquoted(piece: str) -> str:
    """The quotes carry nothing, so the bare name means the same column."""
    return piece[1:-1]


@helper
def _quoted_as_written(piece: str) -> str:
    """A string literal, or a name whose quotes are what make it that column."""
    return piece


_QUOTING: dict[bool, Callable[[str], str]] = {
    True: _unquoted,
    False: _quoted_as_written,
}


@helper
def _folded_quoting(piece: str) -> str:
    """One quoted piece of a predicate, spelled the way PostgreSQL renders it."""
    return _QUOTING[bool(_REDUNDANTLY_QUOTED.match(piece))](piece)


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

    THE SAME SPLIT `_canonical_spacing` USES, rather than a second scanner. A
    hand-written walk over the characters read quotes here and `_STRING_LITERAL`
    read them one screen up, so one file held two answers to "where does a
    quoted piece start and stop". The pieces at odd positions are the quoted
    ones, closed or not, and a rule that must not reach inside a literal is
    applied to those alone.

    THE OUTER PARENS COME OFF THROUGH `_without_outer_parens`, which is what
    the loop that used to sit here spelled out a second time. Checked against
    the loop over 216,136 inputs, every string up to four characters over the
    alphabet that matters here among them: the two agree everywhere.
    """
    text = " ".join(expression.split())
    folded = "".join(
        _folded_quoting(piece) if index % 2 else piece
        for index, piece in enumerate(_STRING_LITERAL.split(text))
    ).strip()
    return _without_outer_parens(folded)


@helper
def _same_index(current: Index, target: Index) -> bool:
    """Do these two definitions declare the same index?

    The whole dict was compared with `!=`, which made the WHERE clause of a
    partial index a string comparison against whatever the engine chose to
    render. Columns and uniqueness compare directly; the predicate folds.
    """
    return (
        list(current.get("columns", [])) == list(target.get("columns", []))
        and bool(current.get("unique")) == bool(target.get("unique"))
        and _same_predicate(current.get("where"), target.get("where"))
    )


@helper
def _same_predicate(current: Any, target: Any) -> bool:
    """Do these two WHERE clauses restrict the index to the same rows?"""
    return _WHEN_A_PREDICATE_IS_ABSENT[current is None or target is None](
        current, target
    )


@helper
def _one_predicate_absent(current: Any, target: Any) -> bool:
    """One side restricts nothing, so the two agree only if neither does."""
    return bool(current == target)


@helper
def _both_predicates_folded(current: Any, target: Any) -> bool:
    """Both sides restrict, so the two spellings fold before they are compared."""
    return _fold_predicate(current) == _fold_predicate(target)


_WHEN_A_PREDICATE_IS_ABSENT: dict[bool, Callable[[Any, Any], bool]] = {
    True: _one_predicate_absent,
    False: _both_predicates_folded,
}


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
    current_indexes = current.get("indexes", {})
    target_indexes = target.get("indexes", {})

    current_names = set(current_indexes.keys())
    target_names = set(target_indexes.keys())

    # An index both sides declare, spelled two different ways. It is dropped
    # and made again, which is the same pair a removed index and an added one
    # produce between them.
    redefined = [
        idx_name
        for idx_name in current_names & target_names
        if not _same_index(current_indexes[idx_name], target_indexes[idx_name])
    ]

    # Drop removed indexes
    dropped: list[Operation] = [
        {
            "op": "drop_index",
            "table": table_name,
            "details": {"index": idx_name},
        }
        for idx_name in current_names - target_names
    ]
    # Add new indexes
    added: list[Operation] = [
        {
            "op": "add_index",
            "table": table_name,
            "details": {"index": idx_name, "definition": target_indexes[idx_name]},
        }
        for idx_name in target_names - current_names
    ]
    # Replace the redefined ones
    replaced: list[Operation] = [
        operation
        for idx_name in redefined
        for operation in _replaced_index(
            table_name, idx_name, target_indexes[idx_name]
        )
    ]
    return [*dropped, *added, *replaced]


@helper
def _replaced_index(table_name: str, idx_name: str, definition: Index) -> list[Operation]:
    """An index the two sides spell differently: the old goes, the new comes."""
    return [
        {
            "op": "drop_index",
            "table": table_name,
            "details": {"index": idx_name},
        },
        {
            "op": "add_index",
            "table": table_name,
            "details": {"index": idx_name, "definition": definition},
        },
    ]


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
    both = {type_of(current), type_of(target)}
    return _WHEN_BOTH_ARE_MAPPINGS[both == {dict}](current, target)


@helper
def _equal_outright(current: Any, target: Any) -> bool:
    """At least one definition is not a mapping, so equality decides it."""
    return bool(current == target)


@helper
def _same_fields(current: Any, target: Any) -> bool:
    """Two mappings, field by field.

    Every field but the expression compares as it stands. `type` is one of
    them: it was compared on its own first and then again in the walk over the
    keys, which asked the same question twice and could only ever give the
    same answer.
    """
    return bool(
        all(
            current.get(key) == target.get(key)
            for key in (set(current) | set(target)) - {"expression"}
        )
        and _canonical_expression(current.get("expression"))
        == _canonical_expression(target.get("expression"))
    )


_WHEN_BOTH_ARE_MAPPINGS: dict[bool, Callable[[Any, Any], bool]] = {
    True: _same_fields,
    False: _equal_outright,
}


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
    return _CONSTRAINTS_ON[dialect](table_name, current, target)


@helper
def _constraints_not_compared(
    _table_name: str,
    _current: Table,
    _target: Table,
) -> list[Operation]:
    """PostgreSQL rewrites the expression it stores, so nothing is compared.

    The reasoning is in `_diff_constraints` above: the stored text is not the
    written text there, so a comparison proposes a change on every deploy.
    """
    return []


@helper
def _compared_constraints(
    table_name: str,
    current: Table,
    target: Table,
) -> list[Operation]:
    """The constraint operations for an engine that stores what was written."""
    current_constraints = current.get("constraints", {})
    target_constraints = target.get("constraints", {})

    current_names = set(current_constraints.keys())
    target_names = set(target_constraints.keys())

    # A constraint both sides declare, spelled two different ways. It is
    # dropped and added again, the same pair a removal and an addition make.
    redefined = [
        const_name
        for const_name in current_names & target_names
        if not _same_constraint_definition(
            current_constraints[const_name], target_constraints[const_name]
        )
    ]

    # Drop removed constraints
    dropped: list[Operation] = [
        {
            "op": "drop_constraint",
            "table": table_name,
            "details": {"constraint": const_name},
        }
        for const_name in current_names - target_names
    ]
    # Add new constraints
    added: list[Operation] = [
        {
            "op": "add_constraint",
            "table": table_name,
            "details": {
                "constraint": const_name,
                "definition": target_constraints[const_name],
            },
        }
        for const_name in target_names - current_names
    ]
    # Replace the redefined ones
    replaced: list[Operation] = [
        operation
        for const_name in redefined
        for operation in _replaced_constraint(
            table_name, const_name, target_constraints[const_name]
        )
    ]
    return [*dropped, *added, *replaced]


@helper
def _replaced_constraint(
    table_name: str, const_name: str, definition: Any
) -> list[Operation]:
    """A constraint the two sides spell differently: the old goes, the new comes."""
    return [
        {
            "op": "drop_constraint",
            "table": table_name,
            "details": {"constraint": const_name},
        },
        {
            "op": "add_constraint",
            "table": table_name,
            "details": {"constraint": const_name, "definition": definition},
        },
    ]


# WHICH ENGINES A NAMED CONSTRAINT IS COMPARED ON, named one by one. Read with
# `[]`, so an engine that joins the vocabulary without an entry here is refused
# rather than quietly treated like PostgreSQL.
_CONSTRAINTS_ON: dict[Dialect, Callable[[str, Table, Table], list[Operation]]] = {
    "sqlite": _compared_constraints,
    "turso": _compared_constraints,
    "postgresql": _constraints_not_compared,
}
