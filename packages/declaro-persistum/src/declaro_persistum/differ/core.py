"""
Core diff logic for schema comparison.

Implements the main diff(, current=NOTHING, target=NOTHING, dialect=NOTHING, decisions=NOTHING) function that computes operations needed
to transform current schema to target schema.
"""

from typing import Any

from declaro_persistum.applier.shared import same_type
from declaro_persistum.boundary import helper
from declaro_persistum.differ.ambiguity import detect_ambiguities

# The tables and what each entry does. Re-exported so nothing that
# imports from here had to change.
from declaro_persistum.differ.comparisons import (  # noqa: F401
    _COLUMN_KEY as _COLUMN_KEY,
)
from declaro_persistum.differ.comparisons import (
    _CONSTRAINT_COMPARISON as _CONSTRAINT_COMPARISON,
)
from declaro_persistum.differ.comparisons import (
    _CONSTRAINTS_ON as _CONSTRAINTS_ON,
)
from declaro_persistum.differ.comparisons import (
    _FOLD_KEYWORD as _FOLD_KEYWORD,
)
from declaro_persistum.differ.comparisons import (
    _NUMBER_IN as _NUMBER_IN,
)
from declaro_persistum.differ.comparisons import (
    _NUMERIC as _NUMERIC,
)
from declaro_persistum.differ.comparisons import (
    _QUOTING as _QUOTING,
)
from declaro_persistum.differ.comparisons import (
    _REDUNDANTLY_QUOTED as _REDUNDANTLY_QUOTED,
)
from declaro_persistum.differ.comparisons import (
    _SPACE_BEFORE_PAREN as _SPACE_BEFORE_PAREN,
)
from declaro_persistum.differ.comparisons import (
    _STRING_LITERAL as _STRING_LITERAL,
)
from declaro_persistum.differ.comparisons import (
    _UNWRAP as _UNWRAP,
)
from declaro_persistum.differ.comparisons import (
    _WHEN_A_PREDICATE_IS_ABSENT as _WHEN_A_PREDICATE_IS_ABSENT,
)
from declaro_persistum.differ.comparisons import (
    _WHEN_ACTION_CHANGED as _WHEN_ACTION_CHANGED,
)
from declaro_persistum.differ.comparisons import (
    _WHEN_BOTH_ARE_MAPPINGS as _WHEN_BOTH_ARE_MAPPINGS,
)
from declaro_persistum.differ.comparisons import (
    _WHEN_BOTH_ARE_NUMBERS as _WHEN_BOTH_ARE_NUMBERS,
)
from declaro_persistum.differ.comparisons import (
    _WHEN_EITHER_IS_ABSENT as _WHEN_EITHER_IS_ABSENT,
)
from declaro_persistum.differ.comparisons import (
    _added_key as _added_key,
)
from declaro_persistum.differ.comparisons import (
    _as_it_stands as _as_it_stands,
)
from declaro_persistum.differ.comparisons import (
    _as_number as _as_number,
)
from declaro_persistum.differ.comparisons import (
    _as_written as _as_written,
)
from declaro_persistum.differ.comparisons import (
    _both_absent as _both_absent,
)
from declaro_persistum.differ.comparisons import (
    _both_predicates_folded as _both_predicates_folded,
)
from declaro_persistum.differ.comparisons import (
    _canonical_expression as _canonical_expression,
)
from declaro_persistum.differ.comparisons import (
    _canonical_spacing as _canonical_spacing,
)
from declaro_persistum.differ.comparisons import (
    _comparable as _comparable,
)
from declaro_persistum.differ.comparisons import (
    _compared_constraints as _compared_constraints,
)
from declaro_persistum.differ.comparisons import (
    _constraints_not_compared as _constraints_not_compared,
)
from declaro_persistum.differ.comparisons import (
    _dropped_key as _dropped_key,
)
from declaro_persistum.differ.comparisons import (
    _equal_outright as _equal_outright,
)
from declaro_persistum.differ.comparisons import (
    _first_balanced as _first_balanced,
)
from declaro_persistum.differ.comparisons import (
    _fold_predicate as _fold_predicate,
)
from declaro_persistum.differ.comparisons import (
    _folded_keyword as _folded_keyword,
)
from declaro_persistum.differ.comparisons import (
    _folded_quoting as _folded_quoting,
)
from declaro_persistum.differ.comparisons import (
    _kept_key as _kept_key,
)
from declaro_persistum.differ.comparisons import (
    _lowered as _lowered,
)
from declaro_persistum.differ.comparisons import (
    _no_key_change as _no_key_change,
)
from declaro_persistum.differ.comparisons import (
    _not_a_number as _not_a_number,
)
from declaro_persistum.differ.comparisons import (
    _one_predicate_absent as _one_predicate_absent,
)
from declaro_persistum.differ.comparisons import (
    _peeled as _peeled,
)
from declaro_persistum.differ.comparisons import (
    _quoted_as_written as _quoted_as_written,
)
from declaro_persistum.differ.comparisons import (
    _replaced_constraint as _replaced_constraint,
)
from declaro_persistum.differ.comparisons import (
    _replaced_key as _replaced_key,
)
from declaro_persistum.differ.comparisons import (
    _restated_key as _restated_key,
)
from declaro_persistum.differ.comparisons import (
    _same_constraint_definition as _same_constraint_definition,
)
from declaro_persistum.differ.comparisons import (
    _same_fields as _same_fields,
)
from declaro_persistum.differ.comparisons import (
    _same_flag as _same_flag,
)
from declaro_persistum.differ.comparisons import (
    _same_keyword as _same_keyword,
)
from declaro_persistum.differ.comparisons import (
    _same_number as _same_number,
)
from declaro_persistum.differ.comparisons import (
    _same_reference as _same_reference,
)
from declaro_persistum.differ.comparisons import (
    _same_spelling as _same_spelling,
)
from declaro_persistum.differ.comparisons import (
    _same_text as _same_text,
)
from declaro_persistum.differ.comparisons import (
    _the_number as _the_number,
)
from declaro_persistum.differ.comparisons import (
    _the_number_in_text as _the_number_in_text,
)
from declaro_persistum.differ.comparisons import (
    _unquoted as _unquoted,
)
from declaro_persistum.differ.comparisons import (
    _without_outer_parens as _without_outer_parens,
)
from declaro_persistum.differ.comparisons import (
    _wraps_the_whole as _wraps_the_whole,
)
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
    is_nullable,
    is_our_table,
    resolved,
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
def _with_unique_folded(table: Table) -> dict[str, Column]:
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

    # A DROP CARRIES THE DEFINITION IT IS DROPPING. The sort needs to know
    # which columns an index covers, so that dropping the index is ordered
    # before dropping or retyping one of those columns. It read
    # `details["definition"]["columns"]` off this operation, which never
    # carried a definition, and `.get("definition", {})` turned that into an
    # empty list. The comparison then matched nothing, no edge was ever added,
    # and the order was whatever Kahn's algorithm happened to emit. SQLite
    # refuses to drop a column an index covers, so the failure depended on
    # which engine ran the migration.
    dropped: list[Operation] = [
        {
            "op": "drop_index",
            "table": table_name,
            "details": {
                "index": idx_name,
                "definition": current_indexes[idx_name],
            },
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
            "details": {"index": idx_name, "definition": definition},
        },
        {
            "op": "add_index",
            "table": table_name,
            "details": {"index": idx_name, "definition": definition},
        },
    ]


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
