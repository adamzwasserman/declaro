"""What changes to a table's columns require, and in what order.

SPLIT OUT OF `core` because `core` passed 1000 code lines the moment
`dropping_a_column` was written, and a file nobody can hold in their head is
where a second answer to one question goes to hide. The column half calls one
thing outside itself and one thing outside calls in, so the cut is clean.

WHAT LIVES HERE. Everything about a column: whether two spellings of one are
the same, what a change to one requires, and what dropping one entails.
`dropping_a_column` is the one to read first. It writes the whole sequence a
drop needs, rather than emitting one operation and leaving the sort to
discover the rest by searching.
"""

from typing import Any

from declaro_persistum.applier.shared import same_type
from declaro_persistum.boundary import helper

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
from declaro_persistum.types import (
    Column,
    Dialect,
    Operation,
    Table,
    is_nullable,
)


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
def _indexes_over(table_name: str, column: str, current: Table) -> list[Operation]:
    """The indexes on this table that name this column.

    Read off the CURRENT table, which is what the database has, rather than
    off the target. Whether the author still declares the index is a separate
    question from whether the database still has one pointing at a column that
    is about to go.
    """
    return [
        {
            "op": "drop_index",
            "table": table_name,
            "details": {"index": index_name, "definition": definition},
        }
        for index_name, definition in (current.get("indexes") or {}).items()
        if column in (definition.get("columns") or [])
    ]


@helper
def _keys_on(table_name: str, column: str, current: Table) -> list[Operation]:
    """The foreign key this column carries, if it carries one."""
    return [
        {
            "op": "drop_foreign_key",
            "table": table_name,
            "details": {"column": column, "references": reference},
        }
        for definition in [(current.get("columns") or {}).get(column) or {}]
        for reference in [definition.get("references")]
        if reference
    ]


@helper
def dropping_a_column(table_name: str, column: str, current: Table) -> list[Operation]:
    """Everything dropping this column requires, in the order it requires it.

    A SEQUENCE, NOT A GRAPH. Whoever drops a column knows what dropping it
    entails: an index over the column has to go first, and so does a key on
    it. Saying so here is the whole of it, and nothing downstream has to work
    it out by searching the other operations for a match.

    THE SEARCH IS WHAT FAILED. The sort used to discover this by scanning
    every operation for a drop_index whose definition named the column. The
    drop_index carried no definition, the scan matched nothing, and the edge
    was never once added in any migration anyone ran. Then there was a second
    hole the scan could not reach at all: an author who removes a column and
    leaves the index declared gets no drop_index to find, and SQLite refuses
    the drop:

        error in index ix_email after drop column: no such column: "email"

    Measured against a real database on 2026-08-24. The migration dies, the
    column survives, and the next attempt emits the same one operation and
    dies the same way. Nothing converges.

    Both holes close the same way: the column drop carries its own
    requirements instead of hoping something downstream infers them.
    """
    return [
        *_keys_on(table_name, column, current),
        *_indexes_over(table_name, column, current),
        {"op": "drop_column", "table": table_name, "details": {"column": column}},
    ]


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

    # DROPPING A COLUMN IS A SEQUENCE, so `dropping_a_column` writes the whole
    # of it. Sorting `actual_drops` so two migrations of one schema emit the
    # same plan in the same order.
    operations.extend(
        operation
        for col_name in sorted(actual_drops)
        for operation in dropping_a_column(table_name, col_name, current)
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
        **{prop: _same_constraint(prop, current.get(prop), target.get(prop)) for prop in compared},
    }
    recorded: dict[str, Any] = {
        "type": {"from": current.get("type"), "to": target.get("type")},
        "nullable": {"from": current_nullable, "to": target_nullable},
        "default": {"from": current_default, "to": target_default},
        **{prop: {"from": current.get(prop), "to": target.get(prop)} for prop in compared},
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
    return _WHEN_EITHER_IS_ABSENT[current is None or target is None](current, target, dialect)


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
    return _COLUMN_KEY[(bool(current_ref), bool(target_ref), current_ref == target_ref)](
        table_name, col_name, current_col, target_col, changed_action
    )
