"""
Core diff logic for schema comparison.

Implements the main diff(, current=NOTHING, target=NOTHING, engine=NOTHING, decisions=NOTHING) function that computes operations needed
to transform current schema to target schema.
"""

from typing import Any

from declaro_persistum.boundary import helper
from declaro_persistum.differ.ambiguity import detect_ambiguities

# THE COLUMN HALF LIVES IN `columns` NOW, because this file passed 1000 code
# lines. Re-exported so nothing that imports from here had to change.
from declaro_persistum.differ.columns import (  # noqa: F401
    _column_key_operations as _column_key_operations,
)
from declaro_persistum.differ.columns import (
    _compute_column_alterations as _compute_column_alterations,
)
from declaro_persistum.differ.columns import (
    _diff_columns as _diff_columns,
)
from declaro_persistum.differ.columns import (
    _diff_foreign_keys as _diff_foreign_keys,
)
from declaro_persistum.differ.columns import (
    _indexes_over as _indexes_over,
)
from declaro_persistum.differ.columns import (
    _keys_on as _keys_on,
)
from declaro_persistum.differ.columns import (
    _same_constraint as _same_constraint,
)
from declaro_persistum.differ.columns import (
    _same_fk_action as _same_fk_action,
)
from declaro_persistum.differ.columns import (
    _with_unique_folded as _with_unique_folded,
)
from declaro_persistum.differ.columns import (
    dropping_a_column as dropping_a_column,
)
from declaro_persistum.differ.columns import (
    same_default as same_default,
)

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
    Decision,
    DiffResult,
    Engine,
    Index,
    Nothing,
    Operation,
    Schema,
    Table,
    is_our_table,
    resolved,
)


@helper
def diff(
    current: Schema,
    target: Schema,
    *,
    engine: Engine,
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

    THE ENGINE IS REQUIRED, AND WITHOUT IT THIS WAS NOT IDEMPOTENT.
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
        engine: The engine, because a type comparison is meaningless without
            one. Required for the same reason `map_type` requires it (Rule 14)
        decisions: Pre-made decisions for ambiguous changes (from pending.toml)

    Returns:
        DiffResult with operations, dependencies, and execution order

    Raises:
        CycleError: If operation dependencies form a cycle

    Example:
        >>> current = {"users": {"columns": {"id": {"type": "integer"}}}}
        >>> target = {"users": {"columns": {"id": {"type": "integer"}, "email": {"type": "text"}}}}
        >>> result = diff(current, target, engine="postgresql", decisions=NOTHING)
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
            engine,
        )
    )

    # Detect ambiguities
    ambiguities = detect_ambiguities(current, target, engine, decisions)

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
    engine: Engine,
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
    column_ops = _diff_columns(table_name, current, target, engine)
    operations.extend(column_ops)

    # AN INDEX A COLUMN DROP ALREADY TAKES IS NOT THE INDEX DIFF'S BUSINESS.
    # `dropping_a_column` writes the drops its column requires, so the index
    # diff would otherwise write a second drop for the same index and the
    # engine would refuse it as missing.
    already_dropped = {
        operation["details"]["index"] for operation in column_ops if operation["op"] == "drop_index"
    }
    index_ops = _diff_indexes(table_name, current, target, already_dropped)
    operations.extend(index_ops)

    # Diff constraints
    constraint_ops = _diff_constraints(table_name, current, target, engine)
    operations.extend(constraint_ops)

    return operations


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
    return _WHEN_A_PREDICATE_IS_ABSENT[current is None or target is None](current, target)


@helper
def _diff_indexes(
    table_name: str,
    current: Table,
    target: Table,
    already_dropped: set[str],
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
        for idx_name in sorted(current_names - target_names - already_dropped)
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
        for operation in _replaced_index(table_name, idx_name, target_indexes[idx_name])
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
    engine: Engine,
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
    return _CONSTRAINTS_ON[engine](table_name, current, target)
