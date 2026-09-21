"""`fn capability_of`: what one engine does about one feature.

Three engines do not offer the same things. PostgreSQL has a materialized view
and SQLite does not, so one is built from a table, its triggers and a registry
row. This is the one place that says which, and `fn mechanism_for` reads it to
pick how to carry a declaration out on the engine in front of it.

IT IS A TABLE AND ONLY A TABLE, AND EVERY CELL IS FILLED. Three engines crossed
with five features is fifteen cells, and all fifteen are below. A lookup with a
fallback would answer for a combination nobody decided, in the voice of one
somebody did, and the wrong answer would not fail here. It would fail later, as
a statement the engine rejects, three layers from the cell that was wrong.

A COMBINATION THAT IS NOT IN THE VOCABULARY RAISES, NAMING BOTH. `Engine` and
`Feature` are closed sets, so a word outside them is a caller asking about a
package that does not exist. The key is the pair, so the `KeyError` carries the
engine and the feature together, and nothing is guessed on the way out. The
declaration gives this function no `Fault` arm, which is the same decision: an
engine always has an answer about a feature, even when that answer is that it
cannot, so there is no failure to hand back as a value.

WHAT A CELL SAYS. `native` is a claim about the engine. `built_from` is empty
when it is native, and otherwise names three things, because a cell that covers
only writing covers half a feature: what creates the capability, what removes
it, and how the inspector recognizes it afterwards. An emulated materialized
view is a real table, so an inspector that cannot tell it from a table reports
a table, and the next difference proposes dropping it.

WHERE THE NATIVE CLAIMS COME FROM. Every one was measured by execution on
2026-09-21 against the engine itself, not read from a compatibility document:
sqlite3 3.x through the standard library, pyturso through `turso.connect`, and
PostgreSQL 17.10 through asyncpg. What was run, and what came back:

    materialized_view   postgresql  CREATE MATERIALIZED VIEW, then REFRESH, both ran
                        sqlite      `near "MATERIALIZED": syntax error`
                        turso       `CREATE MATERIALIZED VIEW is an experimental
                                    feature. Enable with --experimental-views flag`
    check_constraint    all three   the CHECK was declared and a violating row was
                                    refused with an IntegrityError
    partial_index       all three   CREATE INDEX ... WHERE ran, and on both SQLite
                                    engines PRAGMA index_list reported it partial
    enum                postgresql  CREATE TYPE ... AS ENUM, and a row took a value
                        sqlite      `near "TYPE": syntax error`
                        turso       `expected STRUCT or UNION after AS, got ENUM`
    range_type          postgresql  int4range held '[1,3)' and answered `span @> 2`
                        sqlite      the column was accepted and stored text; `@>`
                                    failed with `unrecognized token: "@"`
                        turso       the same, refusing `@>` as an experimental
                                    custom type

The two SQLite engines accept `int4range` as a column type and mean nothing by
it, which is why the measurement asked the containment question rather than
stopping at the CREATE TABLE. A probe that only runs the DDL reports a range
type on an engine that has none.

ONE CELL IS NOT DECIDED BY ANY DOCUMENT HERE, AND IT IS NAMED SO NOBODY MISSES
IT. `range_type` on sqlite and on turso: that neither engine has one is
measured, but nothing in `docs/architecture.md`, in the feature files or in the
declaration says what should build one. The recipe below is the ordinary one --
two bound columns and a check constraint -- and it is written down rather than
left out so the grid is complete, but nobody has agreed to it. Read it before
`mechanism_for` does.
"""

from __future__ import annotations

from declaro_persistum.types import Capability, Engine, Feature

# WHAT BUILDS A MATERIALIZED VIEW ON AN ENGINE THAT HAS NONE. The words are
# `docs/architecture.md`'s own: a table, its triggers and a registry row. The
# registry is what `mechanism_for` asks three questions of -- is this one, what
# was registered, and what is in there -- so recognition is a lookup and never a
# guess from the name. A real table called `sales_summary` is not a view.
_A_MATERIALIZED_VIEW_ON_AN_ENGINE_WITHOUT_ONE = (
    "created: a table holding the rows, the triggers that keep it current, "
    "and a registry row naming it a view; "
    "dropped: the triggers first, then the registry row, then the table, because "
    "DROP VIEW against a table leaves the object sitting there; "
    "recognized: the inspector asks the registry, so the table reads back as the "
    "view it stands for rather than as a table the next difference would drop"
)

# WHAT BUILDS AN ENUM. A CHECK constraint carrying the values was the other
# candidate and it is not portable, so the values go in a table and the column
# points at it. `insert_enum_values` is an operation the differ already emits,
# which is what puts the values there beside the table that holds them.
_AN_ENUM_ON_AN_ENGINE_WITHOUT_ONE = (
    "created: a lookup table holding the permitted values, an insert of those "
    "values into it, and a foreign key from the column to that table; "
    "dropped: the foreign key with its column, then the lookup table once no "
    "column references it; "
    "recognized: the inspector reads the lookup table's rows and reports them as "
    "the values the column permits"
)

# NOBODY HAS DECIDED THIS ONE. See the module docstring: the engines were
# measured and neither has a range type, but no document in this package says
# what should stand in for one. This is the ordinary answer, not an agreed one.
_A_RANGE_ON_AN_ENGINE_WITHOUT_ONE = (
    "created: a column for the lower bound, a column for the upper bound, and a "
    "check constraint that the lower is not above the upper; "
    "dropped: the check constraint, then both columns; "
    "recognized: the inspector reads the pair of columns together with the check "
    "constraint and reports one range column"
)

# A NATIVE CELL BUILDS NOTHING, and says so by carrying nothing. The two halves
# of a cell answer one question between them, so an engine that has the feature
# has no second answer to give about what supplies it.
_THE_ENGINE_HAS_IT = ""

# EVERY ENGINE CROSSED WITH EVERY FEATURE. The key is the pair, so a word
# outside either vocabulary raises naming both, and there is no `.get` and no
# default anywhere below: a cell nobody filled is a KeyError at the lookup
# rather than a wrong answer further down.
_THE_TABLE: dict[tuple[Engine, Feature], tuple[bool, str]] = {
    # --- materialized_view ---------------------------------------------------
    ("postgresql", "materialized_view"): (True, _THE_ENGINE_HAS_IT),
    ("sqlite", "materialized_view"): (False, _A_MATERIALIZED_VIEW_ON_AN_ENGINE_WITHOUT_ONE),
    ("turso", "materialized_view"): (False, _A_MATERIALIZED_VIEW_ON_AN_ENGINE_WITHOUT_ONE),
    # --- check_constraint ----------------------------------------------------
    ("postgresql", "check_constraint"): (True, _THE_ENGINE_HAS_IT),
    ("sqlite", "check_constraint"): (True, _THE_ENGINE_HAS_IT),
    ("turso", "check_constraint"): (True, _THE_ENGINE_HAS_IT),
    # --- partial_index -------------------------------------------------------
    ("postgresql", "partial_index"): (True, _THE_ENGINE_HAS_IT),
    ("sqlite", "partial_index"): (True, _THE_ENGINE_HAS_IT),
    ("turso", "partial_index"): (True, _THE_ENGINE_HAS_IT),
    # --- enum ----------------------------------------------------------------
    ("postgresql", "enum"): (True, _THE_ENGINE_HAS_IT),
    ("sqlite", "enum"): (False, _AN_ENUM_ON_AN_ENGINE_WITHOUT_ONE),
    ("turso", "enum"): (False, _AN_ENUM_ON_AN_ENGINE_WITHOUT_ONE),
    # --- range_type ----------------------------------------------------------
    ("postgresql", "range_type"): (True, _THE_ENGINE_HAS_IT),
    ("sqlite", "range_type"): (False, _A_RANGE_ON_AN_ENGINE_WITHOUT_ONE),
    ("turso", "range_type"): (False, _A_RANGE_ON_AN_ENGINE_WITHOUT_ONE),
}


def capability_of(engine: Engine, feature: Feature) -> Capability:
    """What this engine does about this feature: has it, or is built it from.

    A fresh answer every call, so a caller holding one cannot edit the table
    every other caller reads.
    """
    native, built_from = _THE_TABLE[(engine, feature)]
    return {"native": native, "built_from": built_from}
