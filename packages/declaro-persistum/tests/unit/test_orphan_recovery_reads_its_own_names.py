"""Recovering a table whose rebuild died between the DROP and the RENAME.

WRITTEN BEFORE THE CODE, and after correcting a note that reported a bug here
which does not exist. I claimed `_recover_orphaned_tmp_tables` recovers
`report_20260816` as `report`, having fed the derivation a name with no hex
suffix. Both generators build

    f"_declaro_tmp_{table_name}_{uuid.uuid4().hex[:8]}"

so that name is not producible, and the real function run against a real
database recovers `report_20260816` intact. The rule strips nine characters by
position and the suffix is nine characters wide, so it is right for every name
in the domain.

SO EVERY NAME BELOW COMES FROM `temp_table_name`, THE FUNCTION THAT MAKES THEM.
A test that builds the input from its own idea of the shape tests that idea.
Deriving it from the generator is what makes the round trip meaningful, and it
is why `test_the_round_trip_holds_for_any_table_name` is worth more than any
number of hand-picked cases.

ONE REAL DEFECT SURVIVED THE CORRECTION AND IS PINNED BELOW. The recovery
query says `LIKE '_declaro_tmp_%'`, and SQLite reads `_` as a
single-character wildcard, so it also matches `Xdeclaro_tmp_users_a1b2c3d4` --
a table this package did not create. Measured. Nothing foreign is harmed,
because a Python `startswith` guard drops those rows before anything is
renamed, but the docstring's "only that prefix is matched" is true of the
function and false of its SQL.
"""

from __future__ import annotations

import sqlite3
import uuid

import pytest

from declaro_persistum.migrations import (
    original_table_name,
    orphaned_tmp_tables_sql,
    temp_table_name,
)

pytestmark = pytest.mark.precommit

# Names chosen for what they do to a POSITIONAL rule, not for how they look.
# `report_20260816` ends in an underscore plus eight characters, which is the
# shape the suffix has, so it is the case a naive strip gets wrong.
TABLE_NAMES = [
    "users",
    "report_20260816",
    "orders_2026",
    "a",
    "log_12345678",
    "t_abcdefgh",
    "_leading_underscore",
    "MixedCase",
]


# ---------------------------------------------------------------------------
# The round trip, which is the whole of the naming contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("table", TABLE_NAMES)
def test_the_round_trip_holds_for_any_table_name(table: str) -> None:
    """Generate a tmp name, derive the original back, get what you started with.

    This is the assertion the corrected note is about. It cannot be satisfied
    by a hand-made name because the input comes from `temp_table_name`, so a
    change to either half fails here rather than in production.
    """
    assert original_table_name(temp_table_name(table)) == table


def test_the_generated_suffix_is_the_width_the_rule_strips() -> None:
    """The positional rule is correct only while the suffix is fixed-width.

    Nine characters: one underscore and eight hex. If the generator ever moves
    to a longer id, every recovered name silently keeps part of it, and this is
    the test that says so instead of the next migration creating an empty table.
    """
    generated = temp_table_name("t")
    suffix = generated[len("_declaro_tmp_t") :]

    assert len(suffix) == 9, f"the suffix is {len(suffix)} characters: {suffix!r}"
    assert suffix[0] == "_", suffix
    assert all(c in "0123456789abcdef" for c in suffix[1:]), suffix


def test_two_generated_names_for_one_table_differ() -> None:
    """The suffix exists so a retry does not collide with a live tmp table."""
    assert temp_table_name("t") != temp_table_name("t")


def test_a_name_that_is_not_ours_is_refused() -> None:
    """`original_table_name` must not guess at a prefix it did not write."""
    with pytest.raises(ValueError, match="not a tmp table"):
        original_table_name("Xdeclaro_tmp_users_a1b2c3d4")

    with pytest.raises(ValueError, match="not a tmp table"):
        original_table_name("users")


# ---------------------------------------------------------------------------
# The query, and the wildcard in it
# ---------------------------------------------------------------------------


def test_the_query_selects_tables_by_our_prefix() -> None:
    """The prefix appears ESCAPED, so a literal search for it finds nothing.

    My first version of this asserted `"_declaro_tmp_" in sql` and failed once
    the escaping landed. The assertion was wrong, not the SQL: the pattern is
    `\\_declaro\\_tmp\\_%`, and checking for the raw form would pass again the
    day somebody removed the escaping.
    """
    from declaro_persistum.migrations import TMP_PREFIX

    sql = orphaned_tmp_tables_sql()

    assert "sqlite_master" in sql
    assert "type = 'table'" in sql
    assert TMP_PREFIX.replace("_", "\\_") in sql, sql


def test_the_underscore_in_the_prefix_is_escaped() -> None:
    """SQLite's LIKE reads `_` as a single-character wildcard.

    Unescaped, `LIKE '_declaro_tmp_%'` matches `Xdeclaro_tmp_users_a1b2c3d4`,
    a table this package did not create. Measured against a real database.
    Escaping is what makes the docstring's claim true of the SQL rather than
    only of the Python guard behind it.
    """
    sql = orphaned_tmp_tables_sql()

    assert "ESCAPE" in sql.upper(), (
        f"the prefix is matched with an unescaped LIKE, so `_` is a wildcard "
        f"and the query returns tables we did not write: {sql}"
    )


def test_the_escaped_query_matches_ours_and_nothing_else() -> None:
    """Executed, because a LIKE pattern is only true of the engine that reads it."""
    conn = sqlite3.connect(":memory:")
    ours = temp_table_name("users")
    for name in (ours, "Xdeclaro_tmp_users_a1b2c3d4", "my_declaro_tmp_x", "users"):
        conn.execute(f'CREATE TABLE "{name}" (id INT)')

    matched = {row[0] for row in conn.execute(orphaned_tmp_tables_sql())}

    assert matched == {ours}, (
        f"the query matched {sorted(matched)}. Only {ours!r} was written by "
        f"this package; the others belong to whoever made them."
    )


def test_the_query_finds_a_tmp_table_for_every_name_shape() -> None:
    """The floor. A pattern that matches nothing would pass every test above."""
    conn = sqlite3.connect(":memory:")
    made = {temp_table_name(t) for t in TABLE_NAMES}
    for name in made:
        conn.execute(f'CREATE TABLE "{name}" (id INT)')

    matched = {row[0] for row in conn.execute(orphaned_tmp_tables_sql())}

    assert matched == made, f"missed: {sorted(made - matched)}"


def test_the_prefix_is_one_definition_shared_by_both_halves() -> None:
    """Generator and query must agree, or recovery finds nothing it wrote.

    They were two string literals in two modules. A change to one is silent:
    reconstruction keeps working, recovery keeps returning zero, and the only
    symptom is a table that vanishes after a crash.
    """
    from declaro_persistum.migrations import TMP_PREFIX

    generated = temp_table_name("anything")
    assert generated.startswith(TMP_PREFIX)
    assert TMP_PREFIX.replace("_", "\\_") in orphaned_tmp_tables_sql(), (
        "the query's pattern is not built from TMP_PREFIX, so the generator "
        "and the recovery can drift apart in silence"
    )


def test_a_generated_name_is_a_legal_identifier() -> None:
    """It goes into a CREATE TABLE, so it has to survive quoting."""
    conn = sqlite3.connect(":memory:")
    for table in TABLE_NAMES:
        name = temp_table_name(table)
        conn.execute(f'CREATE TABLE "{name}" (id INT)')
        conn.execute(f'INSERT INTO "{name}" VALUES (1)')

    count = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'").fetchone()[0]
    assert count == len(TABLE_NAMES)


@pytest.mark.parametrize("table", TABLE_NAMES)
def test_a_uuid_is_never_mistaken_for_part_of_the_table(table: str) -> None:
    """The recovered name must not carry a fragment of the id.

    A recovered `report_2026081` looks close enough to read past, and the next
    migration sees `report_20260816` as absent and creates it empty beside it.
    """
    for _ in range(20):
        recovered = original_table_name(temp_table_name(table))
        assert recovered == table, f"{recovered!r} is not {table!r}"
        assert not any(recovered.endswith(h) for h in (uuid.uuid4().hex[:8],)), recovered
