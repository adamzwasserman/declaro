"""A table with a capital letter must migrate to completion on every engine.

WRITTEN BEFORE THE FIX, against a migration that can never finish.

MEASURED 2026-08-18 on Turso, declaring one table named `UserProfile`:

    first deploy   success=True    the table is created
    introspection  ['userprofile'] the declared name is gone
    second deploy  emits create_table UserProfile
                   "Parse error: table userprofile already exists"
                   success=False

And so on forever. Every deploy after the first reports failure, for any
table whose name is not already lower case. The data is not harmed and the
failure is loud, which is the only reason this is not worse: the applier
refuses to call a migration with a skipped operation successful, so the loop
announces itself rather than hiding.

WHERE THE CASE GOES. Turso folds `sqlite_master.name` and keeps the original
in `sqlite_master.sql`, which is what makes this recoverable rather than
merely reportable:

    catalogue name: 'userprofile'
    catalogue sql : 'CREATE TABLE "UserProfile" (id INTEGER PRIMARY KEY, ...)'
    PRAGMA columns: ['id', 'givenName']

Column names are NOT folded, so this is a table-name defect only. SQLite
folds nothing and converges on the same declaration, which is what makes the
two engines differ here despite sharing every renderer.

WHAT THE FIX DOES NOT REACH, AND IT IS MOST OF THE PROBLEM. Turso regenerates
the stored DDL on ALTER TABLE and folds the name into it, so after the first
added column the original case is gone from `name` AND from `sql`:

    after CREATE TABLE "MixedCase" (...)     sql holds "MixedCase"
    after ALTER TABLE ... ADD COLUMN         sql holds  mixedcase

Index names fold the same way and are worse, because that loop is silent. A
declared index `IxUserName` reads back as `ixusername`, so every deploy emits
`drop_index ixusername` then `add_index IxUserName`, both succeed, and the
migration reports success forever while doing the same work every time.

Both are held below as strict xfails. Recovering the case from the catalogue
cannot fix either one: the engine no longer holds it. That needs a decision
about where the declared name is kept, and it is Adam's to make.

WHY NO GATE CAUGHT IT. The operation matrix beside this file applies eleven
operations on three engines and every case named its tables `users`, `posts`,
`customers` or `t`. All lower case. An input space that is uniform in the one
dimension that matters is how a suite reaches full coverage of a defect it
cannot see, and this package has done it before: 100 percent branch coverage
while emitting unparseable SQL, because every test named the table `users`.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.inspector.turso import declared_name

# asyncio mode is AUTO, so async tests need no mark. Marking the module would
# warn on every synchronous test below.
pytestmark = [pytest.mark.precommit]

# Names chosen for what the engine does to them, not for how they read.
MIXED_CASE = ["UserProfile", "ORDERS", "lineItem"]

DECLARED = {
    "columns": {
        "id": {"type": "integer", "primary_key": True},
        "name": {"type": "text"},
    }
}


async def _deploy(connection: Any, dialect: str, declared: dict) -> dict:
    """One deploy: introspect what is there, diff, apply."""
    actual = await introspect(connection, dialect)
    plan = diff(
        {name: actual[name] for name in declared if name in actual},
        declared,
        dialect=dialect,
    )
    return await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        dialect,
        dry_run=False,
    )


async def _turso_connection() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "case.db"))


async def _sqlite_connection() -> Any:
    import aiosqlite

    return await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "case.db"))


ENGINES = {"turso": _turso_connection, "sqlite": _sqlite_connection}


@pytest.mark.parametrize("table_name", MIXED_CASE)
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_the_second_deploy_of_a_mixed_case_table_does_nothing(
    dialect: str, table_name: str
) -> None:
    """Convergence is the whole requirement. A second deploy has no work."""
    declared = {table_name: DECLARED}
    connection = await ENGINES[dialect]()
    try:
        first = await _deploy(connection, dialect, declared)
        assert first["success"], first

        second = await _deploy(connection, dialect, declared)

        assert second["success"], (
            f"the second deploy of {table_name!r} on {dialect} failed: {second}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("table_name", MIXED_CASE)
@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_introspection_returns_the_name_that_was_declared(
    dialect: str, table_name: str
) -> None:
    """The cause, asserted on its own so a fix elsewhere cannot mask it.

    A second deploy that does nothing would also pass if the differ learned to
    compare names case-insensitively. That would be the wrong fix: the
    declared name is what every other part of the package quotes into SQL, so
    introspection has to give it back unchanged.
    """
    declared = {table_name: DECLARED}
    connection = await ENGINES[dialect]()
    try:
        await _deploy(connection, dialect, declared)

        actual = await introspect(connection, dialect)

        assert table_name in actual, (
            f"{dialect} introspected {sorted(actual)}, and {table_name!r} "
            f"is not among them"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("dialect", sorted(ENGINES))
async def test_a_lower_case_table_is_unaffected(dialect: str) -> None:
    """The control. Whatever the fix does, it must not disturb the common case."""
    declared = {"users": DECLARED}
    connection = await ENGINES[dialect]()
    try:
        await _deploy(connection, dialect, declared)
        second = await _deploy(connection, dialect, declared)

        actual = await introspect(connection, dialect)

        assert second["success"] and "users" in actual, (second, sorted(actual))
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# `declared_name` on its own, because the engine cannot produce every spelling
# ---------------------------------------------------------------------------

# WHAT pyturso 0.7.2 DOES TO EACH SPELLING, measured 2026-08-18 by executing
# the DDL and reading `sqlite_master` back:
#
#     CREATE TABLE `BackTick` ...        stored as written
#     CREATE TABLE "DblQuote" ...        stored as written
#     CREATE TABLE BareName ...          stored as written
#     CREATE TABLE [BracKet] ...         rewritten to bare BracKet
#     CREATE TABLE main.QualName ...     the qualifier is stripped
#     CREATE TABLE IF NOT EXISTS X ...   the clause is stripped
#
# So three of the six cannot be reached through the engine on this version.
# They are reachable through the grammar, and this inspector reads databases
# it did not create, so the parser handles them and these tests exercise them
# directly. A later Turso that stores a bracket name unchanged needs no fix.

QUOTINGS = [
    ('CREATE TABLE "DblQuote" (id INTEGER)', "dblquote", "DblQuote"),
    ("CREATE TABLE `BackTick` (id INTEGER)", "backtick", "BackTick"),
    ("CREATE TABLE [BracKet] (id INTEGER)", "bracket", "BracKet"),
    ("CREATE TABLE BareName (id INTEGER)", "barename", "BareName"),
    ("CREATE TABLE IF NOT EXISTS IfNotEx (id INTEGER)", "ifnotex", "IfNotEx"),
    ("create table   Spaced   (id INTEGER)", "spaced", "Spaced"),
]


@pytest.mark.parametrize("ddl,stored,expected", QUOTINGS, ids=[q[2] for q in QUOTINGS])
def test_declared_name_recovers_every_spelling_the_grammar_allows(
    ddl: str, stored: str, expected: str
) -> None:
    assert declared_name(stored, ddl) == expected


def test_a_ddl_that_names_a_different_table_is_not_believed() -> None:
    """A qualified name folds to something the catalogue never held.

    `main.QualName` captures as `main.QualName`, which is not `qualname`, so
    the parse and the catalogue disagree and the catalogue wins. Turso strips
    the qualifier before storing, so this cannot arrive from this engine
    today. It is one version away, and the failure it would cause is a table
    introspected under a name no declaration can match.
    """
    assert declared_name("qualname", "CREATE TABLE main.QualName (id INTEGER)") == (
        "qualname"
    )


def test_a_table_with_no_stored_ddl_keeps_its_catalogue_name() -> None:
    """`sqlite_master.sql` is NULL for some internal objects."""
    assert declared_name("userprofile", None) == "userprofile"


def test_unparseable_ddl_keeps_the_catalogue_name() -> None:
    assert declared_name("userprofile", "NOT A CREATE STATEMENT") == "userprofile"


# ---------------------------------------------------------------------------
# Live defects, held strict so they cannot rot into a passing test quietly
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="Turso rewrites stored DDL on ALTER and folds the name into it, "
    "so the declared case is gone from the catalogue entirely",
)
async def test_a_mixed_case_table_survives_being_altered() -> None:
    """The half of the defect the inspector cannot reach."""
    import turso.aio

    declared = {"UserProfile": DECLARED}
    altered = {
        "UserProfile": {
            "columns": {**DECLARED["columns"], "email": {"type": "text"}}
        }
    }
    connection = await turso.aio.connect(
        os.path.join(tempfile.mkdtemp(), "altered.db")
    )
    try:
        await _deploy(connection, "turso", declared)
        await _deploy(connection, "turso", altered)

        actual = await introspect(connection, "turso")

        assert "UserProfile" in actual, sorted(actual)
    finally:
        await connection.close()


@pytest.mark.xfail(
    strict=True,
    reason="Turso folds a stored index name, and this loop is silent: "
    "drop_index then add_index every deploy, both reporting success",
)
async def test_a_mixed_case_index_name_converges() -> None:
    """The silent half, and the more dangerous one.

    The table defect announces itself: the operation fails and the applier
    refuses to call the migration successful. This one does not. Both
    operations succeed, so every deploy drops and recreates the index and
    reports success, forever.
    """
    import turso.aio

    declared = {
        "users": {
            **DECLARED,
            "indexes": {"IxUserName": {"columns": ["name"]}},
        }
    }
    connection = await turso.aio.connect(
        os.path.join(tempfile.mkdtemp(), "index.db")
    )
    try:
        await _deploy(connection, "turso", declared)

        actual = await introspect(connection, "turso")
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            dialect="turso",
        )["operations"]

        assert remaining == [], [(o["op"], o["table"]) for o in remaining]
    finally:
        await connection.close()
