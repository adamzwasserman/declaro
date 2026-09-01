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

    catalog name: 'userprofile'
    catalog sql : 'CREATE TABLE "UserProfile" (id INTEGER PRIMARY KEY, ...)'
    PRAGMA columns: ['id', 'givenName']

Column names are NOT folded, so this is a table-name defect only. SQLite
folds nothing and converges on the same declaration, which is what makes the
two engines differ here despite sharing every renderer.

WHAT THE FIX DOES NOT REACH, AND IT IS MOST OF THE PROBLEM. Turso regenerates
the stored DDL on ALTER TABLE and folds the name into it, so after the first
added column the original case is gone from `name` AND from `sql`:

    after CREATE TABLE "MixedCase" (...)     sql holds "MixedCase"
    after ALTER TABLE ... ADD COLUMN         sql holds  mixedcase

That one is held below as a strict xfail. Recovering the case from the
catalog cannot fix it, because the engine no longer holds it, and that
needs a decision about where the declared name is kept.

Index and view names fold identically and ARE recoverable, because their own
DDL is never regenerated. Both are fixed and tested below.

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
from declaro_persistum.types import NOTHING

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


async def _deploy(connection: Any, engine: str, declared: dict) -> dict:
    """One deploy: introspect what is there, diff, apply."""
    actual = await introspect(connection, engine)
    plan = diff(
        {name: actual[name] for name in declared if name in actual},
        declared,
        engine=engine,
        decisions=NOTHING,
    )
    return await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        engine,
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
@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_the_second_deploy_of_a_mixed_case_table_does_nothing(
    engine: str, table_name: str
) -> None:
    """Convergence is the whole requirement. A second deploy has no work."""
    declared = {table_name: DECLARED}
    connection = await ENGINES[engine]()
    try:
        first = await _deploy(connection, engine, declared)
        assert first["success"], first

        second = await _deploy(connection, engine, declared)

        assert second["success"], (
            f"the second deploy of {table_name!r} on {engine} failed: {second}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("table_name", MIXED_CASE)
@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_introspection_returns_the_name_that_was_declared(
    engine: str, table_name: str
) -> None:
    """The cause, asserted on its own so a fix elsewhere cannot mask it.

    A second deploy that does nothing would also pass if the differ learned to
    compare names case-insensitively. That would be the wrong fix: the
    declared name is what every other part of the package quotes into SQL, so
    introspection has to give it back unchanged.
    """
    declared = {table_name: DECLARED}
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, declared)

        actual = await introspect(connection, engine)

        assert table_name in actual, (
            f"{engine} introspected {sorted(actual)}, and {table_name!r} is not among them"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_a_lower_case_table_is_unaffected(engine: str) -> None:
    """The control. Whatever the fix does, it must not disturb the common case."""
    declared = {"users": DECLARED}
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, declared)
        second = await _deploy(connection, engine, declared)

        actual = await introspect(connection, engine)

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
    """A qualified name folds to something the catalog never held.

    `main.QualName` captures as `main.QualName`, which is not `qualname`, so
    the parse and the catalog disagree and the catalog wins. Turso strips
    the qualifier before storing, so this cannot arrive from this engine
    today. It is one version away, and the failure it would cause is a table
    introspected under a name no declaration can match.
    """
    assert declared_name("qualname", "CREATE TABLE main.QualName (id INTEGER)") == ("qualname")


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
    "so the declared case is gone from the catalog entirely",
)
async def test_a_mixed_case_table_survives_being_altered() -> None:
    """The half of the defect the inspector cannot reach."""
    import turso.aio

    declared = {"UserProfile": DECLARED}
    altered = {"UserProfile": {"columns": {**DECLARED["columns"], "email": {"type": "text"}}}}
    connection = await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "altered.db"))
    try:
        await _deploy(connection, "turso", declared)
        await _deploy(connection, "turso", altered)

        actual = await introspect(connection, "turso")

        assert "UserProfile" in actual, sorted(actual)
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# A foreign key pointing at a capitalised table
# ---------------------------------------------------------------------------

# THE FOLD HAS MORE THAN ONE CONSEQUENCE and this is the third found. Measured
# 2026-08-18 after creating `Post` with a reference to `UserProfile`:
#
#     turso   PRAGMA foreign_key_list -> ('userprofile', 'uid', 'id', ...)
#     sqlite  PRAGMA foreign_key_list -> ('UserProfile', 'uid', 'id', ...)
#
# So on Turso the column reads back as `references: userprofile.id` against a
# declaration of `UserProfile.id`, the differ sees a change, and every deploy
# emits drop_foreign_key then add_foreign_key. Both go through table
# reconstruction on this engine, so every deploy copies the whole table into a
# new one. It reports success each time.

FK_SCHEMA = {
    "UserProfile": DECLARED,
    "Post": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "uid": {"type": "integer", "references": "UserProfile.id"},
        }
    },
}


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_a_reference_to_a_capitalised_table_converges(engine: str) -> None:
    """A rebuild of the whole table on every deploy, reported as success."""
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, FK_SCHEMA)

        actual = await introspect(connection, engine)
        remaining = diff(
            {n: actual[n] for n in FK_SCHEMA if n in actual},
            FK_SCHEMA,
            engine=engine,
            decisions=NOTHING,
        )["operations"]

        assert remaining == [], (
            f"{engine} rebuilds Post on every deploy: {[(o['op'], o['table']) for o in remaining]}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_the_reference_reads_back_as_it_was_declared(engine: str) -> None:
    """The cause, so a fix in the differ cannot mask it.

    `references` is a string this package writes into SQL. Introspection has
    to hand back the name that was declared, not the name the catalog holds.
    """
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, FK_SCHEMA)

        actual = await introspect(connection, engine)

        assert actual["Post"]["columns"]["uid"]["references"] == "UserProfile.id"
    finally:
        await connection.close()


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_a_lower_case_reference_is_unaffected(engine: str) -> None:
    """The control."""
    schema = {
        "users": DECLARED,
        "posts": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "uid": {"type": "integer", "references": "users.id"},
            }
        },
    }
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, schema)

        actual = await introspect(connection, engine)

        assert actual["posts"]["columns"]["uid"]["references"] == "users.id"
    finally:
        await connection.close()


# ---------------------------------------------------------------------------
# The law, and the two object kinds it still catches
# ---------------------------------------------------------------------------

# EVERY PLACE A NAME COMES BACK OUT OF TURSO, enumerated 2026-08-18 rather
# than found one at a time, after three separate fixes for what turned out to
# be one thing:
#
#     sqlite_master.name (table)   FOLDS
#     sqlite_master.name (index)   FOLDS
#     sqlite_master.name (view)    FOLDS
#     PRAGMA foreign_key_list      FOLDS
#     PRAGMA index_list            FOLDS
#     sqlite_master.sql            keeps
#     PRAGMA table_info            keeps
#     PRAGMA index_info            keeps
#
# SQLite folds none of the eight. The rule is not a list: Turso folds every
# OBJECT name and keeps every COLUMN name and the DDL text. So a column is
# safe everywhere, and a table, an index and a view are not.
#
# The DDL keeps the original, which is what makes all three recoverable by the
# same reading `declared_name` already does for tables.

INDEXED = {
    "users": {
        **DECLARED,
        "indexes": {"IxUserName": {"columns": ["name"]}},
    }
}


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_an_index_reads_back_under_the_name_it_was_given(
    engine: str,
) -> None:
    """The silent loop: drop_index then add_index forever, reporting success."""
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, INDEXED)

        actual = await introspect(connection, engine)

        assert "IxUserName" in actual["users"].get("indexes", {}), (
            f"{engine} introspected {sorted(actual['users'].get('indexes', {}))}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_a_capitalised_index_converges(engine: str) -> None:
    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, INDEXED)

        actual = await introspect(connection, engine)
        remaining = diff(
            {n: actual[n] for n in INDEXED if n in actual},
            INDEXED,
            engine=engine,
            decisions=NOTHING,
        )["operations"]

        assert remaining == [], [(o["op"], o["table"]) for o in remaining]
    finally:
        await connection.close()


@pytest.mark.parametrize("engine", sorted(ENGINES))
async def test_a_view_reads_back_under_the_name_it_was_given(
    engine: str,
) -> None:
    """Views are the fifth site and nothing had asked what folding costs there."""
    from declaro_persistum.inspector import introspect_with_views

    connection = await ENGINES[engine]()
    try:
        await _deploy(connection, engine, {"users": DECLARED})
        await connection.execute('CREATE VIEW "ActiveUsers" AS SELECT id FROM "users"')
        await connection.commit()

        _tables, views = await introspect_with_views(connection, engine)

        assert "ActiveUsers" in views, f"{engine} introspected {sorted(views)}"
    finally:
        await connection.close()
