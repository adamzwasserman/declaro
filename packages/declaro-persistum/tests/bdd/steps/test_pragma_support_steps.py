"""Steps for pragma_support.feature.

Real engines, no doubles. A claim about what a backend supports can only be
checked by asking that backend.
"""

from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum.abstractions.pragma_compat import (
    pragma_foreign_key_list,
    pragma_index_info,
    pragma_index_list,
    pragma_table_info,
)
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE as OUR_OWN_BUG

scenarios("../features/core/capability_of.feature")

# Imported now rather than written out. This block used to define the tuple
# locally, with a note saying `declaro_persistum.exceptions` had no such thing
# and that a memory of mine wrongly claimed it did. It has one now, added when
# `bulk_transfer` needed the same distinction.


class Ctx(TypedDict, total=False):
    conn: Any
    answers: dict[str, list]
    rows: list
    error: Exception | None
    index_name: str


@pytest.fixture
def ctx() -> Ctx:
    return {}


@given("a real SQLite database with a table, an index and a foreign key")
def _given_real_db(ctx, tmp_path):
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT)")
    conn.execute(
        "CREATE TABLE child ("
        "  id INTEGER PRIMARY KEY,"
        "  parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE,"
        "  note TEXT)"
    )
    conn.execute("CREATE INDEX ix_child_parent ON child(parent_id)")
    conn.commit()
    # THE CONNECTION GOES IN RAW. It was wrapped in a hand-written `_SyncConn`
    # adapter, which was ceremony: `pragma_compat._rows` already calls
    # `_maybe_await` on both `execute` and `fetchall`, so a synchronous
    # sqlite3 connection satisfies it unchanged. Measured 2026-08-16 --
    # `pragma_table_info(sqlite3.connect(...), "child")` returns the rows.
    #
    # An adapter here would also be the exact failure this suite has recorded
    # twice: a double whose contract differs from the driver's either invents
    # a defect or hides one, and only the second leaves a passing suite.
    ctx["conn"] = conn


@given("a connection whose PRAGMA support has been withdrawn")
def _given_withdrawn(ctx, tmp_path):
    """A real SQLite connection, closed. Not a hand-written raiser.

    `_Withdrawn` was a class whose every method raised, which tests that our
    code propagates whatever a double chose to throw. A CLOSED connection is a
    backend that genuinely cannot answer, and the error is the driver's own:

        sqlite3.ProgrammingError: Cannot operate on a closed database.

    measured 2026-08-16. So the scenario now exercises a real failure mode a
    consumer can actually hit, and asserts on the type the real engine raises
    rather than on a type we invented.
    """
    conn = sqlite3.connect(tmp_path / "withdrawn.db")
    conn.close()
    ctx["conn"] = conn


@when("each PRAGMA persistum depends on is asked of it")
def _when_all_pragmas(ctx):
    async def run():
        return {
            "table_info": await pragma_table_info(ctx["conn"], "child"),
            "index_list": await pragma_index_list(ctx["conn"], "child"),
            "foreign_key_list": await pragma_foreign_key_list(ctx["conn"], "child"),
            "index_info": await pragma_index_info(ctx["conn"], "ix_child_parent"),
        }

    ctx["answers"] = asyncio.run(run())


@then("every one is answered by the engine")
def _then_all_native(ctx):
    for name, rows in ctx["answers"].items():
        assert rows, f"PRAGMA {name} returned nothing on a real database"


@when("a PRAGMA is asked of it")
def _when_pragma_on_withdrawn(ctx):
    """Narrowed from `except Exception`, which is why this scenario is worth running.

    A broad catch here made the `then` below pass on ANY exception, including a
    TypeError from calling `pragma_table_info` with the wrong arguments. The
    scenario would have reported "the error reaches the caller" while the error
    was ours and the PRAGMA never ran.

    `sqlite3.ProgrammingError` is what a closed connection raises, measured.
    `OUR_OWN_BUG` goes first so a mistake of ours still crashes here instead of
    being recorded as the engine's answer.
    """
    try:
        asyncio.run(pragma_table_info(ctx["conn"], "anything"))
        ctx["error"] = None
    except OUR_OWN_BUG:
        raise
    except sqlite3.ProgrammingError as e:
        ctx["error"] = e


@then("the error reaches the caller")
def _then_raises(ctx):
    """Asserts the TYPE, not merely that something was thrown.

    `assert error is not None` is satisfied by a TypeError from our own call,
    which is how a step can report an engine failure it never provoked.
    """
    assert ctx["error"] is not None, (
        "a backend that cannot answer a PRAGMA was silently absorbed, so the "
        "caller cannot tell an answer from a substitute"
    )
    assert isinstance(ctx["error"], sqlite3.ProgrammingError), (
        f"the caller got {type(ctx['error']).__name__}, not the engine's own "
        f"ProgrammingError. Something between us and the driver replaced it."
    )


@when("table_info is read for that table")
def _when_table_info(ctx):
    ctx["rows"] = asyncio.run(pragma_table_info(ctx["conn"], "child"))


@then("it lists every column of the table")
def _then_lists_columns(ctx):
    names = {r[1] for r in ctx["rows"]}
    assert names == {"id", "parent_id", "note"}, (
        f"table_info reported {names}, not the three declared columns"
    )


@when("index_list is read and then index_info for one of its indexes")
def _when_index_pair(ctx):
    listed = asyncio.run(pragma_index_list(ctx["conn"], "child"))
    assert listed, "index_list found no index on a table that has one"
    ctx["index_name"] = listed[0][1]
    ctx["rows"] = asyncio.run(pragma_index_info(ctx["conn"], ctx["index_name"]))


@then("the index named by the first is described by the second")
def _then_index_pair_agrees(ctx):
    assert ctx["rows"], (
        f"index_info returned nothing for {ctx['index_name']}, which index_list had just reported"
    )
    covered = {r[2] for r in ctx["rows"]}
    assert "parent_id" in covered, (
        f"index_info says {ctx['index_name']} covers {covered}, but it was created on parent_id"
    )


@when("foreign_key_list is read for the referencing table")
def _when_fk_list(ctx):
    ctx["rows"] = asyncio.run(pragma_foreign_key_list(ctx["conn"], "child"))


@then("it names the referenced table")
def _then_names_referenced(ctx):
    assert ctx["rows"], "foreign_key_list found no key on a table that has one"
    assert any("parent" in str(r) for r in ctx["rows"]), (
        f"the referenced table is not named: {ctx['rows']}"
    )
