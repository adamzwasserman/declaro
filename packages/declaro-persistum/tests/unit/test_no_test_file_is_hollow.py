r"""A file named `test_*.py` must contain a test.

NONE DO, AS OF 2026-08-17. Twenty-four did. Each was a docstring, its imports,
sometimes a helper, and then blank space where the tests had been, and the
suite total said nothing about the gap.

Sixteen of the twenty-four came back by copying the last revision that held
tests and running it unchanged. Nothing was rewritten and nothing was adapted:
they pass because the modules they test still behave the way they did, which is
also the evidence that restoring them was safe. The suite went from 299 to 774.

WHAT IS LEFT, AND WHY EACH IS LEFT. Measured by restoring it and running it:


0 tests.

`tests/integration/test_sqlite.py` was the last entry and is FILLED. It was 24
lines: a docstring, an `aiosqlite` fixture yielding `:memory:`, and nothing
else. It holds 12 tests now, on a real file rather than in memory, because
several of the properties cannot be observed in memory at all -- a file
survives being closed and reopened, which is the whole case a migrate-twice
test exists for.

Filling it found the worst defect of the day. A failed migration did not roll
back on SQLite:

    apply raised MigrationError
    tables after the failed migration: ['good']

SQLite's DDL is transactional; Python's `sqlite3` driver does not put it in a
transaction. Its default `isolation_level` is `""`, which opens one implicitly
before DML and NOT before DDL, so every CREATE and DROP ran in autocommit and
the `rollback()` on the failure paths had nothing to undo. `applier/turso.py`
has issued its own BEGIN all along; `applier/sqlite.py` did not. A half-applied
migration was leaving the database in a shape nobody designed while the applier
believed it had rolled back.

`test_triggers.py` and `test_procedures.py` are the last two, RESOLVED BY
DELETION. Each held 20 tests; restored, every failure was an ImportError for a
function that is not there -- `diff_triggers`, `generate_create_trigger`,
`generate_drop_trigger`, `diff_procedures`, `generate_create_function`,
`parse_procedure`.

Both features are gone end to end rather than renamed:

    types.py           `Trigger` and `Procedure` are declared
    __init__.py        exports neither
    pydantic_loader    cannot declare one; the `trigger_sources` there
                       belongs to materialized-view refresh
    differ             emits no trigger or procedure operation
    _SQL_GENERATORS    16 operations, none of them either

A user cannot declare one, the differ cannot notice one, and no applier could
render one. The TypedDicts are the last trace.

Deleting the files rather than leaving a docstring in them is the point of
this gate: a file named `test_triggers.py` holding only prose still reads as
coverage to every tool that counts files, which is the defect this ratchet
exists to catch. The record belongs here, where a reader of the list will find
it.

`applier/sqlite.py::generate_create_trigger` went with them. Nothing in src
called it, and it and `test_triggers.py` were each other's only reason to
exist -- the same pair `generate_enum_check` formed with `test_enums.py`
earlier the same day. A hollow test file is what kept both invisible, twice.

`test_views.py` is the fourth RESOLVED BY FILLING, 2026-08-17. Restored it ran
47 tests, 23 passing, and the 24 failures sorted into three kinds.

TWENTY-THREE WERE THE DE-CLASSING. `SQLiteApplier()`, `PostgreSQLInspector()`
and the rest are gone, and the classes were stateless with module-level
functions beside them, so the module IS the replacement and
`applier.generate_create_view(v)` reads identically. Mechanical.

ONE WAS A BELIEF THAT DOES NOT SURVIVE EITHER ENGINE. A changed view query
asserted one operation, commented "CREATE OR REPLACE handles this". Measured:
SQLite answers `near "OR": syntax error`, and PostgreSQL refuses a changed
column list with "cannot drop columns from view". The differ emits drop then
create, which is what works on both, so the test moved.

TWELVE DESCRIBED REPLACED DESIGNS and are deleted. Views declared in TOML --
`parse_view`, `load_views` -- became `load_declarations` off a models module.
Native materialized views became an emulated table: `refresh_matview_sql`
produces DELETE plus INSERT, and `grep CONCURRENTLY src/` finds nothing, so
the six-test class guarding PostgreSQL's unique-index precondition for a
concurrent refresh guards a statement this package never emits. The precondition
is real and is recorded where that class stood, in case native matviews return.

One more was rewritten rather than ported: it hand-wrote a `MockCursor` and a
`MockConn` and asserted against what they returned. SQLite costs nothing to
create, so it runs against a real one.

`test_write_queue.py` is the third RESOLVED BY FILLING, 2026-08-17. The note
above said "import error: `pool` is abolished". That was no longer true:
restored from 4c6b106 it imported cleanly and ran 15 tests, 11 passing.

The four failures were the OLD CONTRACT, not defects. `drain` returned an int
and returns a `Drained` now, and a non-contention failure raises down the
depositor's ticket instead of being flattened into `receipt["error"]` as a
string. Both changes are better and both are documented in `write_queue.py`,
so the tests moved to them: a caller now gets the exception with its type
intact and can ask whether it was a constraint violation.

Filling it found one defect. `Drained["appended"]` counted ATTEMPTS, including
writes that raised, while the docstring said "how many were appended". I first
changed the count and that was the wrong end -- three existing tests state the
meaning they rely on in their own assertion messages, "every write was
attempted, including the two after the failure", and they were right. Attempts
is the useful number, because it says the queue did not stop at the first
failure. The field is `attempted` now.

`test_query_expressions.py` is the second RESOLVED BY DELETION, 2026-08-17.
Restored, all 30 of its tests failed on one line: `table(name, schema,
pool=None)`. The pool is abolished, and the file describes a `TableProxy` API
with `.alias()`, `._full_name` and `._table_name`; `table(name, schema)`
returns `{"columns": {...}}` now. Table aliasing and self-joins have no
support at all in the rewritten query layer, so 6 of its 30 tests are a
feature request, not a restoration.

The rest of its subject was real and uncovered. Of 22 public callables across
`query/table.py` and `query/expressions.py`, 21 were called by no test in the
suite -- including `count_`, which all three shipped example apps import. They
are covered now by `test_the_query_expression_layer_renders.py`, and the first
test ever written for `_render_case_order_by` found it raising
`'dict' object has no attribute '_bare_sql_fragment'` on every call. Fixed.
`render_subquery` has the identical defect, is a missing feature rather than a
repair, and is pinned as broken there.

`test_enums.py` was the sixth and is RESOLVED BY DELETION, 2026-08-17. Restored
and run, its 17 tests were 5 pass and 12 fail, and every one of the twelve was
an ImportError for a function the package no longer has: `parse_enum`,
`generate_create_enum`, `generate_drop_enum`, `generate_alter_enum_add_value`,
`diff_enums`. `grep "CREATE TYPE"` over `src/` returns nothing. The design
those tests describe -- a native PostgreSQL ENUM type with a SQLite CHECK
fallback -- is gone, replaced by a `_dp_enum_` lookup table and a foreign key.

The FIVE THAT PASSED were the worse half. Three built a dict literal and
asserted its own contents, touching no package code at all. The other two
exercised `generate_enum_check` and `generate_column_sql` in
`applier/sqlite.py`, which had no caller in `src/` and keyed on an `enum:name`
column type no loader has ever produced. Test and code were each other's only
reason to exist, and the file being hollow is what kept the pair invisible.
Both functions are deleted, and the live enum path is covered by
`test_a_declared_enum_value_is_accepted.py` on all three engines.

THIS IS WHY THE LIST IS NOT A BACKLOG OF COPY JOBS. Restoring that file
verbatim would have re-pinned an abandoned architecture and resurrected dead
code to satisfy it. test_arrays.py and test_maps.py are no longer among them: their
four failures all passed the abolished engine "libsql" and were right that it
must not yield PostgreSQL SQL. Fixing that in src (cfc0b1c) brought both files
back, 43 tests.

None of what is left is a copy job. Every failure is a real question about what the
rewritten module should now promise, and answering it by editing the test until
it passes is how a suite becomes decoration.

COUNTED WITH `grep -cE '^\s*(async )?def test_'`. An earlier version of this
docstring said 600 across twenty-four files. That count used a pattern without
`async def`, so it missed every asynchronous test. The true figure was 640.

A HOLLOW TEST FILE IS WORSE THAN A MISSING ONE. A missing file is an obvious
gap. A file called `test_validator.py` sitting in a green suite reads as "the
validator is tested", and every tool that counts files rather than tests agrees
with it. That is how `create_bulk_loader` shipped raising NameError for every
engine it supported, and how `_normalize_view_query` shipped deleted with both
its callers intact: the modules had test files.

THIS RATCHETS, IT DOES NOT DEMAND. Pinned at zero it would fail the suite until
all 160 are answered, and a gate nobody can satisfy gets deleted. So it freezes
the list instead. A NEW hollow file fails. Filling or deleting one of these also
fails, so the list cannot quietly rot into a permanent excuse — every change to
it is a decision someone made on purpose.

The BDD step modules are excluded by directory, not by exception: they hold
`@given`/`@when` definitions for feature files bound elsewhere, so collecting
nothing is what they are for. `test_every_feature_is_bound` covers them.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

UNIT = pathlib.Path(__file__).resolve().parent
TESTS = UNIT.parent

# Verified 2026-08-13 against `pytest --co`: this AST count and pytest's
# collection agree on zero versus non-zero for every file in this directory.
# `tests/integration/` WAS OUTSIDE THIS GATE'S HORIZON until 2026-08-17. The
# scan globbed `tests/unit/` only, so two files there were hollow for as long
# as this ratchet has existed and it reported six.
#
# `test_postgresql.py` was the most expensive of the set and is FILLED as of
# 2026-08-17. It carried a `skipif` on an unset environment variable and a
# connection fixture, so it READ as a suite waiting for a server, and there was
# nothing behind the guard to wait: setting the variable moved the collected
# count from zero to zero. PostgreSQL is the backend this package documents as
# production and had 2 marked tests in a suite of 1241.
#
# It holds 11 now, run against a real server, and the first one written found
# a defect nothing else could see: a round trip emitted two `alter_column`
# operations on an unchanged schema, one of them asking PostgreSQL to make a
# PRIMARY KEY nullable. Every deploy proposed it and it could never succeed.
# Fixed in `differ/core.py`; pinned by
# `test_a_migration_stops_when_nothing_changed.py`.
#
# `test_sqlite.py` stays listed. SQLite is exercised by 74 marked tests
# elsewhere, so an integration file is not the gap there that it was here.
# EMPTY, AND THAT IS THE POINT OF A RATCHET. Each was resolved
# on purpose -- filled, or deleted with the measurement that said why -- and
# the assertion below fails in BOTH directions, so the list cannot quietly
# refill and an empty one cannot quietly grow.
KNOWN_HOLLOW: set[str] = set()


def _holds_a_test(path: pathlib.Path) -> bool:
    """True if pytest would collect anything from this file.

    Counts `def test_*` at any depth, plus `scenarios(...)`, which binds an
    unknown number of feature scenarios but never zero.
    """
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name.startswith(
            "test_"
        ):
            return True
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "scenarios":
            return True
    return False


def test_the_gate_can_see_the_test_files_at_all() -> None:
    """A gate that reads nothing passes forever."""
    files = list(UNIT.glob("test_*.py")) + list((TESTS / "integration").glob("test_*.py"))
    assert len(files) > 30, f"found only {len(files)} test files in {UNIT}"


def test_no_new_test_file_is_hollow() -> None:
    scanned = list(UNIT.glob("test_*.py")) + list((TESTS / "integration").glob("test_*.py"))
    hollow = {p.name for p in scanned if not _holds_a_test(p)}

    appeared = hollow - KNOWN_HOLLOW
    assert not appeared, (
        f"{sorted(appeared)} contain no test. A file named test_*.py that "
        f"collects nothing reads as coverage to every reader and every tool "
        f"that counts files"
    )

    fixed = KNOWN_HOLLOW - hollow
    assert not fixed, (
        f"{sorted(fixed)} now hold tests, or were deleted. Remove them from "
        f"KNOWN_HOLLOW so the list keeps meaning what it says"
    )
