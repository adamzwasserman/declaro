"""The latency helpers, which nothing in the package calls and no test did.

`instrumentation.py` is a consumer-facing toolkit and the docs are honest
about that: "Nothing in the package calls them. There is no
`instrumentation=True` switch and no automatic timing; you time the call and
build the record yourself." So being uncalled from `src/` is by design.

Being untested was not. `LatencyRecord` is exported from the package and the
four functions are documented in README.md and usage.md with a worked example,
and the reachability scan found none of them called by anything in the suite.

MEASURED 2026-08-17, running `classify_sql` over real statements:

    REPLACE INTO t VALUES (1)             -> 'other'   write=False
    WITH x AS (...) DELETE FROM t ...     -> 'select'  write=False

Both are writes. `REPLACE INTO` is SQLite's upsert, and PostgreSQL allows a
data-modifying CTE, so `WITH ... DELETE` and `WITH ... UPDATE` are ordinary
statements that this reported as reads. A consumer using `is_write_op` to
decide what goes through the write queue would send those down the read path.

`classify_sql` looks at the FIRST KEYWORD, which is the right shape -- it is a
dict lookup, not a chain, and an unrecognised statement gets "other" rather
than a guess. The two rows were simply missing, and the CTE needs the keyword
after the closing parenthesis rather than the one before it.
"""

from __future__ import annotations

import json
import logging

import pytest

from declaro_persistum.instrumentation import (
    build_record,
    classify_sql,
    emit_record,
    format_jsonl,
    is_write_op,
)
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit

READS = [
    "SELECT 1",
    "  select 1",
    "SELECT * FROM t WHERE id = :id",
    "WITH x AS (SELECT 1) SELECT * FROM x",
]

WRITES = [
    "INSERT INTO t VALUES (1)",
    "UPDATE t SET a = 1",
    "DELETE FROM t",
    "REPLACE INTO t VALUES (1)",
    "INSERT OR REPLACE INTO t VALUES (1)",
    "WITH x AS (SELECT id FROM s) DELETE FROM t WHERE id IN (SELECT id FROM x)",
    "WITH x AS (SELECT 1) UPDATE t SET a = 1",
    "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
]


@pytest.mark.parametrize("sql", WRITES, ids=lambda s: s[:32])
def test_a_write_is_classified_as_a_write(sql: str) -> None:
    """The measured defect. A consumer routes on this answer."""
    assert is_write_op(classify_sql(sql)) is True, (
        f"{sql!r} classified as {classify_sql(sql)!r}, so a write would be "
        f"sent down the read path"
    )


@pytest.mark.parametrize("sql", READS, ids=lambda s: s[:32])
def test_a_read_is_not_classified_as_a_write(sql: str) -> None:
    """The floor. Calling everything a write passes every test above."""
    assert is_write_op(classify_sql(sql)) is False, classify_sql(sql)


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT 1", "select"),
        ("INSERT INTO t VALUES (1)", "insert"),
        ("UPDATE t SET a = 1", "update"),
        ("DELETE FROM t", "delete"),
        ("CREATE TABLE t (id INT)", "create"),
        ("ALTER TABLE t ADD c INT", "alter"),
        ("DROP TABLE t", "alter"),
        ("PRAGMA foreign_keys = ON", "other"),
        ("BEGIN", "other"),
        ("COMMIT", "other"),
    ],
    ids=lambda v: str(v)[:24],
)
def test_a_statement_gets_its_own_op_name(sql: str, expected: str) -> None:
    assert classify_sql(sql) == expected


@pytest.mark.parametrize("sql", ["", "   ", "\n\t "])
def test_an_empty_statement_is_other_rather_than_a_crash(sql: str) -> None:
    """`sql.strip().split()[0]` on an empty string is an IndexError."""
    assert classify_sql(sql) == "other"


def test_something_nobody_taught_it_is_other_not_a_guess() -> None:
    """A dict lookup with a fallback, which is the right shape here.

    Unlike a dialect or a column type, an unrecognised STATEMENT is not a
    caller error: SQL has more verbs than this table will ever hold, and
    "other" is an honest answer rather than a guess at one of the six.
    """
    assert classify_sql("VACUUM") == "other"
    assert classify_sql("EXPLAIN SELECT 1") == "other"


# ---------------------------------------------------------------------------
# The record a consumer builds and emits
# ---------------------------------------------------------------------------


def test_a_record_carries_what_the_docs_promise() -> None:
    """README names seven fields. A missing one breaks a consumer's parser."""
    record = build_record(
        tier="local", op="insert", duration_ms=1.5, success=True, sql="INSERT INTO t", error=NOTHING)

    assert set(record) == {
        "ts",
        "tier",
        "op",
        "duration_ms",
        "success",
        "sql",
        "error",
    }, sorted(record)


def test_a_successful_record_carries_no_error() -> None:
    record = build_record(
        tier="local", op="select", duration_ms=0.1, success=True, sql="SELECT 1", error=NOTHING)

    assert record["success"] is True
    assert record["error"] == ""


def test_a_failed_record_keeps_the_error() -> None:
    record = build_record(
        tier="local",
        op="insert",
        duration_ms=0.1,
        success=False,
        sql="INSERT INTO t",
        error="UNIQUE constraint failed",
    )

    assert record["success"] is False
    assert "UNIQUE" in record["error"]


def test_the_line_is_one_json_object_per_line() -> None:
    """It is called JSONL. A newline inside it breaks every reader of it."""
    line = format_jsonl(
        build_record(
            tier="local",
            op="insert",
            duration_ms=1.5,
            success=False,
            sql="INSERT INTO t",
            error="it's broken\nsecond line",
        )
    )

    assert "\n" not in line, repr(line)
    assert json.loads(line)["error"] == "it's broken\nsecond line"


def test_the_line_survives_a_quote_in_the_sql() -> None:
    line = format_jsonl(
        build_record(
            tier="local",
            op="insert",
            duration_ms=1.0,
            success=True,
            sql="INSERT INTO t VALUES ('it\"s')", error=NOTHING)
    )

    assert json.loads(line)["sql"] == "INSERT INTO t VALUES ('it\"s')"


def test_emitting_writes_one_line_to_the_logger(caplog) -> None:
    """The one function here that is not pure, exercised through a real logger."""
    logger = logging.getLogger("declaro_persistum.test.instrumentation")

    with caplog.at_level(logging.INFO, logger=logger.name):
        emit_record(
            logger,
            build_record(
                tier="local", op="select", duration_ms=2.0, success=True, sql="SELECT 1", error=NOTHING),
        )

    assert len(caplog.records) == 1, caplog.records
    assert json.loads(caplog.records[0].getMessage())["op"] == "select"
