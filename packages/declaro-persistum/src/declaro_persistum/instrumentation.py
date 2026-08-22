"""
Connection-level latency instrumentation for declaro_persistum.

Records timing data for a query execution.
Zero overhead when disabled — no proxy wrapping, no timing, no logging.

Usage:
"""

import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from itertools import accumulate, groupby
from typing import TypedDict

from declaro_persistum.boundary import boundary, helper, recognizer
from declaro_persistum.types import Nothing, resolved


class LatencyRecord(TypedDict):
    """One timing record per execute() call."""

    ts: str           # ISO 8601 timestamp
    tier: str         # Caller-supplied label (e.g. "central", "project")
    op: str           # Classified from SQL: select|insert|update|delete|create|alter|other
    duration_ms: float
    success: bool
    sql: str          # First 120 chars of SQL
    error: str        # First 200 chars of exception string, or ""


_SQL_OP_MAP: dict[str, str] = {
    "select": "select",
    "insert": "insert",
    "update": "update",
    "delete": "delete",
    "create": "create",
    "alter": "alter",
    "drop": "alter",
    "pragma": "other",
    "with": "select",
    # SQLite's upsert. It was absent, so `REPLACE INTO` classified as "other"
    # and `is_write_op` said False about a statement that rewrites rows.
    "replace": "insert",
}

_WRITE_OPS = {"insert", "update", "delete"}


@recognizer
def classify_sql(sql: str) -> str:
    """Classify SQL statement into an op type via dict lookup on first keyword.

    A CTE IS CLASSIFIED BY THE STATEMENT IT LEADS TO, not by the `WITH`. A
    data-modifying CTE is ordinary SQL -- `WITH x AS (...) DELETE FROM t ...`
    on PostgreSQL, and the same for UPDATE and INSERT -- and reading the first
    word called all of them `select`. Measured 2026-08-17:

        REPLACE INTO t VALUES (1)          -> 'other'   write=False
        WITH x AS (...) DELETE FROM t ...  -> 'select'  write=False

    Both are writes. A consumer using `is_write_op` to decide what goes
    through the write queue would have sent them down the read path.

    An unrecognised statement is still "other" rather than a guess, and that
    fallback is right here in a way it is not elsewhere in this package: SQL
    has more verbs than this table will ever hold, so an unknown one is not a
    caller error the way an unknown dialect is.
    """
    stripped = sql.strip()
    # A STATEMENT WITH NO WORDS HAS NO FIRST KEYWORD, and the empty string is
    # not in the table, so `_verb` answers "other" for it without a branch of
    # its own. This used to be a separate early return saying the same thing.
    first = [*stripped.split(None, 1), ""][0].lower()
    return _LEADS_WITH_A_CTE[first == "with"](stripped, first)


@helper
def _named_verb(word: str, _unrecognised: str) -> str:
    """A keyword this table names."""
    return _SQL_OP_MAP[word]


@helper
def _unnamed_verb(_word: str, unrecognised: str) -> str:
    """A keyword it does not, which is an answer rather than a silence."""
    return unrecognised


# WHETHER THE TABLE NAMES THIS KEYWORD, as a table rather than a `.get`
# fallback. THE OPEN SPACE IS REAL HERE, unlike everywhere else in this
# package: SQL has more verbs than this will ever hold, so an unknown one is
# not a caller error the way an unknown dialect is. Writing the two cases out
# is what tells a reader that, where a second argument to `.get` reads the
# same as the fallbacks this package has spent the day removing.
_KNOWN_VERB: dict[bool, Callable[[str, str], str]] = {
    True: _named_verb,
    False: _unnamed_verb,
}


@recognizer
def _verb(word: str, unrecognised: str) -> str:
    """The operation a keyword names, or `unrecognised` when it names none."""
    return _KNOWN_VERB[word in _SQL_OP_MAP](word, unrecognised)


@helper
def _the_cte_verb(stripped: str, _first: str) -> str:
    """A `WITH ...` is named by the statement that follows its CTE bodies."""
    return _verb(_after_the_cte(stripped), "select")


@helper
def _the_first_verb(_stripped: str, first: str) -> str:
    """Every other statement is named by its own first keyword."""
    return _verb(first, "other")


# WHETHER THE STATEMENT OPENS WITH A CTE, as a table rather than a branch.
_LEADS_WITH_A_CTE: dict[bool, Callable[[str, str], str]] = {
    True: _the_cte_verb,
    False: _the_first_verb,
}


@recognizer
def _after_the_cte(sql: str) -> str:
    """The keyword a `WITH ...` statement actually performs.

    Reads past balanced parentheses so a SELECT inside the CTE body is not
    mistaken for the statement's own verb.

    THE SCAN IS THREE LISTS INSTEAD OF A STATEFUL LOOP, and each says one
    thing. `deltas` is what each character does to the nesting; `depth`
    is where the nesting stood before that character; `visible` keeps the
    characters that sit outside every parenthesis. Words come out of
    `visible`, and the first one the table names is the statement's verb.

    A word never terminated by punctuation is returned as it stands, which is
    what the loop's final `return token.lower()` did.
    """
    body = sql[len("with"):]
    deltas = [(char == "(") - (char == ")") for char in body]
    depth = [0, *accumulate(deltas)][:-1]
    visible = "".join(
        char
        for char, delta, outside in zip(body, deltas, depth, strict=True)
        if delta == 0 and outside == 0
    )
    words = [
        "".join(run) for alpha, run in groupby(visible, str.isalpha) if alpha
    ]
    named = [
        lowered
        for word in words
        for lowered in [word.lower()]
        if lowered in _SQL_OP_MAP and lowered != "with"
    ]
    unterminated = [word.lower() for word in words[-1:] if visible[-1:].isalpha()]
    return [*named, *unterminated, ""][0]


@recognizer
def is_write_op(op: str) -> bool:
    """Return True if the op type is a write (insert/update/delete)."""
    return op in _WRITE_OPS


# RETURNING is whole-word, case-insensitive, and not inside a string literal
# or identifier. The regex below uses ``\b`` word boundaries so column names
# like ``returning_user`` do not match. RETURNING only appears at top level
# in declaro-generated SQL — we do not emit it inside CTEs or subqueries.
_RETURNING_RE = re.compile(r"\bRETURNING\b", re.IGNORECASE)


@recognizer
def has_returning_clause(sql: str) -> bool:
    """Return True if the SQL has a RETURNING clause.

    Used by the executor to decide whether a write op should be routed
    through the fetch path (RETURNING present, rows expected) or the
    count path (no RETURNING, just rowcount). Pure function.
    """
    return _RETURNING_RE.search(sql) is not None


@helper
def build_record(
    *,
    tier: str,
    op: str,
    duration_ms: float,
    success: bool,
    sql: str,
    error: str | Nothing,
) -> LatencyRecord:
    """Build a LatencyRecord from execution metadata."""
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    error = resolved(error, '')
    return {
        "ts": datetime.now(UTC).isoformat(),
        "tier": tier,
        "op": op,
        "duration_ms": round(duration_ms, 3),
        "success": success,
        "sql": sql[:120],
        "error": error[:200],
    }


@helper
def format_jsonl(record: LatencyRecord) -> str:
    """Format a LatencyRecord as a JSONL line (no trailing newline added by caller)."""
    import json

    return json.dumps(record)








@boundary
def emit_record(logger: logging.Logger, record: LatencyRecord) -> None:
    """Write a LatencyRecord to the latency logger."""
    logger.info(format_jsonl(record))


