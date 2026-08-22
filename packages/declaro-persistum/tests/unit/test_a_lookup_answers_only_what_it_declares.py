"""Six lookups answered for inputs nobody wrote a rule for.

`d.get(key, default)` re-opens an input space a dispatch table had closed.
The table states the vocabulary; the second argument says "and anything else
gets this", which is the silent fallback the table existed to remove.

THE RENDER TABLE IS THE ONE THAT WRITES NONSENSE INTO THE SQL. A query
expression carries a `kind` tag, and `_RENDER_ARGUMENT` maps two of them.
Anything else fell to `_render_literal`, which interpolates `str(arg)`. So an
argument tagged `{"kind": "windo"}` -- a typo for a window function --
rendered, measured 2026-08-22, as

    UPPER({'kind': 'windo', 'over': 'x'})

which is a Python dict repr inside a SQL statement. Nothing bound it, nothing
escaped it, and the engine is the first thing that sees the mistake.

Each of the others is a real answer rather than a silence, and each says so
by naming the case in the table instead of hiding it behind a default:

    a literal          the None tag, which is what an untagged value carries
    an untranslated    a default with no dialect spelling, which stays as it is
    other              a statement whose first word is not one we classify
"""

import pytest

from declaro_persistum.applier.shared import sqlite_default
from declaro_persistum.exceptions import ValidationError
from declaro_persistum.instrumentation import classify_sql
from declaro_persistum.query.table import render_function


def test_an_untagged_argument_renders_as_a_literal() -> None:
    """The None tag is a member of the vocabulary, not a fallback."""
    sql, params = render_function(
        {"kind": "function", "name": "UPPER", "args": ["x"], "alias": None},
        "sqlite",
        "p",
    )

    assert sql == "UPPER(x)", sql
    assert params == {}, "a literal is written into the SQL, not bound"


def test_an_argument_tagged_with_a_kind_nobody_renders_is_refused() -> None:
    """The defect. A typo in a tag became a bound parameter holding a dict.

    `_RENDER_ARGUMENT.get(kind_of(arg), _render_literal)` answered for every
    tag, so the two the table declares and every tag it does not were the
    same question. The query ran and returned rows for a value nobody meant.
    """
    with pytest.raises(ValidationError) as raised:
        render_function(
            {
                "kind": "function",
                "name": "UPPER",
                "args": [{"kind": "windo", "over": "x"}],
                "alias": None,
            },
            "sqlite",
            "p",
        )

    assert "windo" in str(raised.value)


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT 1", "select"),
        ("INSERT INTO t VALUES (1)", "insert"),
        ("VACUUM", "other"),
        ("", "other"),
    ],
    ids=["a select", "an insert", "a statement we do not classify", "nothing"],
)
def test_a_statement_is_classified_or_called_other(sql: str, expected: str) -> None:
    """`other` is a category, so it is written in the table.

    Reading it out of a `.get` default made it indistinguishable from a key
    somebody meant to add and forgot.
    """
    assert classify_sql(sql) == expected


def test_a_default_with_no_sqlite_spelling_is_still_refused() -> None:
    """The refusal survives the rewrite.

    `_SQLITE_DEFAULT.get(spelling, _refuse_unspellable_default)` closed the
    space rather than re-opening it, because the fallback raised. It reads by
    subscript now, and the refusal happens before the lookup.
    """
    with pytest.raises(ValueError) as raised:
        sqlite_default({"not": "a value"})

    assert "dict" in str(raised.value)
