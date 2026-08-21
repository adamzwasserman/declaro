"""Two places wrote a SQLite DEFAULT clause, and they had to agree.

`applier/shared.sqlite_default` and the DEFAULT branch of
`abstractions/reconstruction.generate_create_table_sql` both turn a declared
default into the text that follows `DEFAULT` in a CREATE TABLE. Both carried
the same rule, written twice:

    None                 -> DEFAULT NULL
    "(datetime('now'))"  -> DEFAULT ((datetime('now')))
    0                    -> DEFAULT 0

THE DOUBLE WRAP IS THE PART THAT MUST NOT DRIFT. `PRAGMA table_info` strips
one layer of parentheses from an expression default, so a value written with
one layer reads back with none and the differ proposes a change on every
deploy. Writing two layers is what makes the round trip agree, and it is the
kind of rule that gets fixed in one copy.

Reconstruction is where it matters most: it drops the real table and rebuilds
it from these definitions, so a default written differently there is a default
the rebuilt table does not have.
"""

import pytest

from declaro_persistum.abstractions.reconstruction import generate_create_table_sql
from declaro_persistum.applier.shared import sqlite_default

_DEFAULTS = {
    "nothing": None,
    "an expression": "(datetime('now'))",
    "a bare function": "datetime('now')",
    "a number": 0,
    "a negative number": -1,
    "a quoted string": "'pending'",
    "a flag": 1,
}


@pytest.mark.parametrize("shape", sorted(_DEFAULTS), ids=sorted(_DEFAULTS))
def test_the_rebuild_writes_what_the_applier_writes(shape: str) -> None:
    """One rule, so a rebuilt table keeps the default the applier gave it.

    The rendered CREATE TABLE must contain exactly what `sqlite_default`
    produces. Comparing the two outputs is the assertion the two copies could
    not make about each other.
    """
    value = _DEFAULTS[shape]
    columns = {"c": {"type": "text", "default": value}}

    rendered = generate_create_table_sql("t", columns)

    assert sqlite_default(value) in rendered, rendered


def test_an_expression_default_is_wrapped_twice() -> None:
    """The specific rule, stated so its removal is visible.

    One layer survives `PRAGMA table_info`, so two go in and one comes back.
    """
    assert sqlite_default("(datetime('now'))") == "DEFAULT ((datetime('now')))"


def test_a_plain_value_is_not_wrapped() -> None:
    """The control. Wrapping everything would change what a number means."""
    assert sqlite_default(0) == "DEFAULT 0"
    assert sqlite_default("'pending'") == "DEFAULT 'pending'"
    assert sqlite_default(None) == "DEFAULT NULL"


def test_the_rebuild_has_no_default_rule_of_its_own() -> None:
    """The floor. A second copy can drift; a call cannot.

    `generate_create_table_sql` used to build the clause itself, including its
    own `startswith("(")` test. It calls `sqlite_default` now, and this fails
    if the branch comes back.
    """
    import inspect

    source = inspect.getsource(generate_create_table_sql)

    assert "DEFAULT NULL" not in source, (
        "the rebuild is writing its own DEFAULT clause again; call "
        "sqlite_default so the two cannot disagree"
    )
