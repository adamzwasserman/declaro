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
from declaro_persistum.types import NOTHING

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

    rendered = generate_create_table_sql("t", columns, constraints=NOTHING)

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


@pytest.mark.parametrize("shape", sorted(_DEFAULTS), ids=sorted(_DEFAULTS))
def test_the_other_rebuilder_writes_it_the_same_way(shape: str) -> None:
    """THE SECOND REBUILDER HAD ITS OWN RULE AND IT WAS THE WRONG ONE.

    `table_reconstruction._generate_create_table_sql` wrote
    `f"DEFAULT {col_def['default']}"` raw while every other renderer called
    `sqlite_default`. Two consequences, both silent:

    An expression default lost a layer of parentheses on each rebuild, because
    `sqlite_default` wraps it twice and `PRAGMA table_info` strips one. The
    differ then saw a change that was not one and migrated on every deploy.

    A default whose type has no SQL spelling was formatted into the statement
    instead of being refused, and this renderer runs AFTER the real table has
    been dropped.
    """
    from declaro_persistum.abstractions.table_reconstruction import (
        _generate_create_table_sql,
    )

    value = _DEFAULTS[shape]
    rendered = _generate_create_table_sql(
        "t", {"c": {"type": "text", "default": value}}, NOTHING
    )

    assert sqlite_default(value) in rendered, rendered


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


@pytest.mark.parametrize(
    "value,spelling",
    [
        (None, "DEFAULT NULL"),
        ("(datetime('now'))", "DEFAULT ((datetime('now')))"),
        ("datetime('now')", "DEFAULT datetime('now')"),
        (0, "DEFAULT 0"),
        (-1, "DEFAULT -1"),
        (1.5, "DEFAULT 1.5"),
        (True, "DEFAULT True"),
    ],
    ids=["nothing", "an expression", "a bare function", "zero", "negative", "a float", "a flag"],
)
def test_each_spelling_is_keyed_by_the_type_it_answers(
    value: object, spelling: str
) -> None:
    """The vocabulary, stated as a table rather than an ordering.

    The chain this replaces tested `value is None`, then `isinstance(value,
    str) and value.startswith("(")`, then fell through. Falling through is
    what made an unspellable default silent: a dict, a list or a date object
    was formatted into the SQL by `f"DEFAULT {value}"` and the engine met
    `DEFAULT {'a': 1}`.
    """
    assert sqlite_default(value) == spelling


@pytest.mark.parametrize(
    "value,shape",
    [({"a": 1}, "a dict"), (["a"], "a list"), ((1, 2), "a pair")],
    ids=["a dict", "a list", "a pair"],
)
def test_a_default_with_no_sqlite_spelling_is_refused(value: object, shape: str) -> None:
    """A default the writer cannot spell is a question, not a formatted repr.

    `f"DEFAULT {value}"` accepts every Python object, so a value nobody
    declared a spelling for became syntactically valid nonsense in a CREATE
    TABLE. On a rebuild that statement runs after the real table is dropped.
    """
    with pytest.raises(ValueError) as raised:
        sqlite_default(value)

    assert "default" in str(raised.value).lower()
