"""`query.table.table` looks a table up in a schema, and had never run.

MEASURED 2026-08-19 with `coverage json` over the whole suite: none of its
four statements had executed. It survived the removal of `TableProxy` and
`ColumnProxy` because checking a name against the schema at build time is real
work, and nothing has checked that it does it.

TWO PUBLIC FUNCTIONS IN THIS PACKAGE ARE CALLED `table` AND THEY ARE NOT
RELATED:

    declaro_persistum.table(name)                the model decorator
    declaro_persistum.query.table.table(name, schema)   this lookup

Only the first is exported from the package root, so an import cannot pick the
wrong one. It has still cost something: a note in my own memory recorded
`table(name)` as taking a required `schema` argument, which is this function's
signature attached to that one, and it went uncorrected until somebody ran
`inspect.signature`.

WHAT MAKES THE LOOKUP WORTH KEEPING is the failure, not the success. Returning
the definition is a dict access; refusing an absent name and listing what the
schema does hold is the part a caller cannot easily write.
"""

from __future__ import annotations

import inspect

import pytest

from declaro_persistum import table as table_decorator
from declaro_persistum.query.table import table as table_lookup

pytestmark = [pytest.mark.precommit]

SCHEMA = {
    "users": {"columns": {"id": {"type": "integer", "primary_key": True}}},
    "posts": {"columns": {"id": {"type": "integer", "primary_key": True}}},
}


def test_it_returns_the_table_definition() -> None:
    assert table_lookup("users", SCHEMA) == SCHEMA["users"]


def test_it_returns_the_schema_s_own_object() -> None:
    """Not a copy, and not a proxy over one.

    The docstring says it returns the schema's own table definition, which is
    what makes it a lookup rather than a constructor.
    """
    assert table_lookup("users", SCHEMA) is SCHEMA["users"]


def test_a_name_that_is_not_there_is_refused() -> None:
    """The floor, and the reason the function exists.

    `schema[name]` raises KeyError with nothing but the name. A caller who
    mistyped a table wants to know which ones there are.
    """
    with pytest.raises(ValueError) as caught:
        table_lookup("userz", SCHEMA)

    assert "userz" in str(caught.value), caught.value


def test_the_refusal_lists_what_the_schema_does_hold() -> None:
    with pytest.raises(ValueError) as caught:
        table_lookup("userz", SCHEMA)

    message = str(caught.value)
    assert "users" in message and "posts" in message, message


def test_an_empty_schema_is_refused_rather_than_returning_nothing() -> None:
    with pytest.raises(ValueError):
        table_lookup("users", {})


def test_the_two_functions_called_table_are_still_different() -> None:
    """A gate on the collision, not a complaint about it.

    They are unrelated and only one is exported from the package root. This
    fails if they are ever conflated, which is the mistake the collision has
    already caused once in a note that outlived its correction.
    """
    assert table_decorator is not table_lookup
    assert list(inspect.signature(table_decorator).parameters) == ["name"]
    assert list(inspect.signature(table_lookup).parameters) == ["name", "schema"]
