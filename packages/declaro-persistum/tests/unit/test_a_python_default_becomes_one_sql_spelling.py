"""A Python default was spelled for SQL by an isinstance chain, and an index
declared wrongly was dropped without a word.

TWO FINDINGS, both in the code that reads a models file.

FIRST, THE DEFAULT. `field(default=...)` takes a Python value and the loader
writes its SQL spelling. That was an if/elif chain on the type, and it
depended on `bool` being tested before `int`: `isinstance(True, int)` is True
in Python, so reversing two branches would spell `True` as `1` and a schema
declaring a boolean default would migrate on every deploy against an engine
that stores it as `TRUE`. The order was load-bearing and nothing said so.

SECOND, THE INDEX. `Meta.indexes` was read as

    dict(index_from_meta(idx) for idx in indexes if isinstance(idx, dict))

so an entry that was not a dict was skipped. An author who wrote one wrongly
got a table with no such index and no complaint, and found out when a query
was slow or a uniqueness constraint did not hold. Silently dropping a
declaration is the failure this package exists to avoid.
"""

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.pydantic_loader import sql_default


@pytest.mark.parametrize(
    "value,spelling",
    [
        ("pending", "'pending'"),
        ("", "''"),
        (True, "TRUE"),
        (False, "FALSE"),
        (0, "0"),
        (-1, "-1"),
        (1.5, "1.5"),
    ],
    ids=["a string", "an empty string", "true", "false", "zero", "negative", "a float"],
)
def test_a_python_value_has_one_sql_spelling(value: object, spelling: str) -> None:
    assert sql_default(value) == spelling


def test_a_bool_is_not_spelled_as_a_number() -> None:
    """The trap the isinstance chain depended on being avoided by ordering.

    `isinstance(True, int)` is True, so a chain testing int first spells
    `True` as `1`. A schema declaring a boolean default then migrates on every
    deploy against an engine that stores it as TRUE. Reading the exact type
    removes the ordering dependency rather than documenting it.
    """
    assert sql_default(True) == "TRUE"
    assert sql_default(1) == "1"
    assert sql_default(True) != sql_default(1)


def test_a_value_with_no_sql_spelling_is_refused() -> None:
    """A default the loader cannot write is a question, not a silence.

    Returning nothing for it would give the column no default at all, which
    is a schema quietly different from the one declared.
    """
    with pytest.raises(LoaderError) as raised:
        sql_default({"not": "a value"})

    assert "default" in str(raised.value).lower()
