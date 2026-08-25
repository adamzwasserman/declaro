"""An unrecognised dialect must never quietly produce SQL for another engine.

WHAT THIS USED TO TEST. Two generators in `abstractions/arrays.py` and
`abstractions/maps.py`, each of which took `dialect="postgresql"` as a default,
so omitting it was indistinguishable from choosing it. Both modules are gone:
no feature file described them, nothing in `src` called them, and no test ever
executed the SQL they wrote.

The principle outlived them, and it is better placed here than it was there.
`for_dialect` is the one function the whole package resolves a dialect through
-- every applier, inspector and type-mapping table reads its entry that way --
so asking it once covers what asking two generators could not.
"""

from __future__ import annotations

import inspect

import pytest

from declaro_persistum.types import for_dialect

pytestmark = pytest.mark.precommit

A_TABLE = {"postgresql": "pg", "sqlite": "lite", "turso": "turso"}


@pytest.mark.parametrize(
    "dialect", ["mysql", "libsql", "", "nonsense", "SQLite", "POSTGRESQL"]
)
def test_an_unknown_dialect_is_refused_and_named(dialect: str) -> None:
    """Including the two that differ from a real one only in case.

    `libsql` is here because it was a supported dialect until it was
    eradicated, and a caller still passing it must be told rather than served
    whatever the table happens to hold.
    """
    with pytest.raises(ValueError) as raised:
        for_dialect(A_TABLE, dialect, "Test tables")

    assert dialect in str(raised.value) or repr(dialect) in str(raised.value), (
        f"the failure must name the dialect it was given: {raised.value}"
    )


@pytest.mark.parametrize("dialect", sorted(A_TABLE))
def test_a_known_dialect_gets_its_own_entry(dialect: str) -> None:
    """The lookup returns the entry, rather than a nearby one."""
    assert for_dialect(A_TABLE, dialect, "Test tables") == A_TABLE[dialect]


def test_the_refusal_names_what_does_exist() -> None:
    """A bare KeyError says what was asked for and not what was available."""
    with pytest.raises(ValueError) as raised:
        for_dialect(A_TABLE, "mysql", "Test tables")

    message = str(raised.value)
    for dialect in A_TABLE:
        assert dialect in message, f"{dialect} is supported and unnamed: {message}"


def test_no_dispatch_table_is_read_with_a_default() -> None:
    """`.get(dialect, something)` is the shape this whole file exists against.

    A default turns "this engine is not supported" into "here is another
    engine's SQL", which is the failure the two deleted generators had: they
    defaulted to PostgreSQL, so a caller who forgot which engine they were on
    got PostgreSQL SQL and no warning.
    """
    assert "dialect" in inspect.signature(for_dialect).parameters
    assert (
        inspect.signature(for_dialect).parameters["dialect"].default
        is inspect.Parameter.empty
    ), "for_dialect defaults its dialect, so omitting it chooses one silently"
