"""One column vocabulary, one rendering per dialect.

`field(references=..., on_delete="cascade")` and `type: "uuid"` say what a
column MEANS. What it is spelled as belongs to the engine. That is the whole
multi-dialect surface, and half of it was missing:

    map_type("uuid")           -> "TEXT"      # SQLite affinity
    map_type("numeric(10,2)")  -> "REAL"

`map_type` took no dialect. It was the SQLite affinity mapper wearing a neutral
name, `applier/postgresql.py` never imported it, and PostgreSQL got its own
column renderer that emits the declared type verbatim. Two renderers differing
in one line.

THREE BYTE-IDENTICAL COPIES existed: `applier/shared.map_type`,
`abstractions/reconstruction._map_type`, `abstractions/table_reconstruction
._map_type`. Verified identical across ten types before consolidating, because
"they look the same" is not the same claim as "they answer the same".

`dialect` is required, not defaulted (Rule 14). Which engine a type is spelled
for is never something a caller can be assumed to have meant, and a default
would have made PostgreSQL silently receive SQLite affinity, which is the bug
this file exists to stop.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.precommit

# What the declared vocabulary means on each engine. SQLite has five storage
# classes, so uuid, timestamps and json all land in TEXT; PostgreSQL has real
# types for each and takes the declaration as written.
CASES = {
    "uuid": {"sqlite": "TEXT", "turso": "TEXT", "postgresql": "uuid"},
    "timestamptz": {"sqlite": "TEXT", "turso": "TEXT", "postgresql": "timestamptz"},
    "jsonb": {"sqlite": "TEXT", "turso": "TEXT", "postgresql": "jsonb"},
    "boolean": {"sqlite": "INTEGER", "turso": "INTEGER", "postgresql": "boolean"},
    "numeric(10,2)": {"sqlite": "REAL", "turso": "REAL", "postgresql": "numeric(10,2)"},
    "bigint": {"sqlite": "INTEGER", "turso": "INTEGER", "postgresql": "bigint"},
    "bytea": {"sqlite": "BLOB", "turso": "BLOB", "postgresql": "bytea"},
}


def test_the_dialect_is_required() -> None:
    from declaro_persistum.applier.shared import map_type

    params = inspect.signature(map_type).parameters
    assert "dialect" in params, "map_type takes no dialect, so it can only be one"
    assert params["dialect"].default is inspect.Parameter.empty, (
        "map_type defaults its dialect, so a caller who forgot which engine "
        "they target gets one silently"
    )


@pytest.mark.parametrize("declared,expected", CASES.items())
def test_each_dialect_spells_the_declared_type_its_own_way(
    declared: str, expected: dict[str, str]
) -> None:
    from declaro_persistum.applier.shared import map_type

    for dialect, want in expected.items():
        assert map_type(declared, dialect) == want, (
            f"{declared!r} on {dialect}: got {map_type(declared, dialect)!r}, "
            f"want {want!r}"
        )


def test_postgresql_does_not_receive_sqlite_affinity() -> None:
    """The bug in one assertion: uuid must not become TEXT on PostgreSQL."""
    from declaro_persistum.applier.shared import map_type

    assert map_type("uuid", "postgresql") != "TEXT"
    assert map_type("numeric(10,2)", "postgresql") != "REAL"


def test_an_unknown_dialect_is_refused_and_named() -> None:
    from declaro_persistum.applier.shared import map_type

    with pytest.raises(ValueError) as e:
        map_type("uuid", "mysql")
    assert "mysql" in str(e.value)


def test_there_is_one_type_map_not_three() -> None:
    """The mapping BODY exists once. Delegating to it is fine; copying is not.

    Asserted on the source rather than by identity, because one caller wraps
    `sqlite_type` with a non-string fallback it is separately tested for, so
    the two are not the same object and should not be.

    The marker is the last branch of the affinity chain. A module that contains
    it has its own copy of the mapping.
    """
    import inspect

    from declaro_persistum.abstractions import reconstruction, table_reconstruction
    from declaro_persistum.applier import shared

    MARKER = '("blob", "bytea")'
    assert MARKER in inspect.getsource(shared.sqlite_type), (
        "the marker no longer identifies the mapping; this test is blind"
    )
    for module in (reconstruction, table_reconstruction):
        src = inspect.getsource(module)
        assert MARKER not in src, (
            f"{module.__name__} carries its own copy of the affinity mapping; "
            f"three copies is three places for a dialect to be forgotten"
        )
        assert "sqlite_type" in src, (
            f"{module.__name__} does not reach the shared mapping at all"
        )


def test_a_declared_column_renders_per_dialect_end_to_end() -> None:
    """The surface, asserted where a consumer would feel it."""
    from declaro_persistum.applier.shared import column_definition

    col = {"type": "uuid", "primary_key": True}
    assert "TEXT" in column_definition("id", col, "sqlite")
    assert "uuid" in column_definition("id", col, "postgresql")
