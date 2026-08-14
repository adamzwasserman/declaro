"""A dialect these functions do not know must not get PostgreSQL SQL.

    array_reindex_sql("users", "tags", "mysql")     -> ROW_NUMBER() ...
    array_reindex_sql("users", "tags", "")          -> ROW_NUMBER() ...
    array_reindex_sql("users", "tags", "nonsense")  -> ROW_NUMBER() ...

Both functions were `if dialect in ("sqlite", "turso"): ... else: <postgresql>`.
The `else` is not a PostgreSQL branch, it is a catch-all, so every dialect the
function has never heard of silently receives PostgreSQL syntax. A typo returns
SQL for the wrong engine, and the caller finds out when the database rejects it
— or does not find out, if the wrong syntax happens to parse.

FOUND BY RESTORING test_arrays.py AND test_maps.py. Four of their tests pass
"libsql", a dialect this package abolished, and assert the SQL is the SQLite
form. They fail because "libsql" now falls into the catch-all. The tests were
right about the behaviour and wrong only about the name: an unrecognised
dialect should never quietly produce SQL for a different engine.

`dialect` also carried `= "postgresql"`, so omitting it was indistinguishable
from choosing it (Rule 14). These generate engine-specific SQL; which engine is
never a detail the caller can be assumed to have meant.

The fix is the dispatch table (Rule 1): the key is looked up, a miss raises and
names what is supported, and there is no branch left for a value nobody
declared.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.precommit

GENERATORS = ("array_reindex_sql", "map_set_sql")


def _fn(name: str):
    from declaro_persistum.abstractions import arrays, maps

    return getattr(arrays, name, None) or getattr(maps, name)


@pytest.mark.parametrize("name", GENERATORS)
@pytest.mark.parametrize("dialect", ["mysql", "libsql", "", "nonsense", "SQLite"])
def test_an_unknown_dialect_is_refused_and_named(name: str, dialect: str) -> None:
    with pytest.raises(ValueError) as e:
        _fn(name)("users", "tags", dialect)
    assert dialect in str(e.value) or repr(dialect) in str(e.value), (
        f"the failure must name the dialect it was given: {e.value}"
    )


@pytest.mark.parametrize("name", GENERATORS)
def test_the_dialect_is_required(name: str) -> None:
    """Omitting it used to be indistinguishable from choosing PostgreSQL."""
    params = inspect.signature(_fn(name)).parameters
    assert params["dialect"].default is inspect.Parameter.empty, (
        f"{name} defaults its dialect, so a caller who forgot which engine "
        f"they are on gets PostgreSQL SQL and no warning"
    )


def test_each_known_dialect_gets_its_own_reindex() -> None:
    """SQLite has no window functions in this path; PostgreSQL uses one."""
    from declaro_persistum.abstractions.arrays import array_reindex_sql

    assert "ROW_NUMBER()" in array_reindex_sql("users", "tags", "postgresql")
    for dialect in ("sqlite", "turso"):
        sql = array_reindex_sql("users", "tags", dialect)
        assert "COUNT(*)" in sql, dialect
        assert "ROW_NUMBER()" not in sql, dialect


def test_sqlite_and_turso_reindex_identically() -> None:
    """One DB-API form. Two copies would be two places to fix a bug."""
    from declaro_persistum.abstractions.arrays import array_reindex_sql

    assert array_reindex_sql("users", "tags", "sqlite") == array_reindex_sql(
        "users", "tags", "turso"
    )


def test_each_known_dialect_gets_its_own_map_set() -> None:
    from declaro_persistum.abstractions.maps import map_set_sql

    assert "EXCLUDED" in map_set_sql("users", "metadata", "postgresql")
    for dialect in ("sqlite", "turso"):
        sql = map_set_sql("users", "metadata", dialect)
        assert "SET value = :value" in sql, dialect
        assert "EXCLUDED" not in sql, dialect
