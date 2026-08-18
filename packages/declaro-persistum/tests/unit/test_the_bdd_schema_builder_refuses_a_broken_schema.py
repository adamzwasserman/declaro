"""The BDD fixtures build tables from a `Schema`. This is what that must do.

WRITTEN BEFORE THE CODE. The three `setup_*_schema` functions were deleted on
2026-08-16 for eleven silent fallbacks, of which three mattered:

    col_def.get("type", "text")      a column with no type becomes TEXT
    table_def.get("columns", {})     a table with no columns becomes empty
    type_mapping.get(t, t.upper())   an unknown type passes through

The first two are the input side of silent failure. A schema missing a type is
a broken schema, and answering it with TEXT means every scenario built on that
schema tests a table nobody declared. Nothing in `schema_factory.py` omits a
type today, so the fallback was covering a case that never happens, which is
also why nothing would have caught it starting to.

THE THIRD IS NOT A FALLBACK AND IS KEPT. SQLite accepts any type name and
assigns affinity by rule, so "not in the translation table" is a real case
with a defined meaning rather than a missing answer. It is written as a
branch, not a default, so the two are told apart.

THE DDL IS BUILT BY PURE FUNCTIONS AND EXECUTED AT THE BOUNDARY (Rule 4).
`column_sql` and `create_table_sql` take data and return a string, so every
assertion below is `f(input) == expected` with no database (Rule 10). The
40-line type map used to be duplicated verbatim between the SQLite and Turso
builders; it is one function now, and the two engines differ only in who
awaits the execute.
"""

from __future__ import annotations

import pytest

from tests.bdd.factories.connection_factory import column_sql, create_table_sql

pytestmark = pytest.mark.precommit


# ---------------------------------------------------------------------------
# What it refuses
# ---------------------------------------------------------------------------


def test_a_column_with_no_type_raises_and_names_the_column() -> None:
    """The fallback this replaces answered TEXT and said nothing."""
    with pytest.raises(ValueError, match="pending.*no type"):
        column_sql("pending", {"nullable": False}, "sqlite")


def test_a_table_with_no_columns_raises_and_names_the_table() -> None:
    with pytest.raises(ValueError, match="orders.*no columns"):
        create_table_sql("orders", {}, "sqlite")


def test_a_table_whose_columns_are_empty_raises() -> None:
    """`{"columns": {}}` is a declared emptiness, and still not a table."""
    with pytest.raises(ValueError, match="orders.*no columns"):
        create_table_sql("orders", {"columns": {}}, "sqlite")


def test_an_unknown_dialect_raises_and_names_the_ones_that_exist() -> None:
    with pytest.raises(ValueError, match="mysql"):
        column_sql("id", {"type": "uuid"}, "mysql")


# ---------------------------------------------------------------------------
# The type translation, per engine
# ---------------------------------------------------------------------------


# (declared type, sqlite/turso rendering, postgresql rendering)
TRANSLATIONS = [
    ("uuid", "TEXT", "uuid"),
    ("timestamptz", "TEXT", "timestamptz"),
    ("boolean", "INTEGER", "boolean"),
    ("jsonb", "TEXT", "jsonb"),
    ("numeric", "REAL", "numeric"),
    ("text", "TEXT", "text"),
]


@pytest.mark.parametrize(("declared", "sqlite_type", "pg_type"), TRANSLATIONS)
def test_each_declared_type_renders_per_engine(
    declared: str, sqlite_type: str, pg_type: str
) -> None:
    """SQLite has no uuid, no timestamptz, no boolean and no jsonb.

    PostgreSQL takes the declared name unchanged, which is the whole reason
    the schemas are written in PostgreSQL's vocabulary.
    """
    assert column_sql("c", {"type": declared}, "sqlite") == f"c {sqlite_type}"
    assert column_sql("c", {"type": declared}, "turso") == f"c {sqlite_type}"
    assert column_sql("c", {"type": declared}, "postgresql") == f"c {pg_type}"


def test_sqlite_and_turso_translate_identically() -> None:
    """One table, two engines. They used to be two copies of forty lines."""
    for declared, _, _ in TRANSLATIONS:
        assert column_sql("c", {"type": declared}, "sqlite") == column_sql(
            "c", {"type": declared}, "turso"
        )


def test_a_type_outside_the_table_passes_through_upper_cased() -> None:
    """Not a fallback. SQLite assigns affinity by rule to any name.

    This is the branch the deleted `.get(t, t.upper())` hid inside a default.
    It is a real case with a defined meaning, so it is spelled as one.
    """
    assert column_sql("c", {"type": "blob"}, "sqlite") == "c BLOB"
    assert column_sql("c", {"type": "varchar(50)"}, "sqlite") == "c VARCHAR(50)"


def test_a_parameterised_type_matches_on_its_base_name() -> None:
    """`numeric(10,2)` is numeric, and SQLite wants REAL for it."""
    assert column_sql("c", {"type": "numeric(10,2)"}, "sqlite") == "c REAL"


# ---------------------------------------------------------------------------
# The constraints a column can carry
# ---------------------------------------------------------------------------


def test_a_primary_key_column_says_so() -> None:
    assert column_sql("id", {"type": "uuid", "primary_key": True}, "sqlite") == (
        "id TEXT PRIMARY KEY"
    )


def test_nullable_false_becomes_not_null_and_nullable_true_says_nothing() -> None:
    """`nullable: True` is the SQL default, so emitting NULL would be noise."""
    assert column_sql("c", {"type": "text", "nullable": False}, "sqlite") == (
        "c TEXT NOT NULL"
    )
    assert column_sql("c", {"type": "text", "nullable": True}, "sqlite") == "c TEXT"


def test_unique_is_carried() -> None:
    assert column_sql("email", {"type": "text", "unique": True}, "sqlite") == (
        "email TEXT UNIQUE"
    )


DEFAULT_TRANSLATIONS = [
    ("now()", "(datetime('now'))", "now()"),
    ("gen_random_uuid()", "(lower(hex(randomblob(16))))", "gen_random_uuid()"),
    ("'active'", "'active'", "'active'"),
    ("false", "false", "false"),
]


@pytest.mark.parametrize(("declared", "sqlite_default", "pg_default"), DEFAULT_TRANSLATIONS)
def test_a_default_is_translated_per_engine(
    declared: str, sqlite_default: str, pg_default: str
) -> None:
    """SQLite has neither `now()` nor `gen_random_uuid()`.

    This is why `examples/todo_app_native` passes its own timestamp: letting a
    `now()` default reach SQLite raises `unknown function: now()`.
    """
    assert column_sql("c", {"type": "text", "default": declared}, "sqlite") == (
        f"c TEXT DEFAULT {sqlite_default}"
    )
    assert column_sql("c", {"type": "text", "default": declared}, "postgresql") == (
        f"c text DEFAULT {pg_default}"
    )


def test_every_constraint_at_once_keeps_sql_order() -> None:
    """Type, then PRIMARY KEY, then NOT NULL, then UNIQUE, then DEFAULT."""
    column = {
        "type": "uuid",
        "primary_key": True,
        "nullable": False,
        "unique": True,
        "default": "gen_random_uuid()",
    }
    assert column_sql("id", column, "sqlite") == (
        "id TEXT PRIMARY KEY NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16))))"
    )


# ---------------------------------------------------------------------------
# The whole statement
# ---------------------------------------------------------------------------


def test_a_table_renders_every_column_in_declaration_order() -> None:
    table = {
        "columns": {
            "id": {"type": "uuid", "primary_key": True},
            "title": {"type": "text", "nullable": False},
        }
    }
    assert create_table_sql("todos", table, "sqlite") == (
        'CREATE TABLE IF NOT EXISTS todos (id TEXT PRIMARY KEY, title TEXT NOT NULL)'
    )


def test_a_composite_primary_key_becomes_a_table_constraint() -> None:
    """Two columns cannot each carry PRIMARY KEY, so it moves to the table."""
    table = {
        "columns": {
            "order_id": {"type": "uuid"},
            "product_id": {"type": "uuid"},
        },
        "primary_key": ["order_id", "product_id"],
    }
    sql = create_table_sql("order_items", table, "sqlite")

    assert sql.endswith("PRIMARY KEY (order_id, product_id))"), sql
    assert "order_id TEXT," in sql, sql


def test_postgresql_renders_a_foreign_key_reference() -> None:
    """`references` is PostgreSQL-only here, because only its builder read it."""
    table = {
        "columns": {
            "user_id": {"type": "uuid", "references": "users.id", "on_delete": "cascade"},
        }
    }
    sql = create_table_sql("orders", table, "postgresql")

    assert "REFERENCES users(id) ON DELETE CASCADE" in sql, sql


def test_the_real_fixture_schemas_all_render() -> None:
    """The three shipped schemas, on all three engines.

    The floor: every scenario in `tests/bdd` builds its tables from one of
    these, so a schema that cannot render is a suite that cannot run.
    """
    from tests.bdd.factories.schema_factory import (
        complex_ecommerce_schema,
        simple_todos_schema,
        simple_users_schema,
    )

    schemas = [simple_todos_schema(), simple_users_schema(), complex_ecommerce_schema()]
    rendered = [
        create_table_sql(name, table, dialect)
        for schema in schemas
        for name, table in schema.items()
        for dialect in ("sqlite", "turso", "postgresql")
    ]

    assert len(rendered) >= 15, (
        f"only {len(rendered)} statements rendered from three schemas across "
        f"three engines; the scan or the fixtures have shrunk"
    )
    assert all(s.startswith("CREATE TABLE IF NOT EXISTS ") for s in rendered)
