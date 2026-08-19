"""A table with no columns validated clean and rendered unparseable SQL.

MEASURED 2026-08-19:

    validate_schema({"empty": {}})   no errors, no warnings
    the differ emits                 create_table empty
    rendered                         CREATE TABLE "empty" (\\n    \\n)
    sqlite                           OperationalError: near ")": syntax error

THIS IS THE SECOND OF THE THREE FALLBACKS the BDD schema helpers were deleted
for on 2026-08-16. The note in
`test_the_bdd_schema_builder_refuses_a_broken_schema` lists them:

    col_def.get("type", "text")      a column with no type becomes TEXT
    table_def.get("columns", {})     a table with no columns becomes empty
    type_mapping.get(t, t.upper())   an unknown type passes through

The first was fixed in the TOML loader and the validator earlier today. This
is the second, and it lives in the validator, the differ, the renderers and
the transfer, wherever something asks a table for its columns and accepts an
empty answer.

WHAT IT COSTS. A table declared with its columns nested one level wrong, or a
TOML section whose `[users.columns]` header is misspelt, produces a table the
author never described. Nothing objects until the engine sees the SQL, and
the engine's complaint names a parenthesis.

REFUSED AT THE BOUNDARY, not in the renderers, for the reason the typeless
column was: one declarative place that every path which writes DDL already
consults, rather than the same question asked in each renderer.
"""

from __future__ import annotations

import pytest

from declaro_persistum.validator import validate_schema

pytestmark = [pytest.mark.precommit]


def test_a_table_with_no_columns_key_is_an_error() -> None:
    errors = validate_schema({"empty": {}})["errors"]

    assert errors, "a table with no columns validated clean"


def test_a_table_with_an_empty_columns_map_is_an_error() -> None:
    """`columns = {}` is the same table said a different way."""
    errors = validate_schema({"empty": {"columns": {}}})["errors"]

    assert errors, "an empty columns map validated clean"


def test_the_error_names_the_table() -> None:
    errors = validate_schema({"empty": {}})["errors"]

    assert any("empty" in e for e in errors), errors


def test_a_table_with_columns_is_not_an_error() -> None:
    """The control. A check that refused everything would pass the rest."""
    errors = validate_schema(
        {"users": {"columns": {"id": {"type": "integer", "primary_key": True}}}}
    )["errors"]

    assert errors == [], errors


def test_one_empty_table_does_not_hide_the_rest() -> None:
    """Validation reports every problem, not the first one it meets.

    An author with two mistakes should see two, rather than fixing one and
    running again to find the next.
    """
    report = validate_schema(
        {
            "empty": {},
            "also_empty": {"columns": {}},
            "fine": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        }
    )

    named = " ".join(report["errors"])
    assert "empty" in named and "also_empty" in named, report["errors"]
