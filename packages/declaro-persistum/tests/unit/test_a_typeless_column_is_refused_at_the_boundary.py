"""A column with no type still became TEXT everywhere except the TOML loader.

The TOML loader was fixed on 2026-08-19 to refuse one. Measured the same day,
every other route still answers with TEXT and nothing complains:

    column_definition("c", {}, "sqlite")      '"c" TEXT'
    column_definition("c", {}, "turso")       '"c" TEXT'
    column_definition("c", {}, "postgresql")  '"c" text'
    the reconstruction renderer               '"c" TEXT'
    validate_schema                           no errors, no warnings

So a schema built through the API, or by any loader that ever omits a type,
produces a TEXT column on every engine and is reported as valid.

THE BOUNDARY IS WHERE THIS BELONGS, not the renderers. Three renderers each
guarding themselves is the defensive-interior shape this codebase rejects:
the same question answered in three places, drifting independently. The
validator is one place, it is declarative, and as of today every path that
writes DDL asks it -- `apply_migrations_async`, `declaro apply`, and
`declaro migrate-remote`.

WHY TEXT IS THE WORST POSSIBLE GUESS. It is the one type that accepts every
value, so the column takes integers, timestamps and JSON without complaint
and stores each as a string. Nothing downstream can tell that from a column
the author meant to be TEXT, and the first sign of trouble is a comparison or
a sort behaving oddly in production.

WHAT IS LEFT ALONE AND WHY. The renderers keep their defaults. They are now
unreachable for any schema that went through the validator, and adding a
second refusal in each would be three more places to keep in step. This file
records that they still have them.
"""

from __future__ import annotations

import pytest

from declaro_persistum.applier.shared import column_definition
from declaro_persistum.validator import validate_schema

pytestmark = [pytest.mark.precommit]

ENGINES = ["sqlite", "turso", "postgresql"]


def _schema(column: dict) -> dict:
    return {
        "widgets": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "c": column,
            }
        }
    }


def test_a_column_with_no_type_is_an_error() -> None:
    errors = validate_schema(_schema({}))["errors"]

    assert errors, "a typeless column validated clean"


def test_the_error_names_the_table_and_the_column() -> None:
    errors = validate_schema(_schema({}))["errors"]

    assert any("widgets" in e and "'c'" in e for e in errors), errors


def test_a_column_with_only_other_properties_is_still_an_error() -> None:
    """The realistic shape: a typo in the type key, everything else present."""
    errors = validate_schema(_schema({"nullable": False, "unique": True}))["errors"]

    assert errors, "a column with properties but no type validated clean"


def test_an_empty_type_is_an_error_too() -> None:
    """`type = ""` is not a type, and reaches the renderer as one."""
    errors = validate_schema(_schema({"type": ""}))["errors"]

    assert errors, 'type="" validated clean'


def test_a_typed_column_is_not_an_error() -> None:
    """The control. A check that refused everything would pass the rest."""
    errors = validate_schema(_schema({"type": "integer"}))["errors"]

    assert errors == [], errors


def test_an_unknown_type_is_still_only_a_warning() -> None:
    """The distinction this must not blur.

    An unknown type may be a custom type or an enum, and the three migrate
    paths stop on errors and not warnings. Turning this into an error would
    refuse schemas that work.
    """
    report = validate_schema(_schema({"type": "geography"}))

    assert report["errors"] == [], report["errors"]
    assert report["warnings"], report


@pytest.mark.parametrize("engine", ENGINES)
def test_the_renderers_still_default_and_that_is_recorded(engine: str) -> None:
    """Not a demand that they keep doing it. A record that they do.

    They are unreachable for a schema that went through the validator. If a
    renderer is ever given its own refusal, this test is the one that should
    fail, and its failure is the reminder to delete it.
    """
    assert "TEXT" in column_definition("c", {}, engine).upper()
