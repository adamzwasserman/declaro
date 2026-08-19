"""A name this package cannot render must be refused where it is declared.

MEASURED 2026-08-18, declaring one table per name and migrating:

    name          sqlite                       postgresql
    od'd          created, converges           created, converges
    with space    created, converges           created, converges
    quo"ted       MigrationError on create     MigrationError on create
    sel;ect       MigrationError on create     created, converges

The appliers write an identifier as `"{name}"` at 65 places and none of them
escapes a double quote, so `quo"ted` renders `"quo"ted"` and the engine reads
three tokens. The SQLite and Turso appliers then split rendered SQL on `;`
before executing it, so a name holding a semicolon is torn in half; PostgreSQL
executes the statement whole and is unaffected.

REFUSING AT THE BOUNDARY RATHER THAN ESCAPING AT 65 SITES. Escaping every
render site is the other course and it is a mechanical change to every
statement this package writes. The boundary is one place, the check is
declarative, and a name that cannot survive the round trip is a name the
author should hear about while they are writing it rather than during a
deploy. This is the same trade `column_definition` already makes for a
default it cannot spell: it refuses and says what to do instead.

WHAT IS NOT CLAIMED. Refusing the name does not make the renderer safe; it
makes the declaration honest. A database that already holds such a table is
introspected correctly, because the reading side was fixed separately, and
this check never sees it: it validates what was declared.
"""

from __future__ import annotations

import pytest

from declaro_persistum.validator import validate_schema

pytestmark = [pytest.mark.precommit]


def _one_table(name: str) -> dict:
    return {name: {"columns": {"id": {"type": "integer", "primary_key": True}}}}


def _one_column(name: str) -> dict:
    return {
        "widgets": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                name: {"type": "text"},
            }
        }
    }


# The two characters the renderers cannot carry, and what each one breaks.
UNRENDERABLE = ['quo"ted', "sel;ect"]

# Legal, unusual, and rendered correctly. A check that refused these would be
# worse than the defect: it would reject working schemas.
FINE = ["od'd", "with space", "MiXedCase", "_leading", "ünïcode"]


@pytest.mark.parametrize("name", UNRENDERABLE)
def test_a_table_name_the_renderer_cannot_write_is_an_error(name: str) -> None:
    errors = validate_schema(_one_table(name))["errors"]

    assert any(name in e for e in errors), errors


@pytest.mark.parametrize("name", UNRENDERABLE)
def test_a_column_name_the_renderer_cannot_write_is_an_error(name: str) -> None:
    """Columns are rendered the same way and were not checked either."""
    errors = validate_schema(_one_column(name))["errors"]

    assert any(name in e for e in errors), errors


@pytest.mark.parametrize("name", UNRENDERABLE)
def test_the_error_says_why_the_name_cannot_be_written(name: str) -> None:
    """An error the author cannot act on is a crash with better manners.

    THE FIRST VERSION ASSERTED THE CHARACTER WAS IN THE MESSAGE and could not
    fail: the message quotes the name, and the name contains the character. A
    mutation replacing the whole explanation with "a character it cannot
    write" survived it. So this looks for the reason instead.
    """
    errors = [e for e in validate_schema(_one_table(name))["errors"] if name in e]

    assert errors, "no error mentioned the name"
    expected = "double quote" if '"' in name else "semicolon"
    assert expected in errors[0], errors[0]
    assert "Rename it" in errors[0], errors[0]


@pytest.mark.parametrize("name", FINE)
def test_an_unusual_but_renderable_name_is_accepted(name: str) -> None:
    """The control, and the half that matters more.

    A check that refuses anything surprising rejects working schemas. Each of
    these was migrated to SQLite and PostgreSQL and converged.
    """
    errors = validate_schema(_one_table(name))["errors"]

    assert not any(name in e for e in errors), errors


def test_an_ordinary_schema_still_validates_clean() -> None:
    """The floor. A check that errors on everything would pass the tests above."""
    schema = {
        "users": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "email": {"type": "text"},
            }
        }
    }

    assert validate_schema(schema)["errors"] == []
