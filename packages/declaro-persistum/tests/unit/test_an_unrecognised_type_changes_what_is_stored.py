"""An unrecognized type name silently changes the value SQLite stores.

MEASURED 2026-08-19. SQLite assigns column affinity by matching substrings of
the declared type name, and a name it recognizes nothing in gets NUMERIC:

    CREATE TABLE t (a TEXT, b TXET, d GEOGRAPHY)
    INSERT INTO t VALUES ('007', '007', '007')

    column a   stored '007' as text
    column b   stored 7     as integer
    column d   stored 7     as integer

`b` is `text` with two letters swapped. The value written was the string
'007' and the value stored is the number 7: the leading zeros are gone, and
`'10' < '9'` is true while `10 < 9` is false, so the column's sort order
changes too. Nothing reports any of it.

THIS IS THE THIRD OF THE THREE FALLBACKS three BDD schema helpers were deleted
for on 2026-08-16 -- `type_mapping.get(t, t.upper())`, an unknown type passing
through. The other two were fixed earlier today: a column with no type, and a
table with no columns.

IT IS NOT FIXED BY REFUSING UNKNOWN TYPES, and that is the finding rather than
an omission. The validator knows 46 names, which is the PostgreSQL
vocabulary, so an unknown one is either a custom PostgreSQL type -- legitimate,
and the reason the check is a warning -- or a typo. Refusing them would reject
a schema that declares a PostgreSQL custom type and also targets SQLite.
`validate_schema` takes no engine, so it cannot refuse for one engine and
allow it for another without a signature change.

What is fixed is the warning, which said "may be custom type or enum" and read
as reassurance. It names the consequence now.
"""

from __future__ import annotations

import sqlite3

import pytest

from declaro_persistum.validator import validate_schema

pytestmark = [pytest.mark.precommit]


def _schema(type_name: str) -> dict:
    return {
        "widgets": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "c": {"type": type_name},
            }
        }
    }


def test_sqlite_stores_a_string_as_a_number_under_an_unknown_type() -> None:
    """The measurement the warning exists for, run rather than quoted."""
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE t (good TEXT, typo TXET)")
    connection.execute("INSERT INTO t VALUES ('007', '007')")

    good, typo = connection.execute("SELECT typeof(good), typeof(typo) FROM t").fetchone()

    assert good == "text"
    assert typo == "integer", (
        f"a typo in a type name stored the value as {typo}, which would make "
        f"this test obsolete rather than passing"
    )


def test_the_stored_value_itself_differs() -> None:
    """The floor. `typeof` differing is only interesting if the value does."""
    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE t (good TEXT, typo TXET)")
    connection.execute("INSERT INTO t VALUES ('007', '007')")

    good, typo = connection.execute("SELECT good, typo FROM t").fetchone()

    assert good == "007"
    assert typo == 7


def test_an_unknown_type_warns_rather_than_erroring() -> None:
    """A custom PostgreSQL type is legitimate, so this cannot be an error.

    The three paths that write DDL stop on errors and not warnings, so making
    this an error would refuse a schema declaring a PostgreSQL custom type.
    """
    report = validate_schema(_schema("geography"))

    assert report["errors"] == [], report["errors"]
    assert report["warnings"], report


def test_the_warning_names_what_it_costs() -> None:
    """It said "may be custom type or enum", which reads as reassurance.

    An author skimming a warning list needs to know this one can change the
    values their column stores.
    """
    warning = validate_schema(_schema("geography"))["warnings"][0]

    assert "geography" in warning, warning
    assert "affinity" in warning.lower() or "stored" in warning.lower(), warning


def test_a_known_type_warns_about_nothing() -> None:
    """The control. A warning on everything would carry no information."""
    assert validate_schema(_schema("timestamptz"))["warnings"] == []


def test_an_array_of_a_custom_type_is_still_accepted() -> None:
    """The `[]` suffix is handled separately and must keep working."""
    assert validate_schema(_schema("customtype[]"))["warnings"] == []
