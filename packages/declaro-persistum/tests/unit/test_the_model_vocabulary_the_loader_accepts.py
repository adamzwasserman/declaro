"""Most of what a model may declare had never been loaded.

MEASURED 2026-08-20 with branch coverage, which I had not run once before
today. Twenty-five partial branches in `pydantic_loader.py`, and they are the
vocabulary itself:

    a UUID field                     never mapped
    a Decimal field                  never mapped
    on_update on a foreign key       never read
    is_new on a column               never read
    constraints in Meta              never read
    a class without @table           never skipped
    a field named with a leading _   never skipped
    a Literal of non-strings         never refused
    a models directory that is gone  never refused
    a module that will not load      never refused
    the same table declared twice    never refused

The TOML loader's equivalents were covered earlier today. These are the same
facts said the other way, through the path most projects actually use, and
none of them had run.

TWO OF THESE TESTS DO NOT MEASURE WHAT THEIR BRANCH SUGGESTS, and saying so
is cheaper than contorting them. `declared_type` maps UUID and Decimal by a
direct lookup, and `_unwrap_optional` strips an `Optional` before that lookup
runs, so the string-representation fallback at lines 149 and 153 is reached
only by a type whose `str()` contains "UUID" while not being UUID or
Optional[UUID]. Removing either branch leaves both tests passing. What they do
check is that the two types map correctly, by whichever path.

WHY THE SKIPS MATTER AS MUCH AS THE READS. A models directory holds helpers,
base classes and test files beside the tables. Every one of those skips is the
loader deciding something is NOT a table, and a skip that stops working turns
a helper into a table nobody declared.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.pydantic_loader import load_schema_from_models

pytestmark = [pytest.mark.precommit]


def _models(**files: str) -> str:
    directory = Path(tempfile.mkdtemp())
    for name, body in files.items():
        (directory / f"{name}.py").write_text(body)
    return str(directory)


def test_a_uuid_field_becomes_a_uuid_column() -> None:
    schema = load_schema_from_models(
        _models(
            m='''
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: UUID = field(primary_key=True)
'''
        )
    )

    assert schema["widgets"]["columns"]["id"]["type"] == "uuid"


def test_a_decimal_field_becomes_numeric() -> None:
    schema = load_schema_from_models(
        _models(
            m='''
from decimal import Decimal
from pydantic import BaseModel
from declaro_persistum import table, field


@table("prices")
class Price(BaseModel):
    id: int = field(primary_key=True)
    amount: Decimal
'''
        )
    )

    assert schema["prices"]["columns"]["amount"]["type"] == "numeric"


def test_an_on_update_action_is_read() -> None:
    """The half of a foreign key that nothing had declared through a model."""
    schema = load_schema_from_models(
        _models(
            m='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("parents")
class Parent(BaseModel):
    id: int = field(primary_key=True)


@table("children")
class Child(BaseModel):
    id: int = field(primary_key=True)
    pid: int = field(references="parents.id", on_update="cascade")
'''
        )
    )

    assert schema["children"]["columns"]["pid"]["on_update"] == "cascade"


def test_an_is_new_hint_is_read() -> None:
    """How an author says a column really is new rather than renamed."""
    schema = load_schema_from_models(
        _models(
            m='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
    note: str = field(is_new=True)
'''
        )
    )

    assert schema["widgets"]["columns"]["note"]["is_new"] is True


def test_constraints_declared_in_meta_are_read() -> None:
    """The model spelling of what four modules were fixed for yesterday."""
    schema = load_schema_from_models(
        _models(
            m='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("ranges")
class Range(BaseModel):
    id: int = field(primary_key=True)
    lo: int
    hi: int

    class Meta:
        constraints = {
            "ck_order": {"type": "check", "expression": "lo < hi"}
        }
'''
        )
    )

    assert schema["ranges"]["constraints"]["ck_order"]["expression"] == "lo < hi"


def test_a_class_without_the_decorator_is_not_a_table() -> None:
    """A models file holds base classes and value objects beside the tables."""
    schema = load_schema_from_models(
        _models(
            m='''
from pydantic import BaseModel
from declaro_persistum import table, field


class NotATable(BaseModel):
    id: int


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
'''
        )
    )

    assert sorted(schema) == ["widgets"]


def test_a_field_with_a_leading_underscore_is_not_a_column() -> None:
    schema = load_schema_from_models(
        _models(
            m='''
from typing import ClassVar
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    _internal: ClassVar[str] = "not a column"
    id: int = field(primary_key=True)
'''
        )
    )

    assert sorted(schema["widgets"]["columns"]) == ["id"]


def test_a_literal_of_non_strings_is_refused() -> None:
    """An enum lookup table is keyed by a text column, so the values are text."""
    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(
            _models(
                m='''
from typing import Literal
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
    level: Literal[1, 2, 3] = 1
'''
            )
        )

    assert "Literal" in str(caught.value) or "string" in str(caught.value).lower()


def test_a_missing_models_directory_is_refused_by_name() -> None:
    missing = str(Path(tempfile.mkdtemp()) / "not_here")

    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(missing)

    assert "not_here" in str(caught.value), caught.value


def test_a_helper_file_is_skipped_by_name() -> None:
    """A leading underscore means the loader does not even import it.

    That skip is what lets a models directory hold `__init__.py` and shared
    helpers, and it is checked before the file is executed rather than after
    it fails.
    """
    schema = load_schema_from_models(
        _models(
            _helper="this file is not valid python at all (((",
            m='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
''',
        )
    )

    assert sorted(schema) == ["widgets"]


def test_a_test_file_is_skipped_by_name() -> None:
    schema = load_schema_from_models(
        _models(
            test_widgets="this file is not valid python at all (((",
            m='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
''',
        )
    )

    assert sorted(schema) == ["widgets"]


def test_a_directory_declaring_no_tables_is_refused() -> None:
    """Distinct from a directory that is not there, and from one that fails."""
    with pytest.raises(LoaderError):
        load_schema_from_models(
            _models(
                m='''
from pydantic import BaseModel


class NotATable(BaseModel):
    id: int
'''
            )
        )


def test_the_same_table_declared_twice_is_refused() -> None:
    """Two files, one table name, and the second would silently win.

    An author who copies a models file and edits one of them gets a schema
    describing whichever the loader read last.
    """
    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(
            _models(
                a='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
''',
                b='''
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class WidgetAgain(BaseModel):
    id: int = field(primary_key=True)
    other: str
''',
            )
        )

    message = str(caught.value)
    assert "widgets" in message, message
    assert "a.py" in message and "b.py" in message, message
