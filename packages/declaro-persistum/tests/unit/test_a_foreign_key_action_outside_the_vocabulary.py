"""An unrecognised ON DELETE was accepted and rebuilt the table forever.

`types.py` declares the vocabulary:

    on_delete: Literal["cascade", "set null", "restrict", "no action"]

MEASURED 2026-08-19. Nothing enforced it. `validate_schema` accepted
`set_null`, `CASCADE` and `nonsense` alike, with no error and no warning.

WHAT AN UNRECOGNISED VALUE DOES. The renderer writes it out after
`.upper().replace("_", " ")`, so `set_null` reaches the engine as the correct
`SET NULL` and the table is created properly. Introspection then reads the
engine's `SET NULL` and normalises it to `set null`, which does not equal the
declared `set_null`, so the differ sees a change:

    second deploy  drop_foreign_key pid, add_foreign_key pid

On SQLite and Turso both of those go through table reconstruction, so every
deploy rebuilds the whole table, copies every row, and reports success. It
never converges and never complains.

THE VOCABULARY COMES FROM THE TYPE, not from a list here. A list would go
stale the moment a fifth action is allowed, and the whole failure is a
declaration and a check disagreeing about what the values are.

`set_null` is the spelling that catches people, and it caught me: the first
version of the rebuild test beside this one used it and reported the loop as
a rebuild defect.
"""

from __future__ import annotations

import pytest

from declaro_persistum.types import Column
from declaro_persistum.validator import validate_schema

pytestmark = [pytest.mark.precommit]

ALLOWED = sorted(Column.__annotations__["on_delete"].__args__)


def _schema(action_key: str, value: str) -> dict:
    return {
        "parents": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {
                    "type": "integer",
                    "references": "parents.id",
                    action_key: value,
                },
            }
        },
    }


def test_the_vocabulary_is_the_four_the_type_declares() -> None:
    """The floor. Everything below is measured against this list."""
    assert ALLOWED == ["cascade", "no action", "restrict", "set null"], ALLOWED


@pytest.mark.parametrize("action_key", ["on_delete", "on_update"])
@pytest.mark.parametrize("value", ALLOWED)
def test_a_declared_action_is_accepted(action_key: str, value: str) -> None:
    """The control, and it must cover all four rather than a sample."""
    assert validate_schema(_schema(action_key, value))["errors"] == []


@pytest.mark.parametrize("action_key", ["on_delete", "on_update"])
@pytest.mark.parametrize("value", ["set_null", "CASCADE", "nonsense", "", "SET NULL"])
def test_anything_else_is_refused(action_key: str, value: str) -> None:
    """`set_null` and `CASCADE` are the two that look right and are not."""
    errors = validate_schema(_schema(action_key, value))["errors"]

    assert errors, f"{action_key}={value!r} validated clean"


@pytest.mark.parametrize("value", ["set_null", "nonsense"])
def test_the_refusal_names_the_value_and_what_is_allowed(value: str) -> None:
    """An author who wrote `set_null` needs to see `set null` to fix it."""
    errors = validate_schema(_schema("on_delete", value))["errors"]

    message = " ".join(errors)
    assert value in message, message
    assert "set null" in message, message


def test_the_refusal_names_the_column() -> None:
    errors = validate_schema(_schema("on_delete", "nonsense"))["errors"]

    assert any("pid" in e for e in errors), errors


def test_a_foreign_key_with_no_action_is_fine() -> None:
    """Both actions are optional and their absence is not an error."""
    schema = {
        "parents": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {"type": "integer", "references": "parents.id"},
            }
        },
    }

    assert validate_schema(schema)["errors"] == []
