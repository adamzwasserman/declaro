"""
Regression tests: pydantic_loader must resolve string annotations.

Bug fixed in 0.1.3:
    `pydantic_model_to_table` read `cls.__annotations__` directly. Under
    PEP 563 (`from __future__ import annotations`) — standard practice in
    modern Pydantic codebases — `__annotations__` contains *strings*, not
    types ("bool", "datetime | None"). String annotations bypassed every
    type-keyed check in `python_type_to_sql` and silently fell through to
    the `text` default. `is_optional_type` saw a string with no
    `get_origin()` and returned False, so every `T | None` field was
    materialized as NOT NULL.

    Fix: use `typing.get_type_hints(model_cls)` instead of
    `getattr(model_cls, "__annotations__", {})`. get_type_hints resolves
    string annotations against the model's module globals, returning
    actual types regardless of whether PEP 563 is in effect.
"""

from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest

from declaro_persistum.pydantic_loader import load_models_from_module


@pytest.fixture
def model_with_pep563(tmp_path: Path) -> Path:
    """A Pydantic model file using `from __future__ import annotations`."""
    p = tmp_path / "models_pep563.py"
    p.write_text(
        "from __future__ import annotations\n"
        "\n"
        "from datetime import datetime\n"
        "from uuid import UUID\n"
        "\n"
        "from pydantic import BaseModel, Field\n"
        "\n"
        "\n"
        "class Item(BaseModel):\n"
        "    __tablename__ = 'items'\n"
        "    id: UUID = Field(json_schema_extra={'primary': True, 'default': 'gen_random_uuid()'})\n"
        "    public: bool = Field(json_schema_extra={'nullable': False, 'default': 'FALSE'})\n"
        "    deleted_date: datetime | None = Field(default=None)\n"
    )
    return p


@pytest.fixture
def model_without_pep563(tmp_path: Path) -> Path:
    """A Pydantic model file *without* PEP 563 — baseline."""
    p = tmp_path / "models_plain.py"
    p.write_text(
        "from datetime import datetime\n"
        "from uuid import UUID\n"
        "\n"
        "from pydantic import BaseModel, Field\n"
        "\n"
        "\n"
        "class Item(BaseModel):\n"
        "    __tablename__ = 'items'\n"
        "    id: UUID = Field(json_schema_extra={'primary': True, 'default': 'gen_random_uuid()'})\n"
        "    public: bool = Field(json_schema_extra={'nullable': False, 'default': 'FALSE'})\n"
        "    deleted_date: datetime | None = Field(default=None)\n"
    )
    return p


def test_bool_field_with_pep563_resolves_to_boolean(model_with_pep563: Path) -> None:
    """Regression: `bool` under PEP 563 must produce `type: 'boolean'`, not 'text'."""
    schema = load_models_from_module(model_with_pep563)

    public_col = schema["items"]["columns"]["public"]
    assert public_col["type"] == "boolean", (
        f"Expected boolean column type for `bool` field, got {public_col['type']!r}. "
        "Under PEP 563, __annotations__ returns strings; the loader must use "
        "typing.get_type_hints to resolve them."
    )


def test_optional_field_with_pep563_resolves_to_nullable(model_with_pep563: Path) -> None:
    """Regression: `T | None` under PEP 563 must produce `nullable: True`, not False."""
    schema = load_models_from_module(model_with_pep563)

    deleted_col = schema["items"]["columns"]["deleted_date"]
    assert deleted_col.get("nullable") is True, (
        f"Expected nullable=True for `datetime | None` field, got "
        f"nullable={deleted_col.get('nullable')!r}. is_optional_type cannot "
        "introspect string annotations — the loader must resolve types via "
        "typing.get_type_hints first."
    )


def test_pep563_and_non_pep563_produce_identical_schemas(
    model_with_pep563: Path, model_without_pep563: Path
) -> None:
    """The presence of `from __future__ import annotations` must not affect the loaded schema."""
    schema_pep563 = load_models_from_module(model_with_pep563)
    schema_plain = load_models_from_module(model_without_pep563)

    assert schema_pep563["items"]["columns"] == schema_plain["items"]["columns"], (
        "Loaded schema differs depending on whether the model file uses PEP 563. "
        "These should be byte-identical."
    )


def test_optional_datetime_type_resolved_correctly(model_with_pep563: Path) -> None:
    """Sanity: the unwrapped type of `datetime | None` is `timestamptz`, not `text`."""
    schema = load_models_from_module(model_with_pep563)
    deleted_col = schema["items"]["columns"]["deleted_date"]
    assert deleted_col["type"] == "timestamptz"
