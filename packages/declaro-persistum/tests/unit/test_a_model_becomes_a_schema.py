"""The rest of the loader: Meta indexes, views, and field metadata.

Four functions on the path from a declared model to a schema, called by no
test: `index_from_meta`, `views_from_module`, `extract_field_metadata` and
`get_literal_columns`.

THE DECLARATIONS COME FROM THE DOCUMENTED FORM, not from my idea of it.
`docs/usage.md` shows a Meta index as

    class Meta:
        indexes = [
            {"name": "idx_users_email", "columns": ["email"], "unique": True},
            {"name": "idx_users_active", "columns": ["status"],
             "where": "deleted_at IS NULL"},
        ]

so that is what these tests declare. A test that invents the shape tests the
invention, which has cost me six attempts this session.

`index_from_meta` already refuses an unknown key, and its docstring records
why: `uniqe=True` used to go straight into the schema, so the index was not
unique, nothing said so at load time, and the differ then compared a table
carrying `uniqe` against a database that never reports it. Load time is the
only moment anyone can act on that typo. The refusal was untested.
"""

from __future__ import annotations

import pathlib
import types

import pytest

from declaro_persistum.pydantic_loader import (
    get_literal_columns,
    index_from_meta,
    load_schema_from_models,
    views_from_module,
)

pytestmark = pytest.mark.precommit


# ---------------------------------------------------------------------------
# Meta indexes
# ---------------------------------------------------------------------------


def test_an_index_is_keyed_by_its_name_and_does_not_carry_it() -> None:
    """The name is the key in the schema.

    Keeping it inside as well makes the differ see a field introspection never
    reports, and therefore a mismatch on every run.
    """
    name, index = index_from_meta(
        {"name": "idx_users_email", "columns": ["email"], "unique": True}
    )

    assert name == "idx_users_email"
    assert index == {"columns": ["email"], "unique": True}
    assert "name" not in index


def test_a_partial_index_keeps_its_where_clause() -> None:
    _name, index = index_from_meta(
        {"name": "i", "columns": ["status"], "where": "deleted_at IS NULL"}
    )

    assert index["where"] == "deleted_at IS NULL"


def test_a_plain_index_declares_nothing_it_was_not_given() -> None:
    """An absent `unique` must not become `False`.

    Introspection omits the key for a non-unique index, so writing one in
    would be a difference on every run.
    """
    _name, index = index_from_meta({"name": "i", "columns": ["a"]})

    assert index == {"columns": ["a"]}


def test_a_misspelt_key_is_refused_at_load_time() -> None:
    """`uniqe` is the measured typo. It used to reach the schema."""
    with pytest.raises(ValueError) as caught:
        index_from_meta({"name": "i", "columns": ["a"], "uniqe": True})

    message = str(caught.value)
    assert "uniqe" in message, message
    assert "columns" in message, f"the error does not say what an index takes: {message}"


def test_an_index_with_no_name_is_refused() -> None:
    with pytest.raises(ValueError, match="name"):
        index_from_meta({"columns": ["a"]})


def test_an_index_over_no_columns_is_refused() -> None:
    """It fails at DDL otherwise, one layer further from the declaration."""
    with pytest.raises(ValueError, match="columns"):
        index_from_meta({"name": "i", "columns": []})


def test_the_columns_are_copied_not_shared() -> None:
    """A schema that aliases the caller's list changes when they mutate it."""
    declared = ["a"]

    _name, index = index_from_meta({"name": "i", "columns": declared})
    declared.append("b")

    assert index["columns"] == ["a"]


# ---------------------------------------------------------------------------
# Through a real models file
# ---------------------------------------------------------------------------

MODEL = '''
from pydantic import BaseModel
from declaro_persistum import table, field

@table("users")
class User(BaseModel):
    id: str = field(primary_key=True)
    email: str
    status: str | None = None

    class Meta:
        indexes = [
            {"name": "idx_users_email", "columns": ["email"], "unique": True},
            {"name": "idx_users_status", "columns": ["status"]},
        ]
'''


def test_a_declared_index_reaches_the_schema(tmp_path: pathlib.Path) -> None:
    (tmp_path / "m.py").write_text(MODEL)

    schema = load_schema_from_models(tmp_path)

    assert set(schema["users"]["indexes"]) == {"idx_users_email", "idx_users_status"}
    assert schema["users"]["indexes"]["idx_users_email"]["unique"] is True


def test_a_misspelt_index_key_fails_the_load(tmp_path: pathlib.Path) -> None:
    """The refusal has to survive the trip through the loader."""
    (tmp_path / "m.py").write_text(MODEL.replace('"unique": True', '"uniqe": True'))

    with pytest.raises(Exception, match="uniqe"):
        load_schema_from_models(tmp_path)


# ---------------------------------------------------------------------------
# Views, which are data rather than a decorator
# ---------------------------------------------------------------------------


def test_a_view_declared_at_module_level_is_found() -> None:
    """There is no `@view` decorator; usage.md says so. A view is a value."""
    module = types.ModuleType("m")
    module.active = {
        "name": "active_users",
        "query": "SELECT id FROM users WHERE status = 'active'",
    }

    views = views_from_module(module)

    assert "active_users" in views, sorted(views)


def test_a_dict_that_is_not_a_view_is_ignored() -> None:
    """A models file holds constants too, and guessing would break them."""
    module = types.ModuleType("m")
    module.settings = {"retries": 3}
    module.name_only = {"name": "x"}

    assert views_from_module(module) == {}


def test_a_module_with_no_views_yields_none() -> None:
    assert views_from_module(types.ModuleType("m")) == {}


# ---------------------------------------------------------------------------
# Literal columns, which become the enum lookup tables
# ---------------------------------------------------------------------------


def test_the_literal_columns_are_reported_by_table_and_column() -> None:
    """This is what `expand_schema_enums` walks to build the lookup tables."""
    schema = {
        "posts": {
            "columns": {
                "id": {"type": "text"},
                "status": {"type": "text", "literal_values": ["draft", "live"]},
            }
        },
        "plain": {"columns": {"id": {"type": "text"}}},
    }

    assert get_literal_columns(schema) == {"posts": {"status": ["draft", "live"]}}


def test_a_schema_with_no_literals_reports_nothing() -> None:
    assert get_literal_columns({"t": {"columns": {"c": {"type": "text"}}}}) == {}


# ---------------------------------------------------------------------------
# A file that fails to load is not a file that declares no models
# ---------------------------------------------------------------------------
#
# MEASURED 2026-08-17. A one-letter typo in a Meta index:
#
#     {"name": "i", "columns": ["email"], "uniqe": True}
#
# `index_from_meta` refuses it by design, naming the key and what an index
# takes, because load time is the only moment anyone can act on that typo.
# The author saw:
#
#     LoaderError: No @table decorated models found in /path/to/models
#
# `load_schema_from_models` wrapped each file in `except Exception: continue`,
# commented "Skip files that can't be imported (might not be model files)".
# The intent is real -- a models directory may hold helpers -- but a file that
# FAILS to import is not a file that declares nothing, and conflating them
# replaces the one error the author can act on with one that sends them
# looking for a missing decorator.
#
# A helper that imports cleanly and declares no table still contributes
# nothing, which is the case the swallow was reaching for and which needs no
# swallow at all.


def test_a_typo_in_a_models_file_names_the_typo(tmp_path: pathlib.Path) -> None:
    (tmp_path / "m.py").write_text(MODEL.replace('"unique": True', '"uniqe": True'))

    with pytest.raises(Exception) as caught:
        load_schema_from_models(tmp_path)

    message = str(caught.value)
    assert "uniqe" in message, (
        f"the author is not told which key is wrong: {message}"
    )
    assert "m.py" in message, f"the author is not told which file: {message}"


def test_a_models_file_that_cannot_be_imported_says_so(
    tmp_path: pathlib.Path,
) -> None:
    """A missing import is the author's problem and they have to see it."""
    (tmp_path / "m.py").write_text(MODEL)
    (tmp_path / "broken.py").write_text("import a_module_that_is_not_installed\n")

    with pytest.raises(Exception) as caught:
        load_schema_from_models(tmp_path)

    message = str(caught.value)
    assert "broken.py" in message, message
    assert "a_module_that_is_not_installed" in message, message


def test_a_helper_that_declares_no_models_is_still_fine(
    tmp_path: pathlib.Path,
) -> None:
    """The case the swallow was reaching for, which needs no swallow.

    A helper that imports cleanly contributes no tables and is not an error.
    """
    (tmp_path / "m.py").write_text(MODEL)
    (tmp_path / "helpers.py").write_text("def helper():\n    return 1\n")

    schema = load_schema_from_models(tmp_path)

    assert sorted(schema) == ["users"]
