"""Two models declaring the same table name must not silently become one.

WRITTEN BEFORE THE CODE, against a measured data loss.

`load_schema_from_models` walks a directory and merges each file's schema with
`schema.update(file_schema)`. A dict update is last-wins, so two files both
declaring `@table("users")` produce one table, and which one depends on the
order `glob` returns.

MEASURED 2026-08-17:

    a_users.py   @table("users")  columns id, email
    z_other.py   @table("users")  columns id, totally_different

    tables loaded: ['users']
    columns kept:  ['email', 'id']

The losing model's columns are not in the schema, so the differ compares a
database that has them against a target that does not, and emits a DROP. A
copy-pasted decorator costs a column and its data, and nothing is logged at any
point.

THE COLLISION IS NOT DETECTABLE DOWNSTREAM. By the time the schema is a dict,
the second declaration has already overwritten the first and there is nothing
left to compare. It has to be caught during the merge, which is the only moment
both halves exist.

THIS IS NOT THE SAME AS TWO FILES CONTRIBUTING DIFFERENT TABLES, which is the
ordinary case and must keep working. Nor is it the same as one file declaring a
table twice, which Python resolves before the loader ever sees it.
"""

from __future__ import annotations

import pathlib

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.pydantic_loader import load_schema_from_models

pytestmark = pytest.mark.precommit

HEADER = "from pydantic import BaseModel\nfrom declaro_persistum import table, field\n"


def _model(table_name: str, class_name: str, extra: str) -> str:
    return (
        f'{HEADER}@table("{table_name}")\n'
        f"class {class_name}(BaseModel):\n"
        f"    id: str = field(primary_key=True)\n"
        f"    {extra}\n"
    )


def test_two_files_claiming_one_table_raise(tmp_path: pathlib.Path) -> None:
    """The measured loss, in one assertion."""
    (tmp_path / "a_users.py").write_text(_model("users", "User", "email: str"))
    (tmp_path / "z_other.py").write_text(_model("users", "Other", "nickname: str"))

    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(tmp_path)

    message = str(caught.value)
    assert "users" in message, f"the error does not name the table: {message}"


def test_the_error_names_both_files(tmp_path: pathlib.Path) -> None:
    """One filename is not enough to act on.

    A developer told "users is declared twice" has to go looking. Told which
    two files, they open both and delete one decorator.
    """
    (tmp_path / "a_users.py").write_text(_model("users", "User", "email: str"))
    (tmp_path / "z_other.py").write_text(_model("users", "Other", "nickname: str"))

    with pytest.raises(LoaderError) as caught:
        load_schema_from_models(tmp_path)

    message = str(caught.value)
    assert "a_users.py" in message and "z_other.py" in message, (
        f"the error names fewer than both files, so it says a collision "
        f"happened without saying where: {message}"
    )


def test_the_collision_is_caught_whichever_order_the_files_load(
    tmp_path: pathlib.Path,
) -> None:
    """Last-wins made the outcome depend on `glob` order. Raising must not.

    The names here sort the other way, so a rule that only looked at the first
    file, or only the last, passes one of these two tests and fails the other.
    """
    (tmp_path / "z_users.py").write_text(_model("users", "User", "email: str"))
    (tmp_path / "a_other.py").write_text(_model("users", "Other", "nickname: str"))

    with pytest.raises(LoaderError):
        load_schema_from_models(tmp_path)


def test_three_files_claiming_one_table_still_raise(tmp_path: pathlib.Path) -> None:
    for i, name in enumerate(("one", "two", "three")):
        (tmp_path / f"{name}.py").write_text(_model("users", f"M{i}", f"c{i}: str"))

    with pytest.raises(LoaderError):
        load_schema_from_models(tmp_path)


# ---------------------------------------------------------------------------
# What must keep working
# ---------------------------------------------------------------------------


def test_different_tables_in_different_files_still_merge(
    tmp_path: pathlib.Path,
) -> None:
    """The ordinary case, and the reason the merge exists at all."""
    (tmp_path / "users.py").write_text(_model("users", "User", "email: str"))
    (tmp_path / "orders.py").write_text(_model("orders", "Order", "total: int"))

    schema = load_schema_from_models(tmp_path)

    assert sorted(schema) == ["orders", "users"]
    assert "email" in schema["users"]["columns"]
    assert "total" in schema["orders"]["columns"]


def test_several_tables_in_one_file_still_merge(tmp_path: pathlib.Path) -> None:
    """A file may declare as many distinct tables as it likes."""
    (tmp_path / "models.py").write_text(
        f'{HEADER}@table("users")\n'
        f"class User(BaseModel):\n"
        f"    id: str = field(primary_key=True)\n\n"
        f'@table("orders")\n'
        f"class Order(BaseModel):\n"
        f"    id: str = field(primary_key=True)\n"
    )

    assert sorted(load_schema_from_models(tmp_path)) == ["orders", "users"]


def test_a_file_with_no_models_is_skipped_not_refused(
    tmp_path: pathlib.Path,
) -> None:
    """A models directory may hold helpers. Only decorated classes count."""
    (tmp_path / "users.py").write_text(_model("users", "User", "email: str"))
    (tmp_path / "helpers.py").write_text("def helper():\n    return 1\n")

    assert sorted(load_schema_from_models(tmp_path)) == ["users"]


def test_a_nested_directory_still_contributes(tmp_path: pathlib.Path) -> None:
    """The walk is `**/*.py`, so a collision can span directories too."""
    (tmp_path / "users.py").write_text(_model("users", "User", "email: str"))
    nested = tmp_path / "billing"
    nested.mkdir()
    (nested / "orders.py").write_text(_model("orders", "Order", "total: int"))

    assert sorted(load_schema_from_models(tmp_path)) == ["orders", "users"]


def test_a_collision_across_directories_raises(tmp_path: pathlib.Path) -> None:
    """The likeliest real version: two teams, two folders, one table name."""
    (tmp_path / "users.py").write_text(_model("users", "User", "email: str"))
    nested = tmp_path / "billing"
    nested.mkdir()
    (nested / "users.py").write_text(_model("users", "Other", "nickname: str"))

    with pytest.raises(LoaderError):
        load_schema_from_models(tmp_path)
