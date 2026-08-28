"""A column written in a shape the TOML loader did not know just vanished.

`_normalize_columns` read each entry as a table or as the shorthand string:

    if isinstance(col_data, dict):   ...
    elif isinstance(col_data, str):  ...

and there was no third branch. A column written any other way produced no
column, no error and no warning. The author gets a table missing a column they
declared, and finds out when an INSERT fails on a name the database has never
heard of.

TWO SPELLINGS ARE THE VOCABULARY, and asking which arrived is what a boundary
reading hand-written TOML is for. TOML carries no schema, so the loader is the
only thing that can say which of the two an entry is. What it must not do is
treat a third spelling as though the author had written nothing.

The same applies to `primary_key`, which may be one name or a list of them.
"""

import pathlib

import pytest

from declaro_persistum.boundary import boundary
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.loader import load_schema


@boundary
def _schema(tmp_path: pathlib.Path, body: str) -> dict:
    """Write the TOML a test declares, then load it.

    A test of a file loader has to put a file somewhere, so this is where
    these tests meet the filesystem and it says so.
    """
    (tmp_path / "schema.toml").write_text(body)
    return load_schema(tmp_path)


def test_a_column_written_as_a_table_is_read(tmp_path: pathlib.Path) -> None:
    schema = _schema(tmp_path, '[t]\n[t.columns.a]\ntype = "integer"\n')

    assert schema["t"]["columns"]["a"]["type"] == "integer"


def test_a_column_written_as_its_type_is_read(tmp_path: pathlib.Path) -> None:
    """The shorthand: the value is the type."""
    schema = _schema(tmp_path, '[t]\n[t.columns]\na = "integer"\n')

    assert schema["t"]["columns"]["a"]["type"] == "integer"


@pytest.mark.parametrize(
    "written,shape",
    [("a = 7", "a number"), ("a = true", "a flag"), ('a = ["integer"]', "a list")],
    ids=["a number", "a flag", "a list"],
)
def test_a_column_written_any_other_way_is_refused(
    tmp_path: pathlib.Path,
    written: str,
    shape: str,  # noqa: ARG001 - parametrize unpacks it; it names the case
) -> None:
    """Refused by name, rather than dropped.

    The author gets told which column they wrote wrongly. Before, they got a
    table without it and no indication that anything had happened.
    """
    with pytest.raises(LoaderError) as raised:
        _schema(tmp_path, f"[t]\n[t.columns]\n{written}\n")

    assert "a" in str(raised.value)


@pytest.mark.parametrize(
    "written,expected",
    [('primary_key = "id"', ["id"]), ('primary_key = ["a", "b"]', ["a", "b"])],
    ids=["one name", "several names"],
)
def test_a_primary_key_is_read_either_way(
    tmp_path: pathlib.Path, written: str, expected: list[str]
) -> None:
    """One name or a list of them, and both mean a list.

    Two spellings of one thing is a vocabulary, and the boundary is where it
    is resolved into the single form everything downstream reads.
    """
    schema = _schema(
        tmp_path,
        f'[t]\n{written}\n[t.columns]\na = "integer"\nb = "integer"\n',
    )

    assert schema["t"]["primary_key"] == expected
