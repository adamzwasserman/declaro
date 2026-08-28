"""A file that fails to import declares nothing, and that is not the same thing.

MEASURED 2026-08-18 with `coverage json`: `load_views_from_models` ends in

    except LoaderError:
        raise
    except Exception:
        continue

and neither branch had ever run, so nothing had ever put a broken file in
front of the function that collects views.

IT BEHAVES CORRECTLY AND I EXPECTED IT NOT TO. `load_schema_from_models`
carried an identical-looking swallow that was removed on 2026-08-17, after a
one-letter typo in a models file reached the author as "No @table decorated
models found in models/". Finding the same two lines in the sibling, I wrote
these tests expecting a broken file to vanish. It does not: `_import_module_file`
raises `LoaderError` for anything that goes wrong importing, and the branch
above re-raises it, so the file is reported by name.

WHAT THE SWALLOW ACTUALLY CATCHES IS ALMOST NOTHING. Everything below it
raises `LoaderError`: a missing file, an unloadable spec, a module that throws
at import, a view with unknown keys, a view whose values are the wrong type.
The only route left is `getattr` on a module attribute that raises, which
needs a descriptor written to misbehave. It is dead defensive code rather than
a live swallow, and it is left alone here.

WHY IT WOULD HAVE MATTERED. A view that fails to load is a view the differ
never sees, and a declared view the differ cannot see is one it drops.
Dropping a materialized view discards its contents, which is the cache the
view exists to be. That is the failure these tests now hold shut.

A COMPOSITE PRIMARY KEY DECLARED IN `Meta` HAD ALSO NEVER BEEN LOADED. That
branch moves the key from the columns to the table, and the differ was fixed
today to fold a composite key back onto its columns, which depends on it
arriving that way.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.pydantic_loader import (
    load_schema_from_models,
    load_views_from_models,
)

pytestmark = [pytest.mark.precommit]

# A VIEW IS DATA, NOT A DECORATED CLASS, because it has a name and a SELECT
# and no fields for a class to hold. `views_from_module` picks it up by shape:
# a module-level dict carrying at least `name` and `query`.
GOOD_VIEW = """
from pydantic import BaseModel
from declaro_persistum import table


@table("widgets")
class Widget(BaseModel):
    id: int
    name: str


live_widgets = {"name": "live_widgets", "query": "SELECT id FROM widgets"}
"""

BROKEN = """
from pydantic import BaseModel
from declaro_persistum import table

this_name_does_not_exist()


@table("orphans")
class Orphan(BaseModel):
    id: int
"""

# A field-level key AND a Meta key, which is the case the removal branch
# exists for. Without a field-level `primary_key=True` the loop has nothing to
# remove and a mutation that deletes it survives, which is how the first
# version of this file measured nothing.
CONTRADICTORY = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("pairs")
class Pair(BaseModel):
    left: int = field(primary_key=True)
    right: int

    class Meta:
        primary_key = ["left", "right"]
"""

COMPOSITE = """
from pydantic import BaseModel
from declaro_persistum import table


@table("pairs")
class Pair(BaseModel):
    left: int
    right: int

    class Meta:
        primary_key = ["left", "right"]
"""


def _models_dir(**files: str) -> str:
    directory = Path(tempfile.mkdtemp())
    for name, body in files.items():
        (directory / f"{name}.py").write_text(body)
    return str(directory)


def test_a_broken_file_is_reported_when_views_are_collected() -> None:
    """The measured swallow. Silence here becomes a dropped view later."""
    directory = _models_dir(good=GOOD_VIEW, broken=BROKEN)

    with pytest.raises(LoaderError) as caught:
        load_views_from_models(directory)

    assert "broken" in str(caught.value), caught.value


def test_the_report_names_the_file_that_would_not_load() -> None:
    """An error that does not say which file sends the author reading all of them."""
    directory = _models_dir(good=GOOD_VIEW, broken=BROKEN)

    with pytest.raises(LoaderError) as caught:
        load_views_from_models(directory)

    assert "broken.py" in str(caught.value), caught.value


def test_a_good_directory_still_yields_its_views() -> None:
    """The control. Refusing everything would pass the two tests above."""
    directory = _models_dir(good=GOOD_VIEW)

    views = load_views_from_models(directory)

    assert "live_widgets" in views, sorted(views)


def test_a_helper_file_beginning_with_an_underscore_is_still_skipped() -> None:
    """The reason the swallow existed. A models directory may hold helpers.

    Skipping by NAME is the part that was right; skipping by failure is the
    part that was not.
    """
    directory = _models_dir(good=GOOD_VIEW, _helper=BROKEN)

    views = load_views_from_models(directory)

    assert "live_widgets" in views, sorted(views)


def test_a_composite_primary_key_declared_in_meta_reaches_the_schema() -> None:
    """That branch had never run, and the differ now depends on its shape."""
    directory = _models_dir(pairs=COMPOSITE)

    schema = load_schema_from_models(directory)

    assert schema["pairs"].get("primary_key") == ["left", "right"], schema["pairs"]


def test_a_composite_key_leaves_no_flag_on_the_columns() -> None:
    """Declared in one place, not two.

    A column still carrying `primary_key: True` beside a table-level list
    declares two different keys, and SQLite refuses a table with two.

    The model here says the key twice on purpose: once on the field and once
    in `Meta`. A model that says it only in `Meta` gives the removal loop
    nothing to remove, so deleting that loop passes unnoticed.
    """
    directory = _models_dir(pairs=CONTRADICTORY)

    schema = load_schema_from_models(directory)

    assert not any(col.get("primary_key") for col in schema["pairs"]["columns"].values()), schema[
        "pairs"
    ]["columns"]
