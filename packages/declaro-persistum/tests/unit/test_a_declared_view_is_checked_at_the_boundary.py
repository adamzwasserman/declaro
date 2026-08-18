"""A view is a dict picked up by shape, so its shape has to be checked.

`views_from_module` finds every module-level dict carrying the required keys
and returns it. It rejects UNKNOWN keys and never looks at the values, so
presence is the whole test.

MEASURED 2026-08-17:

    v = {"name": 123, "query": None, "materialized": "yes"}
    load_views_from_models(...)  ->  views keyed by the integer 123

Both required keys are there, so it passes. The view then travels as far as an
applier, which renders `CREATE VIEW 123 AS None`, and the failure arrives from
a SQL parser with no idea which models file wrote it.

RULE 13: DECLARE THE SHAPE AT THE BOUNDARY SO THE INTERIOR CAN TRUST IT. The
differ and both appliers already treat `name` as a string and `query` as SQL.
Nothing between here and there re-checks, and nothing should have to.

WHAT IS DELIBERATELY NOT CHECKED: whether the query is valid SQL. That needs an
engine, the engine says so at apply time, and a loader guessing at SQL validity
would reject dialect-specific syntax it does not know.
"""

from __future__ import annotations

import pathlib

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.pydantic_loader import load_views_from_models

pytestmark = pytest.mark.precommit

GOOD = (
    'from declaro_persistum.types import View\n'
    'active: View = {"name": "active_users", '
    '"query": "SELECT id FROM users WHERE status = \'active\'"}\n'
)


def _dir(tmp_path: pathlib.Path, body: str) -> pathlib.Path:
    (tmp_path / "views.py").write_text(body)
    return tmp_path


def test_a_well_formed_view_loads(tmp_path) -> None:
    """The baseline. Every refusal below must not cost this."""
    views = load_views_from_models(_dir(tmp_path, GOOD))

    assert sorted(views) == ["active_users"]
    assert views["active_users"]["query"].startswith("SELECT")


def test_a_directory_with_no_views_is_empty_not_an_error(tmp_path) -> None:
    assert load_views_from_models(_dir(tmp_path, "x = 1\n")) == {}


BAD_VIEWS = [
    ("a name that is not a string", '{"name": 123, "query": "SELECT 1"}'),
    ("an empty name", '{"name": "", "query": "SELECT 1"}'),
    ("a name that is only spaces", '{"name": "   ", "query": "SELECT 1"}'),
    ("a query that is None", '{"name": "v", "query": None}'),
    ("a query that is not a string", '{"name": "v", "query": 42}'),
    ("an empty query", '{"name": "v", "query": ""}'),
    ("materialized that is a string", '{"name": "v", "query": "SELECT 1", "materialized": "yes"}'),
]


@pytest.mark.parametrize(("label", "literal"), BAD_VIEWS, ids=[b[0] for b in BAD_VIEWS])
def test_a_malformed_view_is_refused(tmp_path, label: str, literal: str) -> None:
    """Every one of these has both required keys and is still not a view."""
    with pytest.raises(LoaderError):
        load_views_from_models(_dir(tmp_path, f"v = {literal}\n"))


def test_the_refusal_names_the_view_and_the_field(tmp_path) -> None:
    """A refusal that does not say which field is half an answer.

    A models directory may hold many views. "a view is malformed" sends the
    reader through all of them.
    """
    with pytest.raises(LoaderError) as caught:
        load_views_from_models(_dir(tmp_path, 'v = {"name": "sales", "query": None}\n'))

    message = str(caught.value)
    assert "sales" in message, message
    assert "query" in message, message


def test_an_unknown_key_is_still_refused(tmp_path) -> None:
    """The check that already existed must survive the new one."""
    with pytest.raises(LoaderError, match="unknown"):
        load_views_from_models(
            _dir(tmp_path, 'v = {"name": "v", "query": "SELECT 1", "nonsense": 1}\n')
        )


def test_the_optional_fields_still_load_when_well_formed(tmp_path) -> None:
    """Refusing bad values must not refuse good ones."""
    views = load_views_from_models(
        _dir(
            tmp_path,
            'v = {"name": "v", "query": "SELECT 1", "materialized": True, '
            '"refresh": "on_demand", "depends_on": ["users"]}\n',
        )
    )

    assert views["v"]["materialized"] is True
    assert views["v"]["depends_on"] == ["users"]


def test_a_dict_that_is_not_a_view_is_ignored_not_refused(tmp_path) -> None:
    """Views are picked up BY SHAPE, so a models file may hold other dicts.

    A dict lacking the required keys is somebody's configuration, not a
    malformed view, and refusing it would make the loader reject files it has
    no business reading.
    """
    body = GOOD + 'settings = {"retries": 3, "timeout": 5}\n'

    assert sorted(load_views_from_models(_dir(tmp_path, body))) == ["active_users"]
