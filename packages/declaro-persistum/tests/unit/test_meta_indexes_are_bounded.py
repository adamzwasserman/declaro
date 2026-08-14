"""A Meta index declares four things, and only those four.

`Index` says what an index is: `columns`, `unique`, `where`, `using`. The
loader ignored that and copied whatever the user wrote:

    table["indexes"] = {
        idx["name"]: {k: v for k, v in idx.items() if k != "name"}
        for idx in indexes if isinstance(idx, dict)
    }

Every key but `name` went straight into the schema. So `uniqe=True` — the
typo — becomes a schema field, and the index is not unique. Nothing says so at
load time, and nothing says so afterwards either: introspection never reports a
`uniqe` column, so the differ sees a table that differs from the database on
every run and can never reconcile it.

The comprehension is also why mypy could not check this. It produces
`dict[Any, Any]` where `Index` is required, which is the one real type error
left in this module and the reason the `name` strip above it needed a comment
explaining itself rather than a type saying it.

SO THE VOCABULARY IS ENFORCED AT THE BOUNDARY (Rule 13). `docs/usage.md`
documents exactly `name`, `columns`, `unique` and `where`; `Index` adds
`using`. An unknown key is a mistake, and naming it at load time is the only
moment anyone can act on it. Dropping it silently would trade a permanent
diff for a silently missing index, which is not better.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

pytestmark = pytest.mark.precommit


def _model_with_indexes(indexes: list[dict]) -> type:
    """A table model carrying the indexes under test.

    `__tablename__` is what `pydantic_model_to_table` looks for; a model
    without it returns None rather than raising, so the first version of this
    helper made every test fail for the wrong reason.
    """

    class Widget(BaseModel):
        id: int
        email: str
        status: str

        class Meta:
            indexes: list[dict] = []

    Widget.__tablename__ = "widgets"  # type: ignore[attr-defined]
    Widget.Meta.indexes = indexes
    return Widget


def test_a_documented_index_arrives_intact() -> None:
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    result = pydantic_model_to_table(
        _model_with_indexes(
            [{"name": "idx_email", "columns": ["email"], "unique": True}]
        )
    )
    assert result is not None
    _name, table = result
    assert table["indexes"] == {
        "idx_email": {"columns": ["email"], "unique": True}
    }


def test_the_name_does_not_survive_into_the_value() -> None:
    """It is already the key; introspection never reports it inside."""
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    result = pydantic_model_to_table(
        _model_with_indexes([{"name": "idx_status", "columns": ["status"]}])
    )
    assert result is not None
    assert "name" not in result[1]["indexes"]["idx_status"]


def test_every_declared_field_is_carried() -> None:
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    result = pydantic_model_to_table(
        _model_with_indexes(
            [
                {
                    "name": "idx_all",
                    "columns": ["status"],
                    "unique": False,
                    "where": "deleted_at IS NULL",
                    "using": "btree",
                }
            ]
        )
    )
    assert result is not None
    assert result[1]["indexes"]["idx_all"] == {
        "columns": ["status"],
        "unique": False,
        "where": "deleted_at IS NULL",
        "using": "btree",
    }


def test_an_unknown_key_is_named_and_refused() -> None:
    """`uniqe` is the whole point: a typo that used to reach the schema."""
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    with pytest.raises(ValueError) as e:
        pydantic_model_to_table(
            _model_with_indexes(
                [{"name": "idx_email", "columns": ["email"], "uniqe": True}]
            )
        )
    assert "uniqe" in str(e.value), "the failure must name the offending key"
    assert "idx_email" in str(e.value), "and the index it was written on"


def test_an_index_without_a_name_is_refused() -> None:
    """The name is the key. Without it there is nothing to key on."""
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    with pytest.raises(ValueError, match="name"):
        pydantic_model_to_table(_model_with_indexes([{"columns": ["email"]}]))


def test_an_index_without_columns_is_refused() -> None:
    """An index over no columns cannot be created; it fails at DDL instead."""
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    with pytest.raises(ValueError, match="columns"):
        pydantic_model_to_table(_model_with_indexes([{"name": "idx_nothing"}]))
