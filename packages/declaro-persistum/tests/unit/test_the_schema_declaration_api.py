"""`@table` and `field()` — the half of the surface that names intent.

`map_type` decides how a column is SPELLED per engine. These decide what it
MEANS. Together they are the multi-dialect surface: one declaration, three
dialects, and no engine word anywhere in the model.

BOTH WERE DOCUMENTED AND NEVER WRITTEN. `git log -S"def field("` over the whole
history of the package returns nothing. `usage.md` and `README.md` have opened
with them since c1e6ce2 in December, and `from declaro_persistum import table,
field` raised ImportError the whole time. `extract_field_metadata`'s docstring
cites `field()` as well.

WHAT THEY ARE, EXACTLY. The loader already reads `json_schema_extra` and
`__tablename__`. So `field()` is the typed way to write the first and `@table`
the checked way to set the second. Neither adds a capability; both remove a
silent failure:

    Field(json_schema_extra={"primry_key": True})   # typo, silently ignored
    field(primry_key=True)                          # TypeError at import

That is the same defect fixed in `index_from_meta` yesterday, on the other side
of the same loader.

`primary_key`, NOT `primary`. The docs write `field(primary=True)` and the
loader accepts either, but the schema key, introspection and the SQL all say
`primary_key`. One word for one thing; the docs are what changes. Nothing can
depend on the old spelling because nothing could ever call it.
"""

from __future__ import annotations

import inspect

import pytest
from pydantic import BaseModel

pytestmark = pytest.mark.precommit


def _schema_for(model: type) -> dict:
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    result = pydantic_model_to_table(model)
    assert result is not None, f"{model.__name__} was not recognised as a table"
    return result[1]["columns"]


def test_table_sets_the_name_the_loader_looks_for() -> None:
    from declaro_persistum import table

    @table("users")
    class User(BaseModel):
        id: str

    assert User.__tablename__ == "users"
    from declaro_persistum.pydantic_loader import pydantic_model_to_table

    assert pydantic_model_to_table(User)[0] == "users"


def test_table_returns_the_class_so_it_stacks() -> None:
    from declaro_persistum import table

    @table("users")
    class User(BaseModel):
        id: str

    assert issubclass(User, BaseModel)
    assert User.__name__ == "User"


def test_table_refuses_a_class_the_loader_cannot_read() -> None:
    """A non-Pydantic class silently yields zero columns. Fail at the boundary."""
    from declaro_persistum import table

    with pytest.raises(TypeError, match="BaseModel"):

        @table("users")
        class NotAModel:
            id: str


def test_field_carries_every_column_property_the_loader_reads() -> None:
    from declaro_persistum import field, table

    @table("orders")
    class Order(BaseModel):
        id: str = field(primary_key=True)
        user_id: str = field(references="users.id", on_delete="cascade")
        total: float = field(db_type="numeric(10,2)", check="total >= 0")
        status: str = field(default="'pending'")
        code: str = field(unique=True)

    cols = _schema_for(Order)
    assert cols["id"]["primary_key"] is True
    assert cols["user_id"]["references"] == "users.id"
    assert cols["user_id"]["on_delete"] == "cascade"
    assert cols["total"]["type"] == "numeric(10,2)"
    assert cols["total"]["check"] == "total >= 0"
    assert cols["status"]["default"] == "'pending'"
    assert cols["code"]["unique"] is True


def test_a_misspelled_property_fails_at_import_not_in_the_schema() -> None:
    """The whole reason this exists rather than a bare dict."""
    from declaro_persistum import field

    with pytest.raises(TypeError):
        field(primry_key=True)  # type: ignore[call-arg]


def test_field_declares_a_sql_expression_not_a_python_value() -> None:
    """`default="now()"` is DEFAULT now(), never the string "now()"."""
    from declaro_persistum import field, table

    @table("events")
    class Event(BaseModel):
        at: str = field(default="now()")

    assert _schema_for(Event)["at"]["default"] == "now()"


def test_a_field_with_nothing_to_declare_is_still_a_column() -> None:
    from declaro_persistum import table

    @table("users")
    class User(BaseModel):
        name: str

    assert _schema_for(User)["name"]["type"] == "text"


def test_the_declaration_reaches_every_dialect() -> None:
    """One model, three engines, no engine word in the model."""
    from declaro_persistum import field, table
    from declaro_persistum.applier.shared import column_definition

    @table("orders")
    class Order(BaseModel):
        id: str = field(primary_key=True, db_type="uuid")
        total: float = field(db_type="numeric(10,2)")

    cols = _schema_for(Order)
    assert "TEXT" in column_definition("id", cols["id"], "sqlite")
    assert "TEXT" in column_definition("id", cols["id"], "turso")
    assert "uuid" in column_definition("id", cols["id"], "postgresql")
    assert "REAL" in column_definition("total", cols["total"], "sqlite")
    assert "numeric(10,2)" in column_definition("total", cols["total"], "postgresql")


def test_neither_takes_an_implicit_default() -> None:
    """Rule 14. `@table()` with no name has no sensible guess."""
    from declaro_persistum import table

    params = inspect.signature(table).parameters
    assert params["name"].default is inspect.Parameter.empty
