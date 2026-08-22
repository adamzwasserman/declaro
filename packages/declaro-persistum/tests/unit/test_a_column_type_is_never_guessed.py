"""Two copies of `_map_type` disagreed about a column type they cannot read.

`abstractions/reconstruction._map_type` returned `"TEXT"` for anything that
was not a string. `abstractions/table_reconstruction` calls `sqlite_type`
directly and raises on the same input. Both build the CREATE TABLE for a
rebuild, and the first one's own docstring recorded the disagreement rather
than resolving it:

    `table_reconstruction._map_type` has no such guard and raises on the same
    input. The two disagree, and that is recorded rather than resolved here.

TEXT IS A GUESS, AND A REBUILD IS WHERE A GUESS COSTS MOST. Reconstruction
drops the real table and recreates it from these definitions, so a column
silently typed TEXT is a column whose values are now text, and the original is
gone. Raising loses nothing: the table is still there.

WHY THE GUARD EXISTED AT ALL. The differ emits `{"from": x, "to": y}` for a
changed type, and that record used to reach the renderer unwrapped. It cannot
now: `changed_to` resolves it in one place, and `_validate_columns` refuses a
dict before the database is touched. So the fallback answers only a caller who
passed a dict directly, and the honest answer to that caller is that they did.
"""

import pytest

from declaro_persistum.abstractions.reconstruction import generate_create_table_sql
from declaro_persistum.abstractions.table_reconstruction import (
    _generate_create_table_sql,
)
from declaro_persistum.types import NOTHING

_BUILDERS = {
    "reconstruction": generate_create_table_sql,
    "table_reconstruction": _generate_create_table_sql,
}

_UNREADABLE = {
    "a change record": {"from": "text", "to": "integer"},
    "a number": 42,
    "nothing": None,
    "a list": ["text"],
}


@pytest.mark.parametrize("builder", sorted(_BUILDERS), ids=sorted(_BUILDERS))
@pytest.mark.parametrize("shape", sorted(_UNREADABLE), ids=sorted(_UNREADABLE))
def test_a_type_that_is_not_a_type_is_refused(builder: str, shape: str) -> None:
    """Both builders, the same refusal, whatever the caller passed.

    Parametrising over both is the point. The defect was not that one of them
    guessed; it was that they answered differently, so which one a caller
    reached decided whether they were told.

    THE REFUSAL HAS TO NAME THE COLUMN. This accepted any of TypeError,
    ValueError or AttributeError and asserted nothing about the message, so a
    named refusal and a `.lower()` crash on a dict passed it identically. The
    author of a broken schema needs to be told which column, and a bare
    AttributeError from inside a type mapper does not tell them.
    """
    columns = {"c": {"type": _UNREADABLE[shape]}}

    with pytest.raises((TypeError, ValueError)) as raised:
        _BUILDERS[builder]("t", columns, NOTHING)

    assert "c" in str(raised.value), str(raised.value)


@pytest.mark.parametrize("builder", sorted(_BUILDERS), ids=sorted(_BUILDERS))
def test_a_declared_type_still_renders(builder: str) -> None:
    """The control. Refusing what cannot be read must not refuse what can."""
    rendered = _BUILDERS[builder]("t", {"c": {"type": "integer"}}, NOTHING)

    assert "INTEGER" in rendered.upper()
    assert '"c"' in rendered


@pytest.mark.parametrize("builder", sorted(_BUILDERS), ids=sorted(_BUILDERS))
def test_a_column_with_no_type_declared_is_refused(builder: str) -> None:
    """A column definition with no type is not a TEXT column either.

    `col_def.get("type", "TEXT")` defaults the same way the guard did, one
    line up. A schema that forgot to say gets asked, not answered for.
    """
    with pytest.raises((KeyError, TypeError, ValueError)):
        _BUILDERS[builder]("t", {"c": {}}, NOTHING)
