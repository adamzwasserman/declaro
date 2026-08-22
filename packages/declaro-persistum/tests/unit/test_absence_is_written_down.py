"""An omitted argument was indistinguishable from a chosen one.

`def select(table, where=None, limit=None)` absorbs the caller's omission:
afterwards nothing can tell "asked for no limit" from "forgot the limit", and
the region where the default applies is invisible at every call site, so
nothing exercises it. Honest Code rule 14 calls the `=` default the input-side
of catch-and-swallow.

NOTHING IS THE ABSENCE, WRITTEN DOWN. A parameter is declared `X | Nothing`
with no default, so a caller who wants the absent case says so, and the type
carries exactly two possibilities rather than one plus a silence.

It is `Ellipsis` underneath, which no schema, column, default or query
argument can ever be, so it cannot collide with a value somebody meant. That
also matches what this package already reads as "not given": `_NO_DEFAULT` in
the pydantic loader has listed `...` alongside `None` and `PydanticUndefined`
since the loader was written.
"""

from types import EllipsisType

from declaro_persistum.types import NOTHING, is_nothing, resolved


def test_nothing_is_not_a_value_any_schema_can_hold() -> None:
    """The collision test, which is why this is not a string or None.

    `None` is a legal SQL default and a legal column value, so a sentinel
    spelled `None` cannot tell absence from a declared NULL.
    """
    assert is_nothing(NOTHING)
    assert not is_nothing(None)
    assert not is_nothing(0)
    assert not is_nothing("")
    assert not is_nothing([])


def test_nothing_has_a_type_a_signature_can_name() -> None:
    """`X | Nothing` is a bounded type, so the absent case is checkable."""
    assert isinstance(NOTHING, EllipsisType)


def test_resolved_resolves_absence_at_the_boundary() -> None:
    """The visible resolve rule 14 asks for.

    A default hides the resolution inside the signature. `resolved` puts it in
    the body where a reader sees it happen and a test can reach it.
    """
    assert resolved(NOTHING, "fallback") == "fallback"
    assert resolved("chosen", "fallback") == "chosen"


def test_resolved_keeps_a_value_that_looks_empty() -> None:
    """The bug an `or` would reintroduce.

    `value or fallback` takes the fallback for 0, "" and [], so a declared
    `DEFAULT 0` or an empty WHERE became the fallback silently. Only NOTHING
    means absent.
    """
    assert resolved(0, 99) == 0
    assert resolved("", "x") == ""
    assert resolved(None, "x") is None
