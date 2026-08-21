"""A driver's own subclass is still the type it derives from.

`translated_by` matched the exact type and nothing else, so the one
translation a cross-engine transfer makes was lost: asyncpg hands back
`asyncpg.pgproto.pgproto.UUID`, which derives from `uuid.UUID` rather than
being it. A PostgreSQL to SQLite transfer moved no rows and reported

    ProgrammingError: Error binding parameter 1:
    type 'asyncpg.pgproto.pgproto.UUID' is not supported

WHAT A VALUE INHERITS IS PART OF HOW IT IS WRITTEN, and a lookup that ignores
that silently drops the rule rather than refusing. Every place that asks which
spelling a value is written in has the same exposure, because a library is
free to hand back its own subclass of anything.

The nearest declared spelling still wins, so an exact entry beats the base
class it derives from and no table loses its precision by allowing this.
"""

import uuid

from declaro_persistum.types import translated_by, written_as


class _DriverUUID(uuid.UUID):
    """What a driver hands back: a UUID of its own making."""


def test_a_subclass_gets_its_base_class_rule() -> None:
    value = _DriverUUID(int=1)

    assert translated_by({uuid.UUID: str}, value) == str(value)


def test_an_exact_entry_beats_the_base_class() -> None:
    """Precision is not the price of tolerance.

    A table naming both takes the nearer one, so a rule written for the
    subclass is not shadowed by one written for what it derives from.
    """
    value = _DriverUUID(int=1)

    assert translated_by({uuid.UUID: str, _DriverUUID: repr}, value) == repr(value)


def test_a_spelling_with_no_rule_passes_through() -> None:
    """The control. Tolerance must not turn into translating everything."""
    assert translated_by({uuid.UUID: str}, 42) == 42


def test_written_as_reads_a_subclass_as_its_base() -> None:
    """The same question asked as a yes or no.

    `spelling_of(x) is dict` refuses a mapping a library subclassed, and in
    `extract_field_metadata` that means a column silently missing the
    `primary_key` its author declared.
    """

    class _Extra(dict):
        pass

    assert written_as(dict, _Extra({"primary_key": True}))
    assert written_as(dict, {})
    assert not written_as(dict, [])


def test_written_as_still_tells_a_flag_from_a_number() -> None:
    """`bool` derives from `int`, so the direction of the question matters.

    A flag is written as an int, and a number is not written as a bool. A
    view's `materialized` must be a flag, so 1 is refused.
    """
    assert written_as(int, True)
    assert not written_as(bool, 1)
