"""How a Pydantic annotation becomes a SQL column type. Nine untested functions.

MEASURED 2026-08-17. Of 283 public functions in `src/`, 101 are never called by
name in any test. 41 of those are reached through a dispatch table, so they run
without being named. The remaining 60 are called by nothing at all, and the
largest group of them is here:

    extract_field_metadata  extract_literal_values  get_literal_columns
    index_from_meta  is_literal_type  is_optional_type  declared_type
    unwrap_optional

This is the first code a user's schema touches. `extract_literal_values` is
where the enum values come from, and enums were measured broken on every engine
earlier the same day: a `Literal` column produced a lookup table with no rows,
so the foreign key rejected both declared values.

EVERY EXPECTATION BELOW WAS MEASURED BY RUNNING THE FUNCTION over 23
annotations, then written down. None was taken from the docstrings.

THE SILENT FALLBACK IS RECORDED, NOT CHANGED. `declared_type` ends in
`return "text"`, so an annotation it does not recognise becomes a text column
and says nothing. Measured, these fall through:

    set[str]   tuple[int, int]   Any   complex   object   frozenset

and `list[str]` becomes `jsonb` while `set[str]` becomes `text`, which is a
difference a reader would not predict from the two declarations. The return
value cannot distinguish "this maps to text" from "I did not recognise this and
guessed", because `str` also returns "text".

Changing that is a behaviour change on the schema path -- a user with
`tags: set[str]` has a text column in a live database today -- so this file
pins what happens rather than deciding what should. The pin is what makes the
decision visible when someone makes it.
"""

from __future__ import annotations

import datetime
import decimal
import uuid
from typing import Any, Literal, Optional

import pytest

from declaro_persistum.pydantic_loader import (
    PYTHON_TO_SQL_TYPE,
    declared_type,
    extract_literal_values,
    is_literal_type,
    is_optional_type,
    unwrap_optional,
)

pytestmark = pytest.mark.precommit


# ---------------------------------------------------------------------------
# The types a schema is actually written in
# ---------------------------------------------------------------------------

MAPPED = [
    (str, "text"),
    (int, "integer"),
    (float, "real"),
    (bool, "boolean"),
    (bytes, "bytea"),
    (uuid.UUID, "uuid"),
    (datetime.datetime, "timestamptz"),
    (datetime.date, "date"),
    (datetime.time, "time"),
    (decimal.Decimal, "numeric"),
    (dict, "jsonb"),
    (list, "jsonb"),
]


@pytest.mark.parametrize("annotation,expected", MAPPED, ids=lambda v: str(v))
def test_a_declared_type_maps_to_its_column_type(annotation, expected) -> None:
    assert declared_type(annotation) == expected


def test_bool_maps_before_int() -> None:
    """`bool` is a subclass of `int` in Python, and the order decides.

    A dict lookup gets this right and an `isinstance` chain gets it wrong,
    which is why this is worth an assertion of its own: a boolean column
    silently becoming an integer changes what `WHERE active` means.
    """
    assert declared_type(bool) == "boolean"
    assert declared_type(int) == "integer"


@pytest.mark.parametrize(
    "annotation,expected",
    [(dict[str, int], "jsonb"), (list[str], "jsonb"), (list[dict], "jsonb")],
    ids=lambda v: str(v),
)
def test_a_parameterised_container_still_maps(annotation, expected) -> None:
    """`list[str]` is not `list`, so the plain lookup misses it."""
    assert declared_type(annotation) == expected


# ---------------------------------------------------------------------------
# Optional
# ---------------------------------------------------------------------------


# BOTH SPELLINGS ON PURPOSE. `Optional[int]` and `int | None` are the same
# type to Python and different text to a reader, and `is_optional_type` has to
# recognise either. Rewriting these to the modern form, as UP045 asks, would
# delete the coverage that says so.
@pytest.mark.parametrize(
    "annotation",
    [str | None, Optional[int], None | str],  # noqa: UP045
    ids=lambda v: str(v),
)
def test_an_optional_is_recognised(annotation) -> None:
    assert is_optional_type(annotation) is True


@pytest.mark.parametrize("annotation", [str, int, list[str]], ids=lambda v: str(v))
def test_a_plain_type_is_not_optional(annotation) -> None:
    assert is_optional_type(annotation) is False


def test_unwrapping_an_optional_gives_the_inner_type() -> None:
    assert unwrap_optional(str | None) is str
    assert unwrap_optional(Optional[int]) is int  # noqa: UP045


def test_an_optional_column_takes_the_inner_type() -> None:
    """`int | None` is an integer column that accepts NULL, not a text one."""
    assert declared_type(Optional[int]) == "integer"  # noqa: UP045
    assert declared_type(datetime.datetime | None) == "timestamptz"


# ---------------------------------------------------------------------------
# Literal, which becomes an enum lookup table
# ---------------------------------------------------------------------------


def test_a_literal_is_recognised() -> None:
    assert is_literal_type(Literal["draft", "live"]) is True
    assert is_literal_type(str) is False


def test_a_literal_column_is_text() -> None:
    """The lookup table holds the values; the column holds a foreign key."""
    assert declared_type(Literal["draft", "live"]) == "text"


def test_the_declared_values_come_out_in_order() -> None:
    """Order matters: the lookup table is populated from this list."""
    assert extract_literal_values(Literal["draft", "live", "archived"]) == [
        "draft",
        "live",
        "archived",
    ]


def test_a_single_valued_literal_still_yields_a_list() -> None:
    assert extract_literal_values(Literal["only"]) == ["only"]


def test_a_non_literal_yields_no_values() -> None:
    """Returning `[]` rather than raising is what lets the caller ask freely."""
    assert extract_literal_values(str) == []
    assert extract_literal_values(int) == []


def test_a_literal_with_an_apostrophe_survives_extraction() -> None:
    """It is rendered into SQL text later, so it must arrive intact."""
    assert extract_literal_values(Literal["it's live"]) == ["it's live"]


# ---------------------------------------------------------------------------
# The silent fallback, recorded
# ---------------------------------------------------------------------------

FALLS_THROUGH = [set[str], tuple[int, int], Any, complex, object, frozenset]


def test_a_list_is_jsonb_and_a_set_is_now_refused() -> None:
    """What the silent fallback used to do to the likeliest case.

    `list[str]` was jsonb and `set[str]` was text, and nothing in either
    declaration suggested the two would land in different column types. The
    set is refused now, so the author is asked which they meant instead of
    being given a guess.
    """
    from declaro_persistum.exceptions import LoaderError

    assert declared_type(list[str]) == "jsonb"
    with pytest.raises(LoaderError):
        declared_type(set[str])


def test_the_mapping_table_covers_what_the_tests_claim() -> None:
    """The floor. A shrunken table would make the fallback do all the work.

    Every mapped case above would still pass, silently, because the fallback
    also returns text for `str`.
    """
    assert len(PYTHON_TO_SQL_TYPE) >= 14, sorted(map(str, PYTHON_TO_SQL_TYPE))
    assert PYTHON_TO_SQL_TYPE[int] == "integer"
    assert PYTHON_TO_SQL_TYPE[bool] == "boolean"


# ---------------------------------------------------------------------------
# The inputs that tell a branch from an accident
# ---------------------------------------------------------------------------
#
# FOUND BY A MUTATION CONTROL. Deleting the `is_literal_type` short-circuit
# from `declared_type` left all 38 tests above passing, because a
# `Literal` that loses the branch falls through to the final `return "text"`
# and lands on the same answer by a different route.
#
# It does not always land there. After the dict lookup, the function matches
# the STRING REPRESENTATION of the annotation:
#
#     if "datetime" in type_str: return "timestamptz"
#     if "UUID" in type_str or "uuid" in type_str: return "uuid"
#     if "Decimal" in type_str: return "numeric"
#
# and `str(Literal["datetime"])` contains "datetime". Measured with the branch
# removed:
#
#     Literal['datetime'] -> 'timestamptz'
#     Literal['uuid']     -> 'uuid'
#     Literal['Decimal']  -> 'numeric'
#     Literal['draft']    -> 'text'
#
# So a status column whose legal values happen to include the word "datetime"
# becomes a timestamp column. My tests used `Literal["draft", "live"]`, an
# input on which the branch and its absence agree, which is full coverage of a
# space too uniform to contain the answer.

VALUES_THAT_LOOK_LIKE_TYPE_NAMES = ["datetime", "uuid", "UUID", "Decimal", "date"]


@pytest.mark.parametrize("value", VALUES_THAT_LOOK_LIKE_TYPE_NAMES)
def test_a_literal_is_text_even_when_its_values_name_a_type(value: str) -> None:
    """The short-circuit, at the only inputs where it is observable."""
    assert declared_type(Literal[value]) == "text", (
        f"Literal[{value!r}] was matched by the string-representation "
        f"fallback, so a column of legal values became a column of that type"
    )


@pytest.mark.parametrize("value", VALUES_THAT_LOOK_LIKE_TYPE_NAMES)
def test_such_a_literal_still_yields_its_values(value: str) -> None:
    """The floor. Returning text is only right if the values still arrive."""
    assert extract_literal_values(Literal[value, "other"]) == [value, "other"]


def test_the_string_match_is_recorded_as_the_blunt_instrument_it_is() -> None:
    """PINNED AS MEASURED. `declared_type` matches on `str(annotation)`.

    That is why the `Literal` branch has to run first, and it means any
    annotation whose text contains one of those words takes that type. It is
    how the function copes with annotations it cannot compare by identity, and
    it is also why the branch order above is load-bearing rather than tidy.
    """

    class datetime_like:  # noqa: N801 - the name IS the test
        pass

    assert declared_type(datetime_like) == "timestamptz", (
        "a class merely NAMED like a datetime no longer maps to one, so the "
        "string-representation fallback changed and the Literal branch may no "
        "longer be what protects a Literal from it"
    )


# ---------------------------------------------------------------------------
# The rename, and the end of the silent guess
# ---------------------------------------------------------------------------
#
# `python_type_to_sql` produced no SQL. It returned the CANONICAL column type
# that goes into the schema dict -- "text", "integer", "timestamptz", "jsonb"
# -- and a second function turns that into an engine's spelling:
#
#     datetime  ->  timestamptz  ->  sqlite=TEXT  postgresql=timestamptz
#     dict      ->  jsonb        ->  sqlite=TEXT  postgresql=jsonb
#
# So the pipeline is annotation -> declared type -> engine SQL, and this
# function is only the first arrow. `to_sql` named the last one, which is the
# arrow it has nothing to do with.
#
# `declared_type` is the package's own word for that middle layer. `map_type`
# is documented as "Spell a declared type for one engine" and `same_type` as
# "Do two declared types name the same column ON THIS ENGINE?". The vocabulary
# was already there; only this function was outside it.
#
# AND IT NO LONGER GUESSES. The old last line was `return "text"`, so six
# annotation forms became text columns in silence, and the value could not be
# told apart from a declared `str`. It refuses now and names what it maps,
# which is what `for_engine` does for an unknown engine everywhere else.


def test_the_dishonest_name_is_gone() -> None:
    """A rename that leaves the old name behind has renamed nothing."""
    import declaro_persistum.pydantic_loader as loader

    assert not hasattr(loader, "python_type_to_sql"), (
        "python_type_to_sql still resolves, so both names exist and the "
        "misleading one goes on being imported"
    )


def test_the_honest_name_does_the_work() -> None:
    from declaro_persistum.pydantic_loader import declared_type

    assert declared_type(str) == "text"
    assert declared_type(datetime.datetime) == "timestamptz"


@pytest.mark.parametrize("annotation", FALLS_THROUGH, ids=lambda v: str(v))
def test_an_unrecognised_annotation_is_refused(annotation) -> None:
    """The six that used to become text columns without a word."""
    from declaro_persistum.exceptions import LoaderError
    from declaro_persistum.pydantic_loader import declared_type

    with pytest.raises(LoaderError):
        declared_type(annotation)


def test_the_refusal_names_what_it_does_map() -> None:
    """An error a reader cannot act on is a crash with better manners."""
    from declaro_persistum.exceptions import LoaderError
    from declaro_persistum.pydantic_loader import declared_type

    with pytest.raises(LoaderError) as caught:
        declared_type(set[str])

    message = str(caught.value)
    assert "set" in message, f"the error does not name the annotation: {message}"
    assert "text" in message and "integer" in message, (
        f"the error does not say which types ARE declared, so the reader "
        f"learns only that theirs is wrong: {message}"
    )


def test_an_explicit_db_type_still_bypasses_the_check() -> None:
    """`field(db_type=...)` is how a caller declares something unmapped.

    The refusal must not close the door it is telling people to use. The
    loader reads `db_type` first and only asks this function when there is
    none.
    """
    import inspect

    from declaro_persistum import model_columns as pydantic_loader

    source = inspect.getsource(pydantic_loader)

    assert 'meta.get("db_type") or declared_type(' in source, (
        "the loader no longer prefers an explicit db_type, so a caller has no "
        "way to declare a column the mapping does not know"
    )
