"""A TOML column with no type became TEXT, and a broken pending file vanished.

The comment says it plainly: `col["type"] = "text"  # Default`.

THIS IS THE FALLBACK THE BDD SCHEMA FACTORY WAS DELETED FOR. On 2026-08-16
three `setup_*_schema` helpers were removed for eleven silent fallbacks, and
the first of the three that mattered was `col_def.get("type", "text")`, with
the reason recorded in `test_the_bdd_schema_builder_refuses_a_broken_schema`:

    A schema missing a type is a broken schema, and answering it with TEXT
    means every scenario built on that schema tests a table nobody declared.

The same fallback sat in the production TOML loader the whole time. The test
helper was fixed and the thing it was helping to test was not.

WHAT IT COSTS. A typo in a key -- `typ` for `type` -- produces a TEXT column
with no complaint, and TEXT accepts everything, so an integer column silently
becomes a string column and every value written through it is stored as one.
Nothing later can tell that from a column the author meant to be TEXT.

A BROKEN PENDING FILE ANSWERED THE SAME WAY. `load_decisions` catches
`TOMLDecodeError` and `OSError` and returns an empty dict, which is what it
also returns when there are no decisions to make. Those are opposite
situations: one says the author answered no questions, and the other says
their answers cannot be read. The differ acts on decisions -- they are how a
rename is told from a drop -- so an unreadable file means every recorded
answer is silently discarded and every ambiguity is asked again or resolved
the other way.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.loader import load_decisions, load_schema

pytestmark = [pytest.mark.precommit]

TYPED = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
name = { type = "text" }
"""

# `typ`, not `type`. A one-letter slip.
MISTYPED_KEY = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
count = { typ = "integer" }
"""

NO_TYPE_AT_ALL = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
count = { nullable = false }
"""

# The shorthand: a bare string is the type. Legal and must keep working.
SHORTHAND = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
name = "text"
"""


def _schema_dir(toml: str) -> str:
    directory = Path(tempfile.mkdtemp())
    (directory / "tables.toml").write_text(toml)
    return str(directory)


def test_a_well_formed_schema_still_loads() -> None:
    """The control. A loader that refused everything would pass the rest."""
    schema = load_schema(_schema_dir(TYPED))

    assert schema["users"]["columns"]["name"]["type"] == "text"


def test_the_shorthand_string_form_still_loads() -> None:
    """`name = "text"` is the type, said the short way, and is not a mistake."""
    schema = load_schema(_schema_dir(SHORTHAND))

    assert schema["users"]["columns"]["name"]["type"] == "text"


@pytest.mark.parametrize(
    "toml", [MISTYPED_KEY, NO_TYPE_AT_ALL], ids=["mistyped_key", "no_type_at_all"]
)
def test_a_column_with_no_type_is_refused(toml: str) -> None:
    """TEXT accepts everything, so the wrong answer here is never noticed."""
    with pytest.raises(LoaderError):
        load_schema(_schema_dir(toml))


@pytest.mark.parametrize(
    "toml", [MISTYPED_KEY, NO_TYPE_AT_ALL], ids=["mistyped_key", "no_type_at_all"]
)
def test_the_refusal_names_the_column_and_the_table(toml: str) -> None:
    """An author with fifty tables needs to be told which line to look at."""
    with pytest.raises(LoaderError) as caught:
        load_schema(_schema_dir(toml))

    message = str(caught.value)
    assert "count" in message and "users" in message, message


def test_a_typo_is_not_answered_with_a_text_column() -> None:
    """Stated separately because it is the whole failure.

    If the refusal were removed and the fallback restored, this is the shape
    that would come back: a column the author never declared, typed as the one
    type that accepts every value.
    """
    with pytest.raises(LoaderError):
        schema = load_schema(_schema_dir(MISTYPED_KEY))
        assert schema["users"]["columns"]["count"]["type"] != "text", (
            "a mistyped key produced a TEXT column"
        )


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------


def test_no_pending_file_means_no_decisions() -> None:
    """Absence is a real answer here and must stay one."""
    assert load_decisions(str(Path(tempfile.mkdtemp()))) == {}


def test_an_unreadable_pending_file_is_not_no_decisions() -> None:
    """The two are opposite situations and had one answer.

    Decisions are how a rename is told from a drop. Discarding them silently
    means every ambiguity is asked again, or resolved the other way, and the
    other way loses the column's rows.
    """
    directory = Path(tempfile.mkdtemp())
    (directory / "migrations").mkdir()
    (directory / "migrations" / "pending.toml").write_text("this is not [ valid toml")

    with pytest.raises(LoaderError) as caught:
        load_decisions(str(directory))

    assert "pending.toml" in str(caught.value), caught.value
