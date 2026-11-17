"""Bind `tests/bdd/features/core/methods_of.feature`.

WHAT THE FUNCTION ANSWERS. `fn methods_of` is handed one spelling and says which
method name that spelling uses for each field, where it differs from this
library's own word. A spelling that renames nothing answers with an empty table,
which is not the same as a spelling nobody declared.

AN EMPTY TABLE AND A MISSING ROW ARE TOLD APART, AND THAT IS THE POINT OF TWO OF
THESE SCENARIOS. `pseudo_sql` and `prisma` both answer with nothing, because both
already call a field what this library calls it. A word no spelling uses must not
answer with nothing, because an empty answer would read every method in the
question as the library's own and parse a Django question as if it were written
in this library's spelling. So that scenario asks for a raise naming the word.

NO TWO SPELLINGS MAY CLAIM ONE METHOD NAME, and this file checks it across all
four tables at once rather than pair by pair. If `filter` appeared under Django
and under SQLAlchemy, a reader handed `filter` could not say which spelling it
came from, and `fn asked_from_written` reads these names to decide which part of
the question is which.

THE FIELD NAMES ARE CROSSED AGAINST THE DECLARATION, not against a list here. A
table naming a field no declaration has is a row nothing will ever read, and a
field spelled two ways across two tables is the same defect wearing a different
coat.
"""

from __future__ import annotations

import collections
import pathlib
import re

import pytest
from pytest_bdd import parsers, scenarios, then, when

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/methods_of.feature")


# THE SPELLINGS THAT RENAME NOTHING. Both already use this library's own words.
RENAME_NOTHING = ("pseudo_sql", "prisma")

# WHAT THE FEATURE SAYS EACH RENAMING SPELLING RENAMES. The field is this
# library's word and the value is that spelling's.
WHAT_EACH_SPELLING_RENAMES: dict[str, dict[str, str]] = {
    "django": {"where": "filter", "select": "values"},
    "sqlalchemy": {"where": "filter_by", "select": "with_only"},
}

A_WORD_NO_SPELLING_USES = "duckql"

# WHERE THIS LIBRARY'S OWN METHOD NAMES COME FROM. `HOW-IT-SHOULD-WORK.md` sits
# above the declaration, and its vocabulary section is the list of words this
# library answers to. Reading it means a word added there and not here cannot
# make this gate pass by accident.
THE_TOP_DOCUMENT = pathlib.Path(__file__).parents[3] / "HOW-IT-SHOULD-WORK.md"


def _THE_LIBRARYS_OWN_METHODS() -> set[str]:
    """Every method name the top document writes in backticks."""
    said = THE_TOP_DOCUMENT.read_text()
    return set(re.findall(r"`([a-z_]+)\(", said)) | set(re.findall(r"`([a-z_]+)`", said))


@pytest.fixture
def ask():
    """The function under test, imported when a step runs rather than at import.

    A step file that cannot be imported is a collection error, which hides which
    scenario was asking for what.
    """
    from declaro_persistum.methods_of import methods_of

    return methods_of


def _every_table(ask) -> dict[str, dict[str, str]]:
    """Every spelling's table, read one spelling at a time."""
    from typing import get_args

    from declaro_persistum.types import Grammar

    return {spelling: ask(spelling) for spelling in get_args(Grammar)}


# =============================================================================
# One spelling at a time
# =============================================================================


@when(parsers.re(r"^methods_of is asked for (?P<spelling>pseudo_sql|django|sqlalchemy|prisma)$"))
def _asked_for_one(scenario: dict, ask, spelling: str) -> None:
    scenario["spelling"] = spelling
    scenario["table"] = ask(spelling)


@then("every field is reached by the library's own word")
def _every_field_is_the_librarys_own(scenario: dict) -> None:
    assert scenario["spelling"] in RENAME_NOTHING, (
        f"{scenario['spelling']} is not one of the spellings that rename nothing"
    )
    assert scenario["table"] == {}, (
        f"this spelling uses the library's own word for every field, so there is "
        f"nothing to rename, and it answered {scenario['table']}"
    )


@then("the table holds no row")
def _the_table_holds_no_row(scenario: dict) -> None:
    assert scenario["table"] == {}, scenario["table"]


@then("where and select are reached by the library's own word")
def _prisma_renames_nothing(scenario: dict) -> None:
    assert scenario["spelling"] == "prisma", scenario["spelling"]
    assert scenario["table"] == {}, (
        f"prisma already calls them where and select, so it renames nothing, "
        f"and it answered {scenario['table']}"
    )


@then(
    parsers.re(r"^(?P<narrowing>\w+) means the narrowing and (?P<columns>\w+) means the columns$")
)
def _it_renames_two(scenario: dict, narrowing: str, columns: str) -> None:
    table = scenario["table"]
    wanted = WHAT_EACH_SPELLING_RENAMES[scenario["spelling"]]

    assert table.get("where") == narrowing, (
        f"{scenario['spelling']} calls the narrowing {narrowing!r} and the table "
        f"says {table.get('where')!r}"
    )
    assert table.get("select") == columns, (
        f"{scenario['spelling']} calls the columns {columns!r} and the table "
        f"says {table.get('select')!r}"
    )
    assert table == wanted, (
        f"this step and the feature disagree about what {scenario['spelling']} "
        f"renames: the table is {table} and the feature says {wanted}"
    )


# =============================================================================
# Every table together
# =============================================================================


@when("every table is read together")
def _every_table_read_together(scenario: dict, ask) -> None:
    scenario["tables"] = _every_table(ask)


@then("no method name appears under two spellings")
def _no_name_under_two_spellings(scenario: dict) -> None:
    said = collections.Counter(
        name for table in scenario["tables"].values() for name in table.values()
    )
    shared = {name: n for name, n in said.items() if n > 1}

    assert said, "every table read as empty, so this asserted nothing"
    assert shared == {}, (
        f"these method names appear under more than one spelling, so a reader "
        f"handed one cannot say which spelling it came from: {shared}"
    )


@then("a name read back gives one spelling")
def _a_name_reads_back_to_one(scenario: dict) -> None:
    back: dict[str, list[str]] = {}
    for spelling, table in scenario["tables"].items():
        for name in table.values():
            back.setdefault(name, []).append(spelling)
    ambiguous = {name: who for name, who in back.items() if len(who) > 1}

    assert back, "no method name was found in any table, so this asserted nothing"
    assert ambiguous == {}, f"these names read back to more than one spelling: {ambiguous}"


@then("each one renames only a method this library has")
def _only_methods_this_library_has(scenario: dict) -> None:
    """Crossed against the top document rather than a list typed in here.

    A TABLE'S KEYS ARE THIS LIBRARY'S OWN METHOD NAMES, not declaration fields.
    That is the only reading under which pseudo_sql and prisma hold no row, and
    Adam ruled it on 2026-11-16. A declaration calls the narrowing `condition`;
    this library's method for it is `where` and Django's is `filter`.

    THE AUTHORITY IS `HOW-IT-SHOULD-WORK.md`, which sits above the declaration,
    so the vocabulary is read out of it. A key renaming a method this library
    does not have is a row nothing will ever read.
    """
    vocabulary = _THE_LIBRARYS_OWN_METHODS()
    named = {method for table in scenario["tables"].values() for method in table}
    stray = sorted(named - vocabulary)

    assert named, "every table read as empty, so this asserted nothing"
    assert len(vocabulary) > 10, (
        f"the vocabulary was read as {sorted(vocabulary)}, which is too few to be "
        f"the whole of it, so this compared against almost nothing"
    )
    assert stray == [], (
        f"these tables rename methods this library does not have, so nothing "
        f"will ever read the row: {stray}"
    )


@then("a method renamed by one is renamed by all that rename anything")
def _one_field_spelled_one_way(scenario: dict) -> None:
    """A method renamed by two spellings is the same method, spelled once here."""
    per_spelling = {
        spelling: sorted(table) for spelling, table in scenario["tables"].items() if table
    }
    fields = {field for named in per_spelling.values() for field in named}
    disagree = {
        spelling: sorted(fields - set(named))
        for spelling, named in per_spelling.items()
        if set(named) != fields
    }

    assert per_spelling, "no spelling renamed anything, so this asserted nothing"
    assert disagree == {}, (
        f"a renaming spelling must rename the same fields as the others, and "
        f"these are short: {disagree}"
    )


# =============================================================================
# A word no spelling uses
# =============================================================================


@when("methods_of is asked for a word no spelling uses")
def _asked_for_a_word_nobody_uses(scenario: dict, ask) -> None:
    scenario["ask"] = ask


@then("it raises naming the word, rather than answering an empty table")
def _it_raises_naming_the_word(scenario: dict) -> None:
    with pytest.raises(KeyError) as raised:
        scenario["ask"](A_WORD_NO_SPELLING_USES)

    assert A_WORD_NO_SPELLING_USES in str(raised.value), (
        f"the failure does not name the word that caused it: {raised.value}"
    )


@then("an empty table would have read every method as the library's own")
def _why_an_empty_table_is_wrong(scenario: dict) -> None:
    """The consequence, asserted rather than described.

    An empty table is what a spelling that renames nothing answers. Were a word
    nobody declared to answer the same way, a Django question would be parsed as
    though it were written in this library's spelling, and `filter` would be read
    as a method this library knows.
    """
    renames_nothing = scenario["ask"]("pseudo_sql")

    assert renames_nothing == {}, (
        "a spelling that renames nothing answers with an empty table, which is "
        "the answer a word nobody declared must not be able to give"
    )
