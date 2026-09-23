"""Bind `tests/bdd/features/core/how_to_spell.feature`, and nothing else.

WHAT THE FEATURE ASKS. Two things per pair: the statements a change takes, and
whether the engine gets there by rebuilding the table.

WHY THE REBUILD HAS ITS OWN SCENARIO. SQLite alters a column by making a new
table and copying into it, so one declared change is four statements there and
one on PostgreSQL. The order is the whole of it: copy before drop, or the data
is gone. That is the one row where getting it wrong costs a table.

WHAT IS NOT ASSERTED HERE. The SQL itself. A scenario naming `ALTER TABLE ALTER
COLUMN` would be this file agreeing with the table beside it, and both would be
wrong together the day an engine changes. What is asserted is the count, the
order and the rebuild flag, which is what a caller acts on.
"""

from __future__ import annotations

import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.how_to_spell import how_to_spell
from declaro_persistum.types import Engine, OperationKind, Spelling

scenarios("../features/core/how_to_spell.feature")

EVERY_ENGINE = typing.get_args(Engine)
EVERY_KIND = typing.get_args(OperationKind)
THE_TWO_FIELDS = frozenset(typing.get_type_hints(Spelling))
A_KIND_NOBODY_WROTE_DOWN = "rename_database"
THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


# ONE FUNCTION OVER A TABLE. The two openings differ in which kind is asked for
# and in nothing else, so the kind is the row.
# Each opening carries both the kind it asks for and the engine it asks, so
# nothing here has to decide between them at run time.
WHAT_EACH_OPENING_ASKS = {
    "create_table on any engine": ("create_table", "postgresql"),
    "a kind of change the table does not cover": (A_KIND_NOBODY_WROTE_DOWN, "sqlite"),
}


@given(
    parsers.re(r"^(?P<opening>\w+ on any engine|a kind of change the table does not cover)$"),
    target_fixture="answered",
)
def _one_kind_anywhere(opening: str) -> Spelling:
    """Whichever pair the opening names, read off the table above."""
    kind, engine = WHAT_EACH_OPENING_ASKS[opening]
    return how_to_spell(kind, engine)  # type: ignore[arg-type,return-value]


@given(
    parsers.re(r"^(?P<kind>\w+) and (?P<engine>sqlite|postgresql|turso)$"),
    target_fixture="answered",
)
def _one_pair(kind: str, engine: str) -> Spelling:
    return how_to_spell(kind, engine)  # type: ignore[arg-type,return-value]


@then("the answer is one statement and says nothing is rebuilt")
def _one_statement(answered: Spelling) -> None:
    assert len(answered["statements"]) == 1, (
        f"this change takes {answered['statements']}, and a caller running one "
        f"of several would leave the rest undone with no word about it"
    )
    assert answered["rebuilds"] is False, (
        "this change says the table is rebuilt, so the speller would go looking "
        "for which columns to copy across and find nothing carrying them"
    )


# ONE FUNCTION OVER A TABLE. The two readings differ in the verdict and in
# nothing else.
THE_VERDICT = {"the table is rebuilt": True, "nothing is rebuilt": False}

WHAT_THE_WRONG_ONE_COSTS = {
    True: (
        "the change would be spelled as one statement this engine cannot run, "
        "so the column is never altered and the next migration proposes it again"
    ),
    False: (
        "the speller would rebuild a table that did not need it, which drops it "
        "and puts a new one back, and anything the copy missed is gone"
    ),
}


@then(parsers.re(r"^the answer says (?P<verdict>the table is rebuilt|nothing is rebuilt)$"))
def _whether_it_rebuilds(answered: Spelling, verdict: str) -> None:
    wanted = THE_VERDICT[verdict]
    assert answered["rebuilds"] is wanted, (
        f"this pair says rebuilds is {answered['rebuilds']!r} and the scenario "
        f"says {wanted!r}: {WHAT_THE_WRONG_ONE_COSTS[wanted]}"
    )


@then("the answer carries four statements, and the copy comes before the drop")
def _the_rebuild_is_in_order(answered: Spelling) -> None:
    taken = answered["statements"]

    assert len(taken) == 4, (
        f"a rebuild takes {taken}. Making the new table, copying into it, "
        f"dropping the old one and renaming the new one are four steps, and a "
        f"rebuild missing one of them leaves the table half replaced."
    )
    copied = next(i for i, text in enumerate(taken) if "SELECT" in text)
    dropped = next(i for i, text in enumerate(taken) if text.startswith("DROP"))
    assert copied < dropped, (
        f"the drop comes at {dropped} and the copy at {copied}, so the old "
        f"table is gone before anything was copied out of it and every row in "
        f"it is lost"
    )


@when("all thirteen kinds are asked of all three engines", target_fixture="said")
def _every_pair() -> dict[tuple[str, str], Spelling]:
    """Walked off `OperationKind` and `Engine`, so a missing row fails here."""
    return {
        (kind, engine): how_to_spell(kind, engine)  # type: ignore[misc]
        for kind in EVERY_KIND
        for engine in EVERY_ENGINE
    }


@then("every pair answers")
def _all_of_them(said: dict[tuple[str, str], Spelling]) -> None:
    assert len(said) == len(EVERY_KIND) * len(EVERY_ENGINE) >= 39, (
        f"only {len(said)} pairs were asked, so a kind or an engine could be "
        f"added with no row and nothing here would say so"
    )
    refused = sorted(pair for pair, answer in said.items() if set(answer) != THE_TWO_FIELDS)
    assert not refused, (
        f"{refused} came back as something other than a spelling, so the "
        f"speller has no rule for them"
    )
    empty = sorted(pair for pair, answer in said.items() if not answer["statements"])
    assert not empty, f"{empty} answer with no statements, so the change is spelled as nothing"


@then("a fault comes back naming it and the engine")
def _refused_by_name(answered: Spelling) -> None:
    assert set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a change nobody wrote down was answered with {answered!r} rather than "
        f"refused, so it is spelled as whatever the lookup fell through to"
    )
    said = f"{answered['what_went_wrong']}{answered['what_to_do_instead']}"  # type: ignore[typeddict-item]
    assert A_KIND_NOBODY_WROTE_DOWN in said, f"the refusal does not name it: {said!r}"
    assert "sqlite" in said, f"the refusal does not name the engine: {said!r}"
