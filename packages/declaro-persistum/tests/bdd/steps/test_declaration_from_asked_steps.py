"""Bind `tests/bdd/features/core/declaration_from_asked.feature`.

WHAT THE FUNCTION ANSWERS. `fn declaration_from_asked` takes the parts a reader
lifted and builds the one declaration every later step reads. It is the one place
a part a declaration needs and nobody wrote is told no, and the only place that
checking happens for all four spellings.

THE ELEVEN FIELDS ARE CROSSED AGAINST `type Declaration`, NOT AGAINST A LIST
HERE. The feature says eleven, and a list typed into this file would go stale the
day a twelfth is declared while still asserting eleven. So the count and the
names come from the TypedDict's own annotations, and the scenario that says "all
eleven fields are present" checks every one of them rather than a number.

"EMPTY" IS ASKED OF EACH FIELD'S OWN SHAPE. A field nobody named is empty rather
than missing, and empty means different things: a list is `[]`, a condition is a
condition holding nothing, and paging is the named absence. A single `== None`
across all nine would pass on a declaration that had dropped its condition.

TWO SCENARIOS EXIST TO CATCH A GUESS RATHER THAN A BUG. A caller who named no
columns asked for none, which is not the same as asking for all of them; and a
caller who said nothing about shape gets dictionaries. Both are places where a
reader could help and must not.

THE FOUR SPELLINGS ARE BUILT THROUGH THE REAL READER. The scenario about one
declaration from four spellings runs each question through `fn grammar_of`,
`fn methods_of` and `fn asked_from_written` first, so it tests the path a caller
takes rather than four dicts this file typed.
"""

from __future__ import annotations

import ast
import pathlib
from typing import Any

import pytest
from pytest_bdd import parsers, scenarios, then, when

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/declaration_from_asked.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# THE ONE QUESTION, WRITTEN FOUR WAYS, as `asked_from_written.feature` writes it.
THE_SAME_QUESTION_IN_EACH_SPELLING = (
    'Person.objects.filter(name="bob").values("id", "name")',
    'Person.query.filter_by(name="bob").with_only("id", "name")',
    'Person.find_many(where={"name": "bob"}, select=["id", "name"])',
    'people.select("id", "name").where({"name": "bob"})',
)

A_TWO_CALL_QUESTION = 'Person.objects.filter(name="bob").values("id", "name")'
THE_PARTS_OF_A_TWO_CALL_QUESTION: dict[str, Any] = {
    "model": "Person",
    "wanted": "all",
    "where": {"name": "bob"},
    "select": ["id", "name"],
}

AN_ASKING_WORD_NOBODY_DECLARED = "roughly"

NOTHING_THAT_TOUCHES_THE_WORLD = frozenset(
    {
        "time",
        "datetime",
        "os",
        "pathlib",
        "io",
        "random",
        "socket",
        "asyncio",
        "sqlite3",
        "aiosqlite",
        "asyncpg",
        "turso",
        "subprocess",
    }
)


@pytest.fixture
def build():
    """The function under test, imported when a step runs rather than at import."""
    from declaro_persistum.declaration_from_asked import declaration_from_asked

    return declaration_from_asked


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == THE_THREE_FACTS_IN_A_FAULT


def _the_eleven_fields() -> dict[str, Any]:
    """Every field a declaration carries, from the type rather than a list here."""
    from declaro_persistum.types import Declaration

    return dict(Declaration.__annotations__)


def _what_empty_means_for(field: str) -> Any:
    """What a field nobody named holds, which depends on that field's own shape."""
    from declaro_persistum.types import Condition

    return {
        "columns": [],
        "grouping": [],
        "condition": Condition(each={}, any_of=[], none_of=[]),
        "having": Condition(each={}, any_of=[], none_of=[]),
        "order": {},
        "paging": None,
        "rows": None,
        "across": None,
        "shaping": "dictionaries",
    }[field]


def _through_the_real_reader(written: str) -> Any:
    """One question read the way a caller's question is read."""
    from declaro_persistum.asked_from_written import asked_from_written
    from declaro_persistum.grammar_of import grammar_of
    from declaro_persistum.methods_of import methods_of

    spelling = grammar_of(written)
    assert not _is_fault(spelling), f"reading the spelling of {written!r} failed: {spelling}"
    return asked_from_written(written, methods_of(spelling))


# =============================================================================
# The parts of a question become a declaration
# =============================================================================


@when(
    parsers.re(
        r"^the parts of (?P<written>Person\.objects\.filter\(name=\"bob\"\)"
        r"\.values\(\"id\", \"name\"\)) arrive$"
    )
)
def _the_parts_of_a_two_call_question(scenario: dict, build, written: str) -> None:
    scenario["written"] = written
    scenario["declaration"] = build(_through_the_real_reader(written))


@then("the declaration names the model, those two columns and that condition")
def _it_names_all_three(scenario: dict) -> None:
    made = scenario["declaration"]

    assert not _is_fault(made), f"building {scenario['written']!r} was refused: {made}"
    assert made["model"] == "Person", made
    assert made["columns"] == ["id", "name"], made
    assert made["condition"]["each"].get("name", {}).get("value") == "bob", (
        f"the caller narrowed on name against bob and the condition holds {made['condition']}"
    )


# =============================================================================
# Every field is there to be read
# =============================================================================


@when("the parts of a question naming two of eleven fields arrive")
def _two_of_eleven(scenario: dict, build) -> None:
    scenario["named"] = ("model", "wanted")
    scenario["declaration"] = build({"model": "Person", "wanted": "all"})


@then("all eleven fields are present")
def _all_eleven_present(scenario: dict) -> None:
    made = scenario["declaration"]
    fields = _the_eleven_fields()

    assert not _is_fault(made), f"a question naming a model and a word was refused: {made}"
    assert len(fields) == 11, (
        f"the feature says eleven and `type Declaration` names {len(fields)}, so "
        f"one of the two is stale: {sorted(fields)}"
    )
    missing = sorted(set(fields) - set(made))
    assert missing == [], (
        f"nothing downstream should have to ask whether a field is there, and "
        f"these are absent: {missing}"
    )


@then("the nine the caller did not name are empty")
def _the_nine_are_empty(scenario: dict) -> None:
    made = scenario["declaration"]
    unnamed = sorted(set(_the_eleven_fields()) - set(scenario["named"]))

    assert len(unnamed) == 9, f"eleven less two is nine and this is {len(unnamed)}: {unnamed}"
    wrong = {field: made[field] for field in unnamed if made[field] != _what_empty_means_for(field)}
    assert wrong == {}, (
        f"these fields hold something the caller never asked for: {wrong}. Empty "
        f"means empty for that field's own shape, not None for all of them"
    )


# =============================================================================
# A narrowing becomes a condition
# =============================================================================


@when("the parts carry a narrowing of name against bob")
def _a_narrowing_of_name(scenario: dict, build) -> None:
    scenario["declaration"] = build({"model": "Person", "wanted": "all", "where": {"name": "bob"}})


@then("the declaration holds an eq comparison")
def _an_eq_comparison(scenario: dict) -> None:
    made = scenario["declaration"]

    assert not _is_fault(made), made
    held = made["condition"]["each"].get("name")
    assert held is not None, f"the condition names no `name`: {made['condition']}"
    assert held["operator"] == "eq", (
        f"a bare value is an equality, and the comparison says {held.get('operator')!r}"
    )


@then("`fn condition_from_narrowing` is what turned it")
def _the_declared_card_turned_it(scenario: dict) -> None:
    """Asked by comparing against that card's own answer, not by watching a call.

    The declaration says this orchestrator invokes `condition_from_narrowing`, so
    the claim is that the condition in hand is the one that card builds. A
    different condition means something else built it.
    """
    from declaro_persistum.condition_from_narrowing import condition_from_narrowing

    assert scenario["declaration"]["condition"] == condition_from_narrowing({"name": "bob"}), (
        "the condition is not the one `fn condition_from_narrowing` builds"
    )


# =============================================================================
# The declaration does not remember which spelling built it
# =============================================================================


@when("the parts of the same question arrive from each of the four spellings")
def _from_each_of_the_four(scenario: dict, build) -> None:
    scenario["four"] = {
        written: build(_through_the_real_reader(written))
        for written in THE_SAME_QUESTION_IN_EACH_SPELLING
    }


@then("the four declarations are one declaration")
def _the_four_are_one(scenario: dict) -> None:
    four = scenario["four"]
    refused = {written: made for written, made in four.items() if _is_fault(made)}

    assert refused == {}, f"these spellings were refused: {refused}"
    # The model is the caller's own word for their class, so it is excepted and
    # checked on its own below.
    without_model = {
        written: {field: said for field, said in made.items() if field != "model"}
        for written, made in four.items()
    }
    one = list(without_model.values())[0]
    differ = {written: made for written, made in without_model.items() if made != one}
    assert differ == {}, f"four spellings of one question built different declarations: {differ}"


@then("nothing in it names a spelling")
def _nothing_names_a_spelling(scenario: dict) -> None:
    from typing import get_args

    from declaro_persistum.types import Grammar

    for written, made in scenario["four"].items():
        said = str(made)
        named = [spelling for spelling in get_args(Grammar) if spelling in said]
        assert named == [], (
            f"the declaration built from {written!r} remembers it was written in "
            f"{named}, so a later step could read the spelling off it"
        )


# =============================================================================
# A part a declaration needs and nobody wrote
# =============================================================================


@when(parsers.re(r"^the parts arrive naming no (?P<part>model|wanted|paging|shaping|columns)$"))
def _the_parts_arrive_naming_no(scenario: dict, build, part: str) -> None:
    whole = {"model": "Person", "wanted": "all"}
    scenario["part"] = part
    scenario["declaration"] = build({field: said for field, said in whole.items() if field != part})


@then(parsers.re(r"^it is refused, naming the (?P<part>model|wanted) it needs$"))
def _refused_naming_the_part(scenario: dict, part: str) -> None:
    made = scenario["declaration"]

    assert _is_fault(made), (
        f"a declaration cannot be built without a {part} and this returned {made!r}"
    )
    said = " ".join(str(piece) for piece in made.values())
    assert part in said, f"the refusal never names the {part} it needs: {said}"


@then("the declaration carries nothing for paging and is not refused")
def _paging_may_be_left_out(scenario: dict) -> None:
    made = scenario["declaration"]

    assert not _is_fault(made), f"paging may be left out and this refused: {made}"
    assert made["paging"] is None, made


@then("a field whose shape holds nothing is a field a caller may leave out")
def _a_shape_that_holds_nothing(scenario: dict) -> None:
    """`paging` is typed `dict[str, int] | None`, so the absence has a place to live."""
    fields = _the_eleven_fields()

    assert "None" in str(fields["paging"]), (
        f"paging is typed {fields['paging']!r}, which cannot hold the named "
        f"absence, so leaving it out would have to be refused"
    )


# =============================================================================
# An asking word nobody declared
# =============================================================================


@when('the parts ask for "roughly"')
def _an_asking_word_nobody_declared(scenario: dict, build) -> None:
    scenario["declaration"] = build({"model": "Person", "wanted": AN_ASKING_WORD_NOBODY_DECLARED})


@then("it is refused, naming that word")
def _refused_naming_that_word(scenario: dict) -> None:
    made = scenario["declaration"]

    assert _is_fault(made), f"{AN_ASKING_WORD_NOBODY_DECLARED!r} is not a word this reads: {made!r}"
    said = " ".join(str(piece) for piece in made.values())
    assert AN_ASKING_WORD_NOBODY_DECLARED in said, f"the refusal never names it: {said}"


@then("the refusal names the words it does read")
def _the_refusal_names_the_words(scenario: dict) -> None:
    from typing import get_args

    from declaro_persistum.types import Reading, Writing

    said = " ".join(str(piece) for piece in scenario["declaration"].values())
    every = (*get_args(Reading), *get_args(Writing))
    missing = [word for word in every if word not in said]

    assert missing == [], (
        f"a caller who used the wrong word needs the list, and the refusal never names {missing}"
    )


# =============================================================================
# What a caller leaves out, and what it does not mean
# =============================================================================


@then("the declaration says rows come back as dictionaries")
def _dictionaries_by_default(scenario: dict) -> None:
    made = scenario["declaration"]

    assert not _is_fault(made), made
    assert made["shaping"] == "dictionaries", (
        f"a caller who said nothing about shape gets dictionaries, and this says "
        f"{made['shaping']!r}"
    )


@then("the declaration names no columns")
def _names_no_columns(scenario: dict) -> None:
    made = scenario["declaration"]

    assert not _is_fault(made), made
    assert made["columns"] == [], made


@then("that is not the same as asking for all of them")
def _naming_none_is_not_naming_all(scenario: dict, build) -> None:
    """Asserted against a question that does name columns, so the two differ."""
    named = build({"model": "Person", "wanted": "all", "select": ["id"]})

    assert named["columns"] == ["id"], named
    assert scenario["declaration"]["columns"] != named["columns"], (
        "naming no columns and naming one gave the same declaration, so the "
        "empty list is being read as a request for everything"
    )


@when("the parts name a model and columns")
def _a_model_and_columns(scenario: dict, build) -> None:
    scenario["first"] = build({"model": "Person", "wanted": "all", "select": ["id"]})
    scenario["second"] = build({"model": "Team", "wanted": "all", "select": ["name"]})


@then("the declaration carries both, and moves when they move")
def _it_carries_both_and_moves(scenario: dict) -> None:
    first, second = scenario["first"], scenario["second"]

    assert (first["model"], first["columns"]) == ("Person", ["id"]), first
    assert (second["model"], second["columns"]) == ("Team", ["name"]), second
    assert first != second, "two different questions built one declaration"


# =============================================================================
# A failure comes back in the return
# =============================================================================


@when("the parts cannot become a declaration")
def _parts_that_cannot(scenario: dict, build) -> None:
    scenario["raised"] = None
    try:
        scenario["declaration"] = build({})
    except BaseException as raised:  # noqa: BLE001
        scenario["raised"] = raised
        scenario["declaration"] = None


@then("a fault is returned, and nothing was raised")
def _a_fault_is_returned(scenario: dict) -> None:
    assert scenario["raised"] is None, (
        f"this hands back an answer or a fault, and it raised "
        f"{type(scenario['raised']).__name__}: {scenario['raised']}"
    )
    assert _is_fault(scenario["declaration"]), (
        f"parts naming nothing came back as {scenario['declaration']!r}"
    )


# =============================================================================
# Nothing outside the parts is read
# =============================================================================


@when(parsers.re(r"^the parts of (?P<written>Person\.objects\.filter\(name=\"bob\"\)) arrive$"))
def _the_parts_of_one_call(scenario: dict, build, written: str) -> None:
    scenario["written"] = written
    scenario["asked"] = _through_the_real_reader(written)
    scenario["declaration"] = build(scenario["asked"])


@then("reading them twice reads the same thing")
def _twice_the_same(scenario: dict, build) -> None:
    again = build(scenario["asked"])

    assert again == scenario["declaration"], (
        f"one set of parts built {scenario['declaration']!r} then {again!r}"
    )


@then("no clock, file or connection was reached")
def _nothing_was_reached(scenario: dict) -> None:
    import declaro_persistum.declaration_from_asked as under_test

    tree = ast.parse(pathlib.Path(under_test.__file__).read_text())
    reached = {
        (node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    } | {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    worldly = sorted(reached & NOTHING_THAT_TOUCHES_THE_WORLD)

    assert worldly == [], f"a function declared pure imports {worldly}"
