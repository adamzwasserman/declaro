"""Bind `tests/bdd/features/core/asked_from_written.feature`.

WHAT THE FUNCTION ANSWERS. `fn asked_from_written` takes the question as the
caller typed it and the method names that spelling uses, and hands back the parts
in this library's own words. It reads and does nothing else: turning parts into a
declaration is `fn declaration_from_asked`, and that is where a missing part is
told no.

THE WRITING IS TEXT, WHICH IS WHY THESE STEPS PASS STRINGS. Until 2026-10-05 four
cards each took a dict somebody had already taken apart, and the taking apart
lived in a hand-written table inside each step file. The features wrote the real
expression and the library never saw it. Every When below hands over the
expression itself.

THE FOUR SPELLINGS ARE COMPARED AGAINST DJANGO'S ANSWER, NOT AGAINST A LITERAL.
Three scenarios say "the parts are the ones django gave for the same question",
so django's answer is computed once and the others are crossed against it. A
literal typed here would be four copies of one claim and would pass while all
four readers were wrong the same way.

THE MODEL IS EXCEPTED FROM THAT COMPARISON, and the feature does not say so. A
caller writing django types `Person` and a caller writing this library's own
spelling types `people`, so the model cannot be identical across the four. Every
other part can and must be. Each spelling is asked for the model it actually
wrote, and the rest is compared whole.

PURITY IS READ OFF THE MODULE rather than trusted. The last scenario reads it
twice for one answer, then walks the module's imports with ast to assert it
reaches for no clock, no file and no driver.
"""

from __future__ import annotations

import ast
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/asked_from_written.feature")


# THE ONE QUESTION, WRITTEN FOUR WAYS. Each row is what a caller of that spelling
# types and the model name they type it against.
THE_SAME_QUESTION_IN_EACH_SPELLING: dict[str, tuple[str, str]] = {
    "django": ('Person.objects.filter(name="bob").values("id", "name")', "Person"),
    "sqlalchemy": ('Person.query.filter_by(name="bob").with_only("id", "name")', "Person"),
    "prisma": ('Person.find_many(where={"name": "bob"}, select=["id", "name"])', "Person"),
    "pseudo_sql": ('people.select("id", "name").where({"name": "bob"})', "people"),
}

THE_NARROWING_EVERY_SPELLING_MEANS = {"name": "bob"}
THE_COLUMNS_EVERY_SPELLING_MEANS = ["id", "name"]

A_METHOD_NO_TABLE_NAMES = "roughly"
WRITING_THAT_DOES_NOT_PARSE = "Person.objects.filter(name="
THE_WORD_ONLY = "only"

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

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
def lift():
    """The function under test, imported when a step runs rather than at import."""
    from declaro_persistum.asked_from_written import asked_from_written

    return asked_from_written


@pytest.fixture
def names():
    """The method-name table, which this reader is handed rather than reaching for."""
    from declaro_persistum.methods_of import methods_of

    return methods_of


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == THE_THREE_FACTS_IN_A_FAULT


def _without_the_model(asked: dict) -> dict:
    """Every part but the model, which is the caller's own word for their class."""
    return {part: said for part, said in asked.items() if part != "model"}


# =============================================================================
# Which spelling's method names this reader was handed
# =============================================================================


@given(parsers.re(r"^the method names (?P<spelling>django|sqlalchemy|prisma|pseudo_sql) uses$"))
def _the_method_names(scenario: dict, names, spelling: str) -> None:
    scenario["spelling"] = spelling
    scenario["methods"] = names(spelling)


# =============================================================================
# Reading one question
# =============================================================================


@when(parsers.re(r"^a caller writes (?P<written>.+)$"))
def _a_caller_writes(scenario: dict, lift, written: str) -> None:
    scenario["written"] = written
    scenario["asked"] = lift(written, scenario.get("methods", {}))


@then(parsers.re(r"^the parts name the model (?P<model>\w+)$"))
def _the_parts_name_the_model(scenario: dict, model: str) -> None:
    asked = scenario["asked"]

    assert not _is_fault(asked), f"reading {scenario['written']!r} was refused: {asked}"
    assert asked.get("model") == model, (
        f"the caller wrote the question against {model!r} and the parts name {asked.get('model')!r}"
    )


@then(parsers.re(r"^the narrowing holds (?P<column>\w+) against (?P<value>\w+)$"))
def _the_narrowing_holds(scenario: dict, column: str, value: str) -> None:
    asked = scenario["asked"]

    assert asked.get("where") == {column: value}, (
        f"the caller narrowed on {column}={value!r} and the parts carry {asked.get('where')!r}"
    )


@then(parsers.re(r"^the columns hold (?P<first>\w+) and (?P<second>\w+)$"))
def _the_columns_hold(scenario: dict, first: str, second: str) -> None:
    asked = scenario["asked"]

    assert asked.get("select") == [first, second], (
        f"the caller asked for {[first, second]} and the parts carry {asked.get('select')!r}"
    )


@then("the parts are the ones django gave for the same question")
def _the_same_parts_django_gave(scenario: dict, lift, names) -> None:
    """Django's answer is computed here rather than typed, so one wrong reader
    cannot agree with a literal that is also wrong."""
    written, model = THE_SAME_QUESTION_IN_EACH_SPELLING["django"]
    django = lift(written, names("django"))
    mine = scenario["asked"]

    assert not _is_fault(django), f"django's own reading was refused: {django}"
    assert not _is_fault(mine), f"reading {scenario['written']!r} was refused: {mine}"
    assert _without_the_model(mine) == _without_the_model(django), (
        f"the same question in {scenario['spelling']} and in django gave different "
        f"parts: {_without_the_model(mine)} against {_without_the_model(django)}"
    )
    # The model is the one part that may differ, and it is what the caller typed.
    assert mine.get("model") == THE_SAME_QUESTION_IN_EACH_SPELLING[scenario["spelling"]][1], (
        f"the model is the caller's own word and {scenario['spelling']} got {mine.get('model')!r}"
    )
    assert django.get("where") == THE_NARROWING_EVERY_SPELLING_MEANS, django
    assert django.get("select") == THE_COLUMNS_EVERY_SPELLING_MEANS, django


# =============================================================================
# A caller who calls one method
# =============================================================================


@then("the parts name the model and the narrowing")
def _the_model_and_the_narrowing(scenario: dict) -> None:
    asked = scenario["asked"]

    assert not _is_fault(asked), asked
    assert asked.get("model") == "Person", asked
    assert asked.get("where") == THE_NARROWING_EVERY_SPELLING_MEANS, asked


@then("the parts name no columns")
def _no_columns(scenario: dict) -> None:
    asked = scenario["asked"]

    assert "select" not in asked, (
        f"the caller called no method for columns, and the parts invented "
        f"{asked.get('select')!r}. Naming none and naming all are different asks"
    )


# =============================================================================
# Keyword arguments and a dict both read as a narrowing
# =============================================================================


@when('a caller writes filter(name="bob") in django and where({"name": "bob"}) in pseudo_sql')
def _both_spellings_of_a_narrowing(scenario: dict, lift, names) -> None:
    scenario["both"] = {
        "django": lift('Person.objects.filter(name="bob")', names("django")),
        "pseudo_sql": lift('people.where({"name": "bob"})', names("pseudo_sql")),
    }


@then("both give one narrowing holding name against bob")
def _both_give_one_narrowing(scenario: dict) -> None:
    for spelling, asked in scenario["both"].items():
        assert not _is_fault(asked), f"{spelling} was refused: {asked}"
        assert asked.get("where") == THE_NARROWING_EVERY_SPELLING_MEANS, (
            f"{spelling} carries {asked.get('where')!r}; keyword arguments and a "
            f"dict are two ways of writing one narrowing"
        )


# =============================================================================
# A field this spelling has no method for
# =============================================================================


@then("the parts carry that order")
def _the_parts_carry_that_order(scenario: dict) -> None:
    asked = scenario["asked"]

    assert not _is_fault(asked), f"reading {scenario['written']!r} was refused: {asked}"
    assert asked.get("ordered_by") == {"name": "asc"}, (
        f"django has no method of its own for an order, so the library's word is "
        f"what a caller reaches for, and the parts carry {asked.get('ordered_by')!r}"
    )


@then("a caller who chose this spelling did not choose a smaller library")
def _not_a_smaller_library(scenario: dict) -> None:
    """The narrowing survived beside the order, so the one call did not cost the other."""
    asked = scenario["asked"]

    assert asked.get("where") == THE_NARROWING_EVERY_SPELLING_MEANS, asked
    assert asked.get("model") == "Person", asked


# =============================================================================
# What it refuses
# =============================================================================


@then(parsers.re(r"^it is refused, naming (?P<named>\w+)$"))
def _refused_naming(scenario: dict, named: str) -> None:
    asked = scenario["asked"]

    assert _is_fault(asked), (
        f"a method no table names is not a method this reader knows, and "
        f"{scenario['written']!r} came back as {asked!r}"
    )
    said = " ".join(str(part) for part in asked.values())
    assert named in said, f"the refusal never names {named!r}: {said}"


@then("dropping it would answer a question nobody asked")
def _dropping_it_would_answer_something_else(scenario: dict) -> None:
    """Said as a claim about the answer rather than as prose.

    A reader that ignored an unknown method would hand back the parts it did
    recognise, and the caller would get rows for a question missing a clause
    they wrote.
    """
    asked = scenario["asked"]

    assert _is_fault(asked), asked
    assert "model" not in asked and "where" not in asked, (
        f"the refusal carries parts, so a caller could read it as an answer: {asked}"
    )


@then("it is refused")
def _it_is_refused(scenario: dict) -> None:
    asked = scenario["asked"]

    assert _is_fault(asked), f"{scenario['written']!r} came back as {asked!r}"


@then("the refusal says where the writing stopped making sense")
def _it_says_where_it_stopped(scenario: dict) -> None:
    said = " ".join(str(part) for part in scenario["asked"].values())

    assert any(character.isdigit() for character in said), (
        f"a caller cannot find the mistake without a position, and the refusal carries none: {said}"
    )


@when("a caller writes the word only")
def _a_caller_writes_a_bare_word(scenario: dict, lift, names) -> None:
    scenario["written"] = THE_WORD_ONLY
    scenario["raised"] = None
    try:
        scenario["asked"] = lift(THE_WORD_ONLY, names("django"))
    except BaseException as raised:  # noqa: BLE001
        scenario["raised"] = raised
        scenario["asked"] = None


@then("a fault is returned, and nothing was raised")
def _a_fault_is_returned(scenario: dict) -> None:
    assert scenario["raised"] is None, (
        f"a door hands a caller an answer or a fault, and this raised "
        f"{type(scenario['raised']).__name__}: {scenario['raised']}"
    )
    assert _is_fault(scenario["asked"]), (
        f"{scenario['written']!r} is not a question and came back as {scenario['asked']!r}"
    )


# =============================================================================
# Nothing outside the writing is read
# =============================================================================


@then("reading it twice reads the same thing")
def _reading_it_twice(scenario: dict, lift) -> None:
    again = lift(scenario["written"], scenario["methods"])

    assert again == scenario["asked"], (
        f"the same writing read twice gave {scenario['asked']!r} then {again!r}"
    )


@then("no clock, file or connection was reached")
def _nothing_was_reached(scenario: dict) -> None:
    import declaro_persistum.asked_from_written as under_test

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
