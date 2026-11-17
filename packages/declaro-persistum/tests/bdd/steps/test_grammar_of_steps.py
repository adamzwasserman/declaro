"""Bind `tests/bdd/features/core/grammar_of.feature`.

WHAT THE FUNCTION ANSWERS. `fn grammar_of` is handed the question as the caller
typed it and says which of the four spellings it is written in, or refuses
naming all four. Nothing else in the call says which one it is, which is the
point: the word used to travel beside the question and the two could disagree.

THE SHAPE DECIDES, AND THESE STEPS ASK FOR THAT RATHER THAN FOR AN ANSWER. Two
scenarios exist only to catch a reader that guessed from keywords. One hands it
two Django questions whose methods differ and asks for the same answer twice;
the other hands it a Django question and a SQLAlchemy question whose only method
is `filter` in both, and asks that the attribute before the method decides. A
reader that matched on `filter` passes every other scenario here and fails those
two, which is why they are not redundant.

THE REFUSAL IS ASKED TO NAME ALL FOUR, not merely to arrive. A caller who wrote
raw SQL needs to know which four spellings exist, and a refusal that says only
"unrecognised" sends them to the source to find out.

PURITY IS ASSERTED BY READING THE MODULE, not by trusting the docstring. The last
scenario calls it twice for one answer, and then walks the module's own imports
with ast to assert it reaches for no clock, no file and no driver. A pure
function that grows an import is caught here rather than by a reviewer.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest
from pytest_bdd import parsers, scenarios, then, when

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/grammar_of.feature")


THE_FOUR_SPELLINGS = ("pseudo_sql", "django", "prisma", "sqlalchemy")

# WHAT EACH SPELLING LOOKS LIKE, as the feature writes it. The text is the input
# and the word beside it is the answer, so a row here is one scenario's whole
# question.
HOW_EACH_SPELLING_WRITES_IT: dict[str, str] = {
    'Person.objects.filter(name="bob")': "django",
    'Person.query.filter_by(name="bob")': "sqlalchemy",
    'Person.find_many(where={"name": "bob"})': "prisma",
    'people.select("id", "name").where({"name": "bob"})': "pseudo_sql",
}

# TWO DJANGO QUESTIONS WHOSE METHODS DIFFER. A reader that decided from the
# method name answers these two differently.
TWO_DJANGO_QUESTIONS = ('Person.objects.values("id")', 'Person.objects.filter(name="bob")')

# TWO QUESTIONS WHOSE ONLY METHOD IS `filter`, in two different spellings. The
# word cannot decide between them; the attribute before it can.
THE_SAME_METHOD_IN_TWO_SPELLINGS: dict[str, str] = {
    'Person.objects.filter(name="bob")': "django",
    'Person.query.filter(name="bob")': "sqlalchemy",
}

WRITTEN_IN_NO_SPELLING_IT_KNOWS = "SELECT id FROM people WHERE name = 'bob'"

# WHAT A PURE FUNCTION MAY NOT REACH FOR. Each is a module that touches the
# world; `fn grammar_of` is declared pure and takes only the text.
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

THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


@pytest.fixture
def read():
    """The function under test, imported when a step runs rather than at import.

    A step file that cannot be imported is a collection error, which hides which
    scenario was asking for what.
    """
    from declaro_persistum.grammar_of import grammar_of

    return grammar_of


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and set(piece) == THE_THREE_FACTS_IN_A_FAULT


# =============================================================================
# The shape names the spelling
# =============================================================================


def _one_of(sentences: tuple[str, ...]) -> str:
    """One pattern matching any of them, longest first so a prefix cannot win."""
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@when(
    parsers.re(
        r"^grammar_of reads (?P<written>Person\.objects\.filter\(name=\"bob\"\)"
        r"|Person\.query\.filter_by\(name=\"bob\"\)"
        r"|Person\.find_many\(where=\{\"name\": \"bob\"\}\)"
        r"|people\.select\(\"id\", \"name\"\)\.where\(\{\"name\": \"bob\"\}\))$"
    )
)
def _grammar_of_reads(scenario: dict, read, written: str) -> None:
    scenario["written"] = written
    scenario["answer"] = read(written)


@then(parsers.re(r"^it answers (?P<spelling>django|sqlalchemy|prisma|pseudo_sql)$"))
def _it_answers(scenario: dict, spelling: str) -> None:
    answer = scenario["answer"]

    assert answer == spelling, (
        f"{scenario['written']!r} is written in {spelling} and was read as {answer!r}"
    )
    assert HOW_EACH_SPELLING_WRITES_IT[scenario["written"]] == spelling, (
        "this step and the feature disagree about which spelling that text is, "
        "so one of the two is stale"
    )


# =============================================================================
# The method name decides nothing
# =============================================================================


# WHICH SET OF QUESTIONS EACH SENTENCE ASKS ABOUT. Both scenarios below read a
# set of questions and keep the answers, differing only in which set, so they are
# one step over this table rather than two functions that erase to one.
WHICH_QUESTIONS_EACH_SENTENCE_READS: dict[str, dict[str, str]] = {
    'grammar_of reads Person.objects.values("id") and Person.objects.filter(name="bob")': dict.fromkeys(
        TWO_DJANGO_QUESTIONS, "django"
    ),
    "grammar_of reads a question whose only method is filter": THE_SAME_METHOD_IN_TWO_SPELLINGS,
}


def _one_of_these(sentences: tuple[str, ...]) -> str:
    """One pattern matching any of them, longest first so a prefix cannot win.

    Each sentence is escaped, because they carry the questions themselves and so
    carry brackets, dots and quotes that a regex would otherwise read as syntax.
    """
    return "^(?P<sentence>" + "|".join(re.escape(one) for one in sentences) + ")$"


@when(
    parsers.re(
        _one_of_these(tuple(sorted(WHICH_QUESTIONS_EACH_SENTENCE_READS, key=len, reverse=True)))
    )
)
def _reads_a_set_of_questions(scenario: dict, read, sentence: str) -> None:
    wanted = WHICH_QUESTIONS_EACH_SENTENCE_READS[sentence]
    scenario["answers"] = {written: read(written) for written in wanted}
    scenario["wanted"] = wanted


@then("it answers django both times")
def _django_both_times(scenario: dict) -> None:
    wrong = {
        written: answer for written, answer in scenario["answers"].items() if answer != "django"
    }

    assert wrong == {}, (
        f"two Django questions differing only in their method were read as {wrong}, "
        f"so the method name decided the answer"
    )


@then("it never read a method name to decide")
def _never_read_a_method_name(scenario: dict) -> None:
    """The two questions share no method, so one answer for both is the claim."""
    answers = set(scenario["answers"].values())

    assert len(answers) == 1, (
        f"the two questions share no method name and got {len(answers)} answers: {answers}"
    )
    assert "values" not in str(answers) and "filter" not in str(answers), (
        f"the answer carries a method name: {answers}"
    )


@then("it answers by the attribute before it, not by the word filter")
def _it_answers_by_the_attribute(scenario: dict) -> None:
    read_as = scenario["answers"]

    assert read_as == THE_SAME_METHOD_IN_TWO_SPELLINGS, (
        "both questions call only `filter`, and the attribute before it is "
        f"`.objects.` in one and `.query.` in the other: {read_as}"
    )


# =============================================================================
# Writing in no spelling it knows
# =============================================================================


@when(parsers.re(r"^grammar_of reads SELECT id FROM people WHERE name = 'bob'$"))
def _raw_sql(scenario: dict, read) -> None:
    scenario["answer"] = read(WRITTEN_IN_NO_SPELLING_IT_KNOWS)


@then("it is refused")
def _it_is_refused(scenario: dict) -> None:
    answer = scenario["answer"]

    assert _is_fault(answer), (
        f"raw SQL is not one of the four spellings and came back as {answer!r} "
        f"rather than as a refusal"
    )


@then("the refusal names all four spellings it does read")
def _the_refusal_names_all_four(scenario: dict) -> None:
    said = " ".join(str(part) for part in scenario["answer"].values())
    missing = [spelling for spelling in THE_FOUR_SPELLINGS if spelling not in said]

    assert missing == [], (
        f"a caller who wrote something else needs to know which spellings exist, "
        f"and the refusal never names {missing}: {said}"
    )


# =============================================================================
# Nothing outside the writing is read
# =============================================================================


@then("reading it twice answers the same")
def _reading_it_twice(scenario: dict, read) -> None:
    again = read(scenario["written"])

    assert again == scenario["answer"], (
        f"the same text read twice answered {scenario['answer']!r} then {again!r}"
    )


@then("no clock, file or connection was reached")
def _nothing_was_reached(scenario: dict) -> None:
    """Read off the module's own imports rather than trusted.

    A pure function that grows an import is caught here, and the list it is
    checked against is every module in this package that touches the world.
    """
    import declaro_persistum.grammar_of as under_test

    source = pathlib.Path(under_test.__file__).read_text()
    tree = ast.parse(source)
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
