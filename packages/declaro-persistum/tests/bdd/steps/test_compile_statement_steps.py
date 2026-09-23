"""Bind `tests/bdd/features/rendering/compile_statement.feature`.

WHAT THE FEATURE ASKS FOR. `compile_statement` is handed a declaration and the
name of an engine, and hands back a statement and the values beside it. It is
the last step and the only one that knows which engine it is talking to.

THE ONE THING MOST OF THESE SCENARIOS CHECK. A value never reaches the
statement text. The check that matters is not a look at how the function is
written: it is compiling a declaration carrying a value a caller could have
written as an injection, then reading the two halves that come back. The value
has to be absent from the text and present in the parameters, character for
character, and the text has to be the same text a harmless value produced.

NOTHING HERE READS THE PACKAGE'S SOURCE. A claim about how a module is written
is a claim the gates over `src/` make, for every module rather than for this
one. These steps call the function and read what comes back.

WHAT IS NOT SATISFIED, and is failing rather than faked:

`four grammars render to one statement` needs `declaration_from_pseudo_sql`,
`declaration_from_django`, `declaration_from_prisma` and
`declaration_from_sqlalchemy`, and none of the four is built. The Given calls
them so the failure names the four that are missing.

`none of them takes an engine or names one` asks about the steps that run
before rendering. Those steps are `refuse_undeclared_columns`, `join_between`
and the four grammar builders, and none of them exists yet, so there is no
signature to read.

`nothing but the renderer builds a statement` asks about every module in the
package. That is a gate over `src/`, not a result this function can return.
"""

from __future__ import annotations

import inspect
import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import types as declared_types

scenarios("../features/rendering/compile_statement.feature")

# The three engines and the four grammars, read off the declaration's own
# vocabulary rather than written down a second time here.
THE_THREE_ENGINES = typing.get_args(declared_types.Engine)
THE_FOUR_GRAMMARS = typing.get_args(declared_types.Grammar)

# A CONDITION IS DATA, AND THIS IS THE EMPTY ONE. Three lists, none of them
# narrowing anything.
NOTHING_ASKED: dict = {"each": {}, "any_of": [], "none_of": []}


def _a_declaration(**changed: object) -> dict:
    """One declaration about Person, with every field the declaration names.

    `model` carries the table Person declares, which is `people`, because that
    is the name the statement has to spell.
    """
    return {
        "model": "people",
        "wanted": "all",
        "columns": [],
        "condition": NOTHING_ASKED,
        "order": {},
        "grouping": [],
        "having": NOTHING_ASKED,
        "paging": None,
        "rows": None,
        # The shape a caller wants its rows back in changes nothing about the
        # statement, and the renderer refuses a declaration missing any field,
        # so it is here to be complete rather than to be read.
        "shaping": "dictionaries",
        "across": None,
    } | changed


def _narrowed_by(column: str, value: object) -> dict:
    """The condition a caller writes as `where({column: value})`."""
    return {
        "each": {column: {"operator": "eq", "value": value, "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    }


def _rendered(declaration: object, engine: str) -> object:
    """Hand the renderer what it takes and give back what it says."""
    from declaro_persistum.compile_statement import compile_statement

    return compile_statement(declaration, engine)


def _about_person_narrowed_by_name() -> dict:
    """The declaration four scenarios start from."""
    return _a_declaration(condition=_narrowed_by("name", "bob"))


# --- what a scenario establishes --------------------------------------------

# WHICH NARROWING THE GIVEN CARRIES, as a table rather than as a branch. The two
# phrasings differ by four words and by one field, so they are one step.
_NARROWING: dict[str, dict] = {
    "": NOTHING_ASKED,
    " narrowed by name": _narrowed_by("name", "bob"),
}


@given(parsers.re(r"a declaration about Person(?P<narrowing> narrowed by name)?$"))
def _a_declaration_about_person(scenario: dict, narrowing: str | None) -> None:
    scenario["declaration"] = _a_declaration(condition=_NARROWING[narrowing or ""])


@given(parsers.re(r'a declaration narrowed by the value "(?P<value>.*)"$'))
def _a_declaration_narrowed_by_a_value(scenario: dict, value: str) -> None:
    scenario["value"] = value
    scenario["declaration"] = _a_declaration(condition=_narrowed_by("name", value))


@given("a declaration asking for the id and name of Person")
def _a_declaration_asking_for_two_columns(scenario: dict) -> None:
    scenario["declaration"] = _a_declaration(columns=["id", "name"])


@given(parsers.parse("a declaration rendered for {engine}"))
def _a_declaration_already_rendered(scenario: dict, engine: str) -> None:
    scenario["declaration"] = _about_person_narrowed_by_name()
    scenario["engine"] = engine
    scenario["answer"] = _rendered(scenario["declaration"], engine)


@given("the same question written in pseudo-sql, django, prisma and sqlalchemy")
def _the_same_question_in_four_grammars(scenario: dict) -> None:
    """One question -- every person called bob -- in each grammar's own words.

    NOT FOUR DECLARATIONS WRITTEN BY HAND. Four dicts this file typed out would
    be identical because this file typed them identically, and the scenario
    asks whether four DIFFERENT builders agree. So each row below is the call
    as that grammar's own caller writes it, and the four builders turn them
    into the declarations the renderer then compiles.
    """
    from declaro_persistum.declaration_from_django import declaration_from_django
    from declaro_persistum.declaration_from_prisma import declaration_from_prisma
    from declaro_persistum.declaration_from_pseudo_sql import declaration_from_pseudo_sql
    from declaro_persistum.declaration_from_sqlalchemy import declaration_from_sqlalchemy

    # Each grammar spells the narrowing under its own word, and all four carry
    # the asking word under `wanted`. The question underneath is the one question.
    asked = {
        declaration_from_pseudo_sql: {"model": "people", "wanted": "all", "where": {"name": "bob"}},
        declaration_from_django: {"model": "people", "wanted": "all", "filter": {"name": "bob"}},
        declaration_from_prisma: {
            "model": "people",
            "wanted": "all",
            "where": {"name": "bob"},
        },
        declaration_from_sqlalchemy: {
            "model": "people",
            "wanted": "all",
            "filter_by": {"name": "bob"},
        },
    }
    scenario["declarations"] = [builder(written) for builder, written in asked.items()]


# --- what a scenario does ----------------------------------------------------


@when(parsers.re(r'it is rendered for "?(?P<engine>[a-z_]+)"?$'))
def _it_is_rendered_for(scenario: dict, engine: str) -> None:
    scenario["engine"] = engine
    scenario["answer"] = _rendered(scenario["declaration"], engine)


# TWO ENGINES, OR ONE ENGINE TWICE. The two scenarios render the same
# declaration a second time and compare; they differ by which engines, so the
# engines are a row and the step is one function.
_A_SECOND_TIME: dict[str, tuple[str, str]] = {
    "for postgresql and for sqlite": ("postgresql", "sqlite"),
    "twice for the same engine": ("sqlite", "sqlite"),
}


@when(
    parsers.re(r"it is rendered (?P<how>for postgresql and for sqlite|twice for the same engine)$")
)
def _it_is_rendered_a_second_time(scenario: dict, how: str) -> None:
    scenario["answers"] = [
        _rendered(scenario["declaration"], engine) for engine in _A_SECOND_TIME[how]
    ]


@when("each is rendered for postgresql")
def _each_is_rendered_for_postgresql(scenario: dict) -> None:
    scenario["answers"] = [
        _rendered(declaration, "postgresql") for declaration in scenario["declarations"]
    ]


@when("a caller hands the renderer a string instead of a declaration")
def _a_caller_hands_it_a_string(scenario: dict) -> None:
    scenario["answer"] = _rendered("SELECT * FROM people", "sqlite")


@when("the renderer runs")
def _the_renderer_runs(scenario: dict) -> None:
    scenario["declaration"] = _about_person_narrowed_by_name()
    scenario["answers"] = [
        _rendered(scenario["declaration"], engine) for engine in THE_THREE_ENGINES
    ]


@when("the renderer's own arguments are read")
def _the_renderers_arguments_are_read(scenario: dict) -> None:
    from declaro_persistum.compile_statement import compile_statement

    scenario["parameters"] = inspect.signature(compile_statement).parameters


@when("every step before rendering is read")
def _every_step_before_rendering_is_read(scenario: dict) -> None:
    """The six steps a question goes through before the renderer sees it.

    Four builders turn one grammar's phrasing into a declaration, one refuses a
    column no model declares, and one works out a join from a foreign key.
    """
    import importlib

    scenario["before_rendering"] = {
        name: getattr(importlib.import_module(f"declaro_persistum.{name}"), name)
        for name in [
            "declaration_from_pseudo_sql",
            "declaration_from_django",
            "declaration_from_prisma",
            "declaration_from_sqlalchemy",
            "refuse_undeclared_columns",
            "join_between",
        ]
    }


@when("the steps between a grammar and a statement are counted")
def _the_steps_between_a_grammar_and_a_statement(scenario: dict) -> None:
    from declaro_persistum.compile_statement import compile_statement

    scenario["parameters"] = inspect.signature(compile_statement).parameters
    scenario["declaration"] = _about_person_narrowed_by_name()


# --- what has to be true -----------------------------------------------------


@then("it gives back a statement and its parameters")
def _it_gives_back_a_statement_and_its_parameters(scenario: dict) -> None:
    answer = scenario["answer"]

    assert set(answer) == {"text", "parameters"}, f"a Statement carries two things: {answer}"
    assert isinstance(answer["text"], str) and answer["text"].strip() != ""
    assert isinstance(answer["parameters"], dict)
    assert answer["parameters"] != {}, (
        "the declaration narrows by a value, so a value has to come back beside "
        "the statement; an empty set of parameters means it went into the text"
    )


@then(parsers.parse("the placeholders are the ones {engine} uses"))
def _the_placeholders_are_the_engines(scenario: dict, engine: str) -> None:
    """The text points at each value in the spelling this engine reads.

    A statement carrying another engine's placeholder is a statement the driver
    refuses at run time, which is the failure this scenario exists to catch
    before a connection is anywhere near it.
    """
    answer = scenario["answer"]
    placeholders = {
        "postgresql": [f"${n}" for n in range(1, len(answer["parameters"]) + 1)],
        "sqlite": ["?"] * len(answer["parameters"]),
        "turso": ["?"] * len(answer["parameters"]),
    }[engine]

    for placeholder in placeholders:
        assert placeholder in answer["text"], (
            f"{engine} reads {placeholder} and the statement does not carry one: {answer['text']}"
        )

    # AND NOT ANOTHER ENGINE'S. `$1` and `?` in one statement means two
    # spellings reached the same text, and every driver refuses one of them.
    others = {"$1", "?", ":p1", "%s"} - set(placeholders)
    carried = sorted(spelling for spelling in others if spelling in answer["text"])
    assert carried == [], f"{engine} does not read {carried}: {answer['text']}"


@then("it gives back that engine's statement")
def _it_gives_back_that_engines_statement(scenario: dict) -> None:
    answer = scenario["answer"]

    assert set(answer) == {"text", "parameters"}, f"a Statement carries two things: {answer}"
    assert scenario["engine"] in THE_THREE_ENGINES

    # THE ENGINE IS USED, NOT IGNORED. A renderer that picked one engine and
    # spelled everything its way would satisfy every other Then in this
    # scenario. So the same declaration goes through all three, and the answer
    # for this engine has to be the one this engine's spelling produced.
    each = {engine: _rendered(scenario["declaration"], engine) for engine in THE_THREE_ENGINES}
    assert each[scenario["engine"]] == answer, "the answer moved between two identical calls"
    assert len({statement["text"] for statement in each.values()}) > 1, (
        f"all three engines got the same text, so the engine reached nothing: {answer['text']}"
    )


@then("the declaration itself is unchanged")
def _the_declaration_is_unchanged(scenario: dict) -> None:
    assert scenario["declaration"] == _about_person_narrowed_by_name(), (
        "rendering read the declaration and handed back something new; a "
        "declaration it edited could not be rendered for a second engine"
    )


@then("the two statements differ")
def _the_two_statements_differ(scenario: dict) -> None:
    first, second = scenario["answers"]
    assert first["text"] != second["text"], (
        f"postgresql and sqlite got the same text, so nothing here is engine's: {first['text']}"
    )


@then("the two parameter sets are equal, because nothing about a value is engine's")
def _the_two_parameter_sets_are_equal(scenario: dict) -> None:
    first, second = scenario["answers"]
    assert first["parameters"] == second["parameters"], (
        "a value is a value on every engine; parameters that moved with the "
        f"engine mean the engine reached the values: {first} against {second}"
    )


@then('"bob" is in the parameters')
@then("the value is in the parameters, character for character")
def _the_value_is_in_the_parameters(scenario: dict) -> None:
    answer = scenario["answer"]
    assert scenario["value"] in answer["parameters"].values(), (
        f"the value never arrived beside the statement: {answer}"
    )


@then('"bob" does not appear anywhere in the statement text')
def _the_value_does_not_appear_in_the_text(scenario: dict) -> None:
    text = scenario["answer"]["text"]
    assert scenario["value"] not in text, (
        f"a value reached the statement, so nobody binds it: {text}"
    )


@then('the statement is the same one the value "bob" produced')
def _the_statement_is_the_one_bob_produced(scenario: dict) -> None:
    """The text is a function of the declaration's shape and not of its values.

    This is the string test at the last place it can still be broken. A
    renderer that pasted the value in would give two different texts here, and
    the one carrying `'; DROP TABLE people; --` would read as valid SQL.
    """
    with_bob = _rendered(_about_person_narrowed_by_name(), "sqlite")

    assert scenario["answer"]["text"] == with_bob["text"], (
        "the text moved when the value moved:\n"
        f"  {scenario['answer']['text']}\n  {with_bob['text']}"
    )
    assert scenario["value"] not in with_bob["text"]

    # And it stays put for every value a caller could write, including the ones
    # that read as SQL and the ones that are not text at all. Each of them also
    # has to arrive in the parameters as itself.
    values = ["bob", "", "'", '"', ";--", "1 OR 1=1", "people", 7, 0, True, None, 1.5]
    rendered = [
        _rendered(_a_declaration(condition=_narrowed_by("name", value)), "sqlite")
        for value in values
    ]
    texts = {statement["text"] for statement in rendered}
    assert texts == {with_bob["text"]}, f"the text moved with the value: {sorted(texts)}"

    carried = [list(statement["parameters"].values()) for statement in rendered]
    assert carried == [[value] for value in values], (
        f"a value was edited on its way to the driver: {carried}"
    )


@then("both column names appear in the statement")
def _both_column_names_appear(scenario: dict) -> None:
    text = scenario["answer"]["text"]
    missing = [column for column in ["id", "name"] if column not in text]
    assert missing == [], (
        f"a column is an identifier and nothing binds an identifier, so it has "
        f"to be written into the text; these were not: {missing} in {text}"
    )
    assert scenario["answer"]["parameters"] == {}, (
        "a column was bound as a value, and no engine reads a placeholder where "
        f"a column name belongs: {scenario['answer']}"
    )


@then("each was checked against the model before it got here")
def _each_column_was_checked_before(scenario: dict) -> None:
    """The check is a step of its own, and this function cannot be it.

    `compile_statement` is handed a declaration and an engine. It is handed no
    model, so it has nothing to check a column against, and a column that
    reached it was checked by `refuse_undeclared_columns` or by nobody.
    """
    from declaro_persistum.compile_statement import compile_statement

    parameters = list(inspect.signature(compile_statement).parameters)
    assert parameters == ["declaration", "engine"], (
        f"it would need a model to do the checking itself: {parameters}"
    )

    # A column nobody declared compiles, and that is the point: it is not this
    # function's job to notice, and pretending otherwise would put the check in
    # two places that could disagree.
    undeclared = _rendered(_a_declaration(columns=["nickname"]), "postgresql")
    assert "nickname" in undeclared["text"]


@then("the declaration carries no engine")
def _the_declaration_carries_no_engine(scenario: dict) -> None:
    fields = typing.get_type_hints(declared_types.Declaration)
    carrying = [
        field
        for field, kind in fields.items()
        if "engine" in field.lower() or kind is declared_types.Engine
    ]
    assert carrying == [], f"a declaration field names an engine: {carrying}"


@then("the engine is one it was handed, never one it chose")
def _the_engine_is_one_it_was_handed(scenario: dict) -> None:
    parameters = scenario["parameters"]
    assert list(parameters) == ["declaration", "engine"], (
        f"the renderer takes the declaration and the engine: {list(parameters)}"
    )
    from declaro_persistum.compile_statement import compile_statement

    engine = typing.get_type_hints(compile_statement)["engine"]
    assert engine is declared_types.Engine, (
        "the engine is typed as the declaration's own closed word, so an engine "
        f"nobody declared cannot be spelled: {engine}"
    )

    # THE ANSWER MOVES WITH THE ARGUMENT. A renderer that chose an engine of its
    # own would answer the same way whatever it was handed.
    declaration = _about_person_narrowed_by_name()
    texts = {engine: _rendered(declaration, engine)["text"] for engine in THE_THREE_ENGINES}
    assert len(set(texts.values())) > 1, f"the engine it was handed changed nothing: {texts}"


@then("it has no default for it")
def _it_has_no_default_for_the_engine(scenario: dict) -> None:
    defaulted = [
        name
        for name, parameter in scenario["parameters"].items()
        if parameter.default is not inspect.Parameter.empty
    ]
    assert defaulted == [], (
        f"a default absorbs the caller's omission, and afterwards nothing can "
        f"tell a caller who chose an engine from one who forgot: {defaulted}"
    )


@then("both statements are identical, character for character")
def _both_statements_are_identical(scenario: dict) -> None:
    first, second = scenario["answers"]
    assert first == second, f"two identical calls answered differently:\n  {first}\n  {second}"


@then("neither opened a connection")
def _neither_opened_a_connection(scenario: dict) -> None:
    """It answered for all three engines on a machine running none of them.

    No PostgreSQL connection was opened, no SQLite file was named and no Turso
    replica exists here, and all three answers came back. A renderer that
    opened anything would have failed on at least one of the three.
    """
    declaration = _about_person_narrowed_by_name()
    answers = [_rendered(declaration, engine) for engine in THE_THREE_ENGINES]

    assert [set(answer) for answer in answers] == [{"text", "parameters"}] * 3, answers


@then("it read no clock, no random source and no environment variable")
def _it_read_no_clock(scenario: dict) -> None:
    """Fifty calls, one answer.

    A clock, a counter or a random source shows up here as drift between two
    calls that were handed the same two things. The horizon is real: a source
    that happened to answer the same way fifty times would pass.
    """
    declaration = _about_person_narrowed_by_name()
    answers = [_rendered(declaration, "sqlite") for _ in range(50)]

    assert answers.count(answers[0]) == 50, (
        f"the answer moved across fifty identical calls: {answers[0]} then "
        f"{next(answer for answer in answers if answer != answers[0])}"
    )


@then("it asked no database what it supports")
def _it_asked_no_database(scenario: dict) -> None:
    """Everything it knows about an engine, it knew before anything was running.

    All three engines answered, and nothing on this machine was serving two of
    them. A renderer that asked would answer differently depending on which
    server happened to be up, which is the shape that is right often enough to
    be trusted and wrong silently.
    """
    again = [_rendered(scenario["declaration"], engine) for engine in THE_THREE_ENGINES]
    assert again == scenario["answers"], (
        f"the same three questions answered differently the second time: {again}"
    )


# TWO SCENARIOS ASK SOMETHING NO RESULT ANSWERS, and each row says why. The row
# is keyed by what the When established, so one function serves all three steps
# and the message names the particular thing that is out of reach.
@then("none of them takes an engine or names one")
def _no_earlier_step_takes_an_engine(scenario: dict) -> None:
    """Six steps, and not one of them is handed an engine or gives one back.

    This is what makes four grammars cost four paths rather than twelve. A
    builder that took an engine would need a version per engine, and the four
    of them would have to agree with each other and with the renderer.
    """
    naming = {
        name: sorted(
            word
            for word, kind in typing.get_type_hints(step).items()
            if "engine" in word.lower() or kind is declared_types.Engine
        )
        for name, step in scenario["before_rendering"].items()
    }
    named = {name: words for name, words in naming.items() if words != []}
    assert named == {}, f"a step before rendering names an engine: {named}"

    # And the answer each one gives is engine-free too: four grammars asked the
    # same question compile to one statement, which they could not do if any of
    # them had already spelled something an engine's way.
    assert len(scenario["before_rendering"]) == 6, sorted(scenario["before_rendering"])


@then("what came back is a statement and its parameters")
def _what_came_back_is_a_statement(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == {"text", "parameters"}, f"a Statement carries two things: {answer}"


@then("it cannot be handed back to the renderer")
def _it_cannot_be_rendered_again(scenario: dict) -> None:
    again = _rendered(scenario["answer"], scenario["engine"])
    assert set(again) == {"kind", "what_went_wrong", "what_to_do_instead"}, (
        f"a statement went back through the renderer and came out the other "
        f"side as something: {again}"
    )
    assert again["kind"] == "refused"


@then("what came back carries no model and no columns to narrow")
def _what_came_back_carries_no_model(scenario: dict) -> None:
    answer = scenario["answer"]
    declaration_fields = set(typing.get_type_hints(declared_types.Declaration))
    assert set(answer) & declaration_fields == set(), (
        f"the statement still carries a declaration's fields, so it could be "
        f"asked again in another grammar: {sorted(set(answer) & declaration_fields)}"
    )


@then("the declaration it came from is the only thing that can be asked again")
def _the_declaration_is_the_only_thing_askable(scenario: dict) -> None:
    for engine in THE_THREE_ENGINES:
        again = _rendered(scenario["declaration"], engine)
        assert set(again) == {"text", "parameters"}, (
            f"the declaration could not be rendered a second time, for {engine}: {again}"
        )


@then("the four statements are identical")
def _the_four_statements_are_identical(scenario: dict) -> None:
    texts = {answer["text"] for answer in scenario["answers"]}
    assert len(texts) == 1, f"four spellings of one question gave {len(texts)} statements: {texts}"

    # FOUR EMPTY ANSWERS ARE ALSO IDENTICAL. Each grammar spells the narrowing
    # under its own word, so a builder that threw its word away would agree
    # with the other three about nothing at all. The statement has to carry the
    # narrowing before agreeing about it means anything.
    (text,) = texts
    assert "WHERE" in text, f"the narrowing reached none of the four: {text}"


@then("the four parameter sets are equal")
def _the_four_parameter_sets_are_equal(scenario: dict) -> None:
    first, *rest = [answer["parameters"] for answer in scenario["answers"]]
    differing = [parameters for parameters in rest if parameters != first]
    assert differing == [], f"the four grammars bound different values: {first}, {differing}"
    assert list(first.values()) == ["bob"], (
        f"four equal-but-empty parameter sets agree about nothing: {first}"
    )


@then("a grammar reaches the engine through the declaration")
def _a_grammar_reaches_the_engine_through_the_declaration(scenario: dict) -> None:
    """The renderer is handed no grammar, so every grammar arrives the same way.

    What a fifth grammar would cost is settled by this signature: a grammar
    that reached the engine any other way would have to be named here.
    """
    assert list(scenario["parameters"]) == ["declaration", "engine"], (
        f"the renderer takes a declaration, not a grammar: {list(scenario['parameters'])}"
    )

    # Two declarations that say the same thing compile to the same statement,
    # whoever built them. That is what makes a fifth builder enough.
    first = _rendered(scenario["declaration"], "postgresql")
    second = _rendered(_about_person_narrowed_by_name(), "postgresql")
    assert first == second, f"equal declarations gave different statements:\n  {first}\n  {second}"


@then("no grammar has a step of its own per engine")
def _no_grammar_has_a_step_per_engine(scenario: dict) -> None:
    """One function compiles for all three engines, and there are three of them.

    Four grammars times three engines would be twelve conversions that have to
    agree. What is here is four builders and this one function, which is seven.
    """
    answers = [_rendered(scenario["declaration"], engine) for engine in THE_THREE_ENGINES]

    assert len(answers) == len(THE_THREE_ENGINES) == 3
    assert [set(answer) for answer in answers] == [{"text", "parameters"}] * 3, (
        f"one of the three engines had no path through this function: {answers}"
    )
    assert len(THE_FOUR_GRAMMARS) == 4, THE_FOUR_GRAMMARS


@then("it is refused")
def _it_is_refused(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == {"kind", "what_went_wrong", "what_to_do_instead"}, (
        f"a refusal is a Fault, returned and never raised: {answer}"
    )
    assert answer["kind"] == "refused"
    assert answer["what_to_do_instead"].strip() != "", (
        "a refusal that only says what went wrong leaves the caller nowhere to go"
    )


@then("the refusal names postgresql, sqlite and turso")
def _the_refusal_names_the_three_engines(scenario: dict) -> None:
    said = scenario["answer"]["what_went_wrong"] + scenario["answer"]["what_to_do_instead"]
    unnamed = [engine for engine in THE_THREE_ENGINES if engine not in said]
    assert unnamed == [], (
        f"the refusal does not say which engines exist, so a caller who wrote "
        f"mysql has to go and find out: {unnamed} missing from {said!r}"
    )


@then("the refusal says the renderer is the only thing that writes SQL")
def _the_refusal_says_who_writes_sql(scenario: dict) -> None:
    said = scenario["answer"]["what_went_wrong"] + " " + scenario["answer"]["what_to_do_instead"]
    assert "declaration" in said.lower(), (
        f"the refusal has to tell the caller what to hand over instead, which "
        f"is a declaration: {said!r}"
    )
    assert "sql" in said.lower(), (
        f"a caller who handed over SQL text has to be told that writing it is "
        f"the renderer's job and nobody else's: {said!r}"
    )
