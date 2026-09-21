"""Bind `tests/bdd/features/core/declaration_from_sqlalchemy.feature`.

WHAT THE FEATURE ASKS FOR. A caller who thinks in a query object writes
`Person.query.filter_by(name="bob").with_only("id", "name")`, and the
declaration that comes back names the model, those two columns and that
condition. The scenarios around that one split the claim into the results it
is made of: what a narrowing becomes, what a word this grammar does not have
does, and what is left when the caller says nothing.

WHAT THE SCENARIOS LEAVE OPEN, AND HOW IT IS SETTLED HERE. The feature writes
the expression a caller types; this function is handed the call as a dict, so
something has to say which dict that expression is. `docs/design/persistum.hd`
calls `Asked` the call as the caller phrased it, and the phrasing
here is SQLAlchemy's own method names, so the four words are `model`, `wanted`,
`filter_by` and `with_only`. `wanted` is in that dict although the scenarios'
expressions do not write `.all()`, because a `Declaration` names one of the
asking words and no builder may pick one on the caller's behalf.

THE NARROWING IS CARRIED IN A TABLE, NOT PARSED OUT OF THE SENTENCE. Six
scenarios narrow by six different conditions, and reading the Gherkin text back
into data would turn the caller's structure into a string and then guess it
back. The step reads the sentence's own text as a key, so a narrowing written
in the feature and missing from the table is a KeyError naming it.
"""

import ast
import inspect
import pathlib

from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.types import Declaration

scenarios("../features/core/declaration_from_sqlalchemy.feature")


# What a pure function may not reach for. Read off the module by name, the same
# list `test_travels_together_steps.py` holds a pure function to.
NOTHING_PURE_IMPORTS = (
    "asyncpg",
    "aiosqlite",
    "sqlite3",
    "turso",
    "os",
    "io",
    "time",
    "datetime",
    "random",
    "secrets",
    "uuid",
    "pathlib",
    "socket",
    "subprocess",
    "requests",
    "httpx",
)

# THE NARROWINGS THE SCENARIOS WRITE, EACH AS ITSELF. The key is the sentence's
# own text, so the table and the feature cannot drift apart quietly: a
# narrowing nobody wrote down here raises naming the line that wrote it.
NARROWINGS = {
    '{"name": "bob"}': {"name": "bob"},
    '{"age": {"gt": 18}}': {"age": {"gt": 18}},
    '{"age": {"gt": 18}, "name": "bob"}': {"age": {"gt": 18}, "name": "bob"},
    '{"or": [{"name": "alice"}, {"name": "carol"}]}': {
        "or": [{"name": "alice"}, {"name": "carol"}]
    },
    '{"not": [{"name": "bob"}]}': {"not": [{"name": "bob"}]},
    '{"name": {"like": "bo%", "ignore_case": true}}': {
        "name": {"like": "bo%", "ignore_case": True}
    },
    '{"age": {"roughly": 18}}': {"age": {"roughly": 18}},
}

# The words this grammar has no phrasing for, each with something a caller
# would plausibly write under it. Refused by name rather than passed over.
NOT_IN_THIS_GRAMMAR = {
    "order_by": {"name": "ascending"},
    "group_by": ["status"],
    "having": {"count": {"gt": 5}},
    "page": {"size": 10, "number": 1},
}

# The two parts a caller writes that come back unchanged: the word they are
# written under, the declaration field they fill, and several things to write.
WHAT_MOVES = {
    "model": {
        "word": "model",
        "field": "model",
        "written": ["Person", "Order", "people"],
    },
    "columns": {
        "word": "with_only",
        "field": "columns",
        "written": [["id"], ["id", "name"], ["name", "id"], []],
    },
}


def _answer_for(asked: dict) -> dict:
    from declaro_persistum.declaration_from_sqlalchemy import declaration_from_sqlalchemy

    return declaration_from_sqlalchemy(asked)


def _person_query(**parts: object) -> dict:
    """`Person.query`, asking for every row, plus whatever else was phrased."""
    return {"model": "Person", "wanted": "all", **parts}


def _eq(value: object) -> dict:
    return {"operator": "eq", "value": value, "ignore_case": False}


def _only(each: dict) -> dict:
    """A condition that narrows by these comparisons and nothing else."""
    return {"each": each, "any_of": [], "none_of": []}


# --- what a caller wrote ----------------------------------------------------


@when('a caller writes Person.query.filter_by(name="bob").with_only("id", "name")')
def _the_whole_query(scenario: dict) -> None:
    scenario["asked"] = _person_query(filter_by={"name": "bob"}, with_only=["id", "name"])


@when('a caller writes Person.query.filter_by(name="bob")')
def _a_query_naming_no_columns(scenario: dict) -> None:
    scenario["asked"] = _person_query(filter_by={"name": "bob"})


@when(parsers.re(r"a caller narrows by (?P<narrowing>\{.*\})"))
def _a_caller_narrows_by(scenario: dict, narrowing: str) -> None:
    scenario["asked"] = _person_query(filter_by=NARROWINGS[narrowing])


@when(parsers.parse("a caller writes the word {word}"))
def _a_word_this_grammar_does_not_have(scenario: dict, word: str) -> None:
    scenario["word"] = word
    scenario["asked"] = _person_query(**{word: NOT_IN_THIS_GRAMMAR[word]})


@when(parsers.parse('a caller asks for "{word}"'))
def _a_caller_asks_for(scenario: dict, word: str) -> None:
    scenario["word"] = word
    scenario["asked"] = _person_query(wanted=word)


@when(parsers.parse("a caller writes a question naming a {part}"))
def _a_question_naming_a_part(scenario: dict, part: str) -> None:
    scenario["part"] = part


@when(parsers.parse("a caller writes a question naming no {part}"))
def _a_question_naming_no_part(scenario: dict, part: str) -> None:
    scenario["part"] = part
    scenario["asked"] = {word: value for word, value in _person_query().items() if word != part}


# --- what came back ---------------------------------------------------------


@then("the declaration names the model, those two columns and that condition")
def _it_names_them(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["model"] == "Person"
    assert declaration["columns"] == ["id", "name"]
    assert declaration["condition"] == _only({"name": _eq("bob")})


@then("the condition holds an eq comparison, not a bare value")
def _a_bare_value_became_eq(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["condition"] == _only({"name": _eq("bob")}), (
        "the builders expand a bare value, so by the time a declaration exists "
        "every comparison names its operator"
    )


@then("the condition compares age with gt")
def _a_named_operator_kept_its_name(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["condition"] == _only(
        {"age": {"operator": "gt", "value": 18, "ignore_case": False}}
    )


@then("the condition holds both comparisons and nothing else")
def _two_keys_meant_both(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["condition"] == _only(
        {
            "age": {"operator": "gt", "value": 18, "ignore_case": False},
            "name": _eq("bob"),
        }
    ), "and is what two keys already mean, so neither needs a word of its own"


@then("the declaration holds those two as alternatives")
def _or_became_alternatives(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["condition"]["any_of"] == [
        _only({"name": _eq("alice")}),
        _only({"name": _eq("carol")}),
    ]
    assert declaration["condition"]["each"] == {}


@then("the declaration holds that one as an exclusion")
def _not_became_an_exclusion(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["condition"]["none_of"] == [_only({"name": _eq("bob")})]
    assert declaration["condition"]["each"] == {}


@then(parsers.parse("the word {word} is gone from it"))
def _the_caller_s_word_is_gone(scenario: dict, word: str) -> None:
    declaration = _answer_for(scenario["asked"])

    assert word not in declaration["condition"]["each"], (
        f"{word!r} is a word the caller writes; a declaration holds any_of and "
        f"none_of so that nothing has to be read again later"
    )
    assert set(declaration["condition"]) == {"each", "any_of", "none_of"}


@then("the comparison says case does not matter")
def _case_does_not_matter(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["condition"]["each"] == {
        "name": {"operator": "like", "value": "bo%", "ignore_case": True}
    }, "every text operator takes the companion, and it is carried, not guessed"


@then("the declaration names no columns")
def _it_names_no_columns(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["columns"] == [], (
        "a caller who named no columns asked for none, not for the last ones somebody named"
    )


@then("all ten fields are present")
def _all_ten_fields_are_present(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert set(declaration) == set(inspect.get_annotations(Declaration)), (
        f"a caller reads all ten; this one carries {sorted(declaration)}"
    )


@then("the six this grammar has no phrasing for are empty")
def _the_six_are_empty(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert declaration["order"] == {}
    assert declaration["grouping"] == []
    assert declaration["having"] == _only({})
    assert declaration["paging"] is None
    assert declaration["rows"] is None
    assert declaration["across"] is None


@then("nothing in the declaration names sqlalchemy")
def _it_does_not_name_the_grammar(scenario: dict) -> None:
    declaration = _answer_for(scenario["asked"])

    assert "sqlalchemy" not in repr(declaration), (
        "two grammars build equal questions, which a recorded name would break"
    )


@then(parsers.parse("the declaration carries that {part}, and moves when it moves"))
def _what_was_written_comes_back(scenario: dict, part: str) -> None:
    row = WHAT_MOVES[part]

    carried = [
        _answer_for(_person_query(**{row["word"]: written}))[row["field"]]
        for written in row["written"]
    ]
    assert carried == row["written"], (
        f"the {part} did not survive the trip: {carried} came back for {row['written']}"
    )


@then(parsers.parse("it is refused, naming {word} and the words this grammar does have"))
def _an_unknown_word_is_refused(scenario: dict, word: str) -> None:
    refusal = _answer_for(scenario["asked"])

    assert refusal["kind"] == "refused"
    assert word in refusal["what_went_wrong"], (
        f"{word!r} was dropped rather than refused, which answers a question "
        f"nobody asked: {refusal['what_went_wrong']}"
    )
    for has in ["model", "wanted", "filter_by", "with_only"]:
        assert has in refusal["what_went_wrong"], (
            "a refusal that does not list the words this grammar does have "
            "leaves the caller guessing which one they meant"
        )


@then(parsers.parse("it is refused, naming the {part} it needs"))
def _a_missing_part_is_refused(scenario: dict, part: str) -> None:
    refusal = _answer_for(scenario["asked"])

    assert refusal["kind"] == "refused", (
        f"a question naming no {part} was answered anyway, so a caller who "
        f"forgot cannot be told from one who chose"
    )
    assert part in refusal["what_went_wrong"]


@then("it is refused, naming that word")
def _an_undeclared_asking_word_is_refused(scenario: dict) -> None:
    refusal = _answer_for(scenario["asked"])

    assert refusal["kind"] == "refused"
    assert scenario["word"] in refusal["what_went_wrong"]


@then("it is refused, naming that operator")
def _an_undeclared_operator_is_refused(scenario: dict) -> None:
    refusal = _answer_for(scenario["asked"])

    assert refusal["kind"] == "refused"
    assert "roughly" in refusal["what_went_wrong"]
    assert "gt" in refusal["what_went_wrong"], "it lists the operators it does declare"


@then("it is refused, saying the phrasing carries no rows")
def _a_writing_word_is_refused(scenario: dict) -> None:
    refusal = _answer_for(scenario["asked"])

    assert refusal["kind"] == "refused"
    assert scenario["word"] in refusal["what_went_wrong"]
    assert "no rows" in refusal["what_went_wrong"], (
        "a write needs rows and this phrasing has nowhere to put them, so the "
        "gap is named rather than answered with an empty write"
    )


@then("a fault is returned, and nothing was raised")
def _a_fault_came_back_in_the_return(scenario: dict) -> None:
    returned = _answer_for(scenario["asked"])

    assert set(returned) == {"kind", "what_went_wrong", "what_to_do_instead"}
    assert returned["what_to_do_instead"].strip() != "", (
        "a fault that only says what went wrong leaves the caller nowhere to go"
    )


@then("reading it twice reads the same thing")
def _reading_it_twice_reads_the_same(scenario: dict) -> None:
    assert _answer_for(scenario["asked"]) == _answer_for(scenario["asked"])


@then("no clock, file or connection was reached")
def _nothing_outside_the_question_was_reached(scenario: dict) -> None:
    from declaro_persistum import declaration_from_sqlalchemy as the_module

    tree = ast.parse(pathlib.Path(the_module.__file__).read_text(encoding="utf-8"))
    imported = {
        (node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    } | {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    reached = sorted(imported & set(NOTHING_PURE_IMPORTS))
    assert reached == [], f"a pure function reaches for none of these, and this has {reached}"
