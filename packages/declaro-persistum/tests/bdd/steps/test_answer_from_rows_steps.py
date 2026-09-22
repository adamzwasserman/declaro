"""Bind `tests/bdd/features/core/answer_from_rows.feature`.

WHAT THE FUNCTION ANSWERS. Driver rows go in and the caller's answer comes out.
The asking word decides the shape: the four row words give instances of the
model, `one` gives one instance or a named absence, `exists` gives true or
false, and the six number words give the number itself rather than a row
carrying it.

WHAT AN INSTANCE IS HERE, since there is no Person class to check against. A
`Model` is a TypedDict naming a table and its columns, and an answer is a
`Row`, which is a dict. So an instance of Person is a row carrying every column
Person declares, and a mapping is a row carrying only the columns the caller
named. Those two sets are what the Thens below compare, and they are the only
mechanical difference between the two, so a partial instance is caught by being
neither.

EVERY SCENARIO SETS UP THE SAME THREE THINGS, SO THERE IS ONE `When`. A model, a
declaration and the rows a driver handed back: that is all any of these eleven
scenarios establishes, and eleven step functions differing only in the words
they pasted in would be eleven places for one setup to drift. The words are the
keys of `ASKED` and the setup is the row beside each one.

WHERE THE FEATURE AND THE DECLARATION DISAGREE, written down beside the
assertion each time:

`types.NOTHING` IS `None`. The scenario says NOTHING "is not None". At the
language level it is: `NOTHING = None` in `declaro_persistum/types.py`, with a
comment saying Python has one empty value and the name is what tells a reader
which meaning is meant. So the Then asserts what can be true -- the answer is
the declaration's named absence, and it is not an empty list -- and says so.

AN ANSWER ACROSS TWO MODELS DOES NOT FIT `Row`. `Row` is `dict[str, Value]` and
`Value` holds no dict, so nesting a whole Order beside a whole Person is a
shape the declared types do not describe. The scenarios ask for it anyway, and
the comment on that Then names the gap.

THE FAR MODEL'S COLUMNS ARE NOT IN THE ARGUMENTS. The function is handed one
`Model` and a list of `Join`, and a `Join` carries the far model's NAME, not its
columns. So the function can check that the near model came back whole and
cannot check the far one. The test knows both models and checks both, and the
Then says which half the function owns.
"""

import re

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import types as declared_types

scenarios("../features/core/answer_from_rows.feature")


NOTHING_ASKED: dict = {"each": {}, "any_of": [], "none_of": []}

# A column that points at no other table. Written down rather than left off the
# call, so a call site that declares a reference and one that declares none say
# so in the same place and can be told apart by reading.
NOT_A_REFERENCE = None


def _a_column(name: str, references: str | None) -> dict:
    """One column, with every field a `Column` names present.

    `references` is written `table.column` here because that is how a person
    reads a foreign key, and it becomes the `Reference` record the declaration
    describes before it reaches the function. `refuse` is the deletion rule a
    declaration means when it says nothing: a parent with children stays.
    """
    table, _, column = (references or "").partition(".")
    return {
        "name": name,
        "declared_type": "text",
        "renamed_from": None,
        "references": (
            {"table": table, "column": column, "on_delete": "refuse"}
            if references is not None
            else None
        ),
        "facets": {},
    }


def _a_model(table: str, columns: list[tuple[str, str | None]]) -> dict:
    """One model, with every field a `Model` names present."""
    return {
        "table": table,
        "columns": [_a_column(name, references) for name, references in columns],
        "dropping": [],
        "indexes": [],
        "constraints": [],
    }


def _a_declaration(model: str, wanted: str, **stated: object) -> dict:
    """One declaration, with every field a `Declaration` names present."""
    return {
        "model": model,
        "wanted": wanted,
        "columns": [],
        "condition": NOTHING_ASKED,
        "order": {},
        "grouping": [],
        "having": NOTHING_ASKED,
        "paging": None,
        "rows": None,
        "across": None,
    } | stated


# The two models every scenario draws on. Person declares three columns, so an
# instance of Person carries three. Order declares three of its own, one of them
# a reference to Person, and both models declare a column named `id`.
DECLARED_COLUMNS: dict[str, list[tuple[str, str | None]]] = {
    "people": [
        ("id", NOT_A_REFERENCE),
        ("name", NOT_A_REFERENCE),
        ("status", NOT_A_REFERENCE),
    ],
    "orders": [
        ("id", NOT_A_REFERENCE),
        ("total", NOT_A_REFERENCE),
        ("person_id", "people.id"),
    ],
}

ACROSS_TO_PERSON = [
    {"model": "people", "by_column": "person_id", "to_column": "id", "condition": NOTHING_ASKED}
]

# WHAT EACH SENTENCE SETS UP: the model the caller asked of, the declaration
# their words became, and the rows the driver handed back. The four sentences
# that appear twice are the same setup asked in two tenses.
ASKED: dict[str, dict] = {
    "a caller asked for every person and two rows came back": {
        "model": "people",
        "declaration": _a_declaration("people", "all"),
        "rows": [
            {"id": 1, "name": "ada", "status": "active"},
            {"id": 2, "name": "bob", "status": "paused"},
        ],
    },
    "a caller asks for the rows of Person": {
        "model": "people",
        "declaration": _a_declaration("people", "all"),
        "rows": [
            {"id": 1, "name": "ada", "status": "active"},
            {"id": 2, "name": "bob", "status": "paused"},
        ],
    },
    # The engine answers a count with one row carrying one value. What comes
    # back to the caller has to be the value.
    "a caller asked how many people there are": {
        "model": "people",
        "declaration": _a_declaration("people", "count"),
        "rows": [{"count": 2}],
    },
    "a caller asks how many people there are": {
        "model": "people",
        "declaration": _a_declaration("people", "count"),
        "rows": [{"count": 2}],
    },
    "a caller asked whether any person exists": {
        "model": "people",
        "declaration": _a_declaration("people", "exists"),
        "rows": [{"1": 1}],
    },
    "a caller asked for exactly one person and none matched": {
        "model": "people",
        "declaration": _a_declaration("people", "one"),
        "rows": [],
    },
    # Two columns were asked for, so two columns come back. The third column
    # Person declares is not here and nothing may invent it.
    "a caller asks for the id and name of every Person": {
        "model": "people",
        "declaration": _a_declaration("people", "all", columns=["id", "name"]),
        "rows": [{"id": 1, "name": "ada"}, {"id": 2, "name": "bob"}],
    },
    "a caller asks for only some columns": {
        "model": "people",
        "declaration": _a_declaration("people", "all", columns=["id", "name"]),
        "rows": [{"id": 1, "name": "ada"}, {"id": 2, "name": "bob"}],
    },
    # A row that crosses two models carries each column under the table it came
    # from. Nothing else can carry two columns both named `id`.
    "a caller asks for orders with the person who placed them": {
        "model": "orders",
        "declaration": _a_declaration("orders", "all", across=ACROSS_TO_PERSON),
        "rows": [
            {
                "orders.id": 10,
                "orders.total": 99,
                "orders.person_id": 2,
                "people.id": 2,
                "people.name": "bob",
                "people.status": "active",
            }
        ],
    },
    "a caller asks across both": {
        "model": "orders",
        "declaration": _a_declaration("orders", "all", across=ACROSS_TO_PERSON),
        "rows": [
            {
                "orders.id": 10,
                "orders.total": 99,
                "orders.person_id": 2,
                "people.id": 2,
                "people.name": "bob",
                "people.status": "active",
            }
        ],
    },
    "a caller asks for a count grouped by status": {
        "model": "people",
        "declaration": _a_declaration("people", "count", grouping=["status"]),
        "rows": [{"status": "active", "count": 2}, {"status": "paused", "count": 1}],
    },
}

_ANY_OF_THEM = "|".join(re.escape(sentence) for sentence in ASKED)


@pytest.fixture
def models() -> dict[str, dict]:
    """A fresh Model per declared table, so no scenario can change another's."""
    return {table: _a_model(table, columns) for table, columns in DECLARED_COLUMNS.items()}


def _answer(scenario: dict) -> object:
    """Ask the function what the scenario set up."""
    from declaro_persistum.answer_from_rows import answer_from_rows

    return answer_from_rows(scenario["model"], scenario["rows"], scenario["declaration"])


def _names_of(model: dict) -> set[str]:
    """Every column this model declares."""
    return {column["name"] for column in model["columns"]}


def _is_an_instance_of(model: dict, answer: object) -> bool:
    """Whether this answer carries every column the model declares, and nothing else."""
    return isinstance(answer, dict) and set(answer) == _names_of(model)


# --- what each scenario sets up ---------------------------------------------


@given(parsers.re(r"Order (?:references Person|and Person both declare a column `id`)"))
def _order_and_person(scenario: dict, models: dict) -> None:
    # Both spellings state a fact that is already true of the two models above:
    # Order.person_id references people.id, and both declare `id`. So the Given
    # records which model sits on the far side of the join, which is the half
    # the function is never handed and the Thens check for it.
    scenario["far_model"] = models["people"]


@when(parsers.re(rf"(?P<asking>{_ANY_OF_THEM})"))
def _the_caller_asked(scenario: dict, models: dict, asking: str) -> None:
    setup = ASKED[asking]
    scenario["model"] = models[setup["model"]]
    scenario["declaration"] = setup["declaration"]
    scenario["rows"] = setup["rows"]


# --- what each scenario says has to be true ---------------------------------


@then("two Person instances come back")
@then("each answer is an instance of Person")
def _instances_of_person(scenario: dict, models: dict) -> None:
    person = models["people"]
    answer = _answer(scenario)

    assert isinstance(answer, list), f"rows were asked for, so rows come back: {answer!r}"
    assert len(answer) == len(scenario["rows"]), (
        f"two rows came back from the driver and the caller got {len(answer)}"
    )
    not_instances = [one for one in answer if not _is_an_instance_of(person, one)]
    assert not_instances == [], (
        f"an instance of Person carries every column Person declares, "
        f"{sorted(_names_of(person))}, and these do not: {not_instances}"
    )
    assert answer == scenario["rows"], "the values arrive unchanged"


@then("an integer comes back, not a row carrying one")
@then("the answer is an integer and not a row carrying one")
def _an_integer_not_a_row(scenario: dict) -> None:
    answer = _answer(scenario)

    assert answer == 2
    assert isinstance(answer, int), f"a count is a number, not a {type(answer).__name__}"
    assert not isinstance(answer, list | dict), f"the row was handed straight through: {answer!r}"

    # EVERY NUMBER WORD ANSWERS THE SAME WAY. Six words in the asking
    # vocabulary answer with a number, and a word that gave back the row
    # instead would send the caller unpacking a dict on one word in six.
    number_words = ["count", "sum", "average", "smallest", "largest", "count_distinct"]
    handed_back = {
        word: _answer(scenario | {"declaration": scenario["declaration"] | {"wanted": word}})
        for word in number_words
    }
    wrong = {word: seen for word, seen in handed_back.items() if seen != 2}
    assert wrong == {}, f"these number words did not answer with the number: {wrong}"


@then("true or false comes back, and no rows were read")
def _true_or_false(scenario: dict) -> None:
    answer = _answer(scenario)

    assert answer is True, f"a row came back, so a person exists: {answer!r}"
    assert _answer(scenario | {"rows": []}) is False, "no row came back, so none exists"

    # NO ROWS WERE READ. The answer is whether the driver handed one back, not
    # what was in it. So the contents are moved and the answer has to stay put.
    # This is also why the compiled statement has to be the `SELECT 1 ... LIMIT
    # 1` form: `SELECT EXISTS(...)` always hands back a row, and this function
    # is told nothing about which spelling was used.
    contents = [{"1": 1}, {"1": 0}, {"anything": "at all"}, {}, {"id": 7, "name": "ada"}]
    moved = [row for row in contents if _answer(scenario | {"rows": [row]}) is not True]
    assert moved == [], f"the answer moved with what the row held: {moved}"


@then("NOTHING comes back, which is not None and not an empty list")
def _a_named_absence(scenario: dict, models: dict) -> None:
    answer = _answer(scenario)

    assert answer is declared_types.NOTHING, "absence is the declaration's named absence"
    assert answer != [], "an empty list is a list of no people, not the person who is missing"
    assert not isinstance(answer, list), f"absence is not a container: {answer!r}"

    # WHERE THIS SCENARIO AND THE DECLARATION DISAGREE. The scenario says
    # NOTHING is "not None", and `types.py` says `NOTHING = None`, because
    # Python has one empty value. So the two are the same object and the name
    # is the whole of the difference: what a caller reads is `NOTHING`, and a
    # nullable column that holds nothing is read as a column's value. The
    # assertion above is written against the name for that reason.
    assert declared_types.NOTHING is None

    # A row that matched is not absence, so the two are told apart by the
    # answer and not by the caller guessing.
    matched = _answer(scenario | {"rows": [{"id": 1, "name": "ada", "status": "active"}]})
    assert _is_an_instance_of(models["people"], matched), (
        f"one row matched, so one instance: {matched!r}"
    )


@then("each answer is a mapping carrying only those keys")
def _a_mapping_of_only_those_keys(scenario: dict) -> None:
    answer = _answer(scenario)

    assert isinstance(answer, list), f"rows were asked for, so rows come back: {answer!r}"
    asked_for = set(scenario["declaration"]["columns"])
    wrong = [one for one in answer if set(one) != asked_for]
    assert wrong == [], f"the caller named {sorted(asked_for)} and got {wrong}"


@then("none of them is an instance of Person")
@then("nothing comes back claiming to be a Person with columns missing")
def _none_of_them_is_a_person(scenario: dict, models: dict) -> None:
    person = models["people"]
    answer = _answer(scenario)

    assert isinstance(answer, list), f"rows were asked for, so rows come back: {answer!r}"
    claiming = [one for one in answer if _is_an_instance_of(person, one)]
    assert claiming == [], (
        f"Person declares {sorted(_names_of(person))} and the caller asked for "
        f"{scenario['declaration']['columns']}, so nothing here is a Person: {claiming}"
    )
    short = [one for one in answer if set(one) < set(scenario["declaration"]["columns"])]
    assert short == [], f"a mapping carries what was asked for and no less: {short}"

    # THE OTHER HALF OF THE SAME RULE. Asking for rows and getting back a row
    # missing a declared column is the partial instance the scenario forbids,
    # and it comes back as a Fault rather than as a dict with a hole in it.
    whole = _a_declaration("people", "all")
    refused = _answer({"model": person, "rows": [{"id": 1, "name": "ada"}], "declaration": whole})
    assert isinstance(refused, dict) and refused.get("kind") == "refused", (
        f"a row missing `status` cannot be an instance of Person: {refused!r}"
    )
    assert "status" in refused["what_went_wrong"], refused
    assert refused["what_to_do_instead"].strip() != "", "a refusal says what to do differently"


@then("each answer carries an Order and a Person, each whole")
def _an_order_and_a_person_each_whole(scenario: dict, models: dict) -> None:
    answer = _answer(scenario)

    assert isinstance(answer, list), f"rows were asked for, so rows come back: {answer!r}"
    (crossed,) = answer
    assert set(crossed) == {"orders", "people"}, (
        f"one row across two models is one answer carrying both: {crossed!r}"
    )
    assert _is_an_instance_of(models["orders"], crossed["orders"]), (
        f"the Order is whole: {crossed!r}"
    )
    assert _is_an_instance_of(scenario["far_model"], crossed["people"]), (
        f"the Person is whole: {crossed!r}"
    )
    assert crossed["orders"] == {"id": 10, "total": 99, "person_id": 2}
    assert crossed["people"] == {"id": 2, "name": "bob", "status": "active"}

    # WHAT THIS FUNCTION CAN CHECK AND WHAT IT CANNOT. It is handed ONE Model
    # and a list of Join, and a Join carries the far model's name rather than
    # its columns. So the function checks that the near model came back whole
    # and takes the far model's columns as they arrived. The line above checks
    # the Person too, because this test holds both models and the function
    # never does.

    # THE SHAPE THE DECLARED TYPES DO NOT DESCRIBE. `Row` is
    # `dict[str, Value]`, and `Value` is a string, a number, a bool or None. A
    # whole Order nested beside a whole Person is a dict inside a Row, so the
    # answer the scenario asks for does not fit the type the declaration gives
    # `Answer`. The scenario is the specification, so the nesting is what the
    # function builds, and this is the gap.
    assert isinstance(crossed["orders"], dict)


@then("each `id` is reachable through the model it belongs to")
def _each_id_is_reachable(scenario: dict) -> None:
    answer = _answer(scenario)

    (crossed,) = answer
    assert crossed["orders"]["id"] == 10, "the order's own id"
    assert crossed["people"]["id"] == 2, "the person's own id"

    # NOT AMBIGUOUS MEANS NEITHER ONE WAS LOST. A flat answer holds one key
    # named `id` and the second writes over the first, silently. Both are here
    # and they are different numbers, which is how the loss would show.
    assert crossed["orders"]["id"] != crossed["people"]["id"]
    assert "id" not in crossed, f"there is no unowned `id` to reach for: {crossed!r}"


@then("each answer carries the group's columns and its measures")
def _the_groups_columns_and_measures(scenario: dict) -> None:
    answer = _answer(scenario)

    assert isinstance(answer, list), f"a count per group is a row per group: {answer!r}"
    assert answer == [{"status": "active", "count": 2}, {"status": "paused", "count": 1}]

    grouped_by = set(scenario["declaration"]["grouping"])
    without = [one for one in answer if not grouped_by <= set(one)]
    assert without == [], f"a group has to say which group it is: {without}"
    measureless = [one for one in answer if not set(one) - grouped_by]
    assert measureless == [], f"a group with no measure answers nothing: {measureless}"

    # THE WORD IS `count` AND THE ANSWER IS STILL ROWS. Without a grouping,
    # `count` gives back the number. The grouping is what turns it into a row
    # per group, so a table keyed on the word alone would hand back a number
    # here and lose every group but one.
    assert scenario["declaration"]["wanted"] == "count"
    ungrouped = _answer(
        scenario
        | {
            "declaration": scenario["declaration"] | {"grouping": []},
            "rows": [{"count": 3}],
        }
    )
    assert ungrouped == 3


@then("it is not an instance of the model that was grouped")
def _not_an_instance_of_the_grouped_model(scenario: dict, models: dict) -> None:
    person = models["people"]
    answer = _answer(scenario)

    claiming = [one for one in answer if _is_an_instance_of(person, one)]
    assert claiming == [], (
        f"Person declares {sorted(_names_of(person))}; a group carries the "
        f"columns it grouped by and the numbers over them: {claiming}"
    )
    carries_a_measure = [one for one in answer if "count" in one]
    assert len(carries_a_measure) == len(answer), (
        "every group carries its measure, which is what makes it a group rather "
        f"than a row of the model: {answer}"
    )
