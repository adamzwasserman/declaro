"""Steps for tests/bdd/features/asking/. One feature per function, all red.

WHY THESE ARE RED AND MUST STAY RED. Every function in `declaro_persistum
.asking` raises `NotImplementedError` by name. These steps call the real
function, so a scenario fails at its `When` with a message naming what is
missing. A step that caught that and asserted something else would turn the
suite green over unbuilt behaviour, which is the failure the feature files
exist to prevent.

EVERY STEP IS SYNCHRONOUS AND DRIVES A LOOP ITSELF. pytest-bdd does not await
an `async def` step: the body never runs, the coroutine is discarded, and the
assertions read an untouched context. This file was written with async steps
first and all 80 scenarios failed on that rather than on the missing code.
`tests/unit/test_no_bdd_step_is_left_unawaited.py` is the gate that names it.

WHAT THE `Then` STEPS ARE FOR, given nothing reaches them yet. They are the
assertion the function will have to satisfy, written before it exists. The
expected value is decided while the answer is still unknown, rather than read
off whatever the code turns out to do.

THE DATABASE IS REAL. Each model's table is built from its own declaration,
through the differ and the applier. The seeding writes SQL because building a
fixture is not the surface being specified; nothing under test does.
"""

from __future__ import annotations

import asyncio
import pathlib
import tempfile
from typing import Any, TypedDict

import pytest
from pydantic import create_model
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import asking, field, open_sqlite, table
from declaro_persistum.applier import apply
from declaro_persistum.database import writing
from declaro_persistum.differ.core import diff
from declaro_persistum.pydantic_loader import pydantic_model_to_table
from declaro_persistum.types import NOTHING, Direction, Nothing, is_nothing, resolved

scenarios("../features/asking")


class Ctx(TypedDict, total=False):
    db: Any
    model: type
    other: type
    answer: Any
    refusal: Exception | None
    loop: Any


@pytest.fixture(scope="function")
def loop() -> Any:
    made = asyncio.new_event_loop()
    asyncio.set_event_loop(made)
    yield made
    made.close()


@pytest.fixture
def ctx(loop: Any) -> Ctx:
    return {"db": None, "model": None, "other": None, "answer": None, "refusal": None, "loop": loop}


def _drive(ctx: Ctx, work: Any) -> Any:
    """Run one coroutine inside the scenario's own loop."""
    return ctx["loop"].run_until_complete(work)


# --------------------------------------------------------------- the database


async def _opened() -> Any:
    return await open_sqlite(
        str(pathlib.Path(tempfile.mkdtemp()) / "app.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def _built(db: Any, models: tuple[type, ...]) -> None:
    """Create each table FROM ITS DECLARATION, never from a string."""
    target: dict[str, Any] = {}
    for model in models:
        made = pydantic_model_to_table(model)
        assert made is not None, f"{model.__name__} is not a declared table"
        target[made[0]] = made[1]
    result = diff({}, target, dialect="sqlite", decisions=NOTHING)
    async with writing(db) as conn:
        await apply(conn, result["operations"], result["execution_order"], "sqlite", dry_run=False)


async def _seeded(db: Any, sql: str, rows: list[tuple[Any, ...]]) -> None:
    async with writing(db) as conn:
        for row in rows:
            await conn.execute(sql, row)


def _start(ctx: Ctx, *models: type) -> Ctx:
    ctx["db"] = _drive(ctx, _opened())
    ctx["model"] = models[-1]
    _drive(ctx, _built(ctx["db"], models))
    return ctx


def _seed(ctx: Ctx, sql: str, rows: list[tuple[Any, ...]]) -> None:
    _drive(ctx, _seeded(ctx["db"], sql, rows))


# ------------------------------------------------------------------- Given


# FOUR DECLARATIONS, ONE SHAPE: a table name and its columns. Only the words
# differed, so the words are rows and one function reads them.
_DECLARED: dict[str, tuple[str, str, dict[str, type]]] = {
    "a model Person declaring people with columns id, name and age": (
        "Person",
        "people",
        {"name": str, "age": int},
    ),
    "a model Person declaring people with columns id, name and city": (
        "Person",
        "people",
        {"name": str, "city": str},
    ),
    "a model Order declaring orders with columns id, person_id and total": (
        "Order",
        "orders",
        {"person_id": int, "total": int},
    ),
    "a model Order declaring orders with columns id, status and total": (
        "Order",
        "orders",
        {"status": str, "total": int},
    ),
}


def _a_model(
    named: str,
    table_name: str,
    columns: dict[str, type],
    references: dict[str, str] | Nothing,
) -> type:
    """One declared model, built from the row that names it.

    `id` is on every one of them and is always the key, so it is not a column
    the rows have to repeat. `references` names the columns that point at
    another table, which only the related declaration needs.
    """
    pointing = resolved(references, {})
    made = create_model(
        named,
        id=(int, field(primary_key=True)),
        **{
            name: (kind, field(references=pointing[name]) if name in pointing else ...)
            for name, kind in columns.items()
        },
    )
    return table(table_name)(made)


@given(
    parsers.re(
        r"(?P<said>a model (?:Person declaring people with|Order declaring orders with)[^\n]+)"
    ),
    target_fixture="ctx",
)
def _given_a_model(ctx: Ctx, said: str) -> Ctx:
    return _start(ctx, _a_model(*_DECLARED[said], references=NOTHING))


@given(
    "a model Person declaring people, and a model Order referencing Person", target_fixture="ctx"
)
def _given_related(ctx: Ctx) -> Ctx:
    """The one declaration the table above cannot carry: two models, and one
    referencing the other. It goes through the same builder so no `class`
    statement is written here either."""
    person = _a_model("Person", "people", {"name": str}, references=NOTHING)
    order = _a_model(
        "Order", "orders", {"person_id": int, "total": int}, references={"person_id": "people.id"}
    )
    made = _start(ctx, person, order)
    made["other"] = person
    return made


_SEEDS: dict[str, tuple[str, list[tuple[Any, ...]]]] = {
    "a database holding alice aged 30, bob aged 25 and carol aged 17": (
        'INSERT INTO "people" (id, name, age) VALUES (?, ?, ?)',
        [(1, "alice", 30), (2, "bob", 25), (3, "carol", 17)],
    ),
    "a database holding two people in Paris and one in Berlin": (
        'INSERT INTO "people" (id, name, city) VALUES (?, ?, ?)',
        [(1, "alice", "Paris"), (2, "bob", "Paris"), (3, "carol", "Berlin")],
    ),
    "a database holding three orders for alice totalling 10, 20 and 30": (
        'INSERT INTO "orders" (id, person_id, total) VALUES (?, ?, ?)',
        [(1, 1, 10), (2, 1, 20), (3, 1, 30)],
    ),
    "a database holding two paid orders of 10 and 20, and one unpaid of 5": (
        'INSERT INTO "orders" (id, status, total) VALUES (?, ?, ?)',
        [(1, "paid", 10), (2, "paid", 20), (3, "unpaid", 5)],
    ),
}


@given(
    parsers.re(
        r"(?P<said>a database holding (?:alice aged|two people|three orders|two paid)[^\n]+)"
    )
)
def _seed_one(ctx: Ctx, said: str) -> None:
    """FOUR SEEDS, ONE SHAPE: a statement and its rows, and only words differed."""
    _seed(ctx, *_SEEDS[said])


@given("a database holding alice with two orders and bob with one")
def _seed_related(ctx: Ctx) -> None:
    _seed(ctx, 'INSERT INTO "people" (id, name) VALUES (?, ?)', [(1, "alice"), (2, "bob")])
    _seed(
        ctx,
        'INSERT INTO "orders" (id, person_id, total) VALUES (?, ?, ?)',
        [(1, 1, 10), (2, 1, 20), (3, 2, 30)],
    )


# ------------------------------------------------ reading the step's own words


_PHRASES: dict[str, dict[str, Any]] = {
    'named "bob"': {"name": "bob"},
    'named "nobody"': {"name": "nobody"},
    'named "alice"': {"name": "alice"},
    "aged over 18": {"age": {"gt": 18}},
    "are over 18": {"age": {"gt": 18}},
    "over 100": {"age": {"gt": 100}},
    "over 1000": {"total": {"gt": 1000}},
    "over 15": {"total": {"gt": 15}},
    "under 25": {"total": {"lt": 25}},
    "under 18": {"age": {"lt": 18}},
    "over 18": {"age": {"gt": 18}},
}

# AN ORDER IS A MAPPING, column to direction, and the keys break ties left to
# right. MOST SPECIFIC PHRASE FIRST, because `_order` returns on the first
# substring that matches and "by name" is inside "by name descending".
_ORDERS: dict[str, dict[str, Direction]] = {
    "by name descending": {"name": "descending"},
    "by age then name": {"age": "ascending", "name": "ascending"},
    "by name": {"name": "ascending"},
    '"-name"': {"-name": "ascending"},
}


def _narrowing(text: str) -> dict[str, Any]:
    for phrase, condition in _PHRASES.items():
        if phrase in text:
            return condition
    return {}


def _order(text: str) -> dict[str, Direction]:
    for phrase, order in _ORDERS.items():
        if phrase in text:
            return order
    return {}


def _person(ctx: Ctx, number: int, name: str, age: int) -> Any:
    return ctx["model"](id=number, name=name, age=age)


# ----------------------------------------------------------------- the call


def _call(ctx: Ctx, asked: str, *args: Any, **kwargs: Any) -> None:
    """Call the function. A refusal is an outcome; UNBUILT IS NOT.

    `NotImplementedError` is deliberately NOT caught. Catching it filed
    "this does not exist" under "the library refused", so every scenario
    asserting a refusal passed while the function was a stub. Six did.
    Letting it through is what keeps the red honest.
    """
    try:
        ctx["answer"] = _drive(ctx, getattr(asking, asked)(*args, **kwargs))
        ctx["refusal"] = None
    except NotImplementedError:
        raise
    except Exception as refused:  # noqa: BLE001 - a refusal is an outcome here
        ctx["answer"] = None
        ctx["refusal"] = refused


def _ask(ctx: Ctx, asked: str, *args: Any, **kwargs: Any) -> None:
    """A question takes the model.

    THE PARAMETER IS `asked`, NOT `name`. A narrowing is splatted in as
    keywords and `name` is a column, so calling this parameter `name` made
    `{"name": "bob"}` collide with it: twelve scenarios died on "got multiple
    values for argument 'name'" rather than on the missing function.
    """
    _call(ctx, asked, ctx["db"], ctx["model"], *args, **kwargs)


def _ask_write(ctx: Ctx, asked: str, *args: Any, **kwargs: Any) -> None:
    """A write takes an instance rather than a model."""
    _call(ctx, asked, ctx["db"], *args, **kwargs)


_OVER_ROWS: dict[str, str] = {
    "a caller asks for all people": "all",
    "a caller asks for the one person": "one",
    "a caller asks whether any person": "exists",
    "a caller asks how many people": "count",
}


@when(
    parsers.re(
        r"(?P<said>a caller asks (?:for all people|for the one person"
        r"|whether any person|how many people))(?P<rest>[^\n]*)"
    )
)
def _when_over_rows(ctx: Ctx, said: str, rest: str) -> None:
    """FOUR QUESTIONS OVER ROWS, one shape: a narrowing the words name."""
    _ask(ctx, _OVER_ROWS[said], **_narrowing(rest))


@when(parsers.re(r"a caller asks for the first person(?P<rest>.*)"))
def _when_first(ctx: Ctx, rest: str) -> None:
    _ask(ctx, "first", _order(rest), **_narrowing(rest))


@when(parsers.re(r"a caller asks for page (?P<number>\d+) of size (?P<size>\d+)(?P<rest>.*)"))
def _when_page(ctx: Ctx, number: str, size: str, rest: str) -> None:
    _ask(ctx, "page", int(size), int(number), **_narrowing(rest))


@when(parsers.re(r"a caller asks for people ordered by (?P<rest>.*)"))
def _when_ordered(ctx: Ctx, rest: str) -> None:
    _ask(ctx, "ordered_by", _order("by " + rest))


@when(parsers.re(r"a caller asks for the (?P<columns>.+) of every person"))
def _when_values(ctx: Ctx, columns: str) -> None:
    _ask(ctx, "values", ["id", "name"] if "id and name" in columns else ["nickname"])


@when(parsers.re(r"a caller asks for the distinct (?P<what>.*)"))
def _when_distinct(ctx: Ctx, what: str) -> None:
    named = {"cities": ["city"], "pairs of city and name": ["city", "name"]}.get(what, ["nickname"])
    _ask(ctx, "distinct", named)


@when(parsers.re(r"a caller asks for the total of (?P<rest>.*)"))
def _when_sum(ctx: Ctx, rest: str) -> None:
    _ask(ctx, "sum", "name" if "text column" in rest else "total", **_narrowing(rest))


_OVER_ONE_COLUMN: dict[str, tuple[str, str]] = {
    "a caller asks for the average": ("average", "total"),
    "a caller asks for the smallest order total": ("smallest", "total"),
    "a caller asks for the largest order total": ("largest", "total"),
    "a caller asks how many different cities": ("count_distinct", "city"),
}


@when(
    parsers.re(
        r"(?P<said>a caller asks (?:for the average|for the smallest order total"
        r"|for the largest order total|how many different cities))(?P<rest>[^\n]*)"
    )
)
def _when_over_one_column(ctx: Ctx, said: str, rest: str) -> None:
    """FOUR QUESTIONS ABOUT ONE COLUMN, one shape. The row names the column."""
    asked, column = _OVER_ONE_COLUMN[said]
    _ask(ctx, asked, column, **_narrowing(rest))


@when(
    parsers.re(
        r"a caller asks for (?:the|a) (?P<measure>count|total) (?:of orders )?grouped by (?P<cols>.*)"
    )
)
def _when_grouped(ctx: Ctx, measure: str, cols: str) -> None:
    _ask(ctx, "grouped_by", ["status", "day"] if "day" in cols else ["status"], {measure: "total"})


@when(parsers.re(r"a caller groups by \"(?P<column>[^\"]+)\""))
def _when_grouped_undeclared(ctx: Ctx, column: str) -> None:
    _ask(ctx, "grouped_by", [column], {"count": "id"})


@when(parsers.re(r"a caller asks for statuses whose (?P<rest>.*)"))
def _when_having(ctx: Ctx, rest: str) -> None:
    kept = {"total": {"gt": 25}} if "total" in rest else {"count": {"gt": 1}}
    _ask(ctx, "having", ["status"], {"count": "id"}, kept)


@when(parsers.re(r"a caller narrows (?P<rest>.*)"))
def _when_having_narrowed(ctx: Ctx, rest: str) -> None:
    _ask(ctx, "having", ["status"], {"count": "id"}, {"count": {"gt": 1}}, total={"gt": 5})


# FIVE WRITES, ONE SHAPE: call a function with instances the step names. The
# words were the only difference, so they are a table and the rows carry them.
_WRITES: dict[str, tuple[str, str]] = {
    "a caller declares that dave exists": ("add", "dave"),
    "a caller declares that dave and erin exist together": ("add", "dave and erin"),
    "a caller declares dave and a person whose key is already taken": ("add", "dave and clash"),
    "a caller declares a person whose key is already taken": ("add", "clash"),
    "a caller declares that bob no longer exists": ("remove", "bob"),
    "a caller removes a person the database no longer holds": ("remove", "ghost"),
    "a caller replaces a row that already existed": ("replace", "robert"),
}

_INSTANCES: dict[str, list[tuple[int, str, int]]] = {
    "dave": [(4, "dave", 40)],
    "dave and erin": [(4, "dave", 40), (5, "erin", 50)],
    "dave and clash": [(4, "dave", 40), (1, "clash", 1)],
    "clash": [(1, "clash", 1)],
    "bob": [(2, "bob", 25)],
    "ghost": [(99, "ghost", 1)],
    "robert": [(2, "robert", 26)],
}


def _declared(ctx: Ctx, named: str) -> Any:
    """The instance, or the list of them, a step's words name."""
    made = [_person(ctx, *row) for row in _INSTANCES[named]]
    return {True: made, False: made[0]}[len(made) > 1]


@when(parsers.re(r"(?P<said>a caller (?:declares|removes|replaces) [^\n]+)"))
def _when_a_write(ctx: Ctx, said: str) -> None:
    asked, named = _WRITES[said]
    _ask_write(ctx, asked, _declared(ctx, named))


@when(parsers.re(r"a caller declares that bob is now named \"(?P<name>[^\"]+)\""))
def _when_change(ctx: Ctx, name: str) -> None:
    _ask_write(ctx, "change", _person(ctx, 2, "bob", 25), name=name)


_CHANGES: dict[str, tuple[tuple[int, str, int], dict[str, Any]]] = {
    'a caller changes "nickname" on bob': ((2, "bob", 25), {"nickname": "x"}),
    "a caller changes a person the database no longer holds": ((99, "ghost", 1), {"name": "x"}),
}


@when(parsers.re(r"(?P<said>a caller changes (?:\"nickname\" on bob|a person the database[^\n]+))"))
def _when_a_change(ctx: Ctx, said: str) -> None:
    """TWO CHANGES, ONE SHAPE: a row and the new values, named by the words."""
    row, values = _CHANGES[said]
    _ask_write(ctx, "change", _person(ctx, *row), **values)


@when(parsers.re(r"a caller declares that everyone (?P<rest>.+) is now named \"(?P<name>[^\"]+)\""))
def _when_change_all(ctx: Ctx, rest: str, name: str) -> None:
    _ask(ctx, "change_all", _narrowing(rest), name=name)


@when("a caller changes every row with no narrowing")
def _when_change_all_bare(ctx: Ctx) -> None:
    _ask(ctx, "change_all", {}, name="adult")


@when(parsers.re(r"a caller declares that everyone (?P<rest>.+) no longer exists"))
def _when_remove_all(ctx: Ctx, rest: str) -> None:
    _ask(ctx, "remove_all", _narrowing(rest))


@when("a caller removes every row with no narrowing")
def _when_remove_all_bare(ctx: Ctx) -> None:
    _ask(ctx, "remove_all", {})


@when(parsers.re(r"a caller declares that a person with (?P<rest>.*)"))
def _when_replace(ctx: Ctx, rest: str) -> None:
    _ask_write(ctx, "replace", _person(ctx, 2 if "bob" in rest else 9, "robert", 26))


@when(parsers.re(r"a caller asks for the orders of (?P<rest>.*)"))
def _when_of(ctx: Ctx, rest: str) -> None:
    _ask(ctx, "of", ctx["other"](id=1, name="alice"), **_narrowing(rest))


# ------------------------------------------------------------------- answers


def _answered(ctx: Ctx) -> Any:
    assert ctx["refusal"] is None, ctx["refusal"]
    return ctx["answer"]


@then(parsers.re(r"it is given (?P<count>one|two|three|four|five) instances? of \w+"))
def _then_instances(ctx: Ctx, count: str) -> None:
    wanted = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}[count]
    answer = _answered(ctx)
    assert len(answer) == wanted, answer
    assert all(isinstance(row, ctx["model"]) for row in answer), answer


@then(parsers.re(r"it is given one instance, and that person is (?P<who>\w+)"))
def _then_that_one(ctx: Ctx, who: str) -> None:
    answer = _answered(ctx)
    assert len(answer) == 1, answer
    assert answer[0].name == who, answer


@then(parsers.re(r"it is given (?P<who>alice|bob|carol)(?: alone)?$"))
def _then_that_person(ctx: Ctx, who: str) -> None:
    assert getattr(_answered(ctx), "name", None) == who, ctx["answer"]


@then("it is given no people")
def _then_none(ctx: Ctx) -> None:
    assert _answered(ctx) == [], ctx["answer"]


@then("that is an empty answer, not a named absence")
def _then_empty_not_absent(ctx: Ctx) -> None:
    assert not is_nothing(ctx["answer"]), "an empty result is a list, not NOTHING"


@then(parsers.re(r"it is given a named absence(?:, not the number 0)?$"))
def _then_absent(ctx: Ctx) -> None:
    assert is_nothing(_answered(ctx)), ctx["answer"]


@then(parsers.parse("it is given the number {expected:d}"))
def _then_number(ctx: Ctx, expected: int) -> None:
    assert _answered(ctx) == expected, ctx["answer"]


@then("the answer is an integer")
def _then_integer(ctx: Ctx) -> None:
    assert isinstance(_answered(ctx), int), type(ctx["answer"])


@then(parsers.re(r"it answers (?P<said>true|false)"))
def _then_answers(ctx: Ctx, said: str) -> None:
    assert _answered(ctx) is {"true": True, "false": False}[said], ctx["answer"]


@then(parsers.re(r"the library refuses and names (?P<named>\w+)"))
def _then_refuses_naming(ctx: Ctx, named: str) -> None:
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r}"
    assert named in str(ctx["refusal"]), ctx["refusal"]


@then(parsers.re(r"it (?:is refused|refuses and .*)"))
def _then_refused(ctx: Ctx) -> None:
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r}"


@then(parsers.re(r"it is told (?P<expected>\d+) rows? changed"))
def _then_rows_changed(ctx: Ctx, expected: str) -> None:
    assert _answered(ctx) == int(expected), ctx["answer"]
