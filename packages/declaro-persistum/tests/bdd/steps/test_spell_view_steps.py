"""Bind `tests/bdd/features/schema/spell_view.feature`.

WHAT THE FUNCTION ANSWERS. `fn spell_view` is handed a declared view and an
engine, and it writes the SELECT that view stands for. `HOW-IT-SHOULD-WORK.md`
says it in one line under "Everything gets declared, not just tables": "A view
is a query with a name. You declare it against your models, the way you declare
a table, and the library writes the SELECT." That sentence only means something
if a view holds a declaration rather than a string, which is what `type View`
says in `docs/design/persistum.hd`.

WHAT THIS FILE CAN PROVE, AND WHAT IT CANNOT. Eight scenarios. Four of them ask
what comes back from a view and an engine, and those are asserted below. The
other four were folded in from the old `a_view_is_a_named_declaration.feature`
and each one reaches past this function's two arguments: bringing a schema to
the declared state, refusing a column against the model that declares it,
replacing a view in a database, and reading a view's rows. A function handed a
view and an engine, holding no model and no connection, cannot answer any of
them, so each is `pytest.xfail` naming what must answer it. A skip would say the
line was not worth running; an assertion rewritten until it passed would say the
line was satisfied.

EVERY STEP IS DEFINED, INCLUDING THE ONES AFTER AN `xfail`. Those never execute
today, and they raise rather than pass, so deleting an `xfail` turns its
scenario red rather than green.

THE IDENTIFIERS VARY ON PURPOSE. This package once reached full branch coverage
while emitting SQL no engine could parse, because every test named its table
`users`. So the views below stand over `orders`, `people` and `shipments`, and
the join is moved to a second column to watch the answer move with it.
"""

from __future__ import annotations

import re
from collections.abc import Callable

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.join_between import join_between
from declaro_persistum.spell_view import spell_view
from declaro_persistum.types import Condition, Declaration, Join, Model, View

scenarios("../features/schema/spell_view.feature")


# --- what the scenarios declare ---------------------------------------------


def _a_column(name: str, declared_type: str, references: str | None) -> dict:
    """One column, with every field the declaration gives a Column.

    `references` is written `table.column` here because that is how a person
    reads a foreign key, and it becomes the `Reference` record the declaration
    describes before it reaches the function. `refuse` is the deletion rule a
    declaration means when it says nothing: a parent with children stays.
    """
    table, _, column = (references or "").partition(".")
    return {
        "name": name,
        "declared_type": declared_type,
        "renamed_from": None,
        "references": (
            {"table": table, "column": column, "on_delete": "refuse"}
            if references is not None
            else None
        ),
        "facets": {},
    }


def _a_model(table: str, columns: list[dict]) -> Model:
    """One model, with every field the declaration gives a Model."""
    return {
        "name": table,
        "table": table,
        "columns": columns,
        "dropping": [],
        "indexes": [],
        "constraints": [],
    }


PEOPLE = _a_model(
    "people",
    [_a_column("id", "INTEGER", None), _a_column("name", "TEXT", None)],
)

# The column that points, and the only place the relationship is written down.
THE_COLUMN_THAT_POINTS = "placed_by"

ORDERS = _a_model(
    "orders",
    [
        _a_column("id", "INTEGER", None),
        _a_column("status", "TEXT", None),
        _a_column("total", "INTEGER", None),
        _a_column(THE_COLUMN_THAT_POINTS, "INTEGER", "people.id"),
    ],
)


def _nothing_asked() -> Condition:
    """A condition that narrows nothing, with every field a Condition has."""
    return {"each": {}, "any_of": [], "none_of": []}


def _where(column: str, value: str) -> Condition:
    return {
        "each": {column: {"operator": "eq", "value": value, "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    }


def _a_declaration(
    table: str,
    wanted: str,
    columns: list[str],
    condition: Condition,
    across: list[Join] | None,
) -> Declaration:
    """One declaration, with every field the declaration gives a Declaration."""
    return {
        "model": table,
        "wanted": wanted,
        "columns": columns,
        "condition": condition,
        "order": {},
        "grouping": [],
        "having": _nothing_asked(),
        "paging": None,
        "rows": None,
        # A view's SELECT is the same whichever shape a caller wants its rows
        # back in, so this is here to make the declaration whole rather than to
        # be read.
        "shaping": "dictionaries",
        "across": across,
    }


def _a_view(name: str, declaration: Declaration) -> View:
    """A plain view, which has no freshness to state."""
    return {"name": name, "declaration": declaration, "freshness": None}


# THE VALUE THE FEATURE NAMES, in one place, because two scenarios read it back.
THE_VALUE = "paid"

PAID_ORDERS = _a_view(
    "paid_orders", _a_declaration("orders", "all", [], _where("status", THE_VALUE), None)
)

# A column `orders` does not declare. The model above has id, status, total and
# placed_by, and nothing called `discount`.
THE_COLUMN_ORDERS_DOES_NOT_HAVE = "discount"
DISCOUNTED_ORDERS = _a_view(
    "discounted_orders",
    _a_declaration("orders", "all", [], _where(THE_COLUMN_ORDERS_DOES_NOT_HAVE, "yes"), None),
)

# The same view after somebody changed what it stands for.
REFUNDED_ORDERS = _a_view(
    "paid_orders", _a_declaration("orders", "all", [], _where("status", "refunded"), None)
)

# TWO VIEWS NOTHING CAN SPELL, and they are here because the scenario about a
# refusal is the one place the feature asks what a refusal from here looks like.
# One stands for a write, which is not a question anything stores under a name.
# The other asks to be first with nothing to be first in, which the compiler
# itself refuses.
EMPTIED_BASKETS = _a_view(
    "emptied_baskets", _a_declaration("baskets", "remove_all", [], _where("state", "stale"), None)
)
NEWEST_SHIPMENT = _a_view(
    "newest_shipment", _a_declaration("shipments", "first", ["id"], _nothing_asked(), None)
)

# An engine nobody declared. The three are sqlite, postgresql and turso.
NOBODYS_ENGINE = "mariadb"


def _the_view_joining(near: Model, far: Model) -> View:
    """Orders with the name of the person who placed them.

    The library works the join out from the reference the model already
    declares, which is `fn join_between`, and the view holds what it worked out.
    """
    join = join_between(near, far, _nothing_asked())
    return _a_view(
        "orders_with_buyers",
        _a_declaration(near["table"], "all", ["total", "name"], _nothing_asked(), [join]),
    )


# --- what a scenario established, in one table each --------------------------
#
# ONE STEP FUNCTION PER SHAPE, NOT ONE PER SENTENCE. Four Givens name a view,
# five Whens move the scenario along, and two say the same thing about what this
# function is not handed. So the words are data, and each regex below is built
# from the keys of its own table.

# WHAT EACH GIVEN PUTS IN FRONT OF THE SCENARIO. Four name a view and two name
# the models a view is declared against, and both shapes are a sentence on the
# left and what it established on the right.
WHAT_WAS_DECLARED: dict[str, dict[str, object]] = {
    'a view declared as the orders whose status is "paid"': {"view": PAID_ORDERS},
    'a declared view "paid_orders"': {"view": PAID_ORDERS},
    "a view declared against a column Order does not have": {"view": DISCOUNTED_ORDERS},
    "a declared view already in the database": {"view": PAID_ORDERS},
    "a model Order declaring orders with columns id, status and total": {"models": (ORDERS,)},
    "Order references Person": {"models": (ORDERS, PEOPLE)},
}

WHAT_HAPPENS: dict[str, Callable[[dict], None]] = {
    "it is spelled for sqlite": lambda scenario: scenario.update(engine="sqlite"),
    'a view "paid_orders" is declared as the orders whose status is "paid"': (
        lambda scenario: scenario.update(view=PAID_ORDERS)
    ),
    "a view is declared as orders with the name of the person who placed them": (
        lambda scenario: scenario.update(
            view=_the_view_joining(scenario["models"][0], scenario["models"][1])
        )
    ),
    "its declaration changes": lambda scenario: scenario.update(changed=REFUNDED_ORDERS),
    # DECLARING IT IS HANDING IT OVER. This function is handed a view that has
    # already been declared, so what the scenario establishes here is the answer
    # the library gave for it.
    "it is declared": lambda scenario: scenario.update(
        answer=spell_view(scenario["view"], "sqlite")
    ),
}

# WHAT THIS FUNCTION IS NOT HANDED, and who is. `fn spell_view` takes a view and
# an engine. It holds no model, so it cannot check a column against one; it
# holds no connection, so nothing it returns reaches a database; and it writes
# the SELECT a view stands for rather than running one.
WHAT_SPELL_VIEW_IS_NOT_HANDED: dict[str, str] = {
    "the database is opened": (
        "spell_view writes a SELECT and holds no connection. Carrying a view "
        "into a database is `fn spell_operation` over a create_view operation, "
        "run by `orchestrator fn session_connect`."
    ),
    "a caller asks for its rows": (
        "spell_view writes the SELECT a view stands for; it runs nothing. "
        "Answering a caller's rows is `orchestrator fn query_answer`."
    ),
}


def _sentences(table: dict[str, object], name: str) -> str:
    return f"(?P<{name}>" + "|".join(re.escape(key) for key in table) + ")"


@given(parsers.re(_sentences(WHAT_WAS_DECLARED, "declared")))
def _what_the_scenario_declared(scenario: dict, declared: str) -> None:
    scenario.update(WHAT_WAS_DECLARED[declared])


@when(parsers.re(_sentences(WHAT_HAPPENS, "happens")))
def _what_happens_next(scenario: dict, happens: str) -> None:
    WHAT_HAPPENS[happens](scenario)


@when(parsers.re(_sentences(WHAT_SPELL_VIEW_IS_NOT_HANDED, "reaching")))
def _something_this_function_is_not_handed(scenario: dict, reaching: str) -> None:
    pytest.xfail(WHAT_SPELL_VIEW_IS_NOT_HANDED[reaching])


# --- the four results this function can answer -------------------------------


@then('the statement selects from orders where status is bound to "paid"')
def _it_selects_from_orders_with_the_value_bound(scenario: dict) -> None:
    statement = spell_view(scenario["view"], scenario["engine"])

    assert set(statement) == {"text", "parameters"}, (
        f"a Statement carries text and the values beside it: {statement}"
    )
    text = statement["text"]
    assert text.startswith("SELECT "), f"a view stands for a SELECT, and this is: {text}"
    assert '"orders"' in text, f"the view's own table is what it selects from: {text}"
    assert '"status"' in text, f"the column the view narrows on is named: {text}"

    # THE VALUE IS NOT IN THE TEXT. A value that reached the statement is a
    # value nobody bound, and no later check would see it happen.
    assert THE_VALUE not in text, f"the value was pasted into the statement: {text}"
    assert list(statement["parameters"].values()) == [THE_VALUE], (
        f"the value comes back beside the statement: {statement}"
    )
    assert "?" in text, f"sqlite takes a question mark per value: {text}"

    # THE ENGINE REACHES THE STATEMENT. asyncpg numbers its placeholders, so the
    # same view on postgresql is spelled differently. A function that ignored
    # the engine it was handed would pass every line above and fail here.
    on_postgresql = spell_view(scenario["view"], "postgresql")
    assert "$1" in on_postgresql["text"], (
        f"postgresql numbers its placeholders, and this says: {on_postgresql['text']}"
    )
    assert on_postgresql["parameters"] == statement["parameters"], (
        "the values are the caller's either way; only the placeholder moves"
    )

    # Nothing outside the two arguments is consulted, so the same view and the
    # same engine write the same characters.
    assert spell_view(scenario["view"], scenario["engine"]) == statement


@then('"paid" came back beside the statement')
def _the_value_came_back_beside_the_statement() -> None:
    """This scenario states its own case: a declared value is bound, not pasted."""
    statement = spell_view(PAID_ORDERS, "turso")

    assert list(statement["parameters"].values()) == [THE_VALUE], (
        f"the value has to come back for the driver to bind: {statement}"
    )
    assert THE_VALUE not in statement["text"], f"and it is not in the text: {statement['text']}"

    # AND IT IS THE DECLARED VALUE, NOT A CONSTANT. Change what the view stands
    # for and the value beside the statement moves with it.
    elsewhere = _a_view(
        "late_shipments",
        _a_declaration("shipments", "all", [], _where("carrier", "overnight"), None),
    )
    moved = spell_view(elsewhere, "turso")
    assert list(moved["parameters"].values()) == ["overnight"], (
        f"the values are whatever the view declared: {moved}"
    )
    assert '"shipments"' in moved["text"] and "overnight" not in moved["text"], (
        f"a second view over a second table, with its value still bound: {moved['text']}"
    )


# The words a stored query would be written with, looked for in what the caller
# declared. Matched case-insensitively and on word boundaries: `paid_orders` is
# a name, and it is not `ORDER BY`.
SQL_WORDS = ("select", "from", "where", "join", "union", "order by", "group by")
_SQL = re.compile("|".join(rf"\b{word}\b" for word in SQL_WORDS), re.IGNORECASE)


# EVERY TYPE A DECLARED VIEW CAN HOLD HAS A ROW, and the table is read by
# subscript. A type with no row raises naming itself, rather than being read as
# "no strings in here" by a default nobody wrote.
_INSIDE: dict[type, Callable[[object], list[str]]] = {
    str: lambda held: [held],
    dict: lambda held: [
        found for key, inner in held.items() for found in _strings_in(key) + _strings_in(inner)
    ],
    list: lambda held: [found for inner in held for found in _strings_in(inner)],
    tuple: lambda held: [found for inner in held for found in _strings_in(inner)],
    int: lambda _held: [],
    float: lambda _held: [],
    bool: lambda _held: [],
    type(None): lambda _held: [],
}


def _strings_in(value: object) -> list[str]:
    """Every string anywhere inside a declared view, keys as well as values."""
    return _INSIDE[type(value)](value)


@then("the declaration contains no SELECT")
def _the_declaration_contains_no_select(scenario: dict) -> None:
    view = scenario["view"]

    written = [held for held in _strings_in(view) if _SQL.search(held)]
    assert written == [], (
        f"a view holds a declaration, never a string of SQL, and this one holds {written}"
    )
    assert view["declaration"]["model"] == scenario["models"][0]["table"], (
        "the view is declared against the model, by the name that model declares"
    )

    # THE SELECT EXISTS BECAUSE THIS FUNCTION WROTE IT. The declaration above
    # carries none, and a statement comes back from it all the same.
    statement = spell_view(view, "sqlite")
    assert statement["text"].startswith("SELECT "), (
        f"the library writes the SELECT the declaration stands for: {statement}"
    )
    assert '"orders"' in statement["text"]


@then("the library works out the join from the declared reference")
def _the_join_comes_from_the_declared_reference(scenario: dict) -> None:
    near, far = scenario["models"]
    statement = spell_view(scenario["view"], "postgresql")

    text = statement["text"]
    assert f'JOIN "{far["table"]}"' in text, f"the far model is joined in: {text}"
    assert f'"{near["table"]}"."{THE_COLUMN_THAT_POINTS}"' in text, (
        f"the join is made on the column that points: {text}"
    )
    assert f'"{far["table"]}"."id"' in text, f"and on the column it points at: {text}"
    assert statement["parameters"] == {}, (
        f"a join names columns and binds nothing: {statement['parameters']}"
    )

    # IT FOLLOWS THE REFERENCE, NOT THE COLUMN'S NAME OR ITS POSITION. Move the
    # relationship to another column, pointing at another column, and the
    # statement has to move with it.
    moved = _a_model(
        "orders",
        [
            _a_column("id", "INTEGER", None),
            _a_column("buyer", "TEXT", "people.name"),
            _a_column(THE_COLUMN_THAT_POINTS, "INTEGER", None),
        ],
    )
    after = spell_view(_the_view_joining(moved, far), "postgresql")
    assert '"orders"."buyer" = "people"."name"' in after["text"], (
        f"the join did not follow the declared reference: {after['text']}"
    )


@then("it is refused, and the refusal names Order")
def _it_is_refused_and_the_refusal_names_order(scenario: dict) -> None:
    """Half of this sentence is this function's, and half is not.

    A refusal from here comes back in the return and names the view, which is
    asserted. Naming the model takes the model, and this function is handed a
    view and an engine, so the second half is the `xfail` at the end.
    """
    # A STATEMENT CAME BACK FOR THE VIEW THE SCENARIO DECLARED. `discount` is a
    # column name like any other to a function holding no model to check it
    # against, which is exactly why the refusal has to be made somewhere else.
    assert "text" in scenario["answer"], (
        f"nothing here can tell a declared column from an undeclared one: {scenario['answer']}"
    )

    # WHAT A REFUSAL FROM HERE LOOKS LIKE. An engine nobody declared, a view
    # standing for a write, and a declaration the compiler itself refuses. All
    # three come back in the return, and each one names the view.
    refusals = {
        NOBODYS_ENGINE: spell_view(scenario["view"], NOBODYS_ENGINE),
        EMPTIED_BASKETS["name"]: spell_view(EMPTIED_BASKETS, "sqlite"),
        NEWEST_SHIPMENT["name"]: spell_view(NEWEST_SHIPMENT, "postgresql"),
    }
    for asked, refusal in refusals.items():
        assert set(refusal) == {"kind", "what_went_wrong", "what_to_do_instead"}, (
            f"asking for {asked} came back as something that is not a Fault: {refusal}"
        )
        assert refusal["kind"] == "refused", f"asking for {asked} gave {refusal['kind']}"
        assert refusal["what_to_do_instead"].strip() != "", (
            f"a refusal that says only what went wrong leaves the caller nowhere: {refusal}"
        )

    assert scenario["view"]["name"] in refusals[NOBODYS_ENGINE]["what_went_wrong"]
    assert NOBODYS_ENGINE in refusals[NOBODYS_ENGINE]["what_went_wrong"], (
        "a refusal names the engine it was asked for"
    )
    said = refusals[EMPTIED_BASKETS["name"]]["what_went_wrong"]
    assert EMPTIED_BASKETS["name"] in said and "remove_all" in said, (
        f"a view standing for a write is refused by name: {said}"
    )
    # THE COMPILER'S OWN REASON SURVIVES, with the view's name in front of it.
    short = refusals[NEWEST_SHIPMENT["name"]]["what_went_wrong"]
    assert NEWEST_SHIPMENT["name"] in short and "order" in short.lower(), (
        f"the reason the compiler gave is the reason the caller reads: {short}"
    )

    pytest.xfail(
        "spell_view is handed a view and an engine and no model, so it cannot "
        "tell a column the model declares from one it does not. That check is "
        "`fn refuse_undeclared_columns`, which is handed the model, and nothing "
        "yet calls it for a declared view."
    )


# --- the four the declaration answers somewhere else -------------------------
#
# THREE SENTENCES, ONE FUNCTION, AND IT RAISES. Each sits behind an `xfail`
# above, so none of them runs today. Written to fail loudly rather than to pass,
# which is what makes deleting an `xfail` turn its scenario red.


@then("the database holds a view, and the library wrote its SELECT")
@then("it does not report success while changing nothing")
@then("it answers the same way a model does")
def _nothing_here_answers_this_yet() -> None:
    raise NotImplementedError("CODE NOT WRITTEN")


@then("bringing the schema up to date replaces the view")
def _bringing_the_schema_up_to_date_replaces_the_view(scenario: dict) -> None:
    # The two declarations differ, and this function can say so: each spells its
    # own SELECT. What nothing here can do is replace the one in the database.
    assert scenario["changed"]["declaration"] != scenario["view"]["declaration"]
    pytest.xfail(
        "spell_view writes a SELECT and holds no connection. Working out that "
        "the view changed is `fn difference`, and replacing it is "
        "`fn spell_operation` over a drop_view and a create_view."
    )
