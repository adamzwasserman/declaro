"""Refuse a column no model declares, in the caller's own process.

WHY THIS RUNS BEFORE ANYTHING IS COMPILED. A misspelled column that travels
reaches the engine, which answers `no such column: nmae` and stops. That
sentence is true and useless: it is the engine's word for the caller's typo, it
arrives after a connection was taken and a statement was built, and it leaves
the caller to go and read the model to find out what the column is called. So
`query_answer` asks this first, and the answer names the model and lists the
columns that model does declare.

IT CHECKS AND IT DOES NOT PRODUCE. Handing back the declaration it was given
would make this a second source of declarations beside the builders, and then
there are two answers to where a declaration comes from. A check answers
whether something is wrong: a fault when it is, the named absence when it is
not.

A DECLARATION NAMES COLUMNS IN FIVE PLACES, and all five are checked. The
columns asked for, the condition, the order, the grouping and the having. A
check that reads only the columns asked for passes every example anybody would
write down and still lets an order reach the engine, so the five places are a
table here rather than five lines of prose somewhere else.

NOTHING HERE KNOWS WHICH GRAMMAR ASKED. Four grammars are four phrasings of one
declaration, so one check over the declaration is what makes all four refuse
the same column with the same words. A fifth phrasing gets this refusal without
a line being added.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import NOTHING, Condition, Declaration, Fault, Model


def _named_in(condition: Condition) -> list[str]:
    """Every column a condition names, however deep it is nested.

    A condition holds conditions, so this asks the same question of each one it
    holds. It stops because `any_of` and `none_of` are eventually empty, which
    is the end of the data rather than a test somebody wrote.
    """
    nested = condition["any_of"] + condition["none_of"]
    return list(condition["each"]) + [name for inner in nested for name in _named_in(inner)]


# THE FIVE PLACES A DECLARATION NAMES A COLUMN. Each row reads one of them and
# hands back the names it found, in the order the caller wrote them.
WHERE_COLUMNS_ARE_NAMED: dict[str, Callable[[Declaration], list[str]]] = {
    "columns": lambda declaration: list(declaration["columns"]),
    "condition": lambda declaration: _named_in(declaration["condition"]),
    "order": lambda declaration: list(declaration["order"]),
    "grouping": lambda declaration: list(declaration["grouping"]),
    "having": lambda declaration: _named_in(declaration["having"]),
}


def _fine(_model: Model, _declared: list[str], _undeclared: list[str]) -> Fault | None:
    """Every column asked for is one the model declares, so there is nothing to say."""
    return NOTHING


def _refuse(model: Model, declared: list[str], undeclared: list[str]) -> Fault | None:
    """Name the model, say which columns it does not declare, and list the ones it does."""
    asked = ", ".join(undeclared)
    has = ", ".join(declared)
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You asked {model['name']} for {asked}, and {model['name']} declares no such column."
        ),
        what_to_do_instead=(f"{model['name']} declares {has}. Ask it for one of those."),
    )


_ANSWER: dict[bool, Callable[[Model, list[str], list[str]], Fault | None]] = {
    False: _fine,
    True: _refuse,
}


def _the_model_it_names(model: Model, declaration: Declaration) -> Fault | None:
    """Refuse a question naming a model other than the one it was handed.

    A caller writes `Person` and the model's table is `people`, so the two are
    compared by the model's own name. Either spelling is accepted, because the
    four grammars differ in which one they carry.
    """
    return _WHETHER_IT_IS_THIS_MODEL[declaration["model"] in (model["name"], model["table"])](
        model, declaration
    )


def _it_is_this_model(_model: Model, _declaration: Declaration) -> Fault | None:
    return NOTHING


def _it_is_another_model(model: Model, declaration: Declaration) -> Fault | None:
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The question names {declaration['model']!r} and it was asked of "
            f"{model['name']}, which is a different model."
        ),
        what_to_do_instead=(
            f"Ask {model['name']} for what {model['name']} holds, or hand the "
            f"call the model {declaration['model']!r} names."
        ),
    )


_WHETHER_IT_IS_THIS_MODEL: dict[bool, Callable[[Model, Declaration], Fault | None]] = {
    True: _it_is_this_model,
    False: _it_is_another_model,
}

# A REFUSAL ANYWHERE IS THE ANSWER, and the model's name is checked first because
# a question about another model's columns says nothing a caller can act on.
_THE_FIRST_REFUSAL: dict[bool, Callable[[Fault | None, Fault | None], Fault | None]] = {
    True: lambda _named, columns: columns,
    False: lambda named, _columns: named,
}


def refuse_undeclared_columns(model: Model, declaration: Declaration) -> Fault | None:
    """Refuse the columns this model does not declare; answer with absence when there are none.

    Reads the model and the declaration and nothing else. It opens no
    connection, builds no statement and asks nothing of the clock, so a caller
    gets this answer before anything has been sent anywhere.
    """
    named = _the_model_it_names(model, declaration)

    # THE TABLE IS A NAME TOO, AND NOTHING CHECKED IT UNTIL 2026-09-22. Measured:
    # asking for a model called `people WHERE 1=1; DROP TABLE people; --` reached
    # the engine and came back in the engine's words. Nothing ran, because
    # `fn compile_statement` quotes the name, but this library's rule is that a
    # name no model declares is refused in the caller's own process, and that
    # rule covered the columns and not the table.
    declared = [column["name"] for column in model["columns"]]
    asked = dict.fromkeys(
        name for read in WHERE_COLUMNS_ARE_NAMED.values() for name in read(declaration)
    )
    undeclared = [name for name in asked if name not in declared]
    return _THE_FIRST_REFUSAL[named is NOTHING](
        named, _ANSWER[bool(undeclared)](model, declared, undeclared)
    )
