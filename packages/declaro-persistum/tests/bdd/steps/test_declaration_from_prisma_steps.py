"""Bind `tests/bdd/features/core/declaration_from_prisma.feature`.

WHAT THE FEATURE ASKS FOR. One scenario, and it is the example
`HOW-IT-SHOULD-WORK.md` writes for this grammar: one call carrying named parts,
and out of it a declaration that names the model, the two columns and the
condition.

WHAT THE `When` HAD TO DECIDE. `declaration_from_prisma` is handed an `Asked`,
and `Asked` is the call as the caller phrased it. Prisma phrases one call with
named arguments, so the call above arrives as the model it was made on, the
method that was reached for, and each named argument under its own name. That is
written out here rather than hidden in a helper, because it is the contract
between the entry point and the builder and a reader of this file has to see it.

THE VERB IS PRISMA'S, NOT THE VOCABULARY'S. `find_many` is what a caller writes
and `all` is what the declaration says, and translating it is the builder's job.
If the entry point translated it, the same table would live in two places and
the second one would go stale.

WHAT THE `Then` CHECKS BEYOND THE THREE THINGS NAMED. A question that asked for
nothing else declares nothing else, so the whole declaration is compared and not
only the three fields. The four strings the caller passed are followed to what
they became, which is the test `HOW-IT-SHOULD-WORK.md` states for any function:
`id` and `name` arrive as column names, the key `name` arrives as a key, and
`bob` arrives as a bound value and appears in no longer string anywhere.
"""

from __future__ import annotations

import typing

from pytest_bdd import scenarios, then, when

from declaro_persistum.types import Declaration

scenarios("../features/core/declaration_from_prisma.feature")


# The declaration the call has to build, written out whole. Seven of its ten
# fields were not asked for, and each one says so in its own empty value.
THE_DECLARATION = {
    "model": "Person",
    "wanted": "all",
    "columns": ["id", "name"],
    "condition": {
        "each": {"name": {"operator": "eq", "value": "bob", "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    },
    "order": {},
    "grouping": [],
    "having": {"each": {}, "any_of": [], "none_of": []},
    "paging": None,
    "rows": None,
    "across": None,
}


def _every_string(value: object) -> list[str]:
    """Every string anywhere in a built declaration, keys and values alike."""
    strings = {
        str: lambda text: [text],
        dict: lambda mapping: [
            found
            for key, held in mapping.items()
            for found in [*_every_string(key), *_every_string(held)]
        ],
        list: lambda items: [found for item in items for found in _every_string(item)],
    }
    return strings.get(type(value), lambda _anything: [])(value)


@when('a caller writes Person.find_many(where={"name": "bob"}, select=["id", "name"])')
def _a_caller_writes_that_call(scenario: dict) -> None:
    scenario["asked"] = {
        "model": "Person",
        "wanted": "find_many",
        "where": {"name": "bob"},
        "select": ["id", "name"],
    }


@then("the declaration names the model, those two columns and that condition")
def _the_declaration_names_them(scenario: dict) -> None:
    from declaro_persistum.declaration_from_prisma import declaration_from_prisma

    declaration = declaration_from_prisma(scenario["asked"])

    assert set(declaration) == set(typing.get_type_hints(Declaration)), (
        f"a Declaration carries ten fields and this carries {sorted(declaration)}. "
        f"A Fault reaching here reads the same way, so check what came back."
    )
    assert declaration == THE_DECLARATION

    # THE FOUR STRINGS THE CALLER PASSED, FOLLOWED TO WHAT THEY BECAME.
    # `id` and `name` are looked up as column names, the key `name` is looked up
    # the same way, and `bob` is carried as a value. None of the four may end up
    # inside a longer string, because the only thing left to do with such a
    # string is paste it into a statement.
    pasted = [text for text in _every_string(declaration) if "bob" in text and text != "bob"]
    assert pasted == [], f"the value reached a longer string on its way here: {pasted}"

    # The three things the scenario names move with the call, so none of them is
    # a constant this function hands back whatever it was asked.
    elsewhere = declaration_from_prisma(
        {
            "model": "Order",
            "wanted": "find_many",
            "where": {"status": "paid"},
            "select": ["id"],
        }
    )
    assert elsewhere["model"] == "Order"
    assert elsewhere["columns"] == ["id"]
    assert elsewhere["condition"]["each"] == {
        "status": {"operator": "eq", "value": "paid", "ignore_case": False}
    }

    # Nothing outside the argument is consulted, so asking twice answers twice
    # the same.
    assert declaration_from_prisma(scenario["asked"]) == declaration
