"""Bind `tests/bdd/features/core/refuse_owned_names.feature`.

WHAT THE FEATURE ASKS FOR. Six scenarios and one decision: a model that
declares a name the library or an engine already uses is stopped where the
models are read, and the refusal says which name, which prefix, and who owns
it. `fn refuse_owned_names` declares the answer as an absence or a fault, so
every scenario here reads a return value. Nothing is raised, nothing is logged,
and no scenario needs a database.

WHERE THE PREFIXES COME FROM. The feature's own description names five:
`_declaro_meta`, `_dp_`, `_declaro_tmp_`, and `sqlite_` and `turso_`, which
belong to the engines. `CLAIMED` below reads each table name the feature writes
against that sentence, so the two can disagree and say so.

THE TWO `Then` FAMILIES ARE ONE FUNCTION EACH. The feature writes "it is
refused" three ways and "it is accepted" two ways, and the differences are
words rather than shapes. Each family is one step over a table keyed by the
rest of the sentence, so a sixth wording is a row rather than a function.

WHY THE `Given` ALSO ASKS. Four scenarios write "When it is declared" and two
do not. The function is pure, so asking twice gives the same answer and both
shapes of scenario read the same return.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.refuse_owned_names import refuse_owned_names
from declaro_persistum.types import NOTHING, Declared, Fault

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/refuse_owned_names.feature")


# Every table name the feature writes -> the owned prefix that name claims, or
# NOTHING when it claims none. Read off the scenarios and the description, not
# off the module, so the two can disagree and say so.
CLAIMED: dict[str, str | None] = {
    "_declaro_meta": "_declaro_meta",
    "_dp_enum_": "_dp_",
    "_dp_orders": "_dp_",
    "sqlite_master": "sqlite_",
    "turso_internal": "turso_",
    "people": NOTHING,
    "shop_dp_orders": NOTHING,
}

# The rest of a "Then it is refused..." sentence -> the words that refusal has
# to carry beyond the name itself. Every wording the feature writes has a row.
REFUSAL_ALSO_SAYS: dict[str, Callable[[str], tuple[str, ...]]] = {
    "": lambda _table: (),
    ", naming the prefix": lambda table: (CLAIMED[table],),
    ", and the refusal says the library owns that name": lambda table: (
        CLAIMED[table],
        "library",
    ),
}

# The rest of a "Then it is accepted..." sentence -> the prefixes that sit
# inside the name but not at its start, which is why it was accepted.
ACCEPTED_THOUGH_IT_HOLDS: dict[str, tuple[str, ...]] = {
    "": (),
    ", because the prefix is not at the start": ("_dp_",),
}


def _declaring(table: str) -> Declared:
    """One model declaring `table`, with every field the declaration names.

    The columns are ordinary, so the accepted scenarios exercise the column
    names as well as the table name without any scenario having to say so.
    """
    return {
        "models": [
            {
                "table": table,
                "columns": [
                    {
                        "name": name,
                        "declared_type": "text",
                        "renamed_from": None,
                        "references": None,
                        "facets": {},
                    }
                    for name in ("id", "name")
                ],
                "dropping": [],
                "indexes": [],
                "constraints": [],
            }
        ],
        "views": [],
        "dropping_tables": [],
        "dropping_views": [],
    }


@given(parsers.parse('a model declaring the table "{table}"'))
def _a_model_declaring_the_table(scenario: dict[str, object], table: str) -> None:
    """Build the models and read them, so a scenario with no `When` has an
    answer to look at."""
    scenario["table"] = table
    scenario["declared"] = _declaring(table)
    scenario["refusal"] = refuse_owned_names(scenario["declared"])


@when("it is declared")
def _it_is_declared(scenario: dict[str, object]) -> None:
    """Hand the models to the reader. The function is pure, so this is the
    same answer the `Given` already got."""
    scenario["refusal"] = refuse_owned_names(scenario["declared"])


@then(parsers.re(r"it is refused(?P<rest>.*)$"))
def _it_is_refused(scenario: dict[str, object], rest: str) -> None:
    """A fault comes back naming the model, the name and, where the scenario
    asks for it, the prefix and who owns it.

    A refusal that said only "that name is reserved" would leave the caller
    reading the library's source to find out which name and what to write
    instead.
    """
    table = scenario["table"]
    refusal = scenario["refusal"]

    assert isinstance(refusal, dict), f"{table} was not refused: {refusal!r}"
    assert set(refusal) == set(Fault.__annotations__), (
        f"expected a Fault, got a dict shaped {sorted(refusal)}"
    )
    assert refusal["kind"] == "refused", refusal

    said = " ".join([refusal["what_went_wrong"], refusal["what_to_do_instead"]])
    assert table in said, f"the refusal does not name {table}: {said}"
    assert refusal["what_to_do_instead"].strip(), "the refusal says nothing to do instead"

    for word in REFUSAL_ALSO_SAYS[rest](table):
        assert word in said, f"the refusal does not say {word!r}: {said}"

    # CASE IS NOT PART OF THE QUESTION. The feature never says how a name is
    # capitalised, and this package has already had a database fold a stored
    # table name and drop the table on every deploy. So the same name shouted
    # is the same name, and it is refused too.
    shouted = refuse_owned_names(_declaring(table.upper()))
    assert shouted is not NOTHING, (
        f"{table} is refused and {table.upper()} is not, so whether a name is "
        f"owned depends on how somebody capitalised it"
    )


@then(parsers.re(r"it is accepted(?P<rest>.*)$"))
def _it_is_accepted(scenario: dict[str, object], rest: str) -> None:
    """Nothing comes back, which is the whole answer.

    A function that refused everything would satisfy every refusal scenario
    above, so the accepted names are what tell the two apart.
    """
    table = scenario["table"]
    refusal = scenario["refusal"]

    assert refusal is NOTHING, f"{table} claims no owned name and was refused: {refusal!r}"
    assert CLAIMED[table] is NOTHING, (
        f"the feature says {table} claims {CLAIMED[table]!r}, so this scenario "
        f"and the one above it disagree about the same name"
    )

    for prefix in ACCEPTED_THOUGH_IT_HOLDS[rest]:
        assert prefix in table, f"{table} does not hold {prefix} at all, so it proves nothing"
        assert not table.startswith(prefix), f"{table} does start {prefix}"
