"""Bind `schema/migration_needed.feature`.

WHAT THE FEATURE ASKS FOR. Three scenarios: two schemas that match need
nothing, a model that has gained a column needs a migration, and the
comparison opens nothing. HOW-IT-SHOULD-WORK.md:51 says the library reads the
live database, compares it to your models, and works out the difference; line
53 says the models are the desired state and everything else is derived from
them. So this function is handed both states and decides nothing else.

HOW THE THIRD SCENARIO IS CHECKED. "It read no clock, no connection and no
file" is a claim about what the code can reach, not about what one call
happened to do, so running it once proves nothing. `_reaches` reads the
module's own source and reports every import and every call to a name that
opens a file, asks a clock, draws a random number or drives a connection.
`the purity scan can actually see an offense` runs that same scanner over a
source string written to offend, so "no offenses" is distinguishable from "the
scanner found nothing at all" — this package has shipped a scan that reported
zero because it was looking in the wrong place.
"""

from __future__ import annotations

import ast
import inspect
import pathlib

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

scenarios("../features/schema/migration_needed.feature")


MODULE = (
    pathlib.Path(__file__).resolve().parents[3]
    / "src"
    / "declaro_persistum"
    / "migration_needed.py"
)

# WHAT A CLOCK, A CONNECTION AND A FILE ARE CALLED. Names rather than modules,
# because a call reaches one of these however it was imported.
IMPURE_NAMES = frozenset(
    {
        "open",
        "time",
        "monotonic",
        "perf_counter",
        "now",
        "today",
        "utcnow",
        "random",
        "choice",
        "shuffle",
        "uuid4",
        "getenv",
        "environ",
        "connect",
        "cursor",
        "execute",
        "executemany",
        "fetchall",
        "fetchone",
        "read_text",
        "read_bytes",
        "write_text",
        "input",
    }
)


def _called_names(tree: ast.AST) -> list[str]:
    """Every name this source calls, by its last component."""
    called = [node.func for node in ast.walk(tree) if isinstance(node, ast.Call)]
    return [node.attr for node in called if isinstance(node, ast.Attribute)] + [
        node.id for node in called if isinstance(node, ast.Name)
    ]


def _imported_modules(tree: ast.AST) -> list[str]:
    """Every module this source imports, by its top-level name."""
    plain = [
        alias.name.partition(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    ]
    from_ = [
        (node.module or "").partition(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    ]
    return plain + from_


def _reaches(source: str) -> list[str]:
    """Every way this source could reach a clock, a connection or a file."""
    tree = ast.parse(source)
    return sorted(
        {name for name in _called_names(tree) if name in IMPURE_NAMES}
        | {name for name in _imported_modules(tree) if name in IMPURE_NAMES}
    )


@pytest.fixture
def comparison() -> dict:
    """What one scenario has established so far."""
    return {"live": None, "target": None, "answer": None, "reaches": None}


def _a_schema(columns: list[str]) -> dict:
    """One table called `people`, holding the columns named."""
    return {
        "tables": [
            {
                "name": "people",
                "columns": [
                    {
                        "name": column,
                        "declared_type": "str",
                        "renamed_from": None,
                        "references": None,
                        "facets": {},
                    }
                    for column in columns
                ],
                "indexes": [],
                "constraints": [],
            }
        ],
        "views": [],
    }


# WHAT EACH SITUATION THE FEATURE NAMES IS, as the live columns and the
# declared ones. The two scenarios differ by this pair and by nothing else, so
# the pair is written here as data rather than twice as code.
A_SITUATION: dict[str, tuple[list[str], list[str]]] = {
    "live schema equal to the target": (["id", "name"], ["id", "name"]),
    "model that has gained a column since the database was brought up": (
        ["id", "name"],
        ["id", "name", "email"],
    ),
}

# WHAT EACH VERDICT THE FEATURE ASKS FOR IS, as the bool it means.
A_VERDICT: dict[str, bool] = {"no": False, "a": True}


@given(parsers.parse("a {situation}"))
def _given_the_situation(comparison: dict, situation: str) -> None:
    live, target = A_SITUATION[situation]
    comparison["live"] = _a_schema(live)
    comparison["target"] = _a_schema(target)


@when("the comparison runs")
def _when_the_comparison_runs(comparison: dict) -> None:
    """Run it on two schemas, and read what its source can reach."""
    from declaro_persistum.migration_needed import migration_needed

    comparison["live"] = _a_schema(["id", "name"])
    comparison["target"] = _a_schema(["id", "name"])
    comparison["answer"] = migration_needed(live=comparison["live"], target=comparison["target"])
    comparison["reaches"] = _reaches(MODULE.read_text(encoding="utf-8"))


@then(parsers.parse("{verdict} migration is needed"))
def _then_the_verdict(comparison: dict, verdict: str) -> None:
    from declaro_persistum.migration_needed import migration_needed

    wanted = A_VERDICT[verdict]
    answer = migration_needed(live=comparison["live"], target=comparison["target"])
    assert answer is wanted, (
        f"the live columns are {[c['name'] for c in comparison['live']['tables'][0]['columns']]} "
        f"and the models declare {[c['name'] for c in comparison['target']['tables'][0]['columns']]}, "
        f"so the answer should be {wanted!r} and was {answer!r}"
    )


@then("it read no clock, no connection and no file")
def _then_it_opened_nothing(comparison: dict) -> None:
    from declaro_persistum.migration_needed import migration_needed

    assert comparison["reaches"] == [], (
        f"{MODULE.name} can reach {comparison['reaches']}, and a function given "
        "both schemas has nothing left to ask anyone"
    )

    # IT IS GIVEN BOTH SCHEMAS AND NOTHING ELSE. A third parameter, or a
    # default on either of these, is a second way in that no scenario exercises.
    signature = inspect.signature(migration_needed)
    assert list(signature.parameters) == ["live", "target"], (
        f"migration_needed takes {list(signature.parameters)}; the declaration "
        "says (live: Schema, target: Schema)"
    )
    assert all(
        parameter.default is inspect.Parameter.empty for parameter in signature.parameters.values()
    ), "a default argument cannot tell a caller who chose from a caller who forgot"

    # SAME INPUTS, SAME ANSWER, TWICE. A clock or a random source reached
    # through a name the scan does not know would show up here.
    again = migration_needed(live=comparison["live"], target=comparison["target"])
    assert again == comparison["answer"], (
        "two calls on the same two schemas disagreed, so something outside them is being read"
    )


@pytest.mark.precommit
def test_the_purity_scan_can_actually_see_an_offense():
    """The scan above is worthless if it reports zero because it found nothing.

    A wrong path, a wrong node type or a typo in the vocabulary all produce an
    empty list, which reads exactly like a pure module. So the scanner is run
    over source written to offend, and has to name what it finds.
    """
    offending = (
        "import time\n"
        "import random\n"
        "def f(path):\n"
        "    handle = open(path)\n"
        "    return time.monotonic() + random.random()\n"
    )
    found = _reaches(offending)
    assert found == ["monotonic", "open", "random", "time"], (
        f"the purity scan read source that opens a file, asks a clock and draws "
        f"a random number, and reported {found}"
    )


@pytest.mark.precommit
def test_the_purity_scan_is_reading_the_function_it_claims_to():
    """And it must be pointed at a file that defines `migration_needed`."""
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    defined = [node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    assert "migration_needed" in defined, (
        f"{MODULE} defines {defined}, so the purity scan above is reading the "
        "wrong file and its empty report means nothing"
    )
