"""Steps for `tests/bdd/features/core/refuse_unusable_settings.feature`.

One door accepts the union of what three engines need. `busy_timeout_s` means
nothing to PostgreSQL and `primary` means nothing to SQLite, so handing either
one to the wrong engine comes back as a refusal rather than going quietly
nowhere.

Each scenario hands `refuse_unusable_settings` one engine and one setting and
reads what comes back. Nothing here opens a file or a connection: the function
names the engine as a word, so the whole feature runs without a database.

THE WHEN STEP IS ONE FUNCTION OVER TWO TABLES, not one function per scenario.
Which settings belong to which engine is the table the function under test
keeps, and a step file that pasted that table out as three near-identical
functions would describe the thing it tests in the shape the design refuses.
So the Gherkin words are captured and used to index `ENGINES` and `SETTINGS`,
and a fourth scenario is a row rather than a function.
"""

from __future__ import annotations

from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when  # noqa: F401

from declaro_persistum.refuse_unusable_settings import refuse_unusable_settings
from declaro_persistum.types import Engine, Fault, Value

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/refuse_unusable_settings.feature")

# The word a scenario writes -> the engine the declaration names.
ENGINES: dict[str, Engine] = {
    "PostgreSQL": "postgresql",
    "SQLite": "sqlite",
    "Turso": "turso",
}

# The words a scenario writes -> the setting a caller would actually pass.
SETTINGS: dict[str, dict[str, Value]] = {
    "a busy timeout": {"busy_timeout_s": 5.0},
    "a primary": {"primary": "libsql://team-app.turso.io"},
}


@pytest.fixture
def answer() -> dict[str, Any]:
    """Where the When step puts what came back, for the Then step to read."""
    return {}


@when(parsers.re(r"a caller opens a (?P<engine>\S+) target (?:with|naming) (?P<setting>.+)$"))
def when_a_caller_opens_a_target(answer: dict[str, Any], engine: str, setting: str) -> None:
    """One step for every "a caller opens a X target ..." line in the feature."""
    answer["engine"] = ENGINES[engine]
    answer["settings"] = SETTINGS[setting]
    answer["result"] = refuse_unusable_settings(answer["engine"], answer["settings"])


@then("it is refused")
def then_it_is_refused(answer: dict[str, Any]) -> None:
    """A Fault comes back, it names the setting and the engine, and it says
    what to write instead. A refusal that does not name the setting leaves the
    caller guessing which of their settings was the wrong one."""
    result = answer["result"]
    assert isinstance(result, dict), f"expected a Fault, got {result!r}"
    assert set(result) == set(Fault.__annotations__), (
        f"expected a Fault, got a dict shaped {sorted(result)}"
    )
    assert result["kind"] == "refused", result

    refused = " ".join([result["what_went_wrong"], result["what_to_do_instead"]])
    assert answer["engine"] in refused, f"the refusal does not name {answer['engine']}: {refused}"
    for name in answer["settings"]:
        assert name in refused, f"the refusal does not name {name}: {refused}"

    assert result["what_to_do_instead"].strip(), "the refusal says nothing to do instead"


@then("it is kept")
def then_it_is_kept(answer: dict[str, Any]) -> None:
    """The settings come back unchanged. Coming back is the whole answer: a
    function that dropped the setting it approved would be the silence this
    one exists to stop."""
    result = answer["result"]
    assert result == answer["settings"], f"expected {answer['settings']!r} back, got {result!r}"
