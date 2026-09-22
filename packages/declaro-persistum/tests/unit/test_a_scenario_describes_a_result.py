"""A scenario says what has to be true, never which function to call.

THE RULE, in Adam's words on 2026-09-21: a scenario should not describe a
function, it should describe a result. Each function has its own feature file,
and one function's feature does not refer to another's.

WHY IT IS WORTH A GATE. On the day the rule was stated, 25 scenarios named
functions from the source that had just been deleted: `replicate_if_idle drops
the pass while a writer is waiting`, `map_type is given "boolean"`,
`get_connection_string falls back to the environment`. Every one told a reader
which call to make. None survives the call being renamed, and all of them
outlived the code they named by a day, which is how a specification quietly
becomes a description of something that no longer exists.

WHAT IT CANNOT SEE. Prose that describes a function without naming it, and a
name this package has never declared and never will. It catches the two shapes
that actually happened here: another declared function's name, and a private
name with a leading underscore, which can only be code.
"""

from __future__ import annotations

import collections
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_FEATURES = _ROOT / "tests" / "bdd" / "features"
_DESIGN = _ROOT / "docs" / "design"

# A RATCHET, NOT AN ALLOWLIST. It held five files on 2026-09-21, all of them
# describing the command line or the transfer tool and all of them naming the
# private helpers of the source deleted that day. All five were rewritten
# against their declarations the same day, so it is empty and stays empty: a
# file comes off this list when it is rewritten, and nothing goes on it.
STILL_NAMING_CODE: dict[str, int] = {}

_HEAD = re.compile(r"^\s*(Scenario Outline|Scenario|Background|Rule)\b")
_DECLARED = re.compile(
    r"^  (?:boundary_in fn|boundary_out fn|orchestrator fn|reader fn|fn) (\w+)", re.M
)
_PRIVATE = re.compile(r"(?<![\w`])_[a-z][a-z0-9_]{3,}\b")
_QUOTED = re.compile(r'"[^"]*"|`[^`]*`')


def _prose(body: str) -> str:
    """The sentence without its data.

    A NAME IN QUOTES IS A VALUE, NOT A CALL. `refuse_owned_names` is about the
    table names this library reserves, so its scenarios say "_declaro_meta" and
    mean the string. Scanning that as a private function made the gate flag the
    one feature whose subject is those names.
    """
    return _QUOTED.sub(" ", body)


# READING HAPPENS AT THE EDGE, IN THE FIXTURES. The two functions below take
# text and answer from it, so a test of this gate can hand them a string. That
# is the same rule the gate itself is holding the package to.


@pytest.fixture(scope="module")
def declarations() -> list[str]:
    return [hd.read_text(encoding="utf-8") for hd in sorted(_DESIGN.glob("*.hd"))]


@pytest.fixture(scope="module")
def features() -> list[tuple[pathlib.Path, str]]:
    return [
        (path.relative_to(_FEATURES), path.read_text(encoding="utf-8"))
        for path in sorted(_FEATURES.rglob("*.feature"))
    ]


def _declared_functions(declarations: list[str]) -> set[str]:
    names: set[str] = set()
    for text in declarations:
        names |= set(_DECLARED.findall(text))
    return names


def _scenarios(text: str) -> list[tuple[str, str]]:
    """Every scenario in a feature, as its title and its body, comments dropped."""
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if _HEAD.match(line)]
    out = []
    for first, last in zip(starts, starts[1:] + [len(lines)], strict=True):
        block = [line for line in lines[first:last] if not line.strip().startswith("#")]
        if not block[0].strip().startswith("Scenario"):
            continue
        out.append((block[0].strip(), "\n".join(block)))
    return out


def test_the_gate_can_read_the_features(features, declarations) -> None:
    """A gate that finds nothing to check passes for the wrong reason."""
    assert len(features) > 50, len(features)
    assert len(_declared_functions(declarations)) > 50


def test_no_scenario_names_another_function(features, declarations) -> None:
    declared = _declared_functions(declarations)
    offences = []
    for path, text in features:
        # ONLY A CODE-SHAPED NAME COUNTS. Several declared functions are ordinary
        # English words: `difference`, `replicates`. A scenario saying "the
        # difference is asked of the kind" is a sentence, not a call, so a name
        # has to carry an underscore before this gate reads it as code.
        others = {f for f in declared if "_" in f} - {path.stem}
        for title, body in _scenarios(text):
            named = sorted(f for f in others if re.search(rf"\b{f}\b", _prose(body)))
            if named:
                offences.append(f"{path}: {title} names {named}")
    assert not offences, (
        "a scenario belongs to one function and states a result, so naming another "
        "function's card makes it a description of a call:\n  " + "\n  ".join(offences)
    )


def test_no_scenario_names_a_private_function(features) -> None:
    """A leading underscore is not English. It is somebody's helper."""
    offences = []
    for path, text in features:
        for title, body in _scenarios(text):
            named = sorted(set(_PRIVATE.findall(_prose(body))))
            if named:
                offences.append(f"{path}: {title} names {named}")
    counted = collections.Counter(line.split(":")[0] for line in offences)
    assert counted == STILL_NAMING_CODE, (
        f"the ratchet says {dict(STILL_NAMING_CODE)} and the tree says {dict(counted)}; "
        "a file that improved comes off the list, and a new one does not go on it:\n  "
        + "\n  ".join(
            line
            for line in offences
            if counted[line.split(":")[0]] != STILL_NAMING_CODE.get(line.split(":")[0])
        )
    )
