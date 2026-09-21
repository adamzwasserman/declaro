"""A test that cites the declaration must cite something that is still there.

THE FAILURE THIS EXISTS TO STOP. Step files explain themselves by pointing at
the architecture declaration: "`persistum.hd` line 684 declares this function".
A line number into a file that is still being written is a reference that rots
without saying so. The comment keeps reading like a fact, the test keeps
passing, and the next person follows the pointer to a blank line or, worse, to a
different function's card and believes it.

MEASURED 2026-09-21. Four step files carried a line citation each, all four
written that same day. All four were already wrong by the afternoon, because the
declaration was edited between writing them and reading them back:

    join_between           cited line 125, really at 922
    budget_spent           cited line 541, really at 846
    permitted_connections  cited line 684, really at 723  (now a blank line)
    next_in_order          cited line 779, really at 840

Hours, not months. A name does not move, so a citation is a name.

WHAT THIS CHECKS. Two things, and both are about pointers going stale rather
than about style. No test cites the declaration by line number. And every card
name a test quotes as `fn something`, `orchestrator fn something` or
`boundary_in fn something` is a card the declaration actually declares.

THIS IS THE SAME CLASS AS test_every_feature_is_bound.py and
test_no_step_is_unreachable.py. All three ask a question nobody thinks to ask,
about something that stays quiet while it is wrong.
"""

from __future__ import annotations

import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_TESTS = _ROOT / "tests"
_DECLARATION = _ROOT / "docs" / "design" / "persistum.hd"

# "persistum.hd line 684", "persistum.hd` line 684", "persistum.hd:684"
_BY_LINE = re.compile(r"persistum\.hd`?\s*(?:,\s*)?(?:line\s+\d+|:\d+)")

# A card as a test quotes it: `fn engine_of`, `orchestrator fn query_answer`,
# `boundary_out fn turso_push`. The backticks are what say it is a quotation
# rather than English, which keeps ordinary prose out of this.
_CITED_CARD = re.compile(
    r"`(?:(?:boundary_in|boundary_out|orchestrator|reader)\s+)?fn\s+([a-z_]+)`"
)

# A card as the declaration writes it.
_DECLARED_CARD = re.compile(
    r"^\s*(?:boundary_in|boundary_out|orchestrator|reader)?\s*fn\s+([a-z_]+)\s*:", re.M
)


@pytest.fixture(scope="module")
def sources() -> dict[pathlib.Path, str]:
    """Every test file, as path to text."""
    return {p: p.read_text(encoding="utf-8") for p in sorted(_TESTS.rglob("*.py"))}


@pytest.fixture(scope="module")
def declared_cards() -> frozenset[str]:
    """Every function the declaration declares."""
    return frozenset(_DECLARED_CARD.findall(_DECLARATION.read_text(encoding="utf-8")))


def test_no_test_cites_the_declaration_by_line_number(sources):
    """A line number into a file still being written goes stale in hours."""
    citing = {
        path.relative_to(_ROOT).as_posix(): sorted(set(found))
        for path, text in sources.items()
        if path.name != pathlib.Path(__file__).name and (found := _BY_LINE.findall(text))
    }

    assert not citing, (
        "these tests cite the declaration by line number, which stops being "
        "true the next time it is edited:\n"
        + "\n".join(f"    {name}  {cites}" for name, cites in sorted(citing.items()))
        + "\n\nCite the card by name instead: `fn permitted_connections` rather "
        "than a line. A name does not move."
    )


def test_every_card_a_test_cites_is_declared(sources, declared_cards):
    """A pointer to a card that was renamed away reads like a fact."""
    unknown = {
        path.relative_to(_ROOT).as_posix(): sorted(missing)
        for path, text in sources.items()
        if path.name != pathlib.Path(__file__).name
        and (missing := {name for name in _CITED_CARD.findall(text) if name not in declared_cards})
    }

    assert not unknown, (
        "these tests quote a card the declaration does not declare, so the "
        "pointer is to something renamed or removed:\n"
        + "\n".join(f"    {name}  {cards}" for name, cards in sorted(unknown.items()))
        + f"\n\nRead {_DECLARATION.relative_to(_ROOT)} for the name it has now."
    )


def test_this_gate_can_actually_see_both_sides(sources, declared_cards):
    """A discovery bug would make both tests above pass on an empty set."""
    assert len(sources) >= 40, (
        f"only {len(sources)} test files found under {_TESTS}; the gates above "
        "would pass by examining almost nothing"
    )
    assert len(declared_cards) >= 50, (
        f"only {len(declared_cards)} cards read from the declaration; if the "
        "spelling of a card changed, every citation would look unknown or "
        "every one would look fine, depending which way it broke"
    )
    # Four distinct cards were cited when this was written, measured rather than
    # guessed. A count alone would still pass if the reader broke and happened to
    # find four other things, so one citation that is known to exist is named.
    cited = {
        name
        for path, text in sources.items()
        if path.name != pathlib.Path(__file__).name
        for name in _CITED_CARD.findall(text)
    }
    assert len(cited) >= 4, (
        f"only {len(cited)} distinct cards are cited across every test; the "
        "reader is no longer finding them and the gate above measures nothing"
    )
    assert "permitted_connections" in cited, (
        "`fn permitted_connections` is cited in "
        "tests/bdd/steps/test_permitted_connections_steps.py and the reader did "
        f"not find it, so the citation gate is blind. It found: {sorted(cited)}"
    )
