"""The four grammars the specification names are the four the table holds.

WHAT THIS CATCHES. `HOW-IT-SHOULD-WORK.md` names four ways to phrase a question
and `asking_in_each_grammar.py` holds one row each. Those two can drift in
either direction, and neither drift announces itself: a fifth grammar in the
document with no row is a grammar nobody tests, and a row for a grammar the
document does not name is a surface nobody agreed to.

IT READS THE DOCUMENT, not a copy of it. A list of four names written here
would be a third place the same fact lives, which is the drift this exists to
stop, one level up.

THE ROWS ARE NOT BUILT AND THAT IS NOT WHAT THIS CHECKS. Every call in the
table fails today, because `Person.objects` and the rest do not exist. This
asks whether the table is complete, which is answerable now and is worth
answering now: the shape of the surface is decided before it is written.
"""

from __future__ import annotations

import pathlib
import re

import pytest

from tests.bdd.steps.asking_in_each_grammar import ASKS, GRAMMARS

pytestmark = pytest.mark.precommit

_SPEC = pathlib.Path(__file__).resolve().parents[2] / "HOW-IT-SHOULD-WORK.md"

# READ ONCE, HERE, at the edge of the module. The reader below takes the text
# rather than the path, so it is a pure function of what it was handed and a
# test can hand it something else.
_AS_THE_DOCUMENT_READS = _SPEC.read_text()

# How the specification writes each one, and the key the table uses for it.
# Two spellings of one thing, which is why this mapping is here rather than
# assumed: the document says "Pseudo-SQL" in a heading and the table says
# "pseudo-sql" because a Gherkin Examples column reads better lowercase.
_AS_WRITTEN = {
    "Pseudo-SQL": "pseudo-sql",
    "Django": "django",
    "Prisma": "prisma",
    "SQLAlchemy": "sqlalchemy",
}


def _named_in(text: str) -> set[str]:
    """Every grammar the given text introduces in bold, as table keys."""
    bold = set(re.findall(r"\*\*([A-Za-z-]+)\*\*, (?:the library's own|for people who)", text))
    return {_AS_WRITTEN[name] for name in bold if name in _AS_WRITTEN}


def _named_in_the_document() -> set[str]:
    """The same question asked of the specification as it stands."""
    return _named_in(_AS_THE_DOCUMENT_READS)


def test_the_document_still_introduces_its_grammars_the_same_way() -> None:
    """The floor. Without this, an empty match would make the test below pass."""
    found = _named_in_the_document()
    assert len(found) == 4, (
        f"the specification introduces {len(found)} grammars in the shape this "
        f"reads for, not 4: {sorted(found)}. Either the document changed how it "
        f"introduces them, or a grammar was added or removed."
    )


def test_every_grammar_the_document_names_has_a_row() -> None:
    """A grammar nobody can ask in is a grammar nobody tests."""
    missing = _named_in_the_document() - set(GRAMMARS)
    assert not missing, (
        f"the specification names {sorted(missing)} and the table has no row "
        f"for it, so every scenario outline over grammars silently skips it"
    )


def test_every_row_names_a_grammar_the_document_has() -> None:
    """A row nobody agreed to is a surface that arrived without a decision."""
    extra = set(GRAMMARS) - _named_in_the_document()
    assert not extra, (
        f"the table holds {sorted(extra)} and the specification names no such "
        f"grammar, so something is being tested that nothing agreed to"
    )


def test_every_row_can_ask_everything_the_others_can() -> None:
    """A row missing one question is a grammar that answers less than its peers."""
    short = {
        name: [ask for ask in ASKS if not callable(row.get(ask))] for name, row in GRAMMARS.items()
    }
    short = {name: missing for name, missing in short.items() if missing}
    assert not short, f"these grammars cannot ask what the others can: {short}"
