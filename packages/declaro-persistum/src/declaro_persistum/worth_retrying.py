"""Whether a failure is worth another attempt.

THE DECISION IS ASKED OF THE KIND, NEVER OF THE MESSAGE. Two writers hit the
same row, one of them loses at commit, and trying again works. A column the
engine refused will be refused again just as fast, so handing it back at once
is the answer. Those are two different kinds of failure, and a `Fault` already
says which it is.

The prose a `Fault` carries is for a person to read. A conflict can arrive
saying "database is locked", and a refusal can arrive with the word conflict in
it, so a decision made by reading that prose is a decision made on a sentence
the engine chose. This module names no field of a `Fault` but `kind`.

FOUR KINDS, SO A TABLE AND NOT A BRANCH. `FaultKind` is a closed set of four
words, and the answer for each is written down once below. A kind that is not
one of the four raises a `KeyError` naming itself, which is what should happen
to a word nobody declared.
"""

from __future__ import annotations

from declaro_persistum.types import Fault, FaultKind

# WHAT EACH KIND IS WORTH. Only a conflict is worth going round again: it lost
# a race, and the next run is a different race. A refusal is a statement about
# the write itself and will not change. An exhausted allowance has already had
# every attempt it was given, and asking for one more would spend an allowance
# that is gone. A run cut short was stopped by someone, and starting it again
# is not the retrying loop's call to make.
_WORTH_ANOTHER_ATTEMPT: dict[FaultKind, bool] = {
    "conflict": True,
    "refused": False,
    "budget_spent": False,
    "cut_short": False,
}


def worth_retrying(fault: Fault) -> bool:
    """Whether this failure is worth another attempt."""
    return _WORTH_ANOTHER_ATTEMPT[fault["kind"]]
