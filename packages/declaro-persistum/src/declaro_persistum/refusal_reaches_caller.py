"""Decide whether a fault is raised to its caller or handed back as a receipt.

A caller that cannot tell those apart retries the write that can never succeed
and gives up on the one that would have worked. So `query_deliver` asks this
before it hands anything back.

ASKED OF THE KIND, NEVER OF THE MESSAGE. A message is written for a person to
read, it changes with the driver and the engine, and a decision that reads one
is a decision no test can pin. `FaultKind` names four words and this table
answers all four, so there is nothing to branch on and nothing left over.

The two that are raised are the two the engine did not stay busy for. `refused`
is the request itself being wrong, and only an edit fixes it. `cut_short` is the
platform killing the work part way, which leaves the write's fate unsettled; a
receipt saying "the engine was busy" would be a false account of it.

The two that come back as a receipt are the two that are contention. A receipt
whose `ok` is false says the engine stayed busy and says nothing else, which is
exactly what `conflict` and `budget_spent` are. The caller may deposit again.
"""

from declaro_persistum.types import Fault, FaultKind

# Which faults reach the caller as a raise. The keys are every word in
# `FaultKind`, so a fault of any declared kind gets an answer here.
RAISED_TO_THE_CALLER: dict[FaultKind, bool] = {
    "refused": True,
    "cut_short": True,
    "conflict": False,
    "budget_spent": False,
}


def refusal_reaches_caller(fault: Fault) -> bool:
    """True when this fault is raised to the caller, false when it is a receipt."""
    return RAISED_TO_THE_CALLER[fault["kind"]]
