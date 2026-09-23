"""What the crew did, in the count a caller reads.

    fn answer_from_receipt : (receipt: Receipt) -> Answer | Fault

`HOW-IT-SHOULD-WORK.md:166`: every write says how many rows it touched, and a
`change_all` that matched nothing returns zero, which is an answer you can act
on. So this is the write half of what `fn answer_from_rows` does for a read, and
both end as an `Answer`, which is what lets one door serve the whole vocabulary.

A CALLER NEVER SEES A TICKET. The ticket on the receipt is how the crew joined
the write to its outcome, and it stops here.

THE TWO FAILURES ARE NOT THE SAME AND THIS IS WHERE THEY PART.
`docs/architecture.md:152`: `ok: False` means one thing only, that the engine
stayed busy through the whole retry budget. That is a fault a caller can act on
by handing the write over again. A caller that read it as a refusal would give
up on the one write that would have worked.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Answer, Fault, Receipt


def _how_many_rows_it_touched(receipt: Receipt) -> Answer | Fault:
    """It landed, so the answer is the count."""
    return receipt["touched"]


def _the_failure_it_carries(receipt: Receipt) -> Answer | Fault:
    """It did not land, and the fault the crew wrote travels out unchanged.

    Unchanged because a refusal reworded twice is a refusal whose author nobody
    can find. What the crew found out is what the caller reads.
    """
    return receipt["failure"]  # type: ignore[return-value]


# Whether the write landed. Both rows are written out, so a receipt that failed
# is answered by a row rather than by a count nobody should act on.
_THE_ANSWER: dict[bool, Callable[[Receipt], Answer | Fault]] = {
    True: _how_many_rows_it_touched,
    False: _the_failure_it_carries,
}


def answer_from_receipt(receipt: Receipt) -> Answer | Fault:
    """Turn what the crew did into what the caller reads."""
    return _THE_ANSWER[receipt["ok"]](receipt)
