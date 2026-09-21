"""Whether the retry budget is gone.

`docs/design/persistum.hd` line 778: `fn budget_spent : (attempts: int, budget:
int) -> bool`.

WHY THIS IS A FUNCTION OF ITS OWN. Running out of retries is an answer, not a
failure to report later. `FaultKind` in `types.py` keeps `budget_spent` apart
from `refused` for that reason: a caller who retried as many times as allowed
learns something different from a caller whose request was wrong. Asking that
question in one place means the loop in `lifecycle_catch_up` cannot drift into
a second, slightly different, idea of when to stop.

WHY IT TAKES THE BUDGET RATHER THAN KEEPING ONE. A default here would make the
bound invisible: a caller who forgot to say how many retries they wanted would
be indistinguishable from one who chose this number, and the number would be
untested in every deployment that never set it. The budget arrives from the
declaration that opened the database, so it is a parameter with no default and
omitting it is a TypeError.

WHY IT READS NOTHING. No clock, no random source, no environment, no file, no
connection. The waiting and the jitter are `next_wait` and `clock_read`; this
answers only whether there is another attempt to make, and two integers are
the whole of what that needs.

WHY `>=` AND NOT `>`. A budget of three permits three attempts. After the
third, there is nothing left, so three attempts against a budget of three is
spent. The feature asserts both sides of that edge.
"""

from __future__ import annotations


def budget_spent(attempts: int, budget: int) -> bool:
    """True when `attempts` has used up `budget`, so no attempt remains."""
    return attempts >= budget
