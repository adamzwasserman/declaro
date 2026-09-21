"""How long a caller waits before it tries a write again.

    fn next_wait : (attempt: int, now: float, ticket: Ticket) -> float

WHAT THE FEATURE ASKS FOR, and it is two things. Each wait is longer than the
one before it, so a run of conflicts backs off instead of hammering the same
row. And two callers retrying in the same instant wait different amounts, so
they do not collide again on the next attempt for the same reason they collided
on this one.

WHERE THE SPREAD COMES FROM. Not from a random draw: this function reads no
clock and calls no generator, so the same three arguments always give the same
float and a test compares it with `==` rather than checking a range. The spread
comes from the ticket, which is one caller's claim on one deposited write and
therefore differs between callers by construction. Two callers reading the same
instant hold different tickets, and that is the only thing telling them apart.

WHY THE SPREAD CANNOT BREAK THE GROWTH. The wait is a doubling multiplied by
something in [1.0, 1.5]. Attempt n+1 starts at twice attempt n's doubling, and
attempt n can never reach more than one and a half times its own, so the next
wait is longer than this one for every pair of tickets and every instant. The
two claims do not trade against each other; both hold for all inputs.

WHY `now` IS IN THE SEED. The declaration hands this function the instant, and
`lifecycle_catch_up` reads the clock immediately before calling it. An argument
accepted and ignored is a silent one, so the instant goes into the seed beside
the ticket: one caller retrying a second time later in the day gets a different
offset from the one it got this morning. It changes nothing about the two
claims above, because both are stated for a fixed instant.
"""

from __future__ import annotations

import hashlib

from declaro_persistum.types import Ticket

# WHAT THE FIRST RETRY WAITS, in seconds, before any spread is added. Attempt 0
# waits between 50 and 75 milliseconds, attempt 1 between 100 and 150, and so
# on. This is a constant of the function and not a parameter with a default:
# nothing passes it, so nothing can forget to.
FIRST_WAIT_SECONDS = 0.05

# HOW WIDE THE SPREAD IS. The eight-byte digest is read as an integer and
# divided by 2**65, which lands it in [0.0, 0.5]. Half a doubling is enough to
# separate two callers and small enough that the growth above is arithmetic
# rather than a probability.
SPREAD_DIVISOR = float(1 << 65)


def next_wait(attempt: int, now: float, ticket: Ticket) -> float:
    """Work out how long to wait before the next attempt.

    `attempt` counts from 0 for the wait before the first retry. `now` is the
    instant the caller read from the clock. `ticket` is the caller's claim on
    the write it is retrying.
    """
    doubling = FIRST_WAIT_SECONDS * float(2**attempt)
    seed = f"{ticket}|{now!r}".encode()
    drawn = int.from_bytes(hashlib.blake2b(seed, digest_size=8).digest(), "big")
    spread = drawn / SPREAD_DIVISOR
    return doubling * (1.0 + spread)
