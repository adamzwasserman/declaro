"""Local commits sent up the channel to the primary.

    boundary_out fn channel_push : (channel: Channel) -> NOTHING | Fault

IT IS NOT A WRITE AND IT DOES NOT RATION WRITES. `docs/architecture.md:23` puts
this plainly: the channel carries a log to the primary, and local writes go to
the journal concurrently and are shipped later. A measurement that a second
channel fails is a fact about the channel and says nothing about how many writes
can be in flight. Nothing here touches the journal.

WHEN IT RUNS IS NOT THIS FUNCTION'S DECISION. `orchestrator fn
lifecycle_shut_down` pushes on the way out, because the usual deployment has
disk that does not survive the process, and `orchestrator fn
lifecycle_catch_up` pushes on a free turn. This pushes whenever it is asked.

IT IS AWAITED, AND CALLING IT WITHOUT AWAITING IS THE TRAP THIS DRIVER SETS.
Measured 2026-09-24: `push` on pyturso's `ConnectionSync` hands back a coroutine
and does nothing until it is awaited. Called without, it raises nothing and
returns in under a millisecond, because a coroutine is truthy, so a shutdown
that pushed that way would report success and take every local commit with it.
Awaited against the staging primary it takes about 1.6 seconds, and a second
replica pulling afterwards sees the rows.

IT DOES NOT GO TO A THREAD, and `asyncio.to_thread` here is worse than useless:
the driver's own connect refuses outside a running loop, with "There is no
current event loop in thread". Awaiting is what keeps the loop free, which is
the standing requirement `persistum-nonblocking-cloud-open` set in 0.1.12.

THE CHANNEL IS A HANDLE. It is passed to the driver and never asked what it is.
`boundary_out fn session_open_channel` made it and this door does not know how.

A FAILURE COMES BACK IN THE RETURN, carrying what the driver said. A push that
did not land is local commits still local, which is exactly what shutdown needs
to hear about, and which it can do nothing about except say so.

WHAT IS CAUGHT IS EVERY ERROR AND NOT EVERY EXCEPTION. A primary that refuses, a
network that is not there and a token that expired are three types from two
libraries and they all mean one thing to a caller: these commits did not leave.
A cancelled shutdown raises `CancelledError`, which is not the primary refusing
anything and travels past untouched.
"""

from __future__ import annotations

from declaro_persistum.types import NOTHING, Channel, Fault


def _would_not_send(refused: BaseException) -> Fault:
    """What the driver said, on its way back as a value."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"Local commits could not be sent to the primary. The channel said: "
            f"{type(refused).__name__}: {refused}"
        ),
        what_to_do_instead=(
            "Check that the primary is reachable and that the token this "
            "database was opened with is still good. Until a push gets through, "
            "every commit made here is only here, and disk that does not survive "
            "the process loses them."
        ),
    )


async def channel_push(channel: Channel) -> None | Fault:
    """Send this copy's local commits to the primary.

    Nothing comes back when they landed, including when there was nothing to
    send. A push the primary would not take comes back as a `Fault` carrying
    what the channel said, in the return, never raised.
    """
    try:
        await channel.push()
    except Exception as refused:  # noqa: BLE001
        return _would_not_send(refused)
    return NOTHING
