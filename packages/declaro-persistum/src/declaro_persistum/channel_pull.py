"""What the primary has and this copy does not, brought down.

    reader fn channel_pull : (channel: Channel) -> NOTHING | Fault

IT IS A READER AND NOT A DOOR, AND THE DIFFERENCE IS WHO STARTS IT. Nothing
arrives on its own. The program asks, the primary answers, and what came back is
in the local copy afterwards. That is the world coming in, which is column one.

WHEN IT RUNS IS NOT THIS FUNCTION'S DECISION. `orchestrator fn session_connect`
pulls while opening, and on a cold open it waits for that pull because a copy
with no schema is unusable rather than merely stale. `orchestrator fn
lifecycle_catch_up` pulls on a free turn. This pulls whenever it is asked.

IT IS AWAITED, AND CALLING IT WITHOUT AWAITING IS THE TRAP THIS DRIVER SETS.
Measured 2026-09-24: `pull` hands back a coroutine and does nothing until it is
awaited. Called without, it returns in under a millisecond having brought
nothing down, and every read afterwards answers from a copy that never heard
from the primary. Awaited against the staging primary it takes about 2.2
seconds and the rows are there when it comes back.

NOTHING IS ANSWERED ABOUT WHAT CAME DOWN, and the driver does say. It answers
`True` when it applied changes and `False` when there were none, and this hands
back nothing either way, because a caller cannot do anything different with the
two: a pull that brought nothing down means this copy was already level, which
is the good case and not a quiet failure. Measured 2026-09-24, `False` came back
from a pull that did bring a table down, because the bootstrap had already
fetched it, so the flag answers about one mechanism rather than about the copy.
A caller that wants to know what changed reads the database.

THE CHANNEL IS A HANDLE. It is passed to the driver and never asked what it is.

A FAILURE COMES BACK IN THE RETURN, carrying what the driver said. A pull that
did not happen is a copy that is behind, and a caller opening a database needs
to hear that rather than read stale rows believing them current.
"""

from __future__ import annotations

from declaro_persistum.types import NOTHING, Channel, Fault


def _would_not_come_down(refused: BaseException) -> Fault:
    """What the driver said, on its way back as a value."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"Nothing could be brought down from the primary. The channel said: "
            f"{type(refused).__name__}: {refused}"
        ),
        what_to_do_instead=(
            "Check that the primary is reachable and that the token this "
            "database was opened with is still good. Until a pull gets through, "
            "this copy answers with whatever it had when it last heard from the "
            "primary."
        ),
    )


async def channel_pull(channel: Channel) -> None | Fault:
    """Bring down what the primary has and this copy does not.

    Nothing comes back when it worked, including when there was nothing new to
    bring. A pull the primary would not answer comes back as a `Fault` carrying
    what the channel said, in the return, never raised.
    """
    try:
        await channel.pull()
    except Exception as refused:  # noqa: BLE001
        return _would_not_come_down(refused)
    return NOTHING
