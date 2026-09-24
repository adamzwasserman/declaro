"""One connection put down, which is the last thing shutdown does.

    boundary_out fn session_close_connection : (connection: Connection) -> NOTHING | Fault

WHY ANYTHING CLOSES AT ALL. Every connection is an operating system thread, and a
crew holds one per drainer for the life of the database. A connection left open
is that thread left running after the process has said it is finished.

IT IS LAST, AND THAT ORDER IS NOT THIS FUNCTION'S. `docs/architecture.md:314`
ends shutdown with the close, after the push to the primary, because a
connection closed first is local commits that never left. Which connections
there are and what runs before them is `orchestrator fn lifecycle_shut_down`'s
whole job. This is handed one connection and knows nothing else.

THE CONNECTION IS A HANDLE. It is passed to the driver and never asked what
engine it belongs to, so there is no row per engine here and nothing to look up:
all three drivers close the same way, awaited, and an engine that did not would
be a row in a table this function does not need yet.

CLOSING ONE THAT IS ALREADY SHUT IS QUIET. Shutdown can be reached twice, by a
signal and then by an ordinary exit, and the second pass finds connections the
first one closed. Every driver here answers a second close by doing nothing, so
nothing has to be remembered and nothing has to be asked.

A FAILURE COMES BACK IN THE RETURN, carrying what the driver said. A close that
failed may be a connection still open, which an operator reading the shutdown
needs to know, and shutdown carries on to the next one either way.

WHAT IS CAUGHT IS EVERY ERROR AND NOT EVERY EXCEPTION. Three drivers raise three
unrelated types on a close and none of them shares a base with the others, so a
list here would be a list to keep in step with three libraries, and they all mean
the same thing to a caller: this connection did not close. So `Exception` is
caught and the type and message are quoted, which is the same reading
`boundary_out fn session_open_writer` gives a failed open.

`BaseException` WOULD BE WRONG AND IT IS THE EASY MISTAKE HERE. A shutdown task
cancelled part way through raises `CancelledError`, and a person pressing ctrl-C
raises `KeyboardInterrupt`; neither is a driver refusing anything, and reporting
either as a connection that would not close would tell an operator to go looking
for a thread that is not there. Both travel past this door untouched.
"""

from __future__ import annotations

from declaro_persistum.types import NOTHING, Connection, Fault


def _would_not_close(refused_by_the_driver: BaseException) -> Fault:
    """What the driver said, on its way back to shutdown as a value."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A connection would not close. The driver said: "
            f"{type(refused_by_the_driver).__name__}: {refused_by_the_driver}"
        ),
        what_to_do_instead=(
            "Nothing here. The connection may still be open, and the operating "
            "system thread behind it with it, until the process ends."
        ),
    )


async def session_close_connection(connection: Connection) -> None | Fault:
    """Close one connection, or say what the driver said when it would not.

    Nothing comes back when it closed, and a connection that was already shut
    closes again quietly.
    """
    try:
        await connection.close()
    except Exception as refused_by_the_driver:  # noqa: BLE001
        return _would_not_close(refused_by_the_driver)
    return NOTHING
