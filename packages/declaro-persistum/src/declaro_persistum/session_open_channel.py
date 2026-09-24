"""The one line to the primary, opened and held for the life of the database.

    boundary_out fn session_open_channel : (target: str, primary: str, token: str) -> Channel | Fault

THERE IS EXACTLY ONE, AND THAT IS A FACT ABOUT THE CHANNEL AND NOTHING ELSE.
Measured 2026-08-12 and recorded in `docs/design/mvcc-connection-rule.md`: a
second sync connection to one replica answers `database tape error: database is
busy`, three runs in four outright. `docs/architecture.md:23` says what that
does and does not bound: it bounds the channel, and says nothing about how many
writes can be in flight, because a write goes to the local journal and the
channel ships it later.

IT TAKES THREE THINGS AND INVENTS NONE OF THEM. The local path to replicate
into, the primary's address, and the token. `boundary_out fn
session_open_connection` refuses a replicated Turso target for exactly this
reason: it is handed one of the three and a local path it chose would be a
deployment decision nobody made, a token read from the environment would be
hidden state. This door is the one that is handed all three.

IT MAKES THE COPY IF THERE IS NOT ONE. `bootstrap_if_empty` is the driver's own
answer to a fresh disk, which is the deployment this library is written for: the
first open on a new machine has no local file and the whole database comes down.
`orchestrator fn session_connect` is what decides whether to wait for that.

THE FOLDER IS MADE FIRST, because the driver takes a path and not a directory,
and a path whose folder does not exist is a refusal a caller cannot act on: the
deployment asked for a database at that path and making the folder is part of
putting one there.

IT CANNOT BE PUT ON A THREAD, WHICH IS WORTH SAYING BECAUSE IT LOOKS LIKE IT
SHOULD BE. `turso.aio.sync.connect` copies the whole database on a cold open, so
a thread reads as the obvious answer. `asyncio.to_thread` raises "There is no
current event loop in thread" from inside the driver, measured 2026-09-24. It
belongs on the loop, and what keeps the loop free is that everything it hands
back is awaited.

WHAT IT HANDS BACK IS AWAITED THROUGHOUT. `push`, `pull` and `checkpoint` on
that connection hand back coroutines and do nothing until awaited: called
without, each returns in under a millisecond having done nothing at all, which
is a shutdown reporting success with every commit still local.

THE CHANNEL IS PROVED BEFORE IT IS HANDED BACK, AND THAT PULL IS NOT OPTIONAL.
Measured 2026-09-24: `connect` hands back a `ConnectionSync` for a primary that
does not exist, with no error and nothing different about the object. Every push
and pull on it then raises `AttributeError: 'NoneType' object has no attribute
'pull'`, because the driver never built its own sync half and never said so. A
door that handed that back would be a database that looks replicated, writes
happily for a day, and loses everything at the first shutdown, with a message
about NoneType where the primary's name should be.

So one pull runs here. On a cold open it is the bootstrap the deployment needs
anyway. On a warm open it costs a round trip, about 2 seconds against the
staging primary, and whether to wait for that is
`orchestrator fn session_connect`'s decision rather than this door's.

A FAILURE COMES BACK IN THE RETURN, naming the primary. A channel that did not
open is a database that cannot replicate, which the caller opening it has to
hear about rather than discover at shutdown when the writes do not leave.
"""

from __future__ import annotations

import pathlib

from declaro_persistum.types import Channel, Fault

# HOW A PATH IS READ OUT OF A TARGET, the same way `fn engine_of` reads a scheme
# and `boundary_out fn session_open_connection` reads a path: `str.partition`
# hands back the separator it found, which is `""` when there was none, so the
# two readings are a table keyed by that separator.
_THE_PATH_IN: dict[str, object] = {
    "": lambda target, _rest: target,
    "://": lambda _target, rest: rest.removeprefix("/"),
}


def _could_not_be_opened(primary: str, refused: BaseException) -> Fault:
    """A primary that is named and not reachable, in the driver's own words."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The channel to {primary} could not be opened. The driver said: "
            f"{type(refused).__name__}: {refused}"
        ),
        what_to_do_instead=(
            f"Check that {primary} names a primary this process can reach and "
            f"that the token is still good. A database opened without a channel "
            f"can be written to, and nothing it holds will ever leave this "
            f"machine."
        ),
    )


def _the_path_in(target: str) -> str:
    """Where the copy lives on disk, out of the target the caller gave."""
    _head, separator, rest = target.partition("://")
    return _THE_PATH_IN[separator](target, rest)  # type: ignore[operator, no-any-return]


def _open_it(path: str, primary: str, token: str) -> Channel:
    """The driver's own call, on the loop it insists on being on."""
    import turso.aio.sync

    # THE FOLDER FIRST. The driver takes a path and will not make the directory
    # around it, and a fresh machine has neither.
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    return turso.aio.sync.connect(path, primary, auth_token=token, bootstrap_if_empty=True)


async def session_open_channel(target: str, primary: str, token: str) -> Channel | Fault:
    """Open the one channel between this copy and its primary.

    `target` says where the copy lives, `primary` where it replicates from, and
    `token` is what the primary will accept. What comes back is the driver's own
    object, held for the life of the database and passed to `fn channel_push`
    and `fn channel_pull` unopened.
    """
    try:
        channel = _open_it(_the_path_in(target), primary, token)
        await channel.pull()
    except Exception as refused:  # noqa: BLE001
        return _could_not_be_opened(primary, refused)
    return channel
