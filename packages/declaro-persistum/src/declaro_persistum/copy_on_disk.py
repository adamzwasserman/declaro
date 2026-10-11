"""Whether a copy of the database is already on this machine.

    reader fn copy_on_disk : (target: str) -> bool side_effect reads "disk"

THE CALLING PROGRAM CANNOT KNOW, SO THE LIBRARY LOOKS. A deployment hands over a
path and a primary. Whether a file is at that path depends on whether this
machine is the one that ran yesterday, and the usual deployment has disk that
does not survive the process, so the answer changes between two runs of the same
image. A caller asked to say would be guessing, and a caller that guessed wrong
either waits on the primary for nothing or reads an empty database as the live
schema.

IT IS A READER AND NOT A DOOR. The program asks; nothing arrives on its own. It
stands with the doors, calls nothing, and heads no chain, and the orchestrator
that needs the answer calls it and hands the answer inward.

IT ANSWERS YES OR NO AND NEVER A FAULT. A path the process may not read holds no
copy this library can use, which is the same answer the caller acts on, so a
refusal would add nothing a caller could do differently. `Path.is_file` already
answers no for a missing file, a missing folder, a broken symlink and a folder
the process has no permission to look inside, because it catches the operating
system's error and reports the absence.

IT MAKES NOTHING. `boundary_out fn session_open_channel` makes the folder, since
the driver takes a path and will not make the directory around it. Asking
whether something is there is not opening it, and a reader that made a folder
would answer its own question yes on the next call.

IT ASKS THE DISK EVERY TIME AND REMEMBERS NOTHING. There is no store behind this
and the declaration declares none. A file deleted between two asks is two
different answers, which is the point: the one thing a caller wants to know is
what is on the disk now.

IT TAKES THE TARGET A CALLER WROTE AND READS THE PATH OUT OF IT, which
`boundary_out fn session_open_connection` and `boundary_out fn
session_open_channel` also do inside themselves. An output boundary is a terminus
and cannot invoke a card, which the explorer enforces by name, so a card for the
reading would be a fourth copy of it rather than one place for it. That is the
cost of the rule and it is written down here rather than worked around.

A TARGET NAMING A SERVER HAS NO COPY ON ANY DISK. `postgresql://host/name` reads
as a path of "host/name", which is nothing, so this answers no, and the answer is
right for the wrong reason. Only the two engines that live on a disk ask.

WHAT IT CANNOT SEE. A pyturso replica is four files, the database and its
changes, info and write-ahead log, and the open makes only the first: the other
three arrive with the first push, measured 2026-10-10. So a process that died
between the open and that push leaves a main file alone, and this answers yes
about a copy nothing can read. The caller is safe either way, because yes means
the copy gets pulled and a half-made copy needs pulling.
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable

# HOW A PATH IS READ OUT OF A TARGET, the same way the two doors read one:
# `str.partition` hands back the separator it found, which is `""` when there was
# none, so the two readings are a table keyed by that separator. One leading
# slash belongs to the `://`, so `sqlite:///app.db` is `app.db` and
# `sqlite:////var/app.db` is `/var/app.db`.
_THE_PATH_IN: dict[str, Callable[[str, str], str]] = {
    "": lambda target, _rest: target,
    "://": lambda _target, rest: rest.removeprefix("/"),
}


def copy_on_disk(target: str) -> bool:
    """Say whether a copy is already on this machine, without touching it."""
    _head, separator, rest = target.partition("://")
    return pathlib.Path(_THE_PATH_IN[separator](target, rest)).is_file()
