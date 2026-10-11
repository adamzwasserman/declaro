"""Whether a copy of the database is already on this machine.

    reader fn copy_on_disk : (path: str) -> bool side_effect reads "disk"

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

WHAT IT CANNOT SEE. A pyturso replica is four files, the database and its
changes, info and write-ahead log, and deleting a subset breaks the next open.
This asks about the one path it is handed, so a main file whose sidecars are gone
answers yes. Reading that as a whole replica is a horizon and not a claim: the
card takes one path, and the four-file question belongs to whoever opens them.
"""

from __future__ import annotations

import pathlib


def copy_on_disk(path: str) -> bool:
    """Say whether a file is already at this path, without touching it."""
    return pathlib.Path(path).is_file()
