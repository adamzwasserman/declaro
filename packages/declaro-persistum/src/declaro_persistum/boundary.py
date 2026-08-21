"""Where this package touches the world.

Honest Code rule 4 puts I/O at the boundary and keeps the interior pure. That
only means something if the boundary is written down, because otherwise every
reader has to work out for themselves which functions are allowed to open a
socket and which have quietly started to.

`honest-check` reads this decorator by name. A function carrying it may open
connections, read files, take the clock and catch what those raise; a function
without it may not, and HC-P002, HC-P004 and HC-ST001 say so.

WHAT IT DOES AT RUNTIME IS NOTHING, and that is deliberate. It is a
declaration, not a wrapper: wrapping would put a frame between the caller and
the function, change the traceback an operator reads, and make an async
function's identity depend on the decorator getting that right.

WHAT COUNTS AS A BOUNDARY HERE. Four things, and nothing else:

  the CLI commands            an operator typed something
  opening a database          a socket or a file
  applying a migration        the statements actually run
  introspecting a database    the statements actually read

Everything else is given data and returns data. If a function needs this
decorator and is not one of those four, that is the finding, and moving the
I/O out is the fix rather than adding the marker.
"""

from collections.abc import Callable
from typing import TypeVar

F = TypeVar("F", bound=Callable[..., object])


def boundary(function: F) -> F:
    """This function is where the package meets the world.

    It may open a connection, read or write a file, take the clock, and catch
    what those raise.
    """
    return function


def helper(function: F) -> F:
    """Data in, data out. No I/O, no clock, no hidden state."""
    return function


def link(function: F) -> F:
    """One step of handling a request: takes the work and returns it changed."""
    return function


def recognizer(function: F) -> F:
    """Reads one value and says which declared kind it is, or that it is none."""
    return function


__all__ = ["boundary", "helper", "link", "recognizer"]
