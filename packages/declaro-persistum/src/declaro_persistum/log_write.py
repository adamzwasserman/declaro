"""The one place this package writes a line, and the only line it writes.

WHO THIS IS FOR. `docs/architecture.md` says a failure comes back as a value and
never as a log line, because a function that logs an error and hands you
something you cannot tell from success has two outputs and only one of them is
in its signature. So nothing that a caller can act on arrives here. What arrives
is what a person watching a running process needs to see and no caller is
waiting for: a shutdown that has been pushing to the cloud for ninety seconds, a
write whose caller let go of its ticket before the drainer got to it.

IT CANNOT FAIL BACK AT ANYBODY. `boundary_out fn log_write` returns NOTHING with
no fault arm, and nothing here raises. A caller reaching this is usually already
carrying a failure, and a writer that threw while reporting one would lose the
failure and replace it with its own.

AN EVENT IS FACTS, NOT A SENTENCE. It takes one dict whose values are a string,
a number, a bool or nothing, so there is no exception in the vocabulary and no
formatted message. That is what closed the defect this exists for: the writer it
replaced rendered each keyword as `name=value` and appended it to the message,
so a caller using the standard library's own keyword for "attach the exception
being handled" had those words written down as text and the exception thrown
away. Two call sites did it and neither ever carried a cause.

NOTHING IS HANDED TO THE STANDARD LIBRARY AS A RECORD ATTRIBUTE. Every fact goes
into the message. `message`, `name`, `levelname` and a dozen others are names a
`LogRecord` keeps for itself, and handing one over raises rather than writes. A
caller naming a fact `message` is naming a fact, so it reaches the line like any
other.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

from declaro_persistum.types import NOTHING, Value

# THE NAME AN OPERATOR CONFIGURES. One logger for the package, so a deployment
# turns this up or down in one place and the library installs no handler of its
# own: where the line goes is the host application's decision.
LOGGER_NAME = "declaro_persistum"
_LOG = logging.getLogger(LOGGER_NAME)

# The key a caller sets to say how loudly this should be said. It is the one
# name in an event this function reads; every other name is a fact it carries.
LEVEL = "level"

# THE WHOLE VOCABULARY. A level that is not a row here is not a level. The keys
# are typed as anything a caller may name, because what arrives is a `Value` and
# the question this table answers is whether that value is a level.
NAMED_LEVELS: dict[Value, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

# A CALLER WHO NAMED NO LEVEL IS A ROW, NOT A DEFAULT. `fn progress_line` builds
# a shutdown event out of a database, a time and whether the push finished, and
# names no level at all. That is a case worth writing down rather than a hole to
# be filled on the way past, so absence sits in the table beside the words.
LEVELS: dict[Value, int] = {NOTHING: NAMED_LEVELS["info"], **NAMED_LEVELS}

# What an event with no `level` key in it says when the two are merged. Written
# this way round so the read below needs no fallback.
NO_LEVEL_NAMED: dict[str, Value] = {LEVEL: NOTHING}

# WHERE A REFUSED LINE GOES. A caller naming a level nobody declared is a defect
# in that caller, and dropping the line would hide both the defect and whatever
# the caller was trying to say. So it still gets written, and it gets written
# loudly enough that somebody sees it.
UNUSABLE_LEVELS_GO_TO = "error"

REFUSED = "level_refused"
THE_LEVELS_THERE_ARE = "the levels there are"


def _naming_the_levels(named: Value) -> dict[str, Value]:
    """The two facts a refused line carries beside whatever the caller said."""
    return {REFUSED: named, THE_LEVELS_THERE_ARE: ", ".join(str(word) for word in NAMED_LEVELS)}


def _nothing_to_add(_named: Value) -> dict[str, Value]:
    """A level the vocabulary holds needs no explaining."""
    return {}


# Whether the level the caller named is one there is. Both rows are here, so a
# level nobody declared is answered by a row rather than by a default.
WHAT_THE_LEVEL_ADDS: dict[bool, Callable[[Value], dict[str, Value]]] = {
    True: _nothing_to_add,
    False: _naming_the_levels,
}


def _as_words(facts: dict[str, Value]) -> str:
    """Every fact on one line, each as the name the caller gave it and its value."""
    return " ".join(f"{name}={value}" for name, value in facts.items())


def log_write(event: dict[str, Value]) -> None:
    """Write one event where an operator will read it afterwards.

    The level the caller named decides how loudly; every other name in the event
    is a fact, and all of them reach the line.
    """
    named = {**NO_LEVEL_NAMED, **event}[LEVEL]
    recognised = named in LEVELS
    _LOG.log(
        LEVELS[{True: named, False: UNUSABLE_LEVELS_GO_TO}[recognised]],
        _as_words({**event, **WHAT_THE_LEVEL_ADDS[recognised](named)}),
    )
