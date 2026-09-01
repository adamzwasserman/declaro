"""The one place this package says anything out loud.

TWO RULES, AND THE SECOND IS THE ONE THAT MATTERS.

A FAILURE IS RETURNED, NEVER WRITTEN. A function that logs an error and hands
back a result its caller cannot tell apart from success has two output
channels, and only one of them is in the signature. The caller cannot see the
error, cannot test for it without capturing output, and cannot decline it.
`_refuse_an_invalid_schema` logged every error AND returned them; the log was
a second copy nobody could reach.

ANYTHING ELSE GOES THROUGH `say`. Thirteen modules each called
`logging.getLogger(__name__)` and wrote directly, so thirteen places touched
the world and every function that logged did I/O in the middle of its work.
There is one now. A caller of `say` is not doing I/O any more than a caller of
`connection.execute` is: it is calling a named function of ours, and that
function is the edge.

WHAT THAT BUYS. One place decides the level, the wording and the destination,
and one place can be silenced. A test can read what was said without capturing
a stream. And the audit can see a single boundary rather than thirteen.

WHAT IT IS NOT. This is not a logging framework and does not want to become
one. It writes through the standard library, keeps the module name of the
caller so existing configuration still filters by it, and stops there.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from declaro_persistum.boundary import helper

# The levels this package uses, by the name a caller writes. Read with `[]`,
# so a level nobody declared raises here rather than being written at some
# other level or swallowed.
_LEVELS: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
}


@helper
def _named(module: str) -> logging.Logger:
    """The logger for one module, so existing configuration still filters."""
    return logging.getLogger(module)


@helper
def say(module: str, level: str, message: str, *args: Any, **facts: Any) -> None:
    """Write one line about something that happened.

    `module` is `__name__` from the caller, which keeps the standard library's
    own filtering working: anyone who configured `declaro_persistum.transfer`
    to be quiet still gets a quiet transfer.

    `args` are the standard library's own lazy formatting: a message with
    `%s` in it and its values, which are not interpolated unless the line is
    actually written. That is worth keeping, so it passes straight through.

    `facts` are the newer form, named rather than positional, for a caller
    who writes what happened and lets the wording be decided here.

    NO `error` LEVEL, on purpose. A failure is a value a function returns. If
    this had one, the first thing anybody would do with it is log a failure
    instead of returning it, which is the shape this module exists to remove.

    NO EXCEPTION EITHER. A caller with a cause to record calls `say_because`.
    """
    _A_FACT_IS_A_FACT[bool(facts.keys() & _LOGGING_KEYWORDS)](facts)
    _write(module, level, message, args, facts, None)


@helper
def say_because(
    module: str,
    level: str,
    message: str,
    cause: BaseException,
    *args: Any,
    **facts: Any,
) -> None:
    """Write one line about a failure, carrying the exception that caused it.

    THIS EXISTS BECAUSE `say(..., exc_info=True)` LOOKED LIKE IT WORKED. Every
    keyword `say` takes becomes "name=value" text, so `exc_info` -- which is
    the standard library's own word for "attach the exception being handled"
    -- was written out as the four characters T-r-u-e and the exception was
    thrown away. Two call sites did that, and the comment above one of them
    said "exc_info=True keeps the type and the traceback". It kept neither.

    Measured 2026-08-29 against a real Turso primary: a push failed, the
    warning read "... exc_info=True", and nothing anywhere held the type, the
    message or the traceback of what had actually gone wrong.

    The cause is a positional parameter rather than a keyword, so it cannot be
    forgotten and cannot be confused with a fact.
    """
    _A_FACT_IS_A_FACT[bool(facts.keys() & _LOGGING_KEYWORDS)](facts)
    _write(module, level, message, args, facts, cause)


# The names the standard library's own `log` reads. A caller who passes one of
# these as a fact means the standard library's behavior and will not get it,
# so it is refused rather than rendered as text.
_LOGGING_KEYWORDS: frozenset[str] = frozenset({"exc_info", "stack_info", "stacklevel", "extra"})


@helper
def _facts_are_facts(_facts: dict[str, Any]) -> int:
    """Nothing here means anything to the standard library."""
    return 0


@helper
def _refuse_a_logging_keyword(facts: dict[str, Any]) -> int:
    """A name the standard library owns cannot be carried as text."""
    taken = ", ".join(sorted(facts.keys() & _LOGGING_KEYWORDS))
    raise TypeError(
        f"say does not take {taken}: the standard library owns that name, and "
        f"passing it here would write it out as text instead. Call "
        f"say_because(module, level, message, cause) to record an exception."
    )


_A_FACT_IS_A_FACT: dict[bool, Callable[[dict[str, Any]], int]] = {
    False: _facts_are_facts,
    True: _refuse_a_logging_keyword,
}


@helper
def _write(
    module: str,
    level: str,
    message: str,
    args: tuple[Any, ...],
    facts: dict[str, Any],
    cause: BaseException | None,
) -> None:
    """The one call to the standard library. `cause` is None when there is none."""
    detail = " ".join(f"{name}={value}" for name, value in facts.items())
    _named(module).log(_LEVELS[level], f"{message} {detail}".rstrip(), *args, exc_info=cause)
