"""The injected callables must declare their shape (Rule 13).

`Database` exists to hold engine differences as data, and seven of its fields
are the callables that carry those differences. All seven were typed
`Callable[..., Any]`, which declares nothing: not the arity, not the argument,
not the return. A caller could pass a two-argument function where a
one-argument function is called and nothing would say so until it ran.

The cost is not hypothetical. `writers.py` already defines a `WriteOne`
Protocol saying exactly what a writer is, and `write_queue.py` already defines
`PendingWrite` saying exactly what a write is. Both were discarded at the point
of use: `Database["write_one"]` said `Callable[..., Any]` and `crew.drainer`
said `write: Any`. The declaration existed and the field ignored it.

`writers.py` carries the other half of the same failure. Its `Connection`
Protocol declares `execute` and nothing else, then two of the three writers
call `.commit()` and the third calls `.transaction()`. Three
`# type: ignore[attr-defined]` hold that gap open. The escape is what lets the
Protocol stay wrong: remove the escape and the Protocol has to tell the truth.

This is a ratchet, not a one-off. It reads the annotations rather than the
prose, so a field that quietly widens back to `Any` fails here.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.precommit


def _annotation(owner: type, field: str) -> str:
    """The annotation as written in the source.

    `from __future__ import annotations` keeps these as ForwardRefs, so the
    string is what the author typed.
    """
    value = owner.__annotations__[field]
    return getattr(value, "__forward_arg__", str(value))


def test_no_database_field_is_an_undeclared_callable() -> None:
    from declaro_persistum.database import Database

    vague = {
        name: _annotation(Database, name)
        for name in Database.__annotations__
        if _annotation(Database, name) == "Callable[..., Any]"
    }
    assert not vague, (
        f"{sorted(vague)} declare nothing but 'it is callable' — not the "
        f"arity, not the argument, not the return. `WriteOne` in writers.py "
        f"already says what a writer is and this field discards it"
    )


def test_the_write_lock_names_the_kind_of_lock_it_holds() -> None:
    from declaro_persistum.database import WriteLock

    assert _annotation(WriteLock, "lock") != "Any", (
        "WriteLock.lock is Any, so nothing says it must be acquirable; "
        "`writing` calls .acquire() and .release() on it"
    )


def test_the_replication_loop_names_what_it_waits_on() -> None:
    from declaro_persistum.database import replication_loop

    wanted = inspect.signature(replication_loop).parameters["wanted"]
    assert str(wanted.annotation) != "Any", (
        "replication_loop takes `wanted: Any` and calls .wait() and .clear() "
        "on it; those two calls are the whole contract and it is unwritten"
    )


def test_the_drainer_types_the_write_it_was_handed() -> None:
    from declaro_persistum import crew

    body = inspect.getsource(crew.drainer)
    assert "write: Any" not in body, (
        "drainer types the write as Any while `drain`, which calls it, "
        "declares Callable[[PendingWrite], Awaitable[Any]]. The shape is "
        "already written down one module away"
    )


def test_the_writers_need_no_escape_hatch() -> None:
    """Three ignores holding one wrong Protocol open.

    TOKENS, NOT LINES. A line scan counts the docstring sentence explaining why
    the escapes were removed as an escape, which is the same mistake that made
    the dead-code scan report docstring mentions as live calls. Only a real
    comment token is a real escape.
    """
    import io
    import tokenize

    from declaro_persistum import writers

    source = inspect.getsource(writers)
    offenders = [
        token.string
        for token in tokenize.generate_tokens(io.StringIO(source).readline)
        if token.type == tokenize.COMMENT
        and token.string.startswith("# type: ignore")
    ]
    assert not offenders, (
        f"{len(offenders)} type escapes in writers.py: {offenders}. Each one "
        f"is a call the Connection Protocol does not admit, so the Protocol "
        f"is wrong and the escape is what keeps it wrong"
    )
