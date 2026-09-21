"""Bind `tests/bdd/features/opening/engine_of.feature`.

WHAT THE FEATURE ASKS FOR. Six of its scenarios hand `engine_of` a string and
read the word that comes back. The other six ask harder questions: that the
function opened nothing, sent nothing, read no clock and no environment, that
no function anywhere in the package works an engine out of a connection, and
that a refusal comes back as a value with nothing written to a log.

THE TABLE IS READ, NOT RETYPED. One scenario says "every target in the table
above". Copying those six rows into this file would put the table in two places
and let them drift without either copy looking wrong, so `_the_table()` parses
the Examples block out of the feature file itself.

HOW THE NEGATIVE SCENARIOS ARE ANSWERED, and what each answer cannot see:

  Opened nothing, sent nothing. `socket.socket`, `socket.create_connection` and
  `builtins.open` are replaced with a function that raises, and the call runs
  inside that. It catches a real reach for the outside. It does not see a C
  extension that bypasses all three, which is why the purity scan below runs as
  well and reads what the module is even allowed to import.

  Read no clock, no random source, no environment. The module's own source is
  parsed and every imported module and every name it mentions is compared with
  the words those readings are spelled with. Reading one module's source cannot
  see a helper in another module, so the scan also asserts that the only things
  `engine_of.py` imports at all are `typing`, `collections.abc` and this
  package's own `types`. Those three cannot reach the world, so the horizon is
  closed rather than merely noted.

  No function decides an engine by reading a connection. Every function in
  `src/declaro_persistum` whose return names `Engine` is parsed, and each one
  must take no connection and must never touch `__module__`, `__class__`,
  `__name__` or `type()`. That is the mechanism `docs/architecture.md` refuses
  by name, and the step fails loudly when it finds no such function at all,
  since a scan with nothing in it passes while asserting nothing.
"""

from __future__ import annotations

import ast
import builtins
import logging
import pathlib
import socket
from collections.abc import Callable
from typing import Any

import pytest
from pytest_bdd import parsers, scenarios, then, when

scenarios("../features/opening/engine_of.feature")


_HERE = pathlib.Path(__file__).resolve()
_FEATURE = _HERE.parents[1] / "features" / "opening" / "engine_of.feature"
_SOURCE = _HERE.parents[3] / "src" / "declaro_persistum"
_THE_MODULE = _SOURCE / "engine_of.py"


def _the_table() -> tuple[tuple[str, str], ...]:
    """The Examples rows of the feature, read off the feature itself."""
    rows = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in _FEATURE.read_text(encoding="utf-8").splitlines()
        if line.strip().startswith("|")
    ]
    return tuple((row[0].strip('"'), row[1]) for row in rows[1:])


# WHAT READING THE WORLD IS SPELLED, compared by whole name and never by
# substring, so `os` does not match `close` and `time` does not match `runtime`.
A_READING_OF_THE_WORLD = frozenset(
    {
        "time",
        "datetime",
        "date",
        "monotonic",
        "perf_counter",
        "random",
        "secrets",
        "uuid",
        "uuid4",
        "os",
        "environ",
        "getenv",
        "open",
        "read_text",
        "socket",
        "urlopen",
        "input",
    }
)

# EVERYTHING `engine_of.py` IS ALLOWED TO IMPORT. None of the three can reach a
# clock, a file, the network or the environment, so a module confined to them
# cannot read the world through a helper either.
WHAT_A_PURE_MODULE_MAY_IMPORT = frozenset(
    {"typing", "collections", "declaro_persistum", "__future__"}
)

# WHAT A CONNECTION IS CALLED, by annotation and by argument name. The first
# list is read off `declaro_persistum.types`; the second catches the untyped
# and loosely typed ones an annotation check walks straight past.
A_CONNECTION_BY_TYPE = ("Connection", "Cursor", "Handle")
A_CONNECTION_BY_NAME = ("connection", "conn", "cursor", "handle", "driver", "session")

# READING WHERE A CLASS LIVES. `docs/architecture.md` refuses this in its own
# words: the closest a connection gets to naming its engine is the module its
# class happens to sit in, which makes our SQL depend on somebody else's file
# layout.
WHERE_A_CLASS_LIVES = ("__module__", "__class__", "__name__", "__qualname__", "type")


@pytest.fixture
def answer() -> dict[str, Any]:
    """Where a When leaves what came back and a Then reads it."""
    return {}


@pytest.fixture(autouse=True)
def log_lines() -> Any:
    """Every record any logger emits while one scenario runs.

    Autouse and not asked for by name, because it has to be in place before the
    When calls the function, and the Then that reads it runs long after.
    """
    written: list[logging.LogRecord] = []
    catch_everything = logging.Handler(level=0)
    catch_everything.emit = written.append  # type: ignore[method-assign]
    root = logging.getLogger()
    was = root.level
    root.setLevel(0)
    root.addHandler(catch_everything)
    yield written
    root.removeHandler(catch_everything)
    root.setLevel(was)


def _with_the_outside_refused(monkeypatch: pytest.MonkeyPatch, run: Callable[[], Any]) -> Any:
    """Run `run()` with every ordinary way out of the process wired to raise."""

    def _refuse(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("engine_of reached outside the process")

    monkeypatch.setattr(socket, "socket", _refuse)
    monkeypatch.setattr(socket, "create_connection", _refuse)
    monkeypatch.setattr(socket, "getaddrinfo", _refuse)
    monkeypatch.setattr(builtins, "open", _refuse)
    return run()


# --- what a target says -----------------------------------------------------


@when(parsers.re(r'engine_of is given "(?P<target>[^"]*)"$'))
def _given_one_target(answer: dict[str, Any], target: str) -> None:
    from declaro_persistum.engine_of import engine_of

    answer["it"] = engine_of(target)


@when(parsers.re(r'engine_of is given "(?P<one>[^"]*)" and "(?P<other>[^"]*)"$'))
def _given_two_targets(answer: dict[str, Any], one: str, other: str) -> None:
    from declaro_persistum.engine_of import engine_of

    answer["both"] = (engine_of(one), engine_of(other))


@then(parsers.re(r"it answers (?P<engine>sqlite|postgresql|turso)$"))
def _it_answers(answer: dict[str, Any], engine: str) -> None:
    assert answer["it"] == engine


@then("it decided on the absence of a scheme, not on the file extension")
def _it_decided_on_the_absence_of_a_scheme() -> None:
    from declaro_persistum.engine_of import engine_of

    # None of these is a database file by its name, and every one of them is a
    # bare path, so every one of them is SQLite.
    bare = ("app.db", "app", "data.txt", "/var/lib/app/whatever", "./notes")
    assert [engine_of(one) for one in bare] == ["sqlite"] * len(bare)
    # And a target that names a scheme is read by that scheme, whatever the
    # path on the end of it looks like.
    assert engine_of("postgresql://localhost/app.db") == "postgresql"


@then(parsers.re(r"it answers (?P<engine>sqlite|postgresql|turso) to both$"))
def _it_answers_to_both(answer: dict[str, Any], engine: str) -> None:
    assert answer["both"] == (engine, engine)


@then("a file that does not exist yet answers the same as one that does")
def _a_missing_file_answers_the_same(tmp_path: pathlib.Path) -> None:
    from declaro_persistum.engine_of import engine_of

    made = tmp_path / "made.db"
    made.write_text("")
    never_made = tmp_path / "never-made.db"
    assert made.exists()
    assert not never_made.exists()
    assert engine_of(str(made)) == "sqlite"
    assert engine_of(str(never_made)) == "sqlite"


# --- what it must not do ----------------------------------------------------


@then("nothing was opened, so a host that does not exist changes no answer")
def _nothing_was_opened(monkeypatch: pytest.MonkeyPatch) -> None:
    from declaro_persistum.engine_of import engine_of

    said = _with_the_outside_refused(
        monkeypatch, lambda: engine_of("postgresql://unreachable.invalid/todo")
    )
    assert said == "postgresql"


@when("engine_of is given every target in the table above")
def _given_every_target(answer: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    from declaro_persistum.engine_of import engine_of

    answer["table"] = _with_the_outside_refused(
        monkeypatch, lambda: [(target, engine_of(target)) for target, _ in _the_table()]
    )
    answer["the outside was refused"] = True


@then("no socket was opened and no packet was sent")
def _no_socket_was_opened(answer: dict[str, Any]) -> None:
    assert answer["the outside was refused"] is True
    assert _the_table(), "the Examples table read as empty, so this asserted nothing"
    assert answer["table"] == [(target, engine) for target, engine in _the_table()]


@when("engine_of is given the same target twice")
def _given_the_same_target_twice(answer: dict[str, Any]) -> None:
    from declaro_persistum.engine_of import engine_of

    answer["twice"] = (
        engine_of("postgresql://localhost/todo"),
        engine_of("postgresql://localhost/todo"),
    )


@then("both answers are the same")
def _both_answers_are_the_same(answer: dict[str, Any]) -> None:
    first, second = answer["twice"]
    assert first == second == "postgresql"


@then("it read no clock, no random source and no environment variable")
def _it_read_none_of_those() -> None:
    tree = ast.parse(_THE_MODULE.read_text(encoding="utf-8"))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        (node.module or "").split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    reaches_further = imported - WHAT_A_PURE_MODULE_MAY_IMPORT
    assert not reaches_further, (
        f"engine_of.py imports {sorted(reaches_further)}, so reading its own source "
        "no longer proves what it reads: a helper one module away can read the clock"
    )

    named = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)} | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    read = sorted(named & A_READING_OF_THE_WORLD)
    assert not read, f"engine_of.py names {read}, which is a reading of the world"


def _deciders() -> list[tuple[pathlib.Path, ast.FunctionDef | ast.AsyncFunctionDef]]:
    """Every function in the package whose answer is an engine."""
    found = []
    for path in sorted(_SOURCE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        found += [
            (path, node)
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.returns is not None
            and "Engine" in ast.unparse(node.returns)
        ]
    return found


def _arguments(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.arg]:
    spec = node.args
    named = spec.posonlyargs + spec.args + spec.kwonlyargs
    return named + [one for one in (spec.vararg, spec.kwarg) if one is not None]


@when("every function in this package that decides an engine is read")
def _read_every_decider(answer: dict[str, Any]) -> None:
    answer["deciders"] = _deciders()


@then("none of them takes a connection object")
def _none_takes_a_connection(answer: dict[str, Any]) -> None:
    deciders = answer["deciders"]
    assert deciders, (
        f"no function under {_SOURCE} answers with an Engine, so this scenario "
        "examined an empty set and would pass however the package was written"
    )
    offenders = [
        f"{path.name}: {node.name}({arg.arg})"
        for path, node in deciders
        for arg in _arguments(node)
        if any(named in arg.arg.lower() for named in A_CONNECTION_BY_NAME)
        or any(
            named in ast.unparse(arg.annotation or ast.Constant(""))
            for named in A_CONNECTION_BY_TYPE
        )
    ]
    assert not offenders, f"these decide an engine from a connection: {offenders}"


@then("none of them matches on the module a driver's class happens to live in")
def _none_reads_where_a_class_lives(answer: dict[str, Any]) -> None:
    deciders = answer["deciders"]
    assert deciders, "the scan found no engine-deciding function, so it asserted nothing"
    reads = [
        (path, node.name, spot.attr)
        for path, node in deciders
        for spot in ast.walk(node)
        if isinstance(spot, ast.Attribute) and spot.attr in WHERE_A_CLASS_LIVES
    ] + [
        (path, node.name, spot.id)
        for path, node in deciders
        for spot in ast.walk(node)
        if isinstance(spot, ast.Name) and spot.id in WHERE_A_CLASS_LIVES
    ]
    offenders = [f"{path.name}: {where} reads {spelled}" for path, where, spelled in reads]
    assert not offenders, (
        f"a driver reorganizes its own package and these answers change, silently: {offenders}"
    )


# --- what it refuses, by name -----------------------------------------------


@then("it is refused")
def _it_is_refused(answer: dict[str, Any]) -> None:
    it = answer["it"]
    assert isinstance(it, dict), f"expected a Fault, got {it!r}"
    assert it["kind"] == "refused"


@then("the refusal names sqlite, postgresql and turso")
def _the_refusal_names_the_three(answer: dict[str, Any]) -> None:
    said = " ".join(answer["it"].values()).lower()
    missing = [name for name in ("sqlite", "postgresql", "turso") if name not in said]
    assert not missing, f"the refusal never names {missing}: {answer['it']}"


@then("the refusal lists the schemes that would have worked")
def _the_refusal_lists_the_schemes(answer: dict[str, Any]) -> None:
    instead = answer["it"]["what_to_do_instead"].lower()
    schemes = ("sqlite://", "postgresql://", "postgres://", "libsql://")
    missing = [scheme for scheme in schemes if scheme not in instead]
    assert not missing, f"what_to_do_instead never spells {missing}: {instead}"


@then("it is refused as ambiguous")
def _it_is_refused_as_ambiguous(answer: dict[str, Any]) -> None:
    it = answer["it"]
    assert isinstance(it, dict), f"expected a Fault, got {it!r}"
    assert it["kind"] == "refused"
    assert "ambiguous" in it["what_went_wrong"].lower()


@then("the refusal says an https URL is not enough to name an engine")
def _the_refusal_says_https_is_not_enough(answer: dict[str, Any]) -> None:
    went_wrong = answer["it"]["what_went_wrong"].lower()
    assert "https" in went_wrong
    assert "not enough" in went_wrong


@then("the refusal says a target is required")
def _the_refusal_says_a_target_is_required(answer: dict[str, Any]) -> None:
    it = answer["it"]
    assert "target" in it["what_went_wrong"].lower()
    assert "required" in " ".join(it.values()).lower()


@then("what comes back is a failure a caller can read and hand on")
def _a_failure_a_caller_can_read(answer: dict[str, Any]) -> None:
    it = answer["it"]
    # It came back rather than being raised: the When called `engine_of` with
    # nothing around it and this step is running, so nothing was thrown.
    assert isinstance(it, dict), f"expected a Fault, got {it!r}"
    assert set(it) == {"kind", "what_went_wrong", "what_to_do_instead"}
    assert all(isinstance(said, str) and said for said in it.values())
    # A caller can hand it on unchanged, which is what a plain dict of strings is.
    assert dict(it) == it


@then("nothing was written to a log")
def _nothing_was_written_to_a_log(
    log_lines: list[logging.LogRecord], capsys: pytest.CaptureFixture[str]
) -> None:
    # EVERY RECORD ANY LOGGER SAW, NARROWED TO THE ONES THIS PACKAGE WROTE.
    # pytest-bdd logs its own step resolution at DEBUG, so the handler catches
    # a dozen records that have nothing to do with the function. A record is
    # ours when the file that emitted it is under `src/declaro_persistum`,
    # which is true of a bare `logging.info(...)` as much as of a named logger,
    # so nothing escapes by choosing a logger name.
    ours = [record for record in log_lines if str(_SOURCE) in record.pathname]
    assert [record.getMessage() for record in ours] == []
    printed = capsys.readouterr()
    assert printed.out == ""
    assert printed.err == ""
