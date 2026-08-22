"""Every subcommand must survive being called the way `main` calls it.

WRITTEN BEFORE THE CODE, against a command that has never run once.

MEASURED 2026-08-17, against a real SQLite database and a real models file:

    $ declaro -c sqlite://.../probe.db -s .../models generate
    Error: cmd_generate() got an unexpected keyword argument 'verbose'
    exit=1

`main.py` passes `verbose=args.verbose` to all six commands. Five accept it.
`cmd_generate` does not, so the call raises TypeError before the function body
begins. There is no input that makes it work, and no test covered it.

BEHIND THAT SITS A SECOND ONE, which the first hides:

    cli/commands.py:528   sql_statements = applier.generate_sql(...)

    postgresql   has generate_sql
    sqlite       does NOT
    turso        does NOT

So fixing the signature moves the failure eleven lines down, onto the two
backends a developer is most likely to be using. Both are pinned below, because
fixing only the one you can see is how a command gets declared working twice.

THE GATE IS GENERAL, NOT A PATCH FOR TWO NAMES. It reads the keywords `main.py`
passes at each call site and compares them against the signature it calls, for
every command. A seventh command wired up wrongly fails here on the day it is
written rather than on the day somebody runs it.

WHAT IT CANNOT SEE, stated so its silence is not read as proof: a command whose
arguments all match and whose body is wrong. That is what the executing test at
the bottom is for, and it covers `generate` only.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
import subprocess
import sys

import pytest

from declaro_persistum.cli import commands
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit

MAIN = pathlib.Path(inspect.getfile(commands)).parent / "main.py"


def _call_sites() -> list[tuple[str, set[str], int]]:
    """Every `cmd_*(...)` call in main.py, with the keywords it passes.

    Read from the source rather than by running it, because running it is what
    the defect prevents.
    """
    tree = ast.parse(MAIN.read_text())
    found: list[tuple[str, set[str], int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = getattr(node.func, "id", None)
        if not name or not name.startswith("cmd_"):
            continue
        found.append((name, {k.arg for k in node.keywords if k.arg}, node.lineno))
    return found


def test_the_gate_can_see_the_call_sites_at_all() -> None:
    """A parser that finds nothing passes forever."""
    sites = _call_sites()
    assert len(sites) >= 6, [s[0] for s in sites]


@pytest.mark.parametrize(
    "site", _call_sites(), ids=lambda s: f"{s[0]}:{s[2]}"
)
def test_a_command_accepts_what_main_passes_it(site) -> None:
    """The measured defect, as a rule rather than as a name."""
    name, passed, lineno = site
    fn = getattr(commands, name)
    params = inspect.signature(fn).parameters

    unknown = passed - set(params)
    assert not unknown, (
        f"main.py:{lineno} calls {name}({', '.join(sorted(unknown))}=...) and "
        f"the signature has no such parameter, so the call raises TypeError "
        f"before the command begins. Accepted: {sorted(params)}"
    )


@pytest.mark.parametrize(
    "site", _call_sites(), ids=lambda s: f"{s[0]}:{s[2]}"
)
def test_a_command_is_given_everything_it_requires(site) -> None:
    """The other half. A missing required argument raises the same TypeError."""
    name, passed, lineno = site
    fn = getattr(commands, name)
    params = inspect.signature(fn).parameters

    required = {
        n
        for n, p in params.items()
        if p.default is p.empty and p.kind in (p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD)
    }
    missing = required - passed
    assert not missing, (
        f"main.py:{lineno} calls {name} without {sorted(missing)}, which the "
        f"signature requires"
    )


# ---------------------------------------------------------------------------
# The second defect, which the first one hides
# ---------------------------------------------------------------------------

APPLIER_NAMES = ["postgresql", "sqlite", "turso"]


def test_there_is_one_way_in_and_the_second_door_is_gone() -> None:
    """`create_applier` and `create_inspector` were a second dispatch table.

    THIS IS THE ARCHITECTURAL CAUSE OF THE TWO DEFECTS ABOVE, not a third
    defect beside them. There were two ways to reach a dialect:

        declaro_persistum.applier.apply(conn, ops, order, dialect)   the package
        applier.protocol.create_applier(dialect).apply(conn, ...)    the factory

    The factory returned the MODULE, annotated as a five-member Protocol that
    no module satisfied -- each was missing a different one:

        postgresql   no get_transaction_mode
        sqlite       no generate_sql
        turso        no generate_sql

    All three holes are closed as of 2026-08-19, by different routes.
    `generate_sql` moved to the package with a dispatch table, and the copy
    left behind in `applier/shared.py` was removed once coverage showed it
    dark. `get_transaction_mode` was added to the PostgreSQL applier, because
    `usage.md` documents importing it from there and that import raised
    ImportError for as long as the note above stood.

    A Protocol nothing satisfies constrains nothing, so `generate_sql` could go
    missing from two of three modules and be noticed by nobody until a user ran
    the command. Every caller of the factory used exactly three names, all of
    which the package already had or could hold in one place, so the second
    door and its unsatisfiable Protocol are deleted rather than repaired.
    """
    import importlib

    for module in ("applier", "inspector"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(f"declaro_persistum.{module}.protocol")


def test_no_source_file_still_reaches_for_the_factories() -> None:
    """Deleting a module a caller still imports moves the failure to import time."""
    root = pathlib.Path(inspect.getfile(commands)).parents[1]
    gone = {"create_applier", "create_inspector"}
    offenders: list[str] = []

    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                a.name in gone for a in node.names
            ):
                offenders.append(f"{path.relative_to(root)}:{node.lineno} import")
            elif isinstance(node, ast.Name) and node.id in gone:
                offenders.append(f"{path.relative_to(root)}:{node.lineno} call")

    assert not offenders, offenders


@pytest.mark.parametrize("dialect", APPLIER_NAMES)
def test_the_one_door_renders_sql_for_every_dialect(dialect: str) -> None:
    """The loop that only PostgreSQL had, exercised on all three.

    `generate_sql` was a member of one applier module out of three. It is one
    function now, so this asserts the outcome rather than the presence of a
    name: real SQL, for a real operation, on each engine.
    """
    from declaro_persistum.applier import generate_sql
    from declaro_persistum.differ import diff

    # FROM THE DIFFER, not from memory. My first version wrote
    # `{"type": "create_table", ...}` and the key is `op`, so every dialect
    # raised KeyError and the test measured my spelling.
    plan = diff(
        {},
        {
            "widgets": {
                "columns": {
                    "id": {"type": "text", "primary_key": True},
                    "name": {"type": "text", "nullable": False},
                }
            }
        },
        dialect=dialect, decisions=NOTHING)

    statements = generate_sql(plan["operations"], plan["execution_order"], dialect)

    assert len(statements) == 1, statements
    assert "CREATE TABLE" in statements[0].upper(), statements
    assert "widgets" in statements[0], statements


def test_an_unknown_dialect_is_named_rather_than_a_key_error() -> None:
    """The factory raised ValueError too. Deleting it must not lose that."""
    from declaro_persistum.applier import generate_sql

    with pytest.raises(ValueError) as caught:
        generate_sql([], [], "mysql")

    message = str(caught.value)
    assert "mysql" in message and "sqlite" in message, message


# ---------------------------------------------------------------------------
# Executed, because both checks above read source
# ---------------------------------------------------------------------------


def test_generate_emits_sql_against_a_real_database(tmp_path: pathlib.Path) -> None:
    """The command, run. This is the check that measured the defect.

    Both static gates above pass on a `generate` that runs and prints nothing.
    """
    # THE SCHEMA THE PACKAGE SHIPS, not one I invented. My first version of
    # this test wrote Pydantic models into the directory and `load_schema`
    # reads TOML, so the command failed for a reason that had nothing to do
    # with the defect under test.
    shipped = (
        pathlib.Path(inspect.getfile(commands)).parents[3]
        / "examples"
        / "todo_app_native"
        / "schema"
        / "todos.toml"
    )
    models = tmp_path / "schema"
    models.mkdir()
    (models / "todos.toml").write_text(shipped.read_text())

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
            "-c",
            # FOUR SLASHES. `sqlite:///` plus an absolute path gives three,
            # which the documented convention reads as RELATIVE, so the first
            # version of this asked for `var/folders/...` under the working
            # directory and got "unable to open database file".
            f"sqlite:///{tmp_path / 'probe.db'}",
            "-s",
            str(models),
            "generate",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, (
        f"declaro generate exited {result.returncode}\n"
        f"stderr: {result.stderr[-2000:]}"
    )
    assert "CREATE TABLE" in result.stdout.upper(), (
        f"the command succeeded and emitted no DDL for a table that does not "
        f"exist in the database:\n{result.stdout[:2000]}"
    )
    assert "todos" in result.stdout, result.stdout[:2000]

    # THE SQL HAS TO RUN. Containing the words CREATE TABLE is not the claim
    # the command makes; the claim is that this text migrates the database.
    #
    # MEASURED 2026-08-17. Against the schema this package ships, targeting
    # SQLite, `declaro generate` emitted
    #
    #     "id" TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
    #     "created_at" TEXT DEFAULT now(),
    #
    # and SQLite answered `OperationalError: near "(": syntax error`. Both are
    # PostgreSQL functions, passed through verbatim by the SQLite renderer. The
    # command reported success and exit 0.
    import sqlite3

    probe = sqlite3.connect(":memory:")
    try:
        probe.executescript(result.stdout)
    except sqlite3.Error as exc:
        raise AssertionError(
            f"declaro generate -d sqlite emitted SQL that SQLite refuses: "
            f"{exc}\n\n{result.stdout}"
        ) from exc


# ---------------------------------------------------------------------------
# The third defect in the same command: the two halves of the URL disagree
# ---------------------------------------------------------------------------
#
# MEASURED 2026-08-17. `detect_dialect` decides which engine a URL names, and
# `_connect` turns that URL into something the driver can open. They accept
# different vocabularies:
#
#     'sqlite://foo.db'        detect='sqlite'   opens='sqlite://foo.db'
#     'sqlite:///rel/app.db'   detect='sqlite'   opens='rel/app.db'
#     'sqlite:////abs/app.db'  detect='sqlite'   opens='/abs/app.db'
#
# The first row is the defect. `detect_dialect` says yes on a two-slash URL,
# `_connect` strips a three-slash prefix that is not there, and aiosqlite is
# handed the whole URL as a filename:
#
#     sqlite3.OperationalError: unable to open database file
#
# The user typed a URL this CLI's own detector accepted and is told the
# database cannot be opened. Nothing mentions the URL, so the search starts at
# permissions and disk and never reaches the two missing slashes.
#
# `_connect` used `connection_string.replace("sqlite:///", "")`, which is a
# replace rather than a prefix strip: it would also eat the sequence from the
# middle of a path. Parsing is one function now, and a form it cannot open is
# refused by name instead of being passed on as a filename.


SQLITE_URLS = [
    ("sqlite:///rel/app.db", "rel/app.db"),
    ("sqlite:////abs/app.db", "/abs/app.db"),
]


@pytest.mark.parametrize("url,expected", SQLITE_URLS, ids=lambda v: str(v))
def test_a_sqlite_url_becomes_the_path_it_names(url: str, expected: str) -> None:
    """Three slashes is relative, four is absolute, as the docs show."""
    from declaro_persistum.cli.commands import sqlite_path

    assert sqlite_path(url) == expected


def test_a_url_the_detector_accepts_and_the_driver_cannot_open_is_refused() -> None:
    """The measured mismatch, in one assertion.

    `sqlite://foo.db` is not a path with a prefix on it, so there is nothing
    to strip and nothing sensible to open. Refusing it names the form that
    works; passing it through names a file that does not exist.
    """
    from declaro_persistum.cli.commands import sqlite_path

    with pytest.raises(ValueError) as caught:
        sqlite_path("sqlite://foo.db")

    message = str(caught.value)
    assert "sqlite:///" in message, (
        f"the error does not show the form that works, so the reader learns "
        f"only that theirs is wrong: {message}"
    )


def test_the_prefix_is_stripped_and_not_replaced() -> None:
    """`.replace` eats the sequence wherever it occurs, including mid-path."""
    from declaro_persistum.cli.commands import sqlite_path

    assert sqlite_path("sqlite:////data/sqlite:///odd.db") == (
        "/data/sqlite:///odd.db"
    )


def test_the_detector_and_the_connector_accept_the_same_urls() -> None:
    """The property behind the defect, rather than the one case that showed it.

    Anything `detect_dialect` calls SQLite must be something `sqlite_path` can
    turn into a filename, or the CLI accepts input it cannot act on.
    """
    from declaro_persistum.cli.commands import sqlite_path
    from declaro_persistum.cli.main import detect_dialect

    candidates = [
        "sqlite://foo.db",
        "sqlite:///rel/app.db",
        "sqlite:////abs/app.db",
        "sqlite://",
    ]
    disagreements = []
    for url in candidates:
        try:
            detected = detect_dialect(url)
        except ValueError:
            continue  # refused at the front door, which is an honest answer
        if detected != "sqlite":
            continue
        try:
            sqlite_path(url)
        except ValueError:
            disagreements.append(url)

    assert not disagreements, (
        f"detect_dialect calls {disagreements} SQLite and sqlite_path refuses "
        f"them, so the CLI accepts a URL at the front door that it cannot open "
        f"at the back one"
    )
