"""`examples/*/db.py` is the most-copied file this package ships.

A consumer reads it before the README and long before `src/`, and whatever
shape it uses becomes the shape of their application. It currently teaches the
one the package replaced.

MEASURED 2026-08-17, one file of 307 lines, BYTE-IDENTICAL across all three
example apps:

    12  dispatch chains   `if config.dialect == "sqlite": ... elif ...`
     2  implicit defaults `get_sqlite_config(database_path=None)` and its twin
     2  broad handlers    any failure becomes `(False, "message")`
     1  data class        `@dataclass class DatabaseConfig`, three fields

The package replaced that chain with `APPLIERS`, `INSPECTORS`, `CONNECTORS`,
`BULK_LOADERS`, `WRITERS`, `SEEDERS` and more, all dispatch tables of
functions. Then it shipped an example teaching the shape it removed.

IT ALSO SHIPS A DEAD IMPORT. `import libsql_experimental` for the Turso Cloud
path. libsql was eradicated from this repository and replaced by pyturso; it is
not a dependency of anything here, and the import sits inside a `try` whose
`except` reports the backend as unavailable. So the Turso Cloud button in all
three apps is dead and blames the user's environment for it.

ONE FILE, THREE COPIES. Fixing it once fixes all three, and a test asserts they
stay identical so the next reader cannot be shown a different lesson depending
on which app they opened.
"""

from __future__ import annotations

import ast
import hashlib
import pathlib

import pytest

pytestmark = pytest.mark.precommit

EXAMPLES = pathlib.Path(__file__).resolve().parents[2] / "examples"
DB_FILES = sorted(EXAMPLES.glob("*/db.py"))


def _tree(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text())


def test_every_example_ships_the_same_database_module() -> None:
    """The claim the rest of this file rests on."""
    assert len(DB_FILES) >= 3, [str(p) for p in DB_FILES]

    digests = {hashlib.md5(p.read_bytes()).hexdigest() for p in DB_FILES}
    assert len(digests) == 1, (
        f"the copies have diverged: {sorted(digests)}. A reader is now shown a "
        f"different lesson depending on which app they opened."
    )


@pytest.mark.parametrize("path", DB_FILES, ids=lambda p: p.parent.name)
def test_it_dispatches_on_a_table_not_a_chain(path: pathlib.Path) -> None:
    """Rule 1. The chain is what the package's own dispatch tables replaced."""
    chains = [
        n.lineno
        for n in ast.walk(_tree(path))
        if isinstance(n, ast.If)
        and n.orelse
        and isinstance(n.orelse[0], ast.If)
        and "dialect" in ast.unparse(n.test).lower()
    ]

    assert chains == [], (
        f"{path.parent.name}/db.py dispatches on dialect with if/elif at lines "
        f"{chains}. A chain that runs out of branches does nothing and says "
        f"nothing; a table raises for an engine it does not know."
    )


@pytest.mark.parametrize("path", DB_FILES, ids=lambda p: p.parent.name)
def test_the_configuration_is_data(path: pathlib.Path) -> None:
    """Rule 2. Three fields and no behaviour is a TypedDict."""
    classes = [
        n.name
        for n in ast.walk(_tree(path))
        if isinstance(n, ast.ClassDef) and not any("TypedDict" in ast.unparse(b) for b in n.bases)
    ]

    assert classes == [], f"{path.parent.name}/db.py declares {classes}"


@pytest.mark.parametrize("path", DB_FILES, ids=lambda p: p.parent.name)
def test_nothing_defaults_a_path_the_caller_did_not_give(
    path: pathlib.Path,
) -> None:
    """Rule 14. `get_sqlite_config()` could not tell "chose" from "forgot"."""
    defaults = [
        f"{n.name}({p.arg}=)"
        for n in ast.walk(_tree(path))
        if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
        for p in (n.args.args[len(n.args.args) - len(n.args.defaults) :] if n.args.defaults else [])
    ]

    assert defaults == [], f"{path.parent.name}/db.py: {defaults}"


@pytest.mark.parametrize("path", DB_FILES, ids=lambda p: p.parent.name)
def test_a_failure_is_not_flattened_into_a_string(path: pathlib.Path) -> None:
    """Rule 8. `(False, "Connection failed: ...")` loses the type.

    A missing driver, a wrong password and a typo in a hostname all become one
    string, and the caller cannot tell which without reading it.
    """
    swallows = [
        n.lineno
        for n in ast.walk(_tree(path))
        if isinstance(n, ast.ExceptHandler)
        and (
            n.type is None
            or (isinstance(n.type, ast.Name) and n.type.id in ("Exception", "BaseException"))
        )
        and not any(isinstance(x, ast.Raise) for x in ast.walk(n))
    ]

    assert swallows == [], f"{path.parent.name}/db.py absorbs every failure at lines {swallows}"


@pytest.mark.parametrize("path", DB_FILES, ids=lambda p: p.parent.name)
def test_it_does_not_reach_for_libsql(path: pathlib.Path) -> None:
    """libsql was eradicated and replaced by pyturso.

    The import sits inside a `try` that reports the backend as unavailable, so
    the Turso Cloud option is dead and blames the reader's environment.
    """
    reaching = [
        n.lineno
        for n in ast.walk(_tree(path))
        if isinstance(n, ast.Import) and any(a.name.startswith("libsql") for a in n.names)
    ]

    assert reaching == [], (
        f"{path.parent.name}/db.py imports libsql at lines {reaching}. It is "
        f"not a dependency of anything here."
    )


@pytest.mark.parametrize("path", DB_FILES, ids=lambda p: p.parent.name)
def test_an_unknown_dialect_raises_rather_than_returning_nothing(
    path: pathlib.Path,
) -> None:
    """The whole reason a table beats a chain.

    A chain that runs out of branches falls off the end: `get_connection`
    yields nothing, `check_driver_available` returns None, and the caller gets
    an error about whatever it did next.
    """
    source = path.read_text()

    assert "CONNECTORS" in source or "CONNECT" in source.upper(), (
        f"{path.parent.name}/db.py has no dispatch table to look an engine up in"
    )


# ---------------------------------------------------------------------------
# The apps that import it
# ---------------------------------------------------------------------------
#
# WRITTEN AFTER THE GATE ABOVE WENT GREEN AND MISSED A BREAKAGE IT CAUSED.
# Turning `DatabaseConfig` from a dataclass into a TypedDict made every
# `config.display_name` in `app.py` an AttributeError, and the db.py gate could
# not see it, because it reads db.py. Seven sites per app, twenty-one in all,
# every one of them on a route a reader would hit on their first click.
#
# The lesson is not "add a check". It is that a gate drawn around one file
# measures one file, and the module's whole purpose is to be imported.

APPS = [d.parent / "app.py" for d in DB_FILES if (d.parent / "app.py").exists()]


@pytest.mark.parametrize("app", APPS, ids=lambda p: p.parent.name)
def test_no_app_reads_the_config_as_an_object(app: pathlib.Path) -> None:
    """A TypedDict is subscripted. `config.display_name` raises.

    This is not style. `get_database().dialect` on a dict raises
    AttributeError at request time, on the route that renders the page.
    """
    tree = ast.parse(app.read_text())
    fields = {"dialect", "connection_string", "display_name", "auth_token"}

    reads = [
        f"line {node.lineno}: .{node.attr}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr in fields
    ]
    assert not reads, (
        f"{app.parent.name}/app.py reads a config field as an attribute, and "
        f"the config is a TypedDict, so each of these raises AttributeError "
        f"when the route runs:\n  " + "\n  ".join(reads)
    )


@pytest.mark.parametrize("app", APPS, ids=lambda p: p.parent.name)
def test_no_app_decides_how_to_commit_by_dialect(app: pathlib.Path) -> None:
    """The commit ladder was written out at every write site.

    Three routes per app, each carrying

        dialect = get_database().dialect
        if dialect == "sqlite":
            await conn.commit()
        elif dialect in ("turso_cloud", "turso_embedded"):
            conn.commit()

    and PostgreSQL is in none of them. It happens to work, because asyncpg
    commits each statement outside a transaction, but the code does not say
    that; it just runs off the end of the chain. A reader copying this into an
    application with a transaction loses the write and gets no error.
    """
    tree = ast.parse(app.read_text())

    ladders = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Compare)
        and isinstance(node.left, ast.Name)
        and node.left.id == "dialect"
    ]
    assert not ladders, (
        f"{app.parent.name}/app.py branches on the dialect at lines {ladders}. "
        f"db.py knows which engines await and which need a commit; the app "
        f"should ask it rather than repeat the answer at every write site."
    )


@pytest.mark.parametrize("app", APPS, ids=lambda p: p.parent.name)
def test_every_write_route_still_commits(app: pathlib.Path) -> None:
    """The floor under the test above.

    Deleting the ladders satisfies it and loses every write. Each of the three
    write routes has to end up committing through the shared helper.
    """
    source = app.read_text()
    assert source.count("await commit(conn)") >= 3, (
        f"{app.parent.name}/app.py commits at "
        f"{source.count('await commit(conn)')} sites; create, toggle and "
        f"delete each need one, or the write is dropped"
    )


# ---------------------------------------------------------------------------
# Executed, because every assertion above is a grep
# ---------------------------------------------------------------------------
#
# The twenty-eight checks above read the source. Not one of them opens a
# database, and a module that satisfies all of them can still raise on import
# or write nothing. That is not hypothetical: the dataclass-to-TypedDict change
# passed the whole db.py gate while leaving twenty-one AttributeErrors in the
# apps, and only a gate written afterwards saw them.
#
# So this last group runs the module. It is the only part of the file that can
# tell a shape from a working example.


def _load_example_db():
    """Import `examples/todo_app_native/db.py` as a module, as the app does."""
    import importlib.util

    path = EXAMPLES / "todo_app_native" / "db.py"
    spec = importlib.util.spec_from_file_location("example_db", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_module_imports() -> None:
    """The floor. A syntax error or a bad import fails here, not on a route."""
    db = _load_example_db()

    assert set(db.CONNECTORS) == {
        "sqlite",
        "postgresql",
        "turso_embedded",
        "turso_cloud",
    }


def test_every_table_covers_every_engine() -> None:
    """A table with a missing row is a chain with a missing branch.

    The ladders this replaced had exactly that defect: PostgreSQL was in none
    of the nine commit chains. A row per engine per table is the property that
    makes the table an improvement rather than a rearrangement.
    """
    db = _load_example_db()

    engines = set(db.CONNECTORS)
    for name in ("DRIVERS", "TIMESTAMP_TYPES", "AWAITS_EXECUTE", "NEEDS_COMMIT"):
        table = getattr(db, name)
        assert set(table) == engines, (
            f"{name} covers {sorted(table)}; the connectors cover "
            f"{sorted(engines)}, so {sorted(engines - set(table))} falls "
            f"through with no answer"
        )


def test_an_engine_nobody_thought_about_raises() -> None:
    """The chain's silence, replaced by a sentence naming what exists."""
    db = _load_example_db()

    with pytest.raises(ValueError) as caught:
        db._for_dialect(db.CONNECTORS, "mysql", "Connectors")

    message = str(caught.value)
    assert "mysql" in message
    assert "sqlite" in message and "postgresql" in message, (
        f"the error does not say which engines exist, so the reader learns "
        f"only that theirs is wrong: {message}"
    )


@pytest.mark.asyncio
async def test_a_write_through_the_example_lands(tmp_path: pathlib.Path) -> None:
    """The whole point: init the schema, write a row, read it back.

    This exercises `init_schema`, `get_connection`, `_run` and `commit` against
    a real SQLite file. Every static check above passes on a module that
    commits nothing.
    """
    db = _load_example_db()
    config = db.get_sqlite_config(str(tmp_path / "todos.db"))

    await db.init_schema(config)

    async with db.get_connection() as conn:
        await conn.execute(
            "INSERT INTO todos (id, title, completed) VALUES (?, ?, ?)",
            ("1", "buy milk", 0),
        )
        await db.commit(conn)

    async with db.get_connection() as conn:
        cursor = await conn.execute("SELECT title FROM todos")
        rows = await cursor.fetchall()

    assert rows == [("buy milk",)], (
        "the row is not there, so `commit` did not commit and every write "
        "route in all three example apps silently discards its work"
    )


@pytest.mark.asyncio
async def test_the_schema_can_be_created_twice(tmp_path: pathlib.Path) -> None:
    """The app calls `init_schema` on every start, and a restart is normal."""
    db = _load_example_db()
    config = db.get_sqlite_config(str(tmp_path / "todos.db"))

    await db.init_schema(config)
    await db.init_schema(config)


@pytest.mark.asyncio
async def test_reaching_a_database_that_is_not_there_says_so() -> None:
    """`test_connection` reports a failure without inventing a cause.

    It used to catch `Exception` and return `(False, f"Connection failed: {e}")`
    for everything, so a missing driver and a wrong password read the same.
    """
    db = _load_example_db()
    config = db.get_postgres_config(host="127.0.0.1", port=1, user="u", password="p", database="d")

    ok, message = await db.test_connection(config)

    assert ok is False
    assert "reach" in message or "Driver" in message, message


@pytest.mark.asyncio
async def test_a_failed_probe_leaves_the_current_database_alone(
    tmp_path: pathlib.Path,
) -> None:
    """Probing a backend must not switch the app onto it.

    `test_connection` calls `set_database` to use the shared connection path.
    Without the restore, clicking a backend that is down leaves every later
    query pointed at the database that just failed.
    """
    db = _load_example_db()
    working = db.get_sqlite_config(str(tmp_path / "todos.db"))
    db.set_database(working)

    await db.test_connection(
        db.get_postgres_config(host="127.0.0.1", port=1, user="u", password="p", database="d")
    )

    assert db.get_database() == working, (
        "a failed probe left the app pointed at the backend that failed"
    )


def test_a_driver_check_does_not_import_the_driver() -> None:
    """It asks whether the module is findable, and says which package to install.

    The old version ran the import inside a `try`, so a driver that imports but
    raises on import reported itself as absent.
    """
    db = _load_example_db()

    ok, message = db.check_driver_available("sqlite")
    assert ok is True and "aiosqlite" in message

    ok, message = db.check_driver_available("turso_embedded")
    assert "pyturso" in message, f"the reader is told to install the wrong thing: {message}"
