"""The statements, as values, and the reads that inform them.

Split out of migrations.py at 1604 lines. Every function here returns
SQL or reads the database; none of them decides whether a migration
should run. That decision lives in `migrations`.
"""

import hashlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, cast

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.database import (
    Database,
)
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.pydantic_loader import (
    load_models_from_module,
)
from declaro_persistum.reporting import say
from declaro_persistum.types import (
    NOTHING,
    Dialect,
    Nothing,
    for_dialect,
)

META_TABLE = "_declaro_meta"

logger = logging.getLogger(__name__)


@helper
def _for_ddl(db: Database) -> Any:
    """The database's own DDL door. Never a read, and not always a write.

    This called `writing(db)` for every backend, which is right for three of
    the four cases and silently wrong for the fourth. On a LOCAL Turso
    database `writing` is an MVCC connection, and a table created on one is
    invisible to any other connection that has already read — the migration
    reports success and a live reader goes on seeing the old schema.

    Measured 2026-08-14, pyturso 0.7.2, the other connection open throughout:

        A writes, then DDL                  the other connection is fine
        the other connection READ first     "Parse error: no such table: t"
        ALTER instead of CREATE             fine
        the other connection WROTE first    fine

    A prior read is the trigger, and that row fails every run.

    THE FIX IS NOT A BRANCH HERE. On a replicated database `writing` is the
    only correct answer: the held connection is the one bound to the primary,
    and `migrating` would open a fresh non-sync connection whose DDL never
    leaves the machine. PostgreSQL and SQLite want `writing` too. So the door
    is a property of the database, settled at open, and this function asks the
    value rather than deciding for it.

    On a local Turso database the door is `migrating`, which is WAL and needs
    the file to itself. If another connection is holding a read, it raises
    "database is locked" instead of migrating. That is the point: a loud,
    true failure in place of a silent one, and it is what `crew.py` already
    requires when it says migration must finish before a crew starts.
    """
    return db["for_ddl"](db)


# ---------------------------------------------------------------------------
# Schema hash helpers (skip-if-clean)
# ---------------------------------------------------------------------------


@helper
def _refuse_a_directory(schema_path: Path) -> Path:
    """A DIRECTORY EXISTS, SO `exists()` WAS THE WRONG QUESTION.

    The caller's guard is `schema_path.exists()`, which a models directory
    passes, and `read_bytes` then raised `IsADirectoryError` three frames down
    -- naming a function the caller never called instead of the argument it
    got wrong. Measured 2026-08-17.

    The mistake is the likely one, not an exotic one: this package has both
    shapes, and `docs/usage.md` shows a `models/` directory holding `users.py`
    and `orders.py`. So the refusal names the other function.
    """
    raise ValueError(
        f"{schema_path} is a directory. `apply_migrations_async` hashes a "
        f"single models file. For a directory of them, load it with "
        f"`load_schema_from_models` and pass the file you want stamped."
    )


@helper
def _the_schema_file(schema_path: Path) -> Path:
    """One file, which is what this module hashes and stamps."""
    return schema_path


_WHEN_A_DIRECTORY: dict[bool, Callable[[Path], Path]] = {
    True: _refuse_a_directory,
    False: _the_schema_file,
}


@boundary
def _compute_schema_hash(schema_path: Path, version: str) -> str:
    """Compute SHA-256 hex digest of (schema file bytes, NUL, version) — pure.

    ``version`` is the declaro_persistum version string the caller wants
    mixed into the hash so that any version bump invalidates the
    skip-if-clean cache. Passing it explicitly (rather than reading the
    module-level constant inside this function) keeps the hash a pure
    function of its arguments — testable with explicit version values, no
    monkeypatching of module globals required.

    Mixing the version in is what makes loader/applier fixes propagate to
    existing deployments: a buggy version's stored "clean" hash differs
    from the new version's computed hash, so the runner performs a fresh
    introspection on first startup after upgrade.
    """
    # A models directory is refused by name rather than by an IsADirectoryError
    # three frames down. `_refuse_a_directory` carries the measurement.
    file = _WHEN_A_DIRECTORY[schema_path.is_dir()](schema_path)

    h = hashlib.sha256()
    h.update(file.read_bytes())
    h.update(b"\x00")  # delimiter so file content cannot collide with version
    h.update(version.encode("ascii"))
    return h.hexdigest()


@helper
def _dialect_needs_orphan_recovery(dialect: Dialect) -> bool:
    """True iff this dialect uses the sqlite_master-based temp-table scheme
    that `_recover_orphaned_tmp_tables` was written for.

    Postgres reconstruction does not produce ``_declaro_tmp_*`` tables and
    has no ``sqlite_master`` system table, so running the recovery scan on
    a Postgres database crashes startup with UndefinedTableError.
    """
    return dialect in ("sqlite", "turso")


@helper
def partition_replica_operations(
    operations: list[dict[str, Any]], execution_order: list[int]
) -> tuple[list[int], list[dict[str, Any]]]:
    """Partition migration ops for an embedded replica.

    Reconstruction ops (add/drop FK, alter column) can't replicate through the
    replication engine, so they are deferred to ``declaro migrate-remote``. Returns
    ``(safe_order, skipped)`` where ``safe_order`` is ``execution_order`` filtered
    to safe ops, and ``skipped`` describes each deferred op as ``{"op", "table"}``
    so callers can see what was NOT applied instead of mistaking a no-op for
    success.
    """
    from declaro_persistum.applier.shared import requires_reconstruction

    safe_order = [
        op_idx for op_idx in execution_order if not requires_reconstruction(operations[op_idx])
    ]
    skipped = [
        {"op": operations[op_idx]["op"], "table": operations[op_idx].get("table")}
        for op_idx in execution_order
        if requires_reconstruction(operations[op_idx])
    ]
    return safe_order, skipped


# =============================================================================
# The statements, as values
# =============================================================================
#
# EVERY FUNCTION BELOW RETURNS SQL AND RUNS NOTHING, and that split is a fix
# for a measured failure. Five mutations were made to this module and run
# against two suites:
#
#     mutation                              dict double   real database
#     _stamp_key drops the hash  (Python)   CAUGHT        CAUGHT
#     ON CONFLICT -> DO NOTHING  (SQL)      missed        CAUGHT
#     ON CONFLICT removed        (SQL)      missed        CAUGHT
#     key stops being PRIMARY KEY (DDL)     missed        CAUGHT
#     value becomes nullable      (DDL)     missed        CAUGHT
#
# The double caught the Python mutation and missed every SQL one, because the
# statements were built and executed in the same function and were therefore
# never values anybody could look at. The only test seam left was the
# connection, and the connection is what gives SQL its meaning, so a double
# there is a re-implementation of a database.
#
# `applier/shared.py` has had this shape all along: fourteen functions that
# return SQL, none that execute it, all tested by equality.
#
# THE DIALECT IS A TABLE, NOT `hasattr(conn, "fetch")`. Each function below
# used to carry that branch inline: one arm for asyncpg, one for everything
# else. "Everything else" is not a decision, so a third engine took the SQLite
# arm in silence. An engine that is not in the table now raises.

_PLACEHOLDER = {"sqlite": "?", "postgresql": "$1"}


@helper
def meta_table_sql() -> str:
    """The stamp table. One statement, both engines take it unchanged.

    `key TEXT PRIMARY KEY` is the upsert's conflict target. Without it every
    restamp inserts a row instead of updating one, and two services sharing a
    database accumulate a row per boot.
    """
    return (
        f'CREATE TABLE IF NOT EXISTS "{META_TABLE}" ('
        f"key TEXT PRIMARY KEY, "
        f"value TEXT NOT NULL, "
        f"updated_at TEXT NOT NULL"
        f")"
    )


# honest-code-allow: L1.21.1 - stored_hash_sql, _the_database_holds_tables and temp_table_name share a shape only once the names are erased: one builds SQL from a placeholder, one returns True as the do-nothing half of a pair, one makes a name with a random tail. They answer unrelated questions and one function over a table would need a row whose only column is the whole body
@helper
def stored_hash_sql(dialect: str) -> str:
    """Read one stamp by key. The engines differ only in the placeholder."""
    return f'SELECT value FROM "{META_TABLE}" WHERE key = {_placeholder(dialect)}'


_STORE_HASH = {
    "postgresql": (
        f'INSERT INTO "{META_TABLE}" (key, value, updated_at) '
        f"VALUES ($1, $2, $3) "
        f"ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = $3"
    ),
    "sqlite": (
        f'INSERT INTO "{META_TABLE}" (key, value, updated_at) '
        f"VALUES (?, ?, ?) "
        f"ON CONFLICT (key) DO UPDATE SET "
        f"value = excluded.value, updated_at = excluded.updated_at"
    ),
}


_USER_TABLES = {
    "postgresql": (
        f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
        f"AND tablename != '{META_TABLE}'"
    ),
    "sqlite": (
        f"SELECT name FROM sqlite_master "
        f"WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        f"AND name != '{META_TABLE}'"
    ),
}


# THE TWO PER-DIALECT BUILDERS, one row each. Both had the same body -- look
# the statement up in a per-dialect table and hand it back -- so the table was
# the only thing that differed, and it is data now.
#
# THE DOCSTRING IS IN THE ROW because it carried the reason, and the reason is
# what a reader came for. `DO UPDATE` rather than `DO NOTHING` is the whole
# point of the first one, and it would be lost in a shared body.
_PER_DIALECT: dict[str, tuple[dict[str, str], str]] = {
    "store_hash_sql": (
        _STORE_HASH,
        "Write one stamp, updating it if the key is already there.\n\n"
        "    `DO UPDATE`, never `DO NOTHING`. Both keep the row count at one,\n"
        "    and only the first refreshes the stamp, so `DO NOTHING` leaves a\n"
        "    database reporting a boot that never happened.\n\n"
        "    asyncpg numbers its parameters and repeats them by number; SQLite\n"
        "    reads the row it was about to insert, through `excluded`. The\n"
        "    clause is the same clause.\n    ",
    ),
    "_placeholder": (
        _PLACEHOLDER,
        "How one engine marks a parameter.\n\n"
        "    A fragment rather than a whole statement, and the same question:\n"
        "    asyncpg numbers its parameters and SQLite marks them.\n    ",
    ),
    "user_tables_sql": (
        _USER_TABLES,
        "Any table that is not ours.\n\n"
        "    `_declaro_meta` is excluded because it is ours: a database holding\n"
        "    only it is empty, and counting it would make every fresh database\n"
        "    look populated. SQLite's own `sqlite_%` tables are excluded for the\n"
        "    same reason, and `sqlite_sequence` appears the moment any table\n"
        "    uses AUTOINCREMENT.\n    ",
    ),
}


def _looks_up(name: str) -> Callable[[str], str]:
    """Build the function that reads one statement table by dialect."""

    table, purpose = _PER_DIALECT[name]

    @helper
    def ask(dialect: str) -> str:
        return cast(str, for_dialect(table, dialect, "Statement builders"))

    ask.__name__ = name
    ask.__qualname__ = name
    ask.__doc__ = purpose
    return ask


_placeholder = _looks_up("_placeholder")
store_hash_sql = _looks_up("store_hash_sql")
user_tables_sql = _looks_up("user_tables_sql")


@helper
def _dialect_of(conn: Any) -> str:
    """asyncpg or not, decided once rather than in every statement.

    This is still a shape check rather than a declared dialect, and it is the
    boundary: `apply_migrations` is handed a connection and no dialect. What
    changed is that the answer is computed here, once, instead of inside each
    of five functions where a third engine would have taken the SQLite arm in
    silence.
    """
    return "postgresql" if hasattr(conn, "fetch") else "sqlite"


# HOW A STATEMENT IS RUN, PER ENGINE. asyncpg takes its parameters as
# positional arguments and commits inside its own transaction scope; aiosqlite
# and pyturso take a tuple and commit when told. Neither is "the other one", so
# each is an entry and an engine that is in no entry raises.


@boundary
async def _run_on_postgres(conn: Any, sql: str, params: tuple[Any, ...]) -> None:
    """asyncpg spreads the parameters and needs no commit of ours."""
    await conn.execute(sql, *params)


@boundary
async def _run_on_dbapi(conn: Any, sql: str, params: tuple[Any, ...]) -> None:
    """aiosqlite and pyturso take a tuple, and the write is ours to commit."""
    await conn.execute(sql, params)
    await conn.commit()


_WRITERS: dict[str, Callable[[Any, str, tuple[Any, ...]], Awaitable[None]]] = {
    "postgresql": _run_on_postgres,
    "sqlite": _run_on_dbapi,
}


@helper
async def _ensure_meta_table(conn: Any) -> None:
    """Run `meta_table_sql`. The statement is asserted in its own test."""
    write = for_dialect(_WRITERS, _dialect_of(conn), "Statement writers")
    await write(conn, meta_table_sql(), ())


@helper
def _stamp_key(schema_name: str, schema_hash: str) -> str:
    """Build the skip-if-clean stamp key — pure.

    The hash is part of the key, not only the value. The key used to be the
    schema file's name alone, so two services migrating one database wrote
    the same row. They disagree whenever their hashes differ — different
    models files sharing a filename, or different library versions, since the
    version is mixed into the hash on purpose. Each then read the other's
    stamp, saw a mismatch, re-introspected and re-stamped, forever. Neither
    was wrong and neither could win.

    With the hash in the key, each distinct (schema, version) records its own
    row and no service can evict another's. Two services applying genuinely
    the same schema on the same version still share one row, which is
    correct: they are recording the same fact.

    Rows for superseded hashes are left behind rather than pruned. Pruning by
    schema name is what caused the collision — it would delete the other
    service's stamp. The cost is a few rows in a metadata table.
    """
    return f"schema_hash:{schema_name}:{schema_hash}"


@helper
async def _read_stamp_from_postgres(conn: Any, sql: str, key: str) -> str | None:
    """asyncpg hands the row straight back, keyed by column name."""
    row = await conn.fetchrow(sql, key)
    return row["value"] if row else None


@boundary
async def _read_stamp_from_dbapi(conn: Any, sql: str, key: str) -> str | None:
    """aiosqlite and pyturso answer through a cursor, keyed by position."""
    cursor = await conn.execute(sql, (key,))
    row = await cursor.fetchone()
    return row[0] if row else None


_STAMP_READERS: dict[str, Callable[[Any, str, str], Awaitable[str | None]]] = {
    "postgresql": _read_stamp_from_postgres,
    "sqlite": _read_stamp_from_dbapi,
}


@helper
async def _get_stored_hash(conn: Any, schema_name: str, schema_hash: str) -> str | None:
    """Return the stored stamp for this exact schema and version, else None."""
    dialect = _dialect_of(conn)
    read = for_dialect(_STAMP_READERS, dialect, "Stamp readers")
    return cast(
        str | None,
        await read(conn, stored_hash_sql(dialect), _stamp_key(schema_name, schema_hash)),
    )


@helper
async def _store_hash(conn: Any, schema_name: str, schema_hash: str) -> None:
    """Store (upsert) the schema hash in _declaro_meta."""
    dialect = _dialect_of(conn)
    write = for_dialect(_WRITERS, dialect, "Statement writers")
    await write(
        conn,
        store_hash_sql(dialect),
        (_stamp_key(schema_name, schema_hash), schema_hash, datetime.now(UTC).isoformat()),
    )


@helper
async def _schema_is_clean(conn: Any, schema_path: Path, schema_hash: str) -> bool:
    """Check if schema file hash matches the stored hash (skip-if-clean).

    A hash match alone is not sufficient: if the cloud DB was destroyed and
    recreated, the local replica retains the stale hash while the cloud is
    empty.  We guard against this by verifying that at least one user table
    (anything other than ``_declaro_meta``) exists — unless the schema itself
    defines zero tables (empty module), in which case an empty DB is expected.

    NOTHING IS CAUGHT HERE ANY MORE. Two `except Exception: return False`
    blocks used to wrap this, and "return False" means "the schema is dirty,
    go and migrate". So the answer to "is the schema clean?" was False in
    three unrelated situations that need three different responses:

        the stored hash differs          -> migrate, correct
        the database would not answer    -> migrated against a broken
                                            connection, one layer down
        the schema module will not load  -> reported as dirty, then the
                                            migration re-imports the same
                                            broken module and fails anyway,
                                            with the real ImportError two
                                            call frames from where it was
                                            first seen and swallowed

    The outer block also carried the comment "Meta table doesn't exist", but
    `_ensure_meta_table` is the first thing called and it is CREATE TABLE IF
    NOT EXISTS. Absence was already handled; the catch only hid everything
    else.

    A question this function cannot answer is not a "no". It raises, and the
    caller at `apply_migrations_async` sees the real error at the point it
    happened.
    """
    await _ensure_meta_table(conn)
    stored = await _get_stored_hash(conn, schema_path.name, schema_hash)
    return await _WHEN_THE_STAMP_MATCHES[stored == schema_hash](conn, schema_path)


@helper
async def _the_stamp_disagrees(_conn: Any, _schema_path: Path) -> bool:
    """No stamp, or one written for a different schema. There is work to do."""
    return False


@helper
async def _the_stamp_agrees(conn: Any, schema_path: Path) -> bool:
    """The stamp matches, so the only question left is whether the database
    still holds what it was stamped for.

    A hash match alone is not sufficient: if the cloud DB was destroyed and
    recreated, the local replica retains the stale hash while the cloud is
    empty.
    """
    return _WHEN_THE_DATABASE_IS_POPULATED[await _has_user_tables(conn)](schema_path)


@helper
def _the_database_holds_tables(_schema_path: Path) -> bool:
    """Stamped, and the tables are there. Nothing to migrate."""
    return True


@helper
def _the_database_is_empty(schema_path: Path) -> bool:
    """No user tables, so the hash is only trustworthy when the schema file
    itself produces none. Otherwise the database was destroyed and recreated.
    """
    return len(load_models_from_module(schema_path)) == 0


_WHEN_THE_STAMP_MATCHES: dict[bool, Callable[[Any, Path], Awaitable[bool]]] = {
    True: _the_stamp_agrees,
    False: _the_stamp_disagrees,
}

_WHEN_THE_DATABASE_IS_POPULATED: dict[bool, Callable[[Path], bool]] = {
    True: _the_database_holds_tables,
    False: _the_database_is_empty,
}


@helper
async def _count_tables_on_postgres(conn: Any, sql: str) -> bool:
    """asyncpg returns the rows, so their number is the answer."""
    return bool(await conn.fetch(sql))


@boundary
async def _count_tables_on_dbapi(conn: Any, sql: str) -> bool:
    """One row through the cursor is enough; the rest are not read."""
    cursor = await conn.execute(sql)
    return await cursor.fetchone() is not None


_TABLE_COUNTERS: dict[str, Callable[[Any, str], Awaitable[bool]]] = {
    "postgresql": _count_tables_on_postgres,
    "sqlite": _count_tables_on_dbapi,
}


@helper
async def _has_user_tables(conn: Any) -> bool:
    """Return True if the database has at least one table besides _declaro_meta."""
    dialect = _dialect_of(conn)
    count = for_dialect(_TABLE_COUNTERS, dialect, "Table counters")
    return cast(bool, await count(conn, user_tables_sql(dialect)))


# The prefix persistum owns. ONE definition, read by the generator and by the
# recovery query alike. It was two string literals in two modules: a change to
# one is silent, because reconstruction keeps working and recovery keeps
# returning zero, and the only symptom is a table that vanishes after a crash.
TMP_PREFIX = "_declaro_tmp_"

# One underscore and eight hex characters. The recovery rule below strips this
# many characters by POSITION, which is right only while the width is fixed.
_SUFFIX_WIDTH = 9


@boundary
def temp_table_name(table: str) -> str:
    """The name a reconstruction copies rows into. Pure.

    The random tail is what stops a retry colliding with a tmp table the
    previous attempt left behind.
    """
    return f"{TMP_PREFIX}{table}_{uuid.uuid4().hex[:8]}"


@helper
def original_table_name(tmp_name: str) -> str:
    """The table a tmp name was made from. Pure, and the inverse of the above.

    STRIPPING NINE CHARACTERS BY POSITION IS CORRECT HERE, and I recorded it as
    a bug for a day because I tested it with a name the generator cannot make.
    `temp_table_name` always appends an underscore and eight hex, so the
    remainder is always `{table}_{8}` and removing nine always yields the table
    exactly, dated names included. Verified by round trip over every shape in
    `TABLE_NAMES`, with the input built by the generator rather than by hand.

    A name without our prefix RAISES rather than being guessed at. The old code
    silently `continue`d past those inside the loop, which put the decision in
    the caller and left this function with no opinion about its own domain.
    """
    remainder = _WHEN_NOT_OURS[not tmp_name.startswith(TMP_PREFIX)](tmp_name)
    return _WHEN_A_SUFFIX_IS_THERE[
        len(remainder) > _SUFFIX_WIDTH and remainder[-_SUFFIX_WIDTH] == "_"
    ](remainder)


@helper
def _refuse_a_foreign_name(tmp_name: str) -> str:
    """A name without our prefix is not ours to rename or drop."""
    raise ValueError(
        f"{tmp_name!r} is not a tmp table this package wrote. Only names "
        f"beginning {TMP_PREFIX!r} are ours to rename or drop."
    )


@helper
def _without_our_prefix(tmp_name: str) -> str:
    """Our name, with the prefix taken off, leaving `{table}_{8 hex}`."""
    return tmp_name[len(TMP_PREFIX) :]


@helper
def _without_the_suffix(remainder: str) -> str:
    """The random tail comes off by position, which is right while it is fixed
    width. `test_the_generated_suffix_is_the_width_the_rule_strips` says so."""
    return remainder[:-_SUFFIX_WIDTH]


@helper
def _the_whole_remainder(remainder: str) -> str:
    """Too short to carry a suffix, so all of it is the table's name."""
    return remainder


_WHEN_NOT_OURS: dict[bool, Callable[[str], str]] = {
    True: _refuse_a_foreign_name,
    False: _without_our_prefix,
}

_WHEN_A_SUFFIX_IS_THERE: dict[bool, Callable[[str], str]] = {
    True: _without_the_suffix,
    False: _the_whole_remainder,
}


@helper
def orphaned_tmp_tables_sql() -> str:
    """Every tmp table this package wrote. Returns SQL, runs nothing.

    THE UNDERSCORE IS ESCAPED, AND IT WAS NOT. SQLite reads `_` in a LIKE
    pattern as a single-character wildcard, so `LIKE '_declaro_tmp_%'` also
    matched `Xdeclaro_tmp_users_a1b2c3d4` -- a table this package did not
    create. Measured 2026-08-17 against a real database.

    Nothing foreign was ever harmed, because `original_table_name` refuses a
    name without the prefix, so the extra rows were dropped in Python. But the
    docstring's promise that "only that prefix is matched" was true of the
    function and false of its query, and a guard doing the SQL's job is a guard
    somebody will one day remove as redundant.
    """
    # THE PATTERN IS BUILT OUTSIDE THE f-STRING. A backslash inside one is a
    # syntax error before Python 3.12, and this package declares >=3.11, so the
    # first version of this fix would not have imported on the oldest supported
    # interpreter. The tests passed because they run on 3.14; ruff caught it.
    pattern = TMP_PREFIX.replace("_", r"\_")
    return (
        "SELECT name FROM sqlite_master "
        f"WHERE type = 'table' AND name LIKE '{pattern}%' "
        "ESCAPE '\\'"
    )


@boundary
async def _drop_the_leftover(conn: Any, tmp_name: str, _original_name: str) -> int:
    """The original survived, so the tmp table is junk and nothing is recovered."""
    await conn.execute(f'DROP TABLE "{tmp_name}"')
    await conn.commit()
    say(__name__, "warning", "Dropped leftover tmp table: %s", tmp_name)
    return 0


@boundary
async def _rename_into_place(conn: Any, tmp_name: str, original_name: str) -> int:
    """The original is gone, so the tmp holds the only copy of the rows.

    NO SWALLOW HERE, AND ITS ABSENCE IS THE FIX. This was
    `except Exception as e: logger.error(...)`, so a rename that could not land
    was logged and the loop moved on.

    MEASURED 2026-08-17, with a view occupying the target name:

        Failed to clean up tmp table _declaro_tmp_orders_b2dd4c8f:
          there is already another table or index with this name
        returned 0, and the caller discards the return value

    The migration then ran, saw `orders` as absent and created it empty, while
    the rows sat in the tmp table where nothing would ever query them. The
    swallow was not a caution about the recovery, it WAS the data loss the
    recovery exists to prevent.

    So it raises, and `apply_migrations_async` stops. Stopping is right: a
    migration against a half-recovered database is what produced the tmp table
    in the first place. The rows stay where they are, under a name an operator
    can find (Rule 8).
    """
    statement = f'ALTER TABLE "{tmp_name}" RENAME TO "{original_name}"'
    try:
        await conn.execute(statement)
        await conn.commit()
    except Exception as exc:
        raise MigrationError(
            f"Could not recover {tmp_name!r} to {original_name!r}. "
            f"A reconstruction died between dropping the original "
            f"and renaming its replacement, and the replacement "
            f"cannot be moved into place. The rows are intact in "
            f"{tmp_name!r}. Migrating now would create "
            f"{original_name!r} empty beside them.",
            sql=statement,
            original_error=exc,
            operation=NOTHING,
        ) from exc
    say(__name__, "warning", "Recovered orphaned table: %s -> %s", tmp_name, original_name)
    return 1


_WHEN_THE_ORIGINAL_SURVIVED: dict[bool, Callable[[Any, str, str], Awaitable[int]]] = {
    True: _drop_the_leftover,
    False: _rename_into_place,
}


@boundary
async def _recover_one(conn: Any, tmp_name: str) -> int:
    """Deal with one orphaned tmp table, and say whether it was recovered."""
    # `original_table_name` refuses a name without our prefix, and the query no
    # longer returns one: its LIKE now escapes the underscores it used to treat
    # as wildcards. Both halves check, because the query is the fix and this is
    # the belt.
    original_name = original_table_name(tmp_name)

    check = await conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (original_name,),
    )
    original_exists = await check.fetchone()

    return await _WHEN_THE_ORIGINAL_SURVIVED[bool(original_exists)](conn, tmp_name, original_name)


@helper
async def _leave_the_stamp(_conn: Any, recovered: int) -> int:
    """Nothing was recovered, so the stored hash still describes the database."""
    return recovered


@boundary
async def _clear_the_stamp(conn: Any, recovered: int) -> int:
    """A table came back, so what the stamp records is no longer true.

    THE EXPECTED CASE IS PREVENTED, NOT CAUGHT. A database that has never been
    migrated has no meta table, so the DELETE below used to raise and be
    absorbed along with everything else. Creating the table first makes that
    case impossible, which leaves the handler narrowed to failures that are
    genuinely worth seeing.
    """
    await _ensure_meta_table(conn)
    try:
        await conn.execute(
            f'DELETE FROM "{META_TABLE}" WHERE key LIKE ?',
            ("schema_hash:%",),
        )
        await conn.commit()
        say(__name__, "info", f"Cleared schema hash after recovering {recovered} orphaned table(s)")
    except Exception as exc:
        # A stale schema hash that survived means the next start will believe
        # the schema is current when it is not -- worth seeing rather than
        # hiding.
        say(
            __name__,
            "warning",
            "Could not clear the schema hash after recovering %d "
            "orphaned table(s): %s. If the meta table exists, the next "
            "start may skip a migration it should have run.",
            recovered,
            exc,
        )
    return recovered


_WHEN_ANYTHING_WAS_RECOVERED: dict[bool, Callable[[Any, int], Awaitable[int]]] = {
    True: _clear_the_stamp,
    False: _leave_the_stamp,
}


@boundary
async def _recover_orphaned_tmp_tables(db: Database) -> int:
    """Detect and recover orphaned temp tables from failed reconstruction.

    Reconstruction uses temp tables named ``_declaro_tmp_<table>_<8hex>``.  If
    it partially committed (CREATE tmp + DROP original) but failed before
    RENAME, the database has the tmp table and not the original.  This renames
    them back.

    Only that prefix is matched.  It is a name persistum owns, so anything
    carrying it was created here and is ours to move.

    Returns the number of tables recovered.
    """
    async with _for_ddl(db) as conn:
        cursor = await conn.execute(orphaned_tmp_tables_sql())
        tmp_tables = await cursor.fetchall()

        recovered = sum([await _recover_one(conn, row[0]) for row in tmp_tables])

        # Invalidate the stored hash so the migration re-runs after a recovery.
        return await _WHEN_ANYTHING_WAS_RECOVERED[bool(recovered)](conn, recovered)


# =============================================================================
# The migration, one question at a time
# =============================================================================
#
# `apply_migrations_async` ran the whole sequence in one body: thirteen
# branches and eight places it could return. Each question is now a table with
# one entry per answer. One entry ends the migration with the result it already
# has, the other carries on to the next question, and the questions appear
# below in the order they are asked.


class _Migration(TypedDict):
    """What the migration knows before it reads the schema file.

    `answer` holds the result when the migration is already over -- the file
    is not there, or the stamp says the database matches. NOTHING means there
    is still work to do and `schema_hash` is what a success will stamp.
    """

    db: Database
    dialect: Dialect
    schema_path: Path
    schema_hash: str
    expand_enums: bool
    answer: dict[str, Any] | Nothing
