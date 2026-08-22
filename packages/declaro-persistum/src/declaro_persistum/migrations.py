
"""
Automatic schema migration support for declaro_persistum.

Provides async migration functions that:
1. Load target schema from Pydantic models
2. Introspect current database state
3. Compute diff and apply changes

Uses a SHA-256 hash of the schema file to skip introspection when the schema
hasn't changed since the last successful migration (skip-if-clean).

Usage:
    # Async
    await apply_migrations_async(db, dialect, schema_path, expand_enums=NOTHING, force=NOTHING)
"""

import hashlib
import logging
import uuid
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, cast

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier import apply as apply_operations
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.database import (
    Database,
    is_replicated,
    reading,
    refresh,
)
from declaro_persistum.differ import diff, diff_views
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.inspector import introspect_with_views
from declaro_persistum.pydantic_loader import (
    load_declarations,
    load_models_from_module,
)
from declaro_persistum.types import (
    NOTHING,
    Dialect,
    Nothing,
    for_dialect,
    is_nothing,
    resolved,
)
from declaro_persistum.validator import validate_schema

META_TABLE = "_declaro_meta"

logger = logging.getLogger(__name__)


@boundary
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
        op_idx
        for op_idx in execution_order
        if not requires_reconstruction(operations[op_idx])
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
def _placeholder(dialect: str) -> str:
    return cast(str, for_dialect(_PLACEHOLDER, dialect, "Statement builders"))


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


@helper
def store_hash_sql(dialect: str) -> str:
    """Write one stamp, updating it if the key is already there.

    `DO UPDATE`, never `DO NOTHING`. Both keep the row count at one, and only
    the first refreshes the stamp, so `DO NOTHING` leaves a database reporting
    a boot that never happened.

    asyncpg numbers its parameters and repeats them by number; SQLite reads
    the row it was about to insert, through `excluded`. The clause is the same
    clause.
    """
    return cast(str, for_dialect(_STORE_HASH, dialect, "Statement builders"))


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


@helper
def user_tables_sql(dialect: str) -> str:
    """Any table that is not ours. The one query that genuinely differs.

    `_declaro_meta` is excluded because it is ours: a database holding only it
    is empty, and counting it would make every fresh database look populated.
    SQLite's own `sqlite_%` tables are excluded for the same reason, and
    `sqlite_sequence` appears the moment any table uses AUTOINCREMENT.
    """
    return cast(str, for_dialect(_USER_TABLES, dialect, "Statement builders"))


@boundary
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


_WRITERS: dict[
    str, Callable[[Any, str, tuple[Any, ...]], Coroutine[Any, Any, None]]
] = {
    "postgresql": _run_on_postgres,
    "sqlite": _run_on_dbapi,
}


@boundary
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


@boundary
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


_STAMP_READERS: dict[
    str, Callable[[Any, str, str], Coroutine[Any, Any, str | None]]
] = {
    "postgresql": _read_stamp_from_postgres,
    "sqlite": _read_stamp_from_dbapi,
}


@boundary
async def _get_stored_hash(conn: Any, schema_name: str, schema_hash: str) -> str | None:
    """Return the stored stamp for this exact schema and version, else None."""
    dialect = _dialect_of(conn)
    read = for_dialect(_STAMP_READERS, dialect, "Stamp readers")
    return cast(
        str | None,
        await read(conn, stored_hash_sql(dialect), _stamp_key(schema_name, schema_hash)),
    )


@boundary
async def _store_hash(conn: Any, schema_name: str, schema_hash: str) -> None:
    """Store (upsert) the schema hash in _declaro_meta."""
    dialect = _dialect_of(conn)
    write = for_dialect(_WRITERS, dialect, "Statement writers")
    await write(
        conn,
        store_hash_sql(dialect),
        (_stamp_key(schema_name, schema_hash), schema_hash, datetime.now(UTC).isoformat()),
    )


@boundary
async def _schema_is_clean(
    conn: Any, schema_path: Path, schema_hash: str
) -> bool:
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


@boundary
async def _the_stamp_disagrees(_conn: Any, _schema_path: Path) -> bool:
    """No stamp, or one written for a different schema. There is work to do."""
    return False


@boundary
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


@boundary
def _the_database_is_empty(schema_path: Path) -> bool:
    """No user tables, so the hash is only trustworthy when the schema file
    itself produces none. Otherwise the database was destroyed and recreated.
    """
    return len(load_models_from_module(schema_path)) == 0


_WHEN_THE_STAMP_MATCHES: dict[
    bool, Callable[[Any, Path], Coroutine[Any, Any, bool]]
] = {
    True: _the_stamp_agrees,
    False: _the_stamp_disagrees,
}

_WHEN_THE_DATABASE_IS_POPULATED: dict[bool, Callable[[Path], bool]] = {
    True: _the_database_holds_tables,
    False: _the_database_is_empty,
}


@boundary
async def _count_tables_on_postgres(conn: Any, sql: str) -> bool:
    """asyncpg returns the rows, so their number is the answer."""
    return bool(await conn.fetch(sql))


@boundary
async def _count_tables_on_dbapi(conn: Any, sql: str) -> bool:
    """One row through the cursor is enough; the rest are not read."""
    cursor = await conn.execute(sql)
    return await cursor.fetchone() is not None


_TABLE_COUNTERS: dict[str, Callable[[Any, str], Coroutine[Any, Any, bool]]] = {
    "postgresql": _count_tables_on_postgres,
    "sqlite": _count_tables_on_dbapi,
}


@boundary
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
    return tmp_name[len(TMP_PREFIX):]


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
    logger.warning("Dropped leftover tmp table: %s", tmp_name)
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
            original_error=exc, operation=NOTHING) from exc
    logger.warning("Recovered orphaned table: %s -> %s", tmp_name, original_name)
    return 1


_WHEN_THE_ORIGINAL_SURVIVED: dict[
    bool, Callable[[Any, str, str], Coroutine[Any, Any, int]]
] = {
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

    return await _WHEN_THE_ORIGINAL_SURVIVED[bool(original_exists)](
        conn, tmp_name, original_name
    )


@boundary
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
        logger.info(
            f"Cleared schema hash after recovering {recovered} orphaned table(s)"
        )
    except Exception as exc:
        # A stale schema hash that survived means the next start will believe
        # the schema is current when it is not -- worth seeing rather than
        # hiding.
        logger.warning(
            "Could not clear the schema hash after recovering %d "
            "orphaned table(s): %s. If the meta table exists, the next "
            "start may skip a migration it should have run.",
            recovered, exc,
        )
    return recovered


_WHEN_ANYTHING_WAS_RECOVERED: dict[
    bool, Callable[[Any, int], Coroutine[Any, Any, int]]
] = {
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


@helper
def _still_to_do(
    db: Database,
    dialect: Dialect,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """No answer yet, so the schema gets read and diffed."""
    return {
        "db": db,
        "dialect": dialect,
        "schema_path": schema_path,
        "schema_hash": schema_hash,
        "expand_enums": expand_enums,
        "answer": NOTHING,
    }


@boundary
async def _no_schema_file(
    db: Database,
    dialect: Dialect,
    schema_path: Path,
    expand_enums: bool,
    _force: bool,
) -> _Migration:
    """Nothing to migrate from, and the caller is told which path was tried."""
    logger.warning(f"Schema file not found: {schema_path}")
    return {
        "db": db,
        "dialect": dialect,
        "schema_path": schema_path,
        "schema_hash": "",
        "expand_enums": expand_enums,
        "answer": {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": False,
            "error": f"Schema file not found: {schema_path}",
        },
    }


@boundary
async def _no_orphan_recovery(_db: Database) -> int:
    """This dialect writes no `_declaro_tmp_` tables and has no sqlite_master,
    so there is nothing to scan for and nothing came back."""
    return 0


_ORPHAN_RECOVERY: dict[bool, Callable[[Database], Coroutine[Any, Any, int]]] = {
    True: _recover_orphaned_tmp_tables,
    False: _no_orphan_recovery,
}


@boundary
async def _read_the_schema_file(
    db: Database,
    dialect: Dialect,
    schema_path: Path,
    expand_enums: bool,
    force: bool,
) -> _Migration:
    """Bring the database up to date, recover what a crash left, take the hash.

    A migration diffs the live schema against the target, so the local copy
    must be current first. A warm open does not replicate, so without the
    refresh the diff is computed against whatever the copy held when it was
    last touched and emits DDL for a schema that has moved.
    """
    await refresh(db)

    # Pre-flight: recover orphaned tmp tables from a failed reconstruction.
    # Which dialects have any lives in the pure helper so it can be tested
    # without a database.
    await _ORPHAN_RECOVERY[_dialect_needs_orphan_recovery(dialect)](db)

    # Version is passed explicitly so _compute_schema_hash stays a pure
    # function of its arguments (Honest Code: configuration as parameters).
    from declaro_persistum import __version__

    schema_hash = _compute_schema_hash(schema_path, __version__)
    return await _WHEN_THE_STAMP_IS_WORTH_READING[not force](
        db, dialect, schema_path, expand_enums, schema_hash
    )


_WHEN_THE_SCHEMA_FILE_IS_THERE: dict[
    bool,
    Callable[
        [Database, Dialect, Path, bool, bool], Coroutine[Any, Any, _Migration]
    ],
] = {
    True: _read_the_schema_file,
    False: _no_schema_file,
}


@boundary
async def _migrate_whatever_the_stamp_says(
    db: Database,
    dialect: Dialect,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """`force` was asked for, so the stamp is not read and the work goes on."""
    return _still_to_do(db, dialect, schema_path, expand_enums, schema_hash)


@boundary
async def _nothing_has_changed(
    db: Database,
    dialect: Dialect,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """The stamp matches and the tables are there, so the migration is over."""
    logger.info("Schema unchanged (hash match) — skipping migration")
    return {
        "db": db,
        "dialect": dialect,
        "schema_path": schema_path,
        "schema_hash": schema_hash,
        "expand_enums": expand_enums,
        "answer": {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": True,
            "error": None,
        },
    }


_WHEN_THE_DATABASE_IS_CURRENT: dict[
    bool,
    Callable[
        [Database, Dialect, Path, bool, str], Coroutine[Any, Any, _Migration]
    ],
] = {
    True: _nothing_has_changed,
    False: _migrate_whatever_the_stamp_says,
}


@boundary
async def _consult_the_stamp(
    db: Database,
    dialect: Dialect,
    schema_path: Path,
    expand_enums: bool,
    schema_hash: str,
) -> _Migration:
    """Skip-if-clean: compare the schema file's hash with the stored one."""
    async with reading(db) as conn:
        current = await _schema_is_clean(conn, schema_path, schema_hash)
    return await _WHEN_THE_DATABASE_IS_CURRENT[current](
        db, dialect, schema_path, expand_enums, schema_hash
    )


_WHEN_THE_STAMP_IS_WORTH_READING: dict[
    bool,
    Callable[
        [Database, Dialect, Path, bool, str], Coroutine[Any, Any, _Migration]
    ],
] = {
    True: _consult_the_stamp,
    False: _migrate_whatever_the_stamp_says,
}


@boundary
def _load_the_schema(schema_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """The tables and views one models file declares."""
    logger.info(f"Loading schema from {schema_path}")
    return load_declarations(schema_path)


@boundary
async def _stamp(plan: _Migration) -> None:
    """Record the hash so the next start can skip introspection."""
    async with _for_ddl(plan["db"]) as conn:
        await _ensure_meta_table(conn)
        await _store_hash(conn, plan["schema_path"].name, plan["schema_hash"])


@helper
def _how_the_migration_stands(answer: Any, errors: list[str]) -> str:
    """Which of the three things happens next, named rather than nested.

    The migration may already hold its answer, the schema may be invalid, or
    there are tables to migrate. The first two are asked in that order because
    a migration that never loaded a schema has nothing to validate.
    """
    settled = [
        name
        for name, holds in (
            ("answered", not is_nothing(answer)),
            ("invalid", bool(errors)),
        )
        if holds
    ]
    return next(iter(settled), "carry on")


@boundary
async def _the_answer_already_settled(
    plan: _Migration,
    _target_schema: dict[str, Any],
    _target_views: dict[str, Any],
    _errors: list[str],
) -> dict[str, Any]:
    """The file was missing or the stamp was clean. That answer stands."""
    return cast(dict[str, Any], plan["answer"])


@boundary
async def _refuse_an_invalid_schema(
    _plan: _Migration,
    target_schema: dict[str, Any],
    _target_views: dict[str, Any],
    errors: list[str],
) -> dict[str, Any]:
    """Every error is logged by itself, and all of them reach the caller."""
    [logger.error(f"Schema is not valid: {message}") for message in errors]
    return {
        "success": False,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": 0,
        "skipped": False,
        "error": "; ".join(errors),
    }


@boundary
async def _stamp_an_empty_schema(
    plan: _Migration,
    _target_schema: dict[str, Any],
    _target_views: dict[str, Any],
) -> dict[str, Any]:
    """A models file declaring no table is stamped, so the next call skips."""
    logger.warning("No tables found in schema models")
    await _stamp(plan)
    return {
        "success": True,
        "operations_applied": 0,
        "tables_in_schema": 0,
        "tables_in_database": 0,
        "skipped": False,
        "error": None,
    }


@boundary
def _expand_the_enums(target_schema: dict[str, Any]) -> dict[str, Any]:
    """Every Literal column becomes a lookup table and an FK to it."""
    expanded = expand_schema_enums(target_schema)
    logger.info(
        f"Expanded schema to {len(expanded)} tables (including enum lookup tables)"
    )
    return expanded


@helper
def _the_schema_as_declared(target_schema: dict[str, Any]) -> dict[str, Any]:
    """Expansion was declined, so a Literal column stays a plain column."""
    return target_schema


_ENUM_EXPANSION: dict[bool, Callable[[dict[str, Any]], dict[str, Any]]] = {
    True: _expand_the_enums,
    False: _the_schema_as_declared,
}


@boundary
async def _nothing_to_change(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    _diff_result: dict[str, Any],
) -> dict[str, Any]:
    """The database already holds what the schema declares."""
    logger.info("Schema is up to date - no migrations needed")
    await _stamp(plan)
    return {
        "success": True,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": False,
        "error": None,
    }


@boundary
async def _refuse_ambiguous_changes(
    _plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """A rename and a drop-plus-add look alike, and only a person can say."""
    logger.warning(f"Ambiguous changes detected: {diff_result['ambiguities']}")
    logger.warning("Skipping migration - manual intervention required")
    return {
        "success": False,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": False,
        "error": f"Ambiguous changes: {diff_result['ambiguities']}",
    }


@boundary
async def _record_the_migration(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    skipped_operations: list[dict[str, Any]],
    result: dict[str, Any],
) -> dict[str, Any]:
    """The operations landed, so the hash is stamped and the run reported."""
    logger.info(f"Successfully applied {result['operations_applied']} migrations")
    [
        logger.warning(f"Migration completed with warnings: {warning}")
        for warning in [result.get("error")]
        if warning
    ]
    [logger.debug(f"  Executed: {sql}") for sql in result["executed_sql"]]
    await _stamp(plan)
    return {
        "success": True,
        "operations_applied": result["operations_applied"],
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema) + result["operations_applied"],
        "skipped": bool(skipped_operations),
        "skipped_operations": skipped_operations,
        "error": None,
    }


@boundary
async def _report_a_failed_apply(
    _plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    skipped_operations: list[dict[str, Any]],
    result: dict[str, Any],
) -> dict[str, Any]:
    """The applier refused, and nothing is stamped over a failed run."""
    error_msg = result.get("error", "Unknown error")
    logger.error(f"Migration failed: {error_msg}")
    return {
        "success": False,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": False,
        "skipped_operations": skipped_operations,
        "error": error_msg,
    }


_WHEN_THE_OPERATIONS_LANDED: dict[
    bool,
    Callable[
        [
            _Migration,
            dict[str, Any],
            dict[str, Any],
            list[dict[str, Any]],
            dict[str, Any],
        ],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    True: _record_the_migration,
    False: _report_a_failed_apply,
}


@boundary
async def _apply_and_report(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    operations: list[dict[str, Any]],
    execution_order: list[int],
    skipped_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Run the operations the order names, then report what happened.

    No pause. A migration is a writer, and it holds the serialise lock for the
    whole of `_for_ddl`; opportunistic replication defers to a waiting writer,
    so nothing can push mid-transaction.
    """
    async with _for_ddl(plan["db"]) as conn:
        result = await apply_operations(
            conn, operations, execution_order, plan["dialect"], dry_run=False
        )
    return await _WHEN_THE_OPERATIONS_LANDED[result["success"]](
        plan, target_schema, current_schema, skipped_operations, result
    )


@boundary
async def _run_the_safe_operations(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    diff_result: dict[str, Any],
    safe_order: list[int],
    skipped_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Some operations replicate, so those run and the rest are reported."""
    return await _apply_and_report(
        plan,
        target_schema,
        current_schema,
        diff_result["operations"],
        safe_order,
        skipped_operations,
    )


@boundary
async def _report_everything_deferred(
    _plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    _diff_result: dict[str, Any],
    _safe_order: list[int],
    skipped_operations: list[dict[str, Any]],
) -> dict[str, Any]:
    """Every pending operation needs reconstruction, so none of them run."""
    logger.info(
        "All pending operations require reconstruction — "
        "skipping auto-migration. Use 'declaro migrate-remote'."
    )
    return {
        "success": True,
        "operations_applied": 0,
        "tables_in_schema": len(target_schema),
        "tables_in_database": len(current_schema),
        "skipped": True,
        "skipped_operations": skipped_operations,
        "error": None,
    }


_WHEN_ANYTHING_IS_SAFE_TO_RUN: dict[
    bool,
    Callable[
        [
            _Migration,
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            list[int],
            list[dict[str, Any]],
        ],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    True: _run_the_safe_operations,
    False: _report_everything_deferred,
}


@boundary
async def _run_every_operation(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """Not a replica, so everything the differ emitted runs here."""
    return await _apply_and_report(
        plan,
        target_schema,
        current_schema,
        diff_result["operations"],
        diff_result["execution_order"],
        [],
    )


@boundary
async def _defer_what_cannot_replicate(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """On an embedded replica, reconstruction is left for `migrate-remote`.

    The replication engine cannot replicate DDL, and a partial sync -- the
    DROP reaches the cloud and the CREATE does not -- destroys the table on
    both sides.
    """
    safe_order, skipped_operations = partition_replica_operations(
        diff_result["operations"], diff_result["execution_order"]
    )
    [
        logger.warning(
            f"Skipping {skipped['op']} on {skipped.get('table', 'N/A')} — "
            f"reconstruction is unsafe on embedded replicas. "
            f"Run 'declaro migrate-remote' to apply this change to cloud."
        )
        for skipped in skipped_operations
    ]
    return await _WHEN_ANYTHING_IS_SAFE_TO_RUN[bool(safe_order)](
        plan,
        target_schema,
        current_schema,
        diff_result,
        safe_order,
        skipped_operations,
    )


_WHEN_THE_TARGET_IS_A_REPLICA: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], dict[str, Any]],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    True: _defer_what_cannot_replicate,
    False: _run_every_operation,
}


@boundary
async def _choose_what_to_run(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """A replica runs less than a primary does, and says what it left out."""
    return await _WHEN_THE_TARGET_IS_A_REPLICA[is_replicated(plan["db"])](
        plan, target_schema, current_schema, diff_result
    )


_WHEN_THE_CHANGES_ARE_AMBIGUOUS: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], dict[str, Any]],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    True: _refuse_ambiguous_changes,
    False: _choose_what_to_run,
}


@boundary
async def _consider_what_changed(
    plan: _Migration,
    target_schema: dict[str, Any],
    current_schema: dict[str, Any],
    diff_result: dict[str, Any],
) -> dict[str, Any]:
    """Log the planned operations, then ask whether any of them is ambiguous."""
    logger.info(f"Found {len(diff_result['operations'])} schema differences")
    [
        logger.info(f"  - {op['op']} on table {op.get('table', 'N/A')}")
        for op in diff_result["operations"]
    ]
    return await _WHEN_THE_CHANGES_ARE_AMBIGUOUS[
        bool(diff_result.get("ambiguities"))
    ](plan, target_schema, current_schema, diff_result)


_WHEN_THERE_IS_WORK: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], dict[str, Any]],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    True: _consider_what_changed,
    False: _nothing_to_change,
}


@boundary
async def _migrate_the_tables(
    plan: _Migration,
    target_schema: dict[str, Any],
    target_views: dict[str, Any],
) -> dict[str, Any]:
    """Introspect the database, diff it against the schema, run the result."""
    logger.info(f"Loaded {len(target_schema)} tables from schema models")

    expanded = _ENUM_EXPANSION[plan["expand_enums"]](target_schema)

    # Introspect current database state, VIEWS INCLUDED. Asking without them
    # is what made views unreachable: the inspectors, the differ and the
    # appliers all handled views, and the apply path never carried one.
    async with reading(plan["db"]) as conn:
        current_schema, current_views = await introspect_with_views(
            conn, plan["dialect"]
        )

    logger.info(
        f"Introspected {len(current_schema)} tables and "
        f"{len(current_views)} views from database"
    )

    # Compute diff. Tables and views are diffed by different functions because
    # they are different things: a table is reconciled column by column, a view
    # is replaced whole when its query changes.
    diff_result = diff(
        current_schema, expanded, dialect=plan["dialect"], decisions=NOTHING
    )

    # VIEWS GO LAST, AND THEIR INDICES GO INTO THE ORDER OR NOTHING RUNS THEM.
    # `execution_order` is the differ's topological sort over the TABLE
    # operations, so an operation appended after it is invisible to the
    # applier: the first version of this appended the view ops and not their
    # indices, and the migration reported success having created no view.
    # Appending is also the right order rather than merely a convenient one,
    # since a view selects from tables and must be created after them.
    view_operations = diff_views(current_views, target_views)
    first_view_index = len(diff_result["operations"])
    diff_result["operations"] = [*diff_result["operations"], *view_operations]
    diff_result["execution_order"] = [
        *diff_result["execution_order"],
        *range(first_view_index, first_view_index + len(view_operations)),
    ]

    return await _WHEN_THERE_IS_WORK[bool(diff_result["operations"])](
        plan, expanded, current_schema, diff_result
    )


_WHEN_THE_SCHEMA_DECLARES_TABLES: dict[
    bool,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any]],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    True: _migrate_the_tables,
    False: _stamp_an_empty_schema,
}


@boundary
async def _migrate_the_declared_schema(
    plan: _Migration,
    target_schema: dict[str, Any],
    target_views: dict[str, Any],
    _errors: list[str],
) -> dict[str, Any]:
    """The schema is valid, so the only question left is whether it has any
    tables in it."""
    return await _WHEN_THE_SCHEMA_DECLARES_TABLES[bool(target_schema)](
        plan, target_schema, target_views
    )


_NEXT_STEP: dict[
    str,
    Callable[
        [_Migration, dict[str, Any], dict[str, Any], list[str]],
        Coroutine[Any, Any, dict[str, Any]],
    ],
] = {
    "answered": _the_answer_already_settled,
    "invalid": _refuse_an_invalid_schema,
    "carry on": _migrate_the_declared_schema,
}

# What a schema nobody read has to say for itself. A migration that already
# holds its answer never loads a models file, so there is nothing to validate
# and no error to report.
_NOTHING_TO_VALIDATE: dict[str, list[str]] = {"errors": [], "warnings": []}


@boundary
async def apply_migrations_async(
    db: Database,
    dialect: Dialect,
    schema_path: str | Path,
    *,
    expand_enums: bool | Nothing,
    force: bool | Nothing,
) -> dict[str, Any]:
    """
    Apply automatic schema migrations asynchronously.

    Loads schema from Pydantic models, compares with actual database,
    and applies changes if differences are found.

    Uses a SHA-256 hash of the schema file to skip introspection when
    the schema hasn't changed since the last successful migration.

    Args:
        db: The database
        dialect: Database dialect ('sqlite', 'turso', 'postgresql')
        schema_path: Path to Python module containing Pydantic models
        expand_enums: Whether to expand Literal types to lookup tables (default True)
        force: Bypass the skip-if-clean check and always run full introspection

    Returns:
        Dict with migration result:
        - success: bool
        - operations_applied: int
        - tables_in_schema: int
        - tables_in_database: int
        - skipped: bool (True if schema was clean and migration was skipped)
        - error: str | None

    Raises:
        Exception: If migration fails critically (logs error but may not crash)
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    expand_enums = resolved(expand_enums, True)
    force = resolved(force, False)
    file = Path(schema_path)

    plan = await _WHEN_THE_SCHEMA_FILE_IS_THERE[file.exists()](
        db, dialect, file, expand_enums, force
    )

    # THE VALIDATOR IS ASKED HERE, AND WAS ASKED NOWHERE ON THIS PATH BEFORE.
    # Every rule it holds fired only under `declaro validate`, a command an
    # author has to think to run, so the checks existed and did not protect
    # the operation they were written for.
    #
    # Most invalid schemas fail at the engine too, and what is lost is the
    # message. A table named `quo"ted` reached SQLite as `near "ted": syntax
    # error`, naming a fragment of the name, where the validator names the
    # table, the character, the reason and the action.
    #
    # Some do not fail at all. A foreign key pointing at a table nobody
    # declared is caught by comparing the schema against itself, and SQLite
    # accepts the DDL without complaint because it does not check a reference
    # at creation time. That column then points at nothing and every deploy
    # reports success.
    #
    # WARNINGS ARE NOT ERRORS. The validator separates them and only the
    # errors stop the migration; an unknown type is a warning and keeps going.
    #
    # Measured before wiring it in: every schema this package ships validates
    # clean, and of the fourteen models files the tests build, thirteen
    # validate clean and none has an error.
    #
    # THE FILTER IS THE GUARD. A migration that already has its answer must
    # not import a models file, and a comprehension whose filter excludes
    # everything never evaluates its body. That is what carries the laziness a
    # branch used to carry.
    loaded = [_load_the_schema(file) for _ in [()] if is_nothing(plan["answer"])]
    validated = [validate_schema(schema) for schema, _views in loaded]

    target_schema, target_views = next(iter(loaded), ({}, {}))
    validation = next(iter(validated), _NOTHING_TO_VALIDATE)

    return await _NEXT_STEP[
        _how_the_migration_stands(plan["answer"], validation["errors"])
    ](plan, target_schema, target_views, validation["errors"])
