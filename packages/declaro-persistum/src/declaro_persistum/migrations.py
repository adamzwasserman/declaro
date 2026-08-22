
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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

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
from declaro_persistum.types import NOTHING, Dialect, Nothing, for_dialect, resolved
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
    # A DIRECTORY EXISTS, SO `exists()` WAS THE WRONG QUESTION. The caller's
    # guard is `if not schema_path.exists()`, which a models directory passes,
    # and `read_bytes` then raised `IsADirectoryError` three frames down --
    # naming a function the caller never called instead of the argument it got
    # wrong. Measured 2026-08-17.
    #
    # The mistake is the likely one, not an exotic one: this package has both
    # shapes, and `docs/usage.md` shows a `models/` directory holding
    # `users.py` and `orders.py`. So the refusal names the other function.
    if schema_path.is_dir():
        raise ValueError(
            f"{schema_path} is a directory. `apply_migrations_async` hashes a "
            f"single models file. For a directory of them, load it with "
            f"`load_schema_from_models` and pass the file you want stamped."
        )

    h = hashlib.sha256()
    h.update(schema_path.read_bytes())
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

    safe_order: list[int] = []
    skipped: list[dict[str, Any]] = []
    for op_idx in execution_order:
        op = operations[op_idx]
        if requires_reconstruction(op):
            skipped.append({"op": op["op"], "table": op.get("table")})
        else:
            safe_order.append(op_idx)
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
    if dialect == "postgresql":
        _placeholder(dialect)  # refuses an engine that is not in the table
        return (
            f'INSERT INTO "{META_TABLE}" (key, value, updated_at) '
            f"VALUES ($1, $2, $3) "
            f"ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = $3"
        )
    _placeholder(dialect)
    return (
        f'INSERT INTO "{META_TABLE}" (key, value, updated_at) '
        f"VALUES (?, ?, ?) "
        f"ON CONFLICT (key) DO UPDATE SET "
        f"value = excluded.value, updated_at = excluded.updated_at"
    )


@helper
def user_tables_sql(dialect: str) -> str:
    """Any table that is not ours. The one query that genuinely differs.

    `_declaro_meta` is excluded because it is ours: a database holding only it
    is empty, and counting it would make every fresh database look populated.
    SQLite's own `sqlite_%` tables are excluded for the same reason, and
    `sqlite_sequence` appears the moment any table uses AUTOINCREMENT.
    """
    if dialect == "postgresql":
        _placeholder(dialect)
        return (
            f"SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
            f"AND tablename != '{META_TABLE}'"
        )
    _placeholder(dialect)
    return (
        f"SELECT name FROM sqlite_master "
        f"WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
        f"AND name != '{META_TABLE}'"
    )


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


@boundary
async def _ensure_meta_table(conn: Any) -> None:
    """Run `meta_table_sql`. The statement is asserted in its own test."""
    sql = meta_table_sql()
    if _dialect_of(conn) == "postgresql":
        await conn.execute(sql)
    else:
        await conn.execute(sql, ())
        await conn.commit()


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
async def _get_stored_hash(conn: Any, schema_name: str, schema_hash: str) -> str | None:
    """Return the stored stamp for this exact schema and version, else None."""
    key = _stamp_key(schema_name, schema_hash)
    dialect = _dialect_of(conn)
    sql = stored_hash_sql(dialect)
    if dialect == "postgresql":
        row = await conn.fetchrow(sql, key)
        return row["value"] if row else None
    cursor = await conn.execute(sql, (key,))
    row = await cursor.fetchone()
    return row[0] if row else None


@boundary
async def _store_hash(conn: Any, schema_name: str, schema_hash: str) -> None:
    """Store (upsert) the schema hash in _declaro_meta."""
    key = _stamp_key(schema_name, schema_hash)
    now = datetime.now(UTC).isoformat()
    dialect = _dialect_of(conn)
    sql = store_hash_sql(dialect)
    if dialect == "postgresql":
        await conn.execute(sql, key, schema_hash, now)
    else:
        await conn.execute(sql, (key, schema_hash, now))
        await conn.commit()


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
    if stored != schema_hash:
        return False
    # Hash matches — verify cloud DB is not empty (stale-hash guard).
    # Skip this check for empty schemas (no tables defined).
    has_tables = await _has_user_tables(conn)
    if has_tables:
        return True
    # No user tables — only trust the hash if the schema file itself
    # produces no tables (empty module).  Otherwise, the DB was likely
    # destroyed and recreated.
    target = load_models_from_module(schema_path)
    return len(target) == 0


@boundary
async def _has_user_tables(conn: Any) -> bool:
    """Return True if the database has at least one table besides _declaro_meta."""
    dialect = _dialect_of(conn)
    sql = user_tables_sql(dialect)
    if dialect == "postgresql":
        return bool(await conn.fetch(sql))
    cursor = await conn.execute(sql)
    return await cursor.fetchone() is not None



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
    if not tmp_name.startswith(TMP_PREFIX):
        raise ValueError(
            f"{tmp_name!r} is not a tmp table this package wrote. Only names "
            f"beginning {TMP_PREFIX!r} are ours to rename or drop."
        )
    remainder = tmp_name[len(TMP_PREFIX):]
    if len(remainder) > _SUFFIX_WIDTH and remainder[-_SUFFIX_WIDTH] == "_":
        return remainder[:-_SUFFIX_WIDTH]
    return remainder


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

        recovered = 0
        for row in tmp_tables:
            tmp_name = row[0]
            # `original_table_name` refuses a name without our prefix, and the
            # query no longer returns one: its LIKE now escapes the underscores
            # it used to treat as wildcards. Both halves check, because the
            # query is the fix and this is the belt.
            original_name = original_table_name(tmp_name)

            # Check if the original table exists
            check = await conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                (original_name,),
            )
            original_exists = await check.fetchone()

            # NO `except` HERE, AND ITS ABSENCE IS THE FIX. This was
            # `except Exception as e: logger.error(...)`, so a rename that
            # could not land was logged and the loop moved on.
            #
            # MEASURED 2026-08-17, with a view occupying the target name:
            #
            #     Failed to clean up tmp table _declaro_tmp_orders_b2dd4c8f:
            #       there is already another table or index with this name
            #     returned 0, and the caller discards the return value
            #
            # The migration then ran, saw `orders` as absent and created it
            # empty, while the rows sat in the tmp table where nothing would
            # ever query them. The swallow was not a caution about the
            # recovery, it WAS the data loss the recovery exists to prevent.
            #
            # So it raises, and `apply_migrations_async` stops. Stopping is
            # right: a migration against a half-recovered database is what
            # produced the tmp table in the first place. The rows stay where
            # they are, under a name an operator can find (Rule 8).
            if original_exists:
                # The original survived, so the tmp is leftover junk.
                await conn.execute(f'DROP TABLE "{tmp_name}"')
                await conn.commit()
                logger.warning("Dropped leftover tmp table: %s", tmp_name)
            else:
                try:
                    await conn.execute(
                        f'ALTER TABLE "{tmp_name}" RENAME TO "{original_name}"'
                    )
                    await conn.commit()
                except Exception as exc:
                    raise MigrationError(
                        f"Could not recover {tmp_name!r} to {original_name!r}. "
                        f"A reconstruction died between dropping the original "
                        f"and renaming its replacement, and the replacement "
                        f"cannot be moved into place. The rows are intact in "
                        f"{tmp_name!r}. Migrating now would create "
                        f"{original_name!r} empty beside them.",
                        sql=f'ALTER TABLE "{tmp_name}" RENAME TO "{original_name}"',
                        original_error=exc, operation=NOTHING) from exc
                logger.warning(
                    "Recovered orphaned table: %s -> %s", tmp_name, original_name
                )
                recovered += 1

        # Invalidate stored hash so migration re-runs after recovery
        if recovered:
            # THE EXPECTED CASE IS PREVENTED, NOT CAUGHT. A database that has
            # never been migrated has no meta table, so the DELETE below used
            # to raise and be absorbed along with everything else. Creating the
            # table first makes that case impossible, which leaves the handler
            # narrowed to failures that are genuinely worth seeing.
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
                # The meta table legitimately does not exist on a fresh
                # database. Anything else here means the stale schema hash
                # survived, so the next start will believe the schema is
                # current when it is not -- worth seeing rather than hiding.
                logger.warning(
                    "Could not clear the schema hash after recovering %d "
                    "orphaned table(s): %s. If the meta table exists, the next "
                    "start may skip a migration it should have run.",
                    recovered, exc,
                )

    return recovered


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
    # THE VISIBLE RESOLVE. A default would hide this inside the
    # signature, where no reader sees it happen and no test reaches it.
    expand_enums = resolved(expand_enums, True)
    force = resolved(force, False)
    schema_path = Path(schema_path)

    if not schema_path.exists():
        logger.warning(f"Schema file not found: {schema_path}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": False,
            "error": f"Schema file not found: {schema_path}",
        }

    # Everything below reads the database to decide what to change: the
    # A migration diffs the live schema against the target, so the local copy
    # must be current first. A warm open does not replicate, so without this the
    # diff is computed against whatever the copy held when it was last touched
    # and emits DDL for a schema that has moved.
    await refresh(db)

    # Pre-flight: recover orphaned _new tables from failed reconstruction.
    # Decision lives in the pure helper so it can be tested without mocks.
    if _dialect_needs_orphan_recovery(dialect):
        await _recover_orphaned_tmp_tables(db)

    # Skip-if-clean: compare schema file hash with stored hash.
    # Version is passed explicitly so _compute_schema_hash stays a pure
    # function of its arguments (Honest Code: configuration as parameters).
    from declaro_persistum import __version__

    schema_hash = _compute_schema_hash(schema_path, __version__)

    if not force:
        async with reading(db) as conn:
            if await _schema_is_clean(conn, schema_path, schema_hash):
                logger.info("Schema unchanged (hash match) — skipping migration")
                return {
                    "success": True,
                    "operations_applied": 0,
                    "tables_in_schema": 0,
                    "tables_in_database": 0,
                    "skipped": True,
                    "error": None,
                }

    logger.info(f"Loading schema from {schema_path}")
    target_schema, target_views = load_declarations(schema_path)

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
    # errors stop here; an unknown type is a warning and keeps migrating.
    #
    # Measured before wiring it in: every schema this package ships validates
    # clean, and of the fourteen models files the tests build, thirteen
    # validate clean and none has an error.
    validation = validate_schema(target_schema)
    if validation["errors"]:
        for message in validation["errors"]:
            logger.error(f"Schema is not valid: {message}")
        return {
            "success": False,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": 0,
            "skipped": False,
            "error": "; ".join(validation["errors"]),
        }

    if not target_schema:
        logger.warning("No tables found in schema models")
        # Store hash so next call skips
        async with _for_ddl(db) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": 0,
            "tables_in_database": 0,
            "skipped": False,
            "error": None,
        }

    logger.info(f"Loaded {len(target_schema)} tables from schema models")

    # Expand Literal types to lookup tables + FK constraints
    if expand_enums:
        target_schema = expand_schema_enums(target_schema)
        logger.info(
            f"Expanded schema to {len(target_schema)} tables (including enum lookup tables)"
        )

    # Introspect current database state, VIEWS INCLUDED. Asking without them
    # is what made views unreachable: the inspectors, the differ and the
    # appliers all handled views, and the apply path never carried one.
    async with reading(db) as conn:
        current_schema, current_views = await introspect_with_views(conn, dialect)

    logger.info(
        f"Introspected {len(current_schema)} tables and "
        f"{len(current_views)} views from database"
    )

    # Compute diff. Tables and views are diffed by different functions because
    # they are different things: a table is reconciled column by column, a view
    # is replaced whole when its query changes.
    diff_result = diff(current_schema, target_schema, dialect=dialect, decisions=NOTHING)

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

    if not diff_result["operations"]:
        logger.info("Schema is up to date - no migrations needed")
        # Store hash so next call skips introspection
        async with _for_ddl(db) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": 0,
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema),
            "skipped": False,
            "error": None,
        }

    # Log planned operations
    logger.info(f"Found {len(diff_result['operations'])} schema differences")
    for op in diff_result["operations"]:
        logger.info(f"  - {op['op']} on table {op.get('table', 'N/A')}")

    # Check for ambiguities
    if diff_result.get("ambiguities"):
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

    # On embedded replicas (remote_url set), skip reconstruction ops —
    # the replication engine can't replicate DDL, and partial sync (DROP reaches
    # cloud but CREATE doesn't) destroys tables on both sides.
    # Use `declaro migrate-remote` for schema changes that need reconstruction.
    is_cloud_replica = is_replicated(db)
    operations = diff_result["operations"]
    execution_order = diff_result["execution_order"]
    skipped_operations: list[dict[str, Any]] = []

    if is_cloud_replica:
        execution_order, skipped_operations = partition_replica_operations(
            operations, execution_order
        )
        for skipped in skipped_operations:
            logger.warning(
                f"Skipping {skipped['op']} on {skipped.get('table', 'N/A')} — "
                f"reconstruction is unsafe on embedded replicas. "
                f"Run 'declaro migrate-remote' to apply this change to cloud."
            )

        if not execution_order:
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

    # No pause. A migration is a writer, and it holds the serialise lock for
    # the whole of `_for_ddl`; opportunistic replication defers to a waiting
    # writer, so nothing can push mid-transaction.
    async with _for_ddl(db) as conn:
        result = await apply_operations(conn, operations, execution_order, dialect, dry_run=False)

    if result["success"]:
        logger.info(f"Successfully applied {result['operations_applied']} migrations")
        if result.get("error"):
            logger.warning(f"Migration completed with warnings: {result['error']}")
        for sql in result["executed_sql"]:
            logger.debug(f"  Executed: {sql}")
        # Store hash after successful migration
        async with _for_ddl(db) as conn:
            await _ensure_meta_table(conn)
            await _store_hash(conn, schema_path.name, schema_hash)
        return {
            "success": True,
            "operations_applied": result["operations_applied"],
            "tables_in_schema": len(target_schema),
            "tables_in_database": len(current_schema) + result["operations_applied"],
            "skipped": bool(skipped_operations),
            "skipped_operations": skipped_operations,
            "error": None,
        }
    else:
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
