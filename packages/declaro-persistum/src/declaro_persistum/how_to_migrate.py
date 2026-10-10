"""How far back a failed migration rolls on one engine.

    fn how_to_migrate : (engine: Engine) -> MigrationMode

`docs/architecture.md:56`: what differs between engines lives in a table rather
than in branches, with one engine per row, read by subscript so an engine
nobody declared fails at the lookup instead of being handled as something else.

WHY A CALLER HAS TO KNOW. A migration that stops halfway leaves either nothing
or everything before the failure, and those need different recoveries. Believing
the wrong one means retrying from the wrong place: re-running an operation that
already landed, or skipping one that never did.

NO FALLBACK ON THE READ. `MIGRATION_MODES[engine]`, never `.get`. A default
would file an engine nobody wrote a rule for under a rule written for a
different one, which is the silence a table exists to remove.
"""

from __future__ import annotations

from declaro_persistum.types import Engine, MigrationMode

# ONE ROW PER ENGINE, AND ADDING AN ENGINE IS ONE MORE LINE. All three wrap a
# migration in one transaction, so a failure leaves the database exactly as it
# was.
#
# TURSO SAID `per_operation` UNTIL 2026-10-10, on the reasoning that Turso will
# not wrap a migration. Measured against pyturso that day and it is false. Plain
# `BEGIN` rolls back a created table and an added column, and a three-statement
# batch whose middle statement fails leaves the database holding only what it
# started with. `fn query_run_together` already runs the whole group inside one
# transaction and refuses it whole.
#
# THE BEGIN WORD FOR A SCHEMA CHANGE IS `BEGIN` AND NOT `BEGIN CONCURRENT`, which
# is why the mode is the same on all three now. Turso refuses DDL inside a
# concurrent transaction in its own words: "DDL statements require an exclusive
# transaction (use BEGIN instead of BEGIN CONCURRENT)". `fn how_to_write` hands
# the crew `BEGIN CONCURRENT` for data, and a migration is not data.
MIGRATION_MODES: dict[Engine, MigrationMode] = {
    "sqlite": "all_or_nothing",
    "postgresql": "all_or_nothing",
    "turso": "all_or_nothing",
}


def how_to_migrate(engine: Engine) -> MigrationMode:
    """Say how far back a failed migration rolls on this engine."""
    return MIGRATION_MODES[engine]
