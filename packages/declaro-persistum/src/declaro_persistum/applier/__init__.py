"""Applying migrations: one function per dialect, chosen by a dispatch table.

`create_applier(dialect)` was the same shape as `create_inspector` — a table
over CLASSES that instantiated the winner on the way out. The classes were
stateless, and the SQL generators beside them were already module-level
functions. Only `apply()` lived on the class, and it took a connection and a
list of operations and ran them, which is a function's job.

    result = await apply(conn, operations, order, "sqlite", dry_run=False)

An unknown dialect raises `ValueError` naming the ones that exist, rather than
a bare `KeyError` that says what was asked for and not what was available.
"""

from __future__ import annotations

from typing import Any

from declaro_persistum.applier import postgresql, sqlite, turso
from declaro_persistum.types import Dialect, for_dialect

# A PLAIN DICT. This was `class _Appliers(dict)` with one method, `__missing__`,
# subclassing dict to borrow its behaviour and override a hook. The lookup is
# what varied, so the lookup is a function now: `for_dialect` in `types.py`,
# shared with the inspector table that had the identical class beside it.
APPLIERS: dict[str, Any] = {
    "postgresql": postgresql.apply,
    "sqlite": sqlite.apply,
    "turso": turso.apply,
}


async def apply(
    connection: Any,
    operations: list[dict[str, Any]],
    execution_order: list[int],
    dialect: Dialect,
    *,
    dry_run: bool,
) -> Any:
    """Run migration operations against a database, in the given order.

    `execution_order` is required and is the differ's topological sort —
    indices into `operations`. It is not defaulted to `range(len(operations))`
    because declaration order is not dependency order: creating a table that
    references another before that other exists fails, and a default would
    make that failure look like the database's fault rather than a missing
    sort (Rule 14).

    `dry_run` is required for the same reason and a sharper one: it chose
    between previewing a migration and running it, and it chose RUN by
    omission. Every production caller left it out, so not one of them recorded
    a decision, and the direction that writes DDL to a live database was the
    one you got by forgetting there was a choice.

    THE `**kwargs` THAT USED TO SIT HERE COST MORE THAN TYPE INFORMATION. A
    signature with `**kwargs` accepts every keyword, so no gate can check any
    call to it. The sample two paragraphs above read
    `apply(conn, operations, "sqlite")` -- which binds "sqlite" to
    `execution_order` and never passes `dialect` -- for a full session, in the
    docstring of the package's main entry point, and nothing could see it.
    """
    applier = for_dialect(APPLIERS, dialect, "Appliers")
    return await applier(
        connection, operations, execution_order, dry_run=dry_run
    )


# The per-operation renderer is what varies. The loop over `execution_order`
# does not, and it lived only in `postgresql.py`, so `declaro generate` raised
# AttributeError on SQLite and on Turso. Measured 2026-08-17.
SQL_GENERATORS: dict[str, Any] = {
    "postgresql": postgresql.generate_operation_sql,
    "sqlite": sqlite.generate_operation_sql,
    "turso": turso.generate_operation_sql,
}


def generate_sql(
    operations: list[dict[str, Any]],
    execution_order: list[int],
    dialect: Dialect,
) -> list[str]:
    """Render the operations as SQL, in dependency order, without running them.

    ONE LOOP, NOT THREE. This was a member of each applier module, and only one
    of the three modules had it:

        postgresql   generate_sql   yes
        sqlite       generate_sql   no
        turso        generate_sql   no

    `declaro generate` calls it, so the command worked on PostgreSQL and raised
    AttributeError on the two backends a developer is likeliest to be running.
    Nothing in the loop is dialect-specific, so putting it here removes the
    place the copies could go missing from.
    """
    render = for_dialect(SQL_GENERATORS, dialect, "SQL generators")
    return [render(operations[i]) for i in execution_order]


__all__ = ["APPLIERS", "SQL_GENERATORS", "apply", "generate_sql"]
