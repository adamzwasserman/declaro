"""Applying migrations: one function per dialect, chosen by a dispatch table.

`create_applier(dialect)` was the same shape as `create_inspector` — a table
over CLASSES that instantiated the winner on the way out. The classes were
stateless, and the SQL generators beside them were already module-level
functions. Only `apply()` lived on the class, and it took a connection and a
list of operations and ran them, which is a function's job.

    result = await apply(conn, operations, "sqlite")

An unknown dialect raises `ValueError` naming the ones that exist, rather than
a bare `KeyError` that says what was asked for and not what was available.
"""

from __future__ import annotations

from typing import Any

from declaro_persistum.applier import postgresql, sqlite, turso
from declaro_persistum.types import Dialect


class _Appliers(dict):
    """A dispatch table that explains itself when asked for something absent."""

    def __missing__(self, dialect: Dialect) -> Any:
        raise ValueError(
            f"Unsupported dialect: {dialect}. "
            f"Supported dialects: {', '.join(sorted(self))}"
        )


APPLIERS = _Appliers(
    {
        "postgresql": postgresql.apply,
        "sqlite": sqlite.apply,
        "turso": turso.apply,
    }
)


async def apply(
    connection: Any,
    operations: list[dict[str, Any]],
    execution_order: list[int],
    dialect: Dialect,
    **kwargs: Any,
) -> Any:
    """Run migration operations against a database, in the given order.

    `execution_order` is required and is the differ's topological sort —
    indices into `operations`. It is not defaulted to `range(len(operations))`
    because declaration order is not dependency order: creating a table that
    references another before that other exists fails, and a default would
    make that failure look like the database's fault rather than a missing
    sort (Rule 14).
    """
    return await APPLIERS[dialect](connection, operations, execution_order, **kwargs)


__all__ = ["APPLIERS", "apply"]
