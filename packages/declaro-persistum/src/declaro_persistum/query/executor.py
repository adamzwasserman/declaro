"""
Query execution functions.

Executes Query objects against database connections.
Handles parameter binding for different dialects.
"""

import re
import time
from collections.abc import Callable
from typing import Any, TypeVar

from declaro_persistum.query.builder import Query

T = TypeVar("T")

_DIALECT_MAP = {
    "asyncpg": "postgresql",
    "aiosqlite": "sqlite",
    "turso": "turso",
}


def _conn_module(conn: Any) -> str:
    """Return the connection's dialect identifier.

    Checks for a ``_declaro_dialect`` class attribute first (set on pool-owned
    async wrappers like TursoAsyncConnection).
    Falls back to ``type(conn).__module__`` for raw driver connections.
    """
    return getattr(conn, "_declaro_dialect", type(conn).__module__)


def detect_dialect(connection: Any) -> str:
    """Detect database dialect from connection type."""
    conn_type = _conn_module(connection)
    for key, dialect in _DIALECT_MAP.items():
        if key in conn_type:
            return dialect
    return "postgresql"  # Default


async def execute(
    query: Query,
    connection: Any,
    *,
    row_factory: Callable[[dict[str, Any]], T] | None = None,
) -> list[T] | list[dict[str, Any]]:
    """
    Execute a query and return all results.

    Args:
        query: Query to execute
        connection: Database connection (asyncpg, aiosqlite, or turso)
        row_factory: Optional function to transform rows

    Returns:
        List of rows as dicts, or transformed by row_factory

    Example:
        >>> q = select("*", from_table="users", where="active = :active", params={"active": True})
        >>> users = await execute(q, conn)
        >>> print(users[0]["email"])
    """
    sql, params = _prepare_query(query, connection)
    rows = await _execute_fetch(connection, sql, params)

    if row_factory:
        return [row_factory(row) for row in rows]
    return rows


async def execute_one(
    query: Query,
    connection: Any,
    *,
    row_factory: Callable[[dict[str, Any]], T] | None = None,
) -> T | dict[str, Any] | None:
    """
    Execute a query and return single result or None.

    Args:
        query: Query to execute
        connection: Database connection
        row_factory: Optional function to transform the row

    Returns:
        Single row as dict (or transformed), or None if no results

    Example:
        >>> q = select("*", from_table="users", where="id = :id", params={"id": 1})
        >>> user = await execute_one(q, conn)
        >>> if user:
        ...     print(user["email"])
    """
    sql, params = _prepare_query(query, connection)
    row = await _execute_fetch_one(connection, sql, params)

    if row is None:
        return None

    if row_factory:
        return row_factory(row)
    return row


async def execute_scalar(
    query: Query,
    connection: Any,
) -> Any:
    """
    Execute a query and return scalar value.

    Args:
        query: Query to execute
        connection: Database connection

    Returns:
        Single scalar value from first column of first row

    Example:
        >>> q = select("count(*)", from_table="users")
        >>> count = await execute_scalar(q, conn)
        >>> print(f"Total users: {count}")
    """
    sql, params = _prepare_query(query, connection)
    return await _execute_fetch_scalar(connection, sql, params)


async def execute_many(
    query: Query,
    connection: Any,
    params_list: list[dict[str, Any]],
) -> int:
    """
    Execute a query multiple times with different parameters.

    Useful for bulk inserts or updates.

    Args:
        query: Query template to execute
        connection: Database connection
        params_list: List of parameter dicts

    Returns:
        Total number of rows affected

    Example:
        >>> q = insert("users", {"email": ":email", "name": ":name"})
        >>> rows = await execute_many(q, conn, [
        ...     {"email": "a@b.com", "name": "A"},
        ...     {"email": "c@d.com", "name": "C"},
        ... ])
    """
    total = 0
    for params in params_list:
        merged_query: Query = {
            "sql": query["sql"],
            "params": {**query["params"], **params},
            "dialect": query["dialect"],
        }
        sql, bound_params = _prepare_query(merged_query, connection)
        count = await _execute_update(connection, sql, bound_params)
        total += count
    return total


def _prepare_query(query: Query, connection: Any) -> tuple[str, Any]:
    """
    Prepare query for execution, converting parameters as needed.

    Different databases use different parameter styles:
    - PostgreSQL (asyncpg): $1, $2, ... (positional)
    - SQLite (aiosqlite): :name or ? (named or positional)
    - Turso (pyturso): ? (positional)

    Returns:
        Tuple of (sql, params) ready for execution
    """
    sql = query["sql"]
    params = query["params"]

    conn_type = _conn_module(connection)

    _CONVERTERS = {
        "asyncpg": _convert_to_asyncpg,
        "aiosqlite": _convert_to_aiosqlite,
        "turso": _convert_to_turso,
    }

    for key, converter in _CONVERTERS.items():
        if key in conn_type:
            return converter(sql, params)
    # Default: assume named parameters work
    return sql, params


def _convert_to_asyncpg(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """Convert :name parameters to $N for asyncpg."""
    # Find all :param_name occurrences and replace with $N
    param_list: list[Any] = []
    param_map: dict[str, int] = {}
    result_sql = sql

    for name, value in params.items():
        if name not in param_map:
            param_list.append(value)
            param_map[name] = len(param_list)

        # Replace :name with $N (handle word boundaries)
        result_sql = result_sql.replace(f":{name}", f"${param_map[name]}")

    return result_sql, param_list


def _convert_to_aiosqlite(sql: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """aiosqlite supports :name parameters directly."""
    return sql, params


def _convert_to_turso(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """Convert :name parameters to ? for turso/pyturso, in SQL occurrence order."""
    param_list: list[Any] = []

    def replace_param(match: re.Match) -> str:  # type: ignore[type-arg]
        name = match.group(1)
        if name in params:
            param_list.append(params[name])
            return "?"
        return match.group(0)  # leave unknown :names as-is

    result_sql = re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", replace_param, sql)
    return result_sql, param_list


async def _execute_fetch(connection: Any, sql: str, params: Any) -> list[dict[str, Any]]:
    """Execute and fetch all rows."""
    conn_type = _conn_module(connection)

    async def _fetch_asyncpg() -> list[dict[str, Any]]:
        rows = await connection.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def _fetch_aiosqlite() -> list[dict[str, Any]]:
        connection.row_factory = _dict_factory
        cursor = await connection.execute(sql, params)
        rows = await cursor.fetchall()
        return list(rows)

    async def _fetch_turso() -> list[dict[str, Any]]:
        cursor = await connection.execute(sql, params if params else ())
        rows = await cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [dict(zip(columns, row, strict=False)) for row in rows]

    _FETCHERS = {
        "asyncpg": _fetch_asyncpg,
        "aiosqlite": _fetch_aiosqlite,
        "turso": _fetch_turso,
    }

    for key, fetcher in _FETCHERS.items():
        if key in conn_type:
            return await fetcher()
    raise ValueError(f"Unsupported connection type: {conn_type}")


async def _execute_fetch_one(connection: Any, sql: str, params: Any) -> dict[str, Any] | None:
    """Execute and fetch single row."""
    conn_type = _conn_module(connection)

    async def _fetch_one_asyncpg() -> dict[str, Any] | None:
        row = await connection.fetchrow(sql, *params)
        return dict(row) if row else None

    async def _fetch_one_aiosqlite() -> dict[str, Any] | None:
        connection.row_factory = _dict_factory
        cursor = await connection.execute(sql, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def _fetch_one_turso() -> dict[str, Any] | None:
        cursor = await connection.execute(sql, params if params else ())
        row = await cursor.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return dict(zip(columns, row, strict=True))

    _FETCHERS = {
        "asyncpg": _fetch_one_asyncpg,
        "aiosqlite": _fetch_one_aiosqlite,
        "turso": _fetch_one_turso,
    }

    for key, fetcher in _FETCHERS.items():
        if key in conn_type:
            return await fetcher()
    raise ValueError(f"Unsupported connection type: {conn_type}")


async def _execute_fetch_scalar(connection: Any, sql: str, params: Any) -> Any:
    """Execute and fetch scalar value."""
    conn_type = _conn_module(connection)

    async def _scalar_asyncpg() -> Any:
        return await connection.fetchval(sql, *params)

    async def _scalar_aiosqlite() -> Any:
        cursor = await connection.execute(sql, params)
        row = await cursor.fetchone()
        return row[0] if row else None

    async def _scalar_turso() -> Any:
        cursor = await connection.execute(sql, params if params else ())
        row = await cursor.fetchone()
        return row[0] if row else None

    _FETCHERS = {
        "asyncpg": _scalar_asyncpg,
        "aiosqlite": _scalar_aiosqlite,
        "turso": _scalar_turso,
    }

    for key, fetcher in _FETCHERS.items():
        if key in conn_type:
            return await fetcher()
    raise ValueError(f"Unsupported connection type: {conn_type}")


async def _execute_update(connection: Any, sql: str, params: Any) -> int:
    """Execute and return rows affected."""
    conn_type = _conn_module(connection)

    async def _update_asyncpg() -> int:
        result = await connection.execute(sql, *params)
        parts = result.split()
        return int(parts[-1]) if parts else 0

    async def _update_aiosqlite() -> int:
        cursor = await connection.execute(sql, params)
        await connection.commit()
        return int(cursor.rowcount)

    async def _update_turso() -> int:
        cursor = await connection.execute(sql, params if params else ())
        await connection.commit()
        return int(cursor.rowcount)

    _UPDATERS = {
        "asyncpg": _update_asyncpg,
        "aiosqlite": _update_aiosqlite,
        "turso": _update_turso,
    }

    for key, updater in _UPDATERS.items():
        if key in conn_type:
            return await updater()
    raise ValueError(f"Unsupported connection type: {conn_type}")


def _dict_factory(cursor: Any, row: tuple[Any, ...]) -> dict[str, Any]:
    """Row factory for aiosqlite to return dicts."""
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row, strict=True))


async def execute_with_pool(
    pool: Any,
    to_query: Callable[..., Query],
    mode: str = "all",
    *,
    table_name: str = "",
    pk_column: str = "",
    pk_value: Any = None,
    data: dict[str, Any] | None = None,
    join_tables: list[str] | None = None,
    schema: dict[str, Any] | None = None,
) -> Any:
    """
    Acquire a connection from pool, detect dialect, build query, execute.

    Times the execution and records latency if the pool has instrumentation
    enabled (pool._latency_logger is set).

    For pools with a ``dialect`` property, the dialect is read directly —
    no connection is acquired just for detection.  Write operations are
    dispatched to ``pool.acquire_write()`` when available (e.g. TursoPool
    with MVCC ``BEGIN CONCURRENT``).

    Args:
        pool: Connection pool with acquire() context manager
        to_query: Callable that takes (dialect: str) and returns a Query dict
        mode: "all" | "one" | "scalar"
        table_name: Table being queried/written (unused, kept for compat)
        pk_column: Primary key column name (unused, kept for compat)
        pk_value: Primary key value (unused, kept for compat)
        data: Row data (unused, kept for compat)
        join_tables: Additional tables (unused, kept for compat)
        schema: Full schema dict (unused, kept for compat)
    """
    from declaro_persistum.instrumentation import (
        classify_sql,
        has_returning_clause,
        is_write_op,
        record_execution,
    )

    _MODE_DISPATCH: dict[str, Callable[..., Any]] = {
        "all": execute,
        "one": execute_one,
        "scalar": execute_scalar,
    }
    executor_fn = _MODE_DISPATCH[mode]

    # Fast path: pool knows its dialect — skip read-connection for detection
    pool_dialect = getattr(pool, "dialect", None)
    if pool_dialect:
        query = to_query(pool_dialect)
        sql = query.get("sql", "")
        op = classify_sql(sql)

        # Write via acquire_write when pool supports it (e.g. MVCC).
        # Two sub-cases:
        #   - SQL has RETURNING → use the fetch path on the write conn so
        #     the caller gets the returned rows (the documented contract
        #     for prisma create / update / update_many / delete).
        #   - SQL has no RETURNING → use the count path; result is rowcount.
        # Without this split, every write op on an acquire_write pool went
        # through the count path and silently returned int instead of rows,
        # crashing prisma update_many's len() and breaking the documented
        # dict return type of update_one / create / delete.
        if is_write_op(op) and hasattr(pool, "acquire_write"):
            t0 = time.monotonic()
            try:
                async with pool.acquire_write() as wconn:
                    if has_returning_clause(sql):
                        result = await executor_fn(query, wconn)
                    else:
                        prepared_sql, prepared_params = _prepare_query(query, wconn)
                        result = await _execute_update(
                            wconn, prepared_sql, prepared_params
                        )
                duration_ms = (time.monotonic() - t0) * 1000
                record_execution(pool, sql, duration_ms, success=True)
                return result
            except Exception as exc:
                duration_ms = (time.monotonic() - t0) * 1000
                record_execution(pool, sql, duration_ms, success=False, error=str(exc))
                raise

    # Standard path: acquire connection, detect dialect if needed
    async with pool.acquire() as conn:
        if pool_dialect is None:
            dialect = detect_dialect(conn)
            query = to_query(dialect)
            sql = query.get("sql", "")
            op = classify_sql(sql)

        t0 = time.monotonic()
        try:
            if is_write_op(op) and hasattr(pool, "acquire_write"):
                # Same split as the fast path above: RETURNING -> fetch, else -> count.
                async with pool.acquire_write() as wconn:
                    if has_returning_clause(sql):
                        result = await executor_fn(query, wconn)
                    else:
                        prepared_sql, prepared_params = _prepare_query(query, wconn)
                        result = await _execute_update(
                            wconn, prepared_sql, prepared_params
                        )
            else:
                result = await executor_fn(query, conn)
                # aiosqlite and turso require explicit commit after DML.
                _cm = _conn_module(conn)
                if is_write_op(op) and ("aiosqlite" in _cm or "turso" in _cm):
                    await conn.commit()
            duration_ms = (time.monotonic() - t0) * 1000
            record_execution(pool, sql, duration_ms, success=True)
            return result
        except Exception as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            record_execution(pool, sql, duration_ms, success=False, error=str(exc))
            raise
