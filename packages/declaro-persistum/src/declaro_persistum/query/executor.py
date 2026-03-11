"""
Query execution functions.

Executes Query objects against database connections.
Handles parameter binding for different dialects.
"""

import asyncio
import time
from collections.abc import Callable
from typing import Any, TypeVar

from declaro_persistum.query.builder import Query

T = TypeVar("T")

_DIALECT_MAP = {
    "asyncpg": "postgresql",
    "aiosqlite": "sqlite",
    "libsql": "turso",
}


def detect_dialect(connection: Any) -> str:
    """Detect database dialect from connection type."""
    conn_type = type(connection).__module__
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
        connection: Database connection (asyncpg, aiosqlite, or libsql)
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
    - Turso (libsql): ? (positional)

    Returns:
        Tuple of (sql, params) ready for execution
    """
    sql = query["sql"]
    params = query["params"]

    # Detect connection type
    conn_type = type(connection).__module__

    _CONVERTERS = {
        "asyncpg": _convert_to_asyncpg,
        "aiosqlite": _convert_to_aiosqlite,
        "libsql": _convert_to_libsql,
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


def _convert_to_libsql(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """Convert :name parameters to ? for libsql."""
    # Find all :param_name occurrences and replace with ?
    param_list: list[Any] = []
    result_sql = sql

    # Sort by length descending to avoid partial replacements
    sorted_names = sorted(params.keys(), key=len, reverse=True)

    for name in sorted_names:
        if f":{name}" in result_sql:
            param_list.append(params[name])
            result_sql = result_sql.replace(f":{name}", "?", 1)

    return result_sql, param_list


async def _execute_fetch(connection: Any, sql: str, params: Any) -> list[dict[str, Any]]:
    """Execute and fetch all rows."""
    conn_type = type(connection).__module__

    async def _fetch_asyncpg() -> list[dict[str, Any]]:
        rows = await connection.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def _fetch_aiosqlite() -> list[dict[str, Any]]:
        connection.row_factory = _dict_factory
        cursor = await connection.execute(sql, params)
        rows = await cursor.fetchall()
        return list(rows)

    async def _fetch_libsql() -> list[dict[str, Any]]:
        cursor = connection.execute(sql, params if params else ())
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [dict(zip(columns, row, strict=False)) for row in rows]

    _FETCHERS = {"asyncpg": _fetch_asyncpg, "aiosqlite": _fetch_aiosqlite, "libsql": _fetch_libsql}

    for key, fetcher in _FETCHERS.items():
        if key in conn_type:
            return await fetcher()
    raise ValueError(f"Unsupported connection type: {conn_type}")


async def _execute_fetch_one(connection: Any, sql: str, params: Any) -> dict[str, Any] | None:
    """Execute and fetch single row."""
    conn_type = type(connection).__module__

    async def _fetch_one_asyncpg() -> dict[str, Any] | None:
        row = await connection.fetchrow(sql, *params)
        return dict(row) if row else None

    async def _fetch_one_aiosqlite() -> dict[str, Any] | None:
        connection.row_factory = _dict_factory
        cursor = await connection.execute(sql, params)
        row = await cursor.fetchone()
        return dict(row) if row else None

    async def _fetch_one_libsql() -> dict[str, Any] | None:
        cursor = connection.execute(sql, params if params else ())
        row = cursor.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return dict(zip(columns, row, strict=True))

    _FETCHERS = {"asyncpg": _fetch_one_asyncpg, "aiosqlite": _fetch_one_aiosqlite, "libsql": _fetch_one_libsql}

    for key, fetcher in _FETCHERS.items():
        if key in conn_type:
            return await fetcher()
    raise ValueError(f"Unsupported connection type: {conn_type}")


async def _execute_fetch_scalar(connection: Any, sql: str, params: Any) -> Any:
    """Execute and fetch scalar value."""
    conn_type = type(connection).__module__

    async def _scalar_asyncpg() -> Any:
        return await connection.fetchval(sql, *params)

    async def _scalar_aiosqlite() -> Any:
        cursor = await connection.execute(sql, params)
        row = await cursor.fetchone()
        return row[0] if row else None

    async def _scalar_libsql() -> Any:
        cursor = connection.execute(sql, params if params else ())
        row = cursor.fetchone()
        return row[0] if row else None

    _FETCHERS = {"asyncpg": _scalar_asyncpg, "aiosqlite": _scalar_aiosqlite, "libsql": _scalar_libsql}

    for key, fetcher in _FETCHERS.items():
        if key in conn_type:
            return await fetcher()
    raise ValueError(f"Unsupported connection type: {conn_type}")


async def _execute_update(connection: Any, sql: str, params: Any) -> int:
    """Execute and return rows affected."""
    conn_type = type(connection).__module__

    async def _update_asyncpg() -> int:
        result = await connection.execute(sql, *params)
        parts = result.split()
        return int(parts[-1]) if parts else 0

    async def _update_aiosqlite() -> int:
        cursor = await connection.execute(sql, params)
        await connection.commit()
        return int(cursor.rowcount)

    async def _update_libsql() -> int:
        cursor = connection.execute(sql, params if params else ())
        connection.commit()
        return int(cursor.rowcount)

    _UPDATERS = {"asyncpg": _update_asyncpg, "aiosqlite": _update_aiosqlite, "libsql": _update_libsql}

    for key, updater in _UPDATERS.items():
        if key in conn_type:
            return await updater()
    raise ValueError(f"Unsupported connection type: {conn_type}")


def _dict_factory(cursor: Any, row: tuple[Any, ...]) -> dict[str, Any]:
    """Row factory for aiosqlite to return dicts."""
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row, strict=True))


async def _race_write(
    pool: Any,
    query: "Query",
    queue: Any,
    table_name: str,
    pk_column: str,
    pk_value: Any,
    op: str,
    data: dict[str, Any],
    dialect: str,
) -> Any:
    """
    Race a write against the 50ms threshold.

    - If the write completes within threshold: queue is never touched.
    - If it times out: enqueue immediately and return data to caller.
      The write continues in background; on completion it removes itself
      from the queue.
    """
    sql = query["sql"]
    params = query["params"]

    async def _do_write() -> Any:
        async with pool.acquire() as conn:
            return await _execute_update(conn, sql, params)

    write_task = asyncio.create_task(_do_write())
    try:
        result = await asyncio.wait_for(asyncio.shield(write_task), timeout=queue._threshold_ms / 1000.0)
        return result  # fast path — queue never touched
    except asyncio.TimeoutError:
        queue.enqueue(table_name, pk_column, pk_value, op, data, sql, params, dialect)

        async def _on_complete() -> None:
            try:
                await write_task
                queue.remove_entry(table_name, pk_value)
            except Exception as exc:
                key = f"{table_name}:{pk_value}"
                if key in queue._queue:
                    queue._queue[key]["attempt_count"] += 1
                    queue._queue[key]["last_error"] = str(exc)[:200]
                    queue._persist_to_disk()

        asyncio.create_task(_on_complete())
        return data  # return immediately — write continues in background


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

    For write operations (insert/update/delete), if the pool has a write queue
    attached and the write exceeds the threshold, the entry is queued and the
    caller receives the data immediately.

    For SELECT results, pending write queue entries are merged in and results
    are re-sorted to match the query's ORDER BY clause.

    Args:
        pool: Connection pool with acquire() context manager
        to_query: Callable that takes (dialect: str) and returns a Query dict
        mode: "all" | "one" | "scalar"
        table_name: Table being queried/written (for write queue key)
        pk_column: Primary key column name (for write queue key)
        pk_value: Primary key value (for write queue key)
        data: Row data to return immediately on queue (for write queue)
        join_tables: Additional tables joined in this query (for JOIN merge)
        schema: Full schema dict (for FK-aware JOIN merge)
    """
    from declaro_persistum.instrumentation import classify_sql, is_write_op, record_execution

    _MODE_DISPATCH: dict[str, Callable[..., Any]] = {
        "all": execute,
        "one": execute_one,
        "scalar": execute_scalar,
    }
    executor_fn = _MODE_DISPATCH[mode]

    async with pool.acquire() as conn:
        dialect = detect_dialect(conn)
        query = to_query(dialect)
        sql = query.get("sql", "")
        op = classify_sql(sql)

        # Write queue race — only when queue is attached and all metadata provided
        queue = getattr(pool, "_write_queue", None)
        if queue is not None and is_write_op(op) and table_name and pk_column and pk_value is not None:
            t0 = time.monotonic()
            try:
                result = await _race_write(
                    pool, query, queue, table_name, pk_column, pk_value, op,
                    data or {}, dialect
                )
                duration_ms = (time.monotonic() - t0) * 1000
                record_execution(pool, sql, duration_ms, success=True)
                return result
            except Exception as exc:
                duration_ms = (time.monotonic() - t0) * 1000
                record_execution(pool, sql, duration_ms, success=False, error=str(exc))
                raise

        t0 = time.monotonic()
        error = ""
        try:
            result = await executor_fn(query, conn)
            # aiosqlite requires explicit commit for DML (INSERT/UPDATE/DELETE)
            # asyncpg and libsql are autocommit; aiosqlite is not
            if is_write_op(op) and "aiosqlite" in type(conn).__module__:
                await conn.commit()
            duration_ms = (time.monotonic() - t0) * 1000
            record_execution(pool, sql, duration_ms, success=True)

            # Read merge — merge pending write queue entries into SELECT results
            if queue is not None and not is_write_op(op) and table_name and pk_column:
                if isinstance(result, list):
                    from declaro_persistum.write_queue import (
                        _extract_order_by,
                        _resort,
                        merge_pending_into_join_results,
                        merge_pending_into_results,
                    )
                    pending = queue.get_pending_for_table(table_name)
                    if pending:
                        result = merge_pending_into_results(result, pending, pk_column)
                    if join_tables and schema:
                        pending_by_table = {t: queue.get_pending_for_table(t) for t in join_tables}
                        if any(pending_by_table.values()):
                            result = merge_pending_into_join_results(
                                result, pending_by_table, schema, join_tables, table_name
                            )
                    order_by = _extract_order_by(sql)
                    if order_by:
                        result = _resort(result, order_by)

            return result
        except Exception as exc:
            duration_ms = (time.monotonic() - t0) * 1000
            error = str(exc)
            record_execution(pool, sql, duration_ms, success=False, error=error)
            raise
