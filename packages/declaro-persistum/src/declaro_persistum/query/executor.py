"""
Query execution functions.

Executes Query objects against database connections.
Handles parameter binding for different dialects.
"""

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
