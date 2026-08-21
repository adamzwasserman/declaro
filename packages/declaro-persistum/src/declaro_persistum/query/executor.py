"""
Query execution functions.

Executes Query objects against database connections.
Handles parameter binding for different dialects.
"""

import re
from collections.abc import Callable
from typing import Any, TypeVar

from declaro_persistum.query.builder import Query
from declaro_persistum.types import written_as

T = TypeVar("T")

_DIALECT_MAP = {
    "asyncpg": "postgresql",
    "aiosqlite": "sqlite",
    "turso": "turso",
}


def _conn_module(conn: Any) -> str:
    """Return the connection's dialect identifier.

    Checks for a ``_declaro_dialect`` class attribute first (set on driver-owned
    async wrappers like TursoAsyncConnection).
    Falls back to ``type(conn).__module__`` for raw driver connections.
    """
    return getattr(conn, "_declaro_dialect", type(conn).__module__)  # honest: ignore HC-P005: the driver's own module is the only marker


def detect_dialect(connection: Any) -> str:
    """Which engine this connection belongs to, or a refusal naming the ones
    that are recognised.

    THE LAST LINE WAS `return "postgresql"  # Default`. Parameter binding is
    the thing this decides -- $1 for asyncpg, ? and :name for the others -- so
    an unrecognised connection was handed PostgreSQL placeholders and failed
    somewhere downstream with a message about SQL syntax rather than about the
    connection. Measured 2026-08-17:

        detect_dialect(SomeOtherDriver()) -> 'postgresql'
        detect_dialect(None)              -> 'postgresql'

    A driver nobody has taught this package about is not PostgreSQL, and
    saying so is the difference between a clear error here and a confusing one
    three layers away.
    """
    conn_type = _conn_module(connection)
    for key, dialect in _DIALECT_MAP.items():
        if key in conn_type:
            return dialect
    raise ValueError(
        f"cannot tell which engine {conn_type!r} belongs to. Recognised: "
        f"{', '.join(sorted(set(_DIALECT_MAP.values())))}."
    )


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


_PLACEHOLDER = re.compile(r":([a-zA-Z_][a-zA-Z0-9_]*)")


def _placeholders(sql: str) -> list[tuple[int, int, str]]:
    """Where the `:name` placeholders are, and only those.

    A COLON IS NOT ALWAYS A PLACEHOLDER, and both converters used to assume it
    was. Three things wear one and are not:

        'time :name here'   a colon inside a string literal is DATA. Rewriting
                            it changes the value the query compares against,
                            so the statement still parses, still runs, and
                            returns the wrong rows without raising.
        "a:b"               a quoted identifier is one column called `a:b`.
        n::text             a cast. A parameter named `text` turned this into
                            `n:$1`, which no engine parses.

    So the scan walks the statement rather than searching it, carrying whether
    it stands inside a literal or a quoted name, and skipping a doubled colon
    whole. Returned as spans so a caller can rewrite from the end and leave
    every earlier offset valid.
    """
    spans: list[tuple[int, int, str]] = []
    position = 0
    while position < len(sql):
        char = sql[position]
        if char in "'\"":
            closing = sql.find(char, position + 1)
            position = len(sql) if closing == -1 else closing + 1
            continue
        if char == ":":
            if sql.startswith("::", position):
                position += 2
                continue
            match = _PLACEHOLDER.match(sql, position)
            if match:
                spans.append((match.start(), match.end(), match.group(1)))
                position = match.end()
                continue
        position += 1
    return spans


def _convert_to_asyncpg(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """Convert :name parameters to $N for asyncpg.

    NUMBERED BY FIRST APPEARANCE IN THE SQL, not by the order the caller built
    the dict in. `$N` is explicit, so a name written twice takes the same
    number twice and its value is passed once.

    This used `str.replace(f":{name}", ...)` under a comment claiming it
    handled word boundaries. It did not: `:id` matched the front of `:id_2`
    and produced `$1_2`, and whether it did depended on dictionary order, so
    the same query worked or failed by how its parameters happened to be
    written.
    """
    param_list: list[Any] = []
    numbers: dict[str, int] = {}
    pieces: list[str] = []
    cursor = 0

    for start, end, name in _placeholders(sql):
        if name not in params:
            continue
        if name not in numbers:
            param_list.append(params[name])
            numbers[name] = len(param_list)
        pieces.append(sql[cursor:start])
        pieces.append(f"${numbers[name]}")
        cursor = end

    pieces.append(sql[cursor:])
    return "".join(pieces), param_list


def _convert_to_aiosqlite(sql: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """aiosqlite supports :name parameters directly."""
    return sql, params


def _convert_to_turso(sql: str, params: dict[str, Any]) -> tuple[str, list[Any]]:
    """Convert :name parameters to ? for turso/pyturso, in SQL occurrence order.

    ONE VALUE PER OCCURRENCE, which is where this differs from asyncpg and
    must keep differing. A `?` carries no number, so a name written twice
    needs its value passed twice; `$1` written twice does not.

    The regex this used respected word boundaries, so it never had asyncpg's
    prefix defect, but it searched the statement rather than walking it and so
    rewrote a colon inside a string literal, appending a second value that
    pushed every later bind one position out.
    """
    param_list: list[Any] = []
    pieces: list[str] = []
    cursor = 0

    for start, end, name in _placeholders(sql):
        if name not in params:
            continue
        param_list.append(params[name])
        pieces.append(sql[cursor:start])
        pieces.append("?")
        cursor = end

    pieces.append(sql[cursor:])
    return "".join(pieces), param_list


async def _execute_fetch(connection: Any, sql: str, params: Any) -> list[dict[str, Any]]:
    """Execute and fetch all rows."""
    conn_type = _conn_module(connection)

    async def _fetch_asyncpg() -> list[dict[str, Any]]:
        rows = await connection.fetch(sql, *params)
        return [dict(row) for row in rows]

    async def _fetch_aiosqlite() -> list[dict[str, Any]]:
        # BORROWED AND GIVEN BACK. This assigned `connection.row_factory` and
        # left it assigned. The connection belongs to the CALLER -- it is
        # passed in, not owned here -- so after one query every raw
        # `conn.execute(...)` they made returned dicts instead of tuples, for
        # the life of the connection.
        #
        # That is also how the scalar defect hid. On a fresh connection a row
        # is a tuple and `row[0]` works; run any other query first and it is a
        # dict and `row[0]` is a lookup for the key `0`. The behaviour
        # depended on what had run before it.
        previous = connection.row_factory
        connection.row_factory = _dict_factory
        try:
            cursor = await connection.execute(sql, params)
            return list(await cursor.fetchall())
        finally:
            connection.row_factory = previous

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
        # BORROWED AND GIVEN BACK, the same way `_fetch_aiosqlite` does it and
        # for the same reason. This assigned the factory and left it assigned,
        # so after one `execute_one` every raw `conn.execute(...)` the CALLER
        # made returned dicts instead of tuples, for the life of a connection
        # this module does not own.
        #
        # The fix was written thirty lines above and did not reach here, which
        # is how `row[0]` came to raise `KeyError: 0` on somebody else's query
        # and read as a driver quirk: it depended on whether an `execute_one`
        # had run first.
        previous = connection.row_factory
        connection.row_factory = _dict_factory
        try:
            cursor = await connection.execute(sql, params)
            row = await cursor.fetchone()
            return dict(row) if row else None
        finally:
            connection.row_factory = previous

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


def _first_named_value(row: Any) -> Any:
    """A mapping: the first value, whatever its column was called."""
    return next(iter(row.values()), None)


def _first_positional_value(row: Any) -> Any:
    """A sequence: the value in the first column."""
    return row[0]


# HOW THE DRIVER WROTE THE ROW, as a table rather than a chain.
_FIRST_VALUE: dict[bool, Callable[[Any], Any]] = {
    True: _first_named_value,
    False: _first_positional_value,
}


def _first_value(row: Any) -> Any:
    """The single value a scalar query selected, however the driver spells a row.

    `row[0]` was here, and a row arrives as a MAPPING from these drivers, so
    that is a lookup for the key `0` and raises:

        total = await execute_scalar(select("COUNT(*) AS c", ...), conn)
        KeyError: 0

    Measured on SQLite 2026-08-17. `COUNT(*)` is the commonest scalar query
    there is, and the shipped example app reaches for `execute_one` and then
    `list(result.values())[0]`, which is this workaround written at the call
    site by somebody who hit the same wall.
    """
    if row is None:
        return None
    # A DRIVER HANDS BACK A MAPPING OR A SEQUENCE, and either way the scalar
    # is its first value. aiosqlite gives a Row that indexes by position,
    # asyncpg a Record that also reads by name, and a row_factory can make it
    # a plain dict.
    return _FIRST_VALUE[written_as(dict, row)](row)


async def _execute_fetch_scalar(connection: Any, sql: str, params: Any) -> Any:
    """Execute and fetch scalar value."""
    conn_type = _conn_module(connection)

    async def _scalar_asyncpg() -> Any:
        return await connection.fetchval(sql, *params)

    async def _scalar_aiosqlite() -> Any:
        cursor = await connection.execute(sql, params)
        return _first_value(await cursor.fetchone())

    async def _scalar_turso() -> Any:
        cursor = await connection.execute(sql, params if params else ())
        return _first_value(await cursor.fetchone())

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


