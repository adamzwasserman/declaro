"""
Query execution functions.

Executes Query objects against database connections.
Handles parameter binding for different dialects.
"""

import re
from collections.abc import Callable
from typing import Any, TypeVar

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.query.builder import Query
from declaro_persistum.types import Nothing, resolved, written_as

T = TypeVar("T")

# A ROW AS THE DRIVER WROTE IT. Each driver hands back its own row object,
# and this module's job is to give the caller either that or whatever their
# factory makes of it, so the row itself is genuinely the one thing here that
# cannot be named more tightly.
Row = dict[str, Any]

# What the caller asked rows to be turned into, or nothing at all. Written
# down once because the four functions below and the two tables that hold them
# all take it, and every one of them said `Any`.
RowFactory = Callable[[Row], T] | None


@helper
def _rows_as_the_driver_wrote_them(
    rows: list[Row], _row_factory: RowFactory[T]
) -> list[Row] | list[T]:
    """No factory, so the rows go back exactly as they came off the cursor."""
    return rows


@helper
def _rows_through_the_factory(
    rows: list[Row], row_factory: Callable[[Row], T]
) -> list[T]:
    """Each row is handed to the caller's factory, in order."""
    return [row_factory(row) for row in rows]


# THE KEY AND THE ARGUMENT TYPES CO-VARY, which Python cannot say. The True
# entry is only ever called when there is a factory, so it takes one that is
# not optional; the False entry ignores whatever it is handed. `...` states
# that the arguments are not constrained here rather than calling them `Any`,
# which would have said nothing and hidden the difference.
_ALL_ROWS: dict[bool, Callable[..., list[Row] | list[Any]]] = {
    True: _rows_through_the_factory,
    False: _rows_as_the_driver_wrote_them,
}


@helper
def _row_as_the_driver_wrote_it(
    row: Row | None, _row_factory: RowFactory[T]
) -> Row | T | None:
    """Either there is no row to shape or no factory to shape it with."""
    return row


@helper
def _row_through_the_factory(row: Row, row_factory: Callable[[Row], T]) -> T:
    """There is a row and a factory, so the factory shapes it."""
    return row_factory(row)


# A ROW THAT IS ABSENT IS NOT PUT THROUGH A FACTORY. `execute_one` asks both
# halves of that in one key, because a factory called on a missing row would
# be handed None and asked to make a record of it.
_ONE_ROW: dict[bool, Callable[..., Row | Any | None]] = {
    True: _row_through_the_factory,
    False: _row_as_the_driver_wrote_it,
}

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


@helper
def _recognised_keys(conn_type: str) -> list[str]:
    """Which driver names in `_DIALECT_MAP` appear in this module path.

    The scan is by substring because a driver keeps its connection in a
    submodule: `asyncpg.connection`, `aiosqlite.core`. `_DIALECT_MAP` is
    ordered, so the first name that appears is the answer, which is what the
    loop with a `return` inside it used to give.
    """
    return [key for key in _DIALECT_MAP if key in conn_type]


@helper
def _refuse_unnamed_engine(conn_type: str, _keys: list[str]) -> str:
    """A driver nobody has taught this package about, refused by name."""
    raise ValueError(
        f"cannot tell which engine {conn_type!r} belongs to. Recognised: "
        f"{', '.join(sorted(set(_DIALECT_MAP.values())))}."
    )


@helper
def _the_named_engine(_conn_type: str, keys: list[str]) -> str:
    """The module path names a driver, so its dialect is the answer."""
    return _DIALECT_MAP[keys[0]]


_WHEN_ENGINE_UNNAMED: dict[bool, Callable[[str, list[str]], str]] = {
    True: _the_named_engine,
    False: _refuse_unnamed_engine,
}


@helper
def _refuse_unsupported_connection(conn_type: str, _keys: list[str]) -> str:
    """The same refusal, in the words the execution paths have always used."""
    raise ValueError(f"Unsupported connection type: {conn_type}")


@helper
def _the_supported_key(_conn_type: str, keys: list[str]) -> str:
    """The driver name the per-dialect tables are keyed on."""
    return keys[0]


_WHEN_CONNECTION_UNSUPPORTED: dict[bool, Callable[[str, list[str]], str]] = {
    True: _the_supported_key,
    False: _refuse_unsupported_connection,
}


@helper
def _driver_key(conn_type: str) -> str:
    """The key every per-dialect table in this module is read with.

    A CONNECTION NO TABLE HOLDS IS REFUSED, not defaulted. `_prepare_query`
    used to fall through to `return sql, params` under a comment reading
    "assume named parameters work", which handed an unrecognised driver the
    statement unconverted and let it fail further downstream.
    """
    return [
        _WHEN_CONNECTION_UNSUPPORTED[bool(keys)](conn_type, keys)
        for keys in [_recognised_keys(conn_type)]
    ][0]


@helper
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
    return [
        _WHEN_ENGINE_UNNAMED[bool(keys)](conn_type, keys)
        for keys in [_recognised_keys(conn_type)]
    ][0]


@helper
async def execute(
    query: Query,
    connection: Any,
    *,
    row_factory: Callable[[dict[str, Any]], T] | None | Nothing,
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
        >>> q = select("*", from_table="users", where="active = :active", params={"active": True}, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING)
        >>> users = await execute(q, conn, row_factory=NOTHING)
        >>> print(users[0]["email"])
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    row_factory = resolved(row_factory, None)
    sql, params = _prepare_query(query, connection)
    rows = await _execute_fetch(connection, sql, params)

    return _ALL_ROWS[bool(row_factory)](rows, row_factory)


@helper
async def execute_one(
    query: Query,
    connection: Any,
    *,
    row_factory: Callable[[dict[str, Any]], T] | None | Nothing,
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
        >>> q = select("*", from_table="users", where="id = :id", params={"id": 1}, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING)
        >>> user = await execute_one(q, conn, row_factory=NOTHING)
        >>> if user:
        ...     print(user["email"])
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    row_factory = resolved(row_factory, None)
    sql, params = _prepare_query(query, connection)
    row = await _execute_fetch_one(connection, sql, params)

    return _ONE_ROW[row is not None and bool(row_factory)](row, row_factory)


@helper
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
        >>> q = select("count(*)", from_table="users", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING)
        >>> count = await execute_scalar(q, conn)
        >>> print(f"Total users: {count}")
    """
    sql, params = _prepare_query(query, connection)
    return await _execute_fetch_scalar(connection, sql, params)


@helper
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
        >>> q = insert("users", {"email": ":email", "name": ":name"}, returning=NOTHING, on_conflict=NOTHING)
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


@helper
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

    return _CONVERTERS[_driver_key(_conn_module(connection))](sql, params)


_SCAN = re.compile(
    r"'[^']*'?"  # a string literal, closed or running to the end of the text
    r'|"[^"]*"?'  # a quoted identifier, likewise
    r"|::"  # a cast, taken whole so its second colon starts nothing
    r"|:([a-zA-Z_][a-zA-Z0-9_]*)"  # and the only thing that is a placeholder
)


@helper
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

    So the scan reads the statement as a sequence of four things rather than
    searching it for one. Alternation is ordered and matches are leftmost and
    non-overlapping, so a literal, a quoted name and a cast each swallow their
    own colons before a placeholder can be read out of them. Returned as spans
    so a caller can rewrite around them and leave every other offset valid.
    """
    return [
        (found.start(), found.end(), found.group(1))
        for found in _SCAN.finditer(sql)
        if found.group(1) is not None
    ]


@helper
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
    # A PLACEHOLDER THE CALLER SUPPLIED NO VALUE FOR IS LEFT ALONE, which is
    # what the `continue` did. It reaches the driver as it was written and the
    # driver says so.
    spans = [span for span in _placeholders(sql) if span[2] in params]
    # `dict.fromkeys` keeps first appearance and drops the repeats, so a name
    # written twice takes the same number twice and its value is passed once.
    ordered = list(dict.fromkeys(name for _, _, name in spans))
    numbers = {name: i + 1 for i, name in enumerate(ordered)}
    param_list = [params[name] for name in ordered]

    # Where the untouched text between placeholders starts: the front of the
    # statement, then the end of each span in turn.
    resume = [0, *[end for _, end, _ in spans]]
    pieces = [
        piece
        for (start, _end, name), cursor in zip(spans, resume, strict=False)
        for piece in (sql[cursor:start], f"${numbers[name]}")
    ]
    return "".join([*pieces, sql[resume[-1] :]]), param_list


@helper
def _convert_to_aiosqlite(sql: str, params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """aiosqlite supports :name parameters directly."""
    return sql, params


@helper
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
    # A PLACEHOLDER THE CALLER SUPPLIED NO VALUE FOR IS LEFT ALONE, as above.
    spans = [span for span in _placeholders(sql) if span[2] in params]
    param_list = [params[name] for _, _, name in spans]

    resume = [0, *[end for _, end, _ in spans]]
    pieces = [
        piece
        for (start, _end, _name), cursor in zip(spans, resume, strict=False)
        for piece in (sql[cursor:start], "?")
    ]
    return "".join([*pieces, sql[resume[-1] :]]), param_list


# HOW EACH DRIVER SPELLS A BOUND PARAMETER. Read with [], so a driver no
# converter answers for is refused by `_driver_key` rather than handed the
# statement unconverted under a comment reading "assume named parameters
# work".
_CONVERTERS: dict[str, Callable[[str, dict[str, Any]], tuple[str, Any]]] = {
    "asyncpg": _convert_to_asyncpg,
    "aiosqlite": _convert_to_aiosqlite,
    "turso": _convert_to_turso,
}


@boundary
async def _execute_fetch(connection: Any, sql: str, params: Any) -> list[dict[str, Any]]:
    """Execute and fetch all rows."""
    conn_type = _conn_module(connection)

    @helper
    async def _fetch_asyncpg() -> list[dict[str, Any]]:
        rows = await connection.fetch(sql, *params)
        return [dict(row) for row in rows]

    @boundary
    async def _fetch_aiosqlite() -> list[dict[str, Any]]:
        # THE CURSOR CARRIES THE FACTORY, so nothing shared is touched.
        #
        # This borrowed `connection.row_factory` and gave it back in a
        # `finally`, which is a read, an await, and a write to state the
        # caller owns. Two queries on one connection interleave: the second
        # saves what the first had already set, the first restores the driver
        # default, and the second restores the dict factory. Measured
        # 2026-08-25 with six concurrent queries, the connection was left with
        # `_dict_factory` set for good, so every later raw `conn.execute` the
        # caller made returned dicts instead of tuples.
        #
        # The comment that stood here described exactly that failure as a past
        # defect: on a fresh connection a row is a tuple and `row[0]` works;
        # run any other query first and `row[0]` is a lookup for the key `0`.
        # Restoring in a `finally` fixed the single-query case and left the
        # concurrent one, because a save-and-restore is not a guard when
        # anything can run between the save and the restore.
        #
        # A cursor is not shared. Setting the factory there is the same
        # operation with nothing to race over.
        cursor = await connection.execute(sql, params)
        cursor.row_factory = _dict_factory
        return list(await cursor.fetchall())

    @boundary
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

    return await _FETCHERS[_driver_key(conn_type)]()


@boundary
async def _execute_fetch_one(connection: Any, sql: str, params: Any) -> dict[str, Any] | None:
    """Execute and fetch single row."""
    conn_type = _conn_module(connection)

    @helper
    async def _fetch_one_asyncpg() -> dict[str, Any] | None:
        row = await connection.fetchrow(sql, *params)
        return dict(row) if row else None

    @boundary
    async def _fetch_one_aiosqlite() -> dict[str, Any] | None:
        # THE CURSOR CARRIES THE FACTORY, as in `_fetch_aiosqlite` above and
        # for the same reason: a save-and-restore around an await is a window
        # two callers can interleave in, and the connection belongs to whoever
        # passed it. A cursor belongs to this call alone.
        cursor = await connection.execute(sql, params)
        cursor.row_factory = _dict_factory
        row = await cursor.fetchone()
        return dict(row) if row else None

    @boundary
    async def _fetch_one_turso() -> dict[str, Any] | None:
        cursor = await connection.execute(sql, params if params else ())
        row = await cursor.fetchone()
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        # AN ABSENT ROW STAYS ABSENT. Only a row that is there gets named.
        named = [
            dict(zip(columns, present, strict=True)) for present in [row] if present
        ]
        return [*named, None][0]

    _FETCHERS = {
        "asyncpg": _fetch_one_asyncpg,
        "aiosqlite": _fetch_one_aiosqlite,
        "turso": _fetch_one_turso,
    }

    return await _FETCHERS[_driver_key(conn_type)]()


@helper
def _first_named_value(row: Any) -> Any:
    """A mapping: the first value, whatever its column was called."""
    return next(iter(row.values()), None)


@helper
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

        total = await execute_scalar(select("COUNT(*) AS c", ..., from_table=NOTHING, where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING), conn)
        KeyError: 0

    Measured on SQLite 2026-08-17. `COUNT(*)` is the commonest scalar query
    there is, and the shipped example app reaches for `execute_one` and then
    `list(result.values())[0]`, which is this workaround written at the call
    site by somebody who hit the same wall.
    """
    # A DRIVER HANDS BACK A MAPPING OR A SEQUENCE, and either way the scalar
    # is its first value. aiosqlite gives a Row that indexes by position,
    # asyncpg a Record that also reads by name, and a row_factory can make it
    # a plain dict. A query that matched nothing has no row and no first value.
    found = [
        _FIRST_VALUE[written_as(dict, present)](present)
        for present in [row]
        if present is not None
    ]
    return [*found, None][0]


@boundary
async def _execute_fetch_scalar(connection: Any, sql: str, params: Any) -> Any:
    """Execute and fetch scalar value."""
    conn_type = _conn_module(connection)

    @helper
    async def _scalar_asyncpg() -> Any:
        return await connection.fetchval(sql, *params)

    @boundary
    async def _scalar_aiosqlite() -> Any:
        cursor = await connection.execute(sql, params)
        return _first_value(await cursor.fetchone())

    @boundary
    async def _scalar_turso() -> Any:
        cursor = await connection.execute(sql, params if params else ())
        return _first_value(await cursor.fetchone())

    _FETCHERS = {
        "asyncpg": _scalar_asyncpg,
        "aiosqlite": _scalar_aiosqlite,
        "turso": _scalar_turso,
    }

    return await _FETCHERS[_driver_key(conn_type)]()


@boundary
async def _execute_update(connection: Any, sql: str, params: Any) -> int:
    """Execute and return rows affected."""
    conn_type = _conn_module(connection)

    @boundary
    async def _update_asyncpg() -> int:
        result = await connection.execute(sql, *params)
        parts = result.split()
        return int(parts[-1]) if parts else 0

    @boundary
    async def _update_aiosqlite() -> int:
        cursor = await connection.execute(sql, params)
        await connection.commit()
        return int(cursor.rowcount)

    @boundary
    async def _update_turso() -> int:
        cursor = await connection.execute(sql, params if params else ())
        await connection.commit()
        return int(cursor.rowcount)

    _UPDATERS = {
        "asyncpg": _update_asyncpg,
        "aiosqlite": _update_aiosqlite,
        "turso": _update_turso,
    }

    return await _UPDATERS[_driver_key(conn_type)]()


@helper
def _dict_factory(cursor: Any, row: tuple[Any, ...]) -> dict[str, Any]:
    """Row factory for aiosqlite to return dicts."""
    columns = [col[0] for col in cursor.description]
    return dict(zip(columns, row, strict=True))


