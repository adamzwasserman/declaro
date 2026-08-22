"""
Functional query builder.

Builds parameterized SQL queries without ORM overhead.
All functions are pure and return Query objects.
"""

import re
from collections.abc import Callable
from typing import Any, TypedDict

from declaro_persistum.types import Nothing, resolved, written_as


class Query(TypedDict):
    """
    An executable query.

    Attributes:
        sql: The SQL string with parameter placeholders
        params: Parameter values keyed by name
        dialect: Optional dialect hint for execution
    """

    sql: str
    params: dict[str, Any]
    dialect: str | None



def _clause(
    where: str | dict[str, Any] | None,
    params: dict[str, Any] | None,
    path: str,
) -> tuple[str, dict[str, Any]]:
    """Turn a `where` into SQL and params, whichever form it arrived in.

    A DICT is the condition language (`query/conditions.py`) and is what a
    caller should write:

        where={"age": {"gt": 18}, "status": "active"}

    A STRING is raw SQL with `:name` placeholders and the caller's own params.
    It is kept because an escape hatch for SQL the condition language cannot
    express is worth having, and removing it would push people back to
    string-building with no parameters at all.

    They are not mixed: a dict brings its own params, a string relies on the
    caller's, and merging is done here so no caller has to remember which.
    """
    given = dict(params or {})
    if where is None:
        return "", given
    # A CLAUSE IS WRITTEN AS TEXT OR AS A MAPPING, and text brings no
    # parameters of its own.
    if written_as(str, where):
        return where, given

    from declaro_persistum.query.conditions import render

    sql, built = render(where, path)
    # The caller's params win. A dict-built name is derived from the path and
    # cannot collide with one a caller chose, so an overlap means the caller
    # meant it.
    return sql, {**built, **given}


def select(
    *columns: str,
    from_table: str,
    where: str | dict[str, Any] | Nothing,
    params: dict[str, Any] | Nothing,
    order_by: list[str] | Nothing,
    limit: int | Nothing,
    offset: int | Nothing,
    joins: list[dict[str, str]] | Nothing,
    group_by: list[str] | Nothing,
    having: str | dict[str, Any] | Nothing,
) -> Query:
    """
    Build a SELECT query.

    This is a pure function - same inputs always produce same outputs.

    Args:
        columns: Column names to select (* for all, or use table.column for joins)
        from_table: Table name to select from
        where: WHERE clause (use :param_name for parameters)
        params: Parameter values for the query
        order_by: ORDER BY columns (prefix with - for DESC, e.g., "-created_at")
        limit: LIMIT value
        offset: OFFSET value
        joins: List of join specs: [{"type": "LEFT", "table": "t", "on": "..."}]
        group_by: GROUP BY columns
        having: HAVING clause

    Returns:
        Query dict that can be executed

    Example:
        >>> q = select("id", "email", from_table="users", where="id = :id", params={"id": 1}, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING)
        >>> print(q["sql"])
        SELECT "id", "email" FROM "users" WHERE id = :id
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    where = resolved(where, None)
    params = resolved(params, None)
    order_by = resolved(order_by, None)
    limit = resolved(limit, None)
    offset = resolved(offset, None)
    joins = resolved(joins, None)
    group_by = resolved(group_by, None)
    having = resolved(having, None)
    params = params or {}

    # Build column list
    if not columns or columns == ("*",):
        cols_sql = "*"
    else:
        cols_sql = ", ".join(_quote_column(c) for c in columns)

    # Start building SQL
    sql_parts = [f"SELECT {cols_sql}", f'FROM "{from_table}"']

    # Add joins
    if joins:
        for join in joins:
            join_type = join.get("type", "INNER")
            join_table = join["table"]
            join_on = join["on"]
            sql_parts.append(f'{join_type} JOIN "{join_table}" ON {join_on}')

    # Add WHERE
    where_sql, params = _clause(where, params, "w")
    if where_sql:
        sql_parts.append(f"WHERE {where_sql}")

    # Add GROUP BY
    if group_by:
        group_cols = ", ".join(_quote_column(c) for c in group_by)
        sql_parts.append(f"GROUP BY {group_cols}")

    # Add HAVING
    having_sql, params = _clause(having, params, "h")
    if having_sql:
        sql_parts.append(f"HAVING {having_sql}")

    # Add ORDER BY
    if order_by:
        order_parts = []
        for col in order_by:
            if col.startswith("-"):
                order_parts.append(f"{_quote_column(col[1:])} DESC")
            else:
                order_parts.append(f"{_quote_column(col)} ASC")
        sql_parts.append(f"ORDER BY {', '.join(order_parts)}")

    # Add LIMIT
    if limit is not None:
        sql_parts.append(f"LIMIT {limit}")

    # Add OFFSET
    if offset is not None:
        sql_parts.append(f"OFFSET {offset}")

    return {
        "sql": " ".join(sql_parts),
        "params": params,
        "dialect": None,
    }


def _one_row(values: Any) -> list[Any]:
    """A single mapping: one row, written without the surrounding list."""
    return [values]


def _many_rows(values: Any) -> list[Any]:
    """Already a list of rows."""
    return list(values)


# WHETHER ONE ROW WAS WRITTEN OR SEVERAL, as a table rather than a
# conditional expression.
_ROWS: dict[bool, Callable[[Any], list[Any]]] = {
    True: _one_row,
    False: _many_rows,
}



def insert(
    into_table: str,
    values: dict[str, Any] | list[dict[str, Any]],
    *,
    returning: list[str] | Nothing,
    on_conflict: str | Nothing,
) -> Query:
    """
    Build an INSERT query.

    Args:
        into_table: Table name to insert into
        values: Column-value mapping or list of mappings for bulk insert
        returning: Columns to return (RETURNING clause)
        on_conflict: ON CONFLICT clause for upserts

    Returns:
        Query dict that can be executed

    Example:
        >>> q = insert("users", {"email": "test@example.com", "name": "Test"}, returning=NOTHING, on_conflict=NOTHING)
        >>> print(q["sql"])
        INSERT INTO "users" ("email", "name") VALUES (:email, :name)
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    returning = resolved(returning, None)
    on_conflict = resolved(on_conflict, None)
    # ONE ROW OR SEVERAL, AND BOTH MEAN SEVERAL. Two spellings of one thing
    # is a vocabulary, resolved here into the single form the rest reads.
    values_list = _ROWS[written_as(dict, values)](values)

    if not values_list:
        raise ValueError("values cannot be empty")

    # Get columns from first row
    columns = list(values_list[0].keys())
    cols_sql = ", ".join(f'"{c}"' for c in columns)

    # Build VALUES clause
    params: dict[str, Any] = {}

    if len(values_list) == 1:
        # Single row - use named parameters
        placeholders = ", ".join(f":{c}" for c in columns)
        values_sql = f"({placeholders})"
        params = values_list[0]
    else:
        # Multiple rows - use indexed parameters
        value_groups = []
        for i, row in enumerate(values_list):
            row_placeholders = []
            for c in columns:
                param_name = f"{c}_{i}"
                row_placeholders.append(f":{param_name}")
                params[param_name] = row.get(c)
            value_groups.append(f"({', '.join(row_placeholders)})")
        values_sql = ", ".join(value_groups)

    sql_parts = [f'INSERT INTO "{into_table}" ({cols_sql})', f"VALUES {values_sql}"]

    # Add ON CONFLICT
    if on_conflict:
        sql_parts.append(f"ON CONFLICT {on_conflict}")

    # Add RETURNING
    if returning:
        ret_cols = ", ".join(f'"{c}"' for c in returning)
        sql_parts.append(f"RETURNING {ret_cols}")

    return {
        "sql": " ".join(sql_parts),
        "params": params,
        "dialect": None,
    }


def update(
    table: str,
    set_values: dict[str, Any],
    *,
    where: str | dict[str, Any],
    params: dict[str, Any] | Nothing,
    returning: list[str] | Nothing,
) -> Query:
    """
    Build an UPDATE query.

    Args:
        table: Table name to update
        set_values: Column-value mapping for SET clause
        where: WHERE clause (required for safety - use "1=1" if you really want all rows)
        params: Additional parameters for WHERE clause
        returning: Columns to return (RETURNING clause)

    Returns:
        Query dict that can be executed

    Example:
        >>> q = update("users", {"name": "New Name"}, where="id = :id", params={"id": 1}, returning=NOTHING)
        >>> print(q["sql"])
        UPDATE "users" SET "name" = :name WHERE id = :id
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    params = resolved(params, None)
    returning = resolved(returning, None)
    if not set_values:
        raise ValueError("set_values cannot be empty")

    if not where:
        # Required on purpose: an UPDATE with no WHERE rewrites every row, and
        # a caller who means that should have to say so.
        raise ValueError("where clause is required for UPDATE (use '1=1' for all rows)")

    # Build SET clause
    set_parts = []
    all_params: dict[str, Any] = {}

    for col, val in set_values.items():
        param_name = f"set_{col}"
        set_parts.append(f'"{col}" = :{param_name}')
        all_params[param_name] = val

    # Add WHERE params
    if params:
        all_params.update(params)

    set_sql = ", ".join(set_parts)

    # THE MERGED PARAMS, NOT A LOCAL NOBODY READS. This said
    #
    #     where_sql, params = _clause(where, params, "w")
    #
    # and then returned `all_params`, which was built from `set_values` and
    # the caller's original `params` BEFORE `_clause` ran. So a dict WHERE
    # rendered `:w_0_id` and bound nothing:
    #
    #     update("t", {"a": 1}, where={"id": "x"}, params=NOTHING, returning=NOTHING)
    #     sql    'UPDATE "t" SET "a" = :set_a WHERE id = :w_0_id'
    #     params {'set_a': 1}
    #     sqlite3.ProgrammingError: You did not supply a value for binding
    #     parameter :w_0_id.
    #
    # `delete` returns what `_clause` gives it, which is why only this one was
    # wrong, and only for a dict WHERE: a string WHERE takes its bindings from
    # the caller's `params`, which `all_params` already had.
    where_sql, where_params = _clause(where, params, "w")
    all_params.update(where_params)
    sql_parts = [f'UPDATE "{table}"', f"SET {set_sql}", f"WHERE {where_sql}"]

    # Add RETURNING
    if returning:
        ret_cols = ", ".join(f'"{c}"' for c in returning)
        sql_parts.append(f"RETURNING {ret_cols}")

    return {
        "sql": " ".join(sql_parts),
        "params": all_params,
        "dialect": None,
    }


def delete(
    from_table: str,
    *,
    where: str | dict[str, Any],
    params: dict[str, Any] | Nothing,
    returning: list[str] | Nothing,
) -> Query:
    """
    Build a DELETE query.

    Args:
        from_table: Table name to delete from
        where: WHERE clause (required for safety - use "1=1" if you really want all rows)
        params: Parameters for WHERE clause
        returning: Columns to return (RETURNING clause)

    Returns:
        Query dict that can be executed

    Example:
        >>> q = delete("users", where="id = :id", params={"id": 1}, returning=NOTHING)
        >>> print(q["sql"])
        DELETE FROM "users" WHERE id = :id
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    params = resolved(params, None)
    returning = resolved(returning, None)
    if not where:
        raise ValueError("where clause is required for DELETE (use '1=1' for all rows)")

    where_sql, params = _clause(where, params, "w")
    sql_parts = [f'DELETE FROM "{from_table}"', f"WHERE {where_sql}"]

    # Add RETURNING
    if returning:
        ret_cols = ", ".join(f'"{c}"' for c in returning)
        sql_parts.append(f"RETURNING {ret_cols}")

    return {
        "sql": " ".join(sql_parts),
        "params": params,
        "dialect": None,
    }


def raw(sql: str, params: dict[str, Any] | Nothing) -> Query:
    """
    Create a query from raw SQL.

    Use this when the query builder doesn't support your use case,
    but be careful about SQL injection.

    Args:
        sql: Raw SQL string
        params: Parameters for the query

    Returns:
        Query dict
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    params = resolved(params, None)
    return {
        "sql": sql,
        "params": params or {},
        "dialect": None,
    }


# What tells an EXPRESSION from a column name: parentheses, a comma, or an
# ` AS ` alias. A bare word, even `count`, is a column.
_EXPRESSION = re.compile(r"[(),]|\s+AS\s+", re.IGNORECASE)


def is_expression(col: str) -> bool:
    """Is this a thing to evaluate rather than a name to quote?"""
    return bool(_EXPRESSION.search(col))


def _quote_column(col: str) -> str:
    """Quote a column name, handling table.column notation.

    AN EXPRESSION IS PASSED THROUGH. This quoted everything except `*` and
    `table.column`, so `COUNT(*) AS c` became the identifier
    `"COUNT(*) AS c"`. SQLite treats a double-quoted string it cannot resolve
    as a string LITERAL, so the query succeeded and returned the text of the
    expression:

        select("COUNT(*) AS c", from_table="t", where=NOTHING, params=NOTHING, order_by=NOTHING, limit=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING)
        ->  SELECT "COUNT(*) AS c" FROM "t"
        ->  'COUNT(*) AS c'

    Measured 2026-08-17. PostgreSQL raises on the same SQL, which is at least
    loud; SQLite answers a plausible wrong thing. Every aggregate the query
    layer offers -- count_, sum_, avg_, min_, max_, now_ -- produces this
    shape, and all three shipped example apps import count_.

    Passing an expression through unquoted is the only thing that can work,
    and it is worth saying what it means: a SELECT list is developer-authored,
    like a `Literal` vocabulary, and is not a place for user input. Quoting
    would not make that safe either, because a quoted expression is not an
    expression, it is a wrong answer.
    """
    if col == "*":
        return "*"
    if is_expression(col):
        return col
    if "." in col:
        parts = col.split(".", 1)
        return f'"{parts[0]}"."{parts[1]}"'
    return f'"{col}"'


# Query composition helpers


def with_limit(query: Query, limit: int) -> Query:
    """Add or replace LIMIT on a query."""
    sql = query["sql"]

    # Remove existing LIMIT if present
    if " LIMIT " in sql:
        sql = sql.split(" LIMIT ")[0]

    return {
        "sql": f"{sql} LIMIT {limit}",
        "params": query["params"],
        "dialect": query["dialect"],
    }


def with_offset(query: Query, offset: int) -> Query:
    """Add or replace OFFSET on a query."""
    sql = query["sql"]

    # Remove existing OFFSET if present
    if " OFFSET " in sql:
        parts = sql.split(" OFFSET ")
        sql = parts[0]
        # Preserve anything after OFFSET value (shouldn't be anything, but just in case)

    return {
        "sql": f"{sql} OFFSET {offset}",
        "params": query["params"],
        "dialect": query["dialect"],
    }


def with_params(query: Query, **new_params: Any) -> Query:
    """Add or replace parameters on a query."""
    return {
        "sql": query["sql"],
        "params": {**query["params"], **new_params},
        "dialect": query["dialect"],
    }
