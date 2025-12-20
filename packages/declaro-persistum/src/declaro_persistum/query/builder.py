"""
Functional query builder.

Builds parameterized SQL queries without ORM overhead.
All functions are pure and return Query objects.
"""

from typing import Any, TypedDict


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


def select(
    *columns: str,
    from_table: str,
    where: str | None = None,
    params: dict[str, Any] | None = None,
    order_by: list[str] | None = None,
    limit: int | None = None,
    offset: int | None = None,
    joins: list[dict[str, str]] | None = None,
    group_by: list[str] | None = None,
    having: str | None = None,
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
        >>> q = select("id", "email", from_table="users", where="id = :id", params={"id": 1})
        >>> print(q["sql"])
        SELECT "id", "email" FROM "users" WHERE id = :id
    """
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
    if where:
        sql_parts.append(f"WHERE {where}")

    # Add GROUP BY
    if group_by:
        group_cols = ", ".join(_quote_column(c) for c in group_by)
        sql_parts.append(f"GROUP BY {group_cols}")

    # Add HAVING
    if having:
        sql_parts.append(f"HAVING {having}")

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


def insert(
    into_table: str,
    values: dict[str, Any] | list[dict[str, Any]],
    *,
    returning: list[str] | None = None,
    on_conflict: str | None = None,
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
        >>> q = insert("users", {"email": "test@example.com", "name": "Test"})
        >>> print(q["sql"])
        INSERT INTO "users" ("email", "name") VALUES (:email, :name)
    """
    # Normalize to list
    values_list = [values] if isinstance(values, dict) else values

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
    where: str,
    params: dict[str, Any] | None = None,
    returning: list[str] | None = None,
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
        >>> q = update("users", {"name": "New Name"}, where="id = :id", params={"id": 1})
        >>> print(q["sql"])
        UPDATE "users" SET "name" = :name WHERE id = :id
    """
    if not set_values:
        raise ValueError("set_values cannot be empty")

    if not where:
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
    sql_parts = [f'UPDATE "{table}"', f"SET {set_sql}", f"WHERE {where}"]

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
    where: str,
    params: dict[str, Any] | None = None,
    returning: list[str] | None = None,
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
        >>> q = delete("users", where="id = :id", params={"id": 1})
        >>> print(q["sql"])
        DELETE FROM "users" WHERE id = :id
    """
    if not where:
        raise ValueError("where clause is required for DELETE (use '1=1' for all rows)")

    params = params or {}
    sql_parts = [f'DELETE FROM "{from_table}"', f"WHERE {where}"]

    # Add RETURNING
    if returning:
        ret_cols = ", ".join(f'"{c}"' for c in returning)
        sql_parts.append(f"RETURNING {ret_cols}")

    return {
        "sql": " ".join(sql_parts),
        "params": params,
        "dialect": None,
    }


def raw(sql: str, params: dict[str, Any] | None = None) -> Query:
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
    return {
        "sql": sql,
        "params": params or {},
        "dialect": None,
    }


def _quote_column(col: str) -> str:
    """Quote a column name, handling table.column notation."""
    if "." in col:
        parts = col.split(".", 1)
        return f'"{parts[0]}"."{parts[1]}"'
    elif col == "*":
        return "*"
    else:
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
