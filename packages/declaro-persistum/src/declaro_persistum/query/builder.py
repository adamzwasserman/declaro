"""
Functional query builder.

Builds parameterized SQL queries without ORM overhead.
All functions are pure and return Query objects.
"""

import re
from collections.abc import Callable
from types import NoneType
from typing import Any, TypedDict

from declaro_persistum.types import Nothing, resolved, type_of, written_as


class Query(TypedDict):
    """
    An executable query.

    Attributes:
        sql: The SQL string with parameter placeholders
        params: Parameter values keyed by name
        engine: Optional engine hint for execution
    """

    sql: str
    params: dict[str, Any]
    engine: str | None


# ---------------------------------------------------------------------------
# The shapes a WHERE clause arrives in
# ---------------------------------------------------------------------------


def _no_clause(_where: Any, given: dict[str, Any], _path: str) -> tuple[str, dict[str, Any]]:
    """Nothing was given, so there is no SQL and no parameter of ours."""
    return "", given


def _refuse_written_sql(
    where: Any, _given: dict[str, Any], _path: str
) -> tuple[str, dict[str, Any]]:
    """A WHERE given as text, which is the caller writing the SQL.

    THIS USED TO PASS THE STRING STRAIGHT THROUGH. It is the escape hatch the
    whole surface exists to remove: a string that is neither a declared name
    nor a bound value has nowhere to go but into the statement.
    """
    raise TypeError(
        f"a WHERE clause is a condition mapping, and {where!r} is text. "
        f'Write {{"active": True}} rather than "active = :active", and '
        f'{{"age": {{"gt": 18}}}} rather than "age > :age". Every string you '
        f"pass is checked against your model or bound as a parameter, and text "
        f"is neither."
    )


def _built_clause(where: Any, given: dict[str, Any], path: str) -> tuple[str, dict[str, Any]]:
    """The condition language, rendered into SQL and the params it names.

    The caller's params win. A dict-built name is derived from the path and
    cannot collide with one a caller chose, so an overlap means the caller
    meant it.
    """
    from declaro_persistum.query.conditions import render

    sql, built = render(where, path)
    return sql, {**built, **given}


def _refuse_a_clause(where: Any, _given: dict[str, Any], _path: str) -> tuple[str, dict[str, Any]]:
    """Anything else. A WHERE is a mapping and nothing else."""
    raise TypeError(
        f"a WHERE clause is a condition mapping, and "
        f"{type_of(where).__name__} is not one: {where!r}"
    )


# The type of the clause is the key, so `str` and `dict` are each an entry and
# `object` is the entry every other type reaches along its own class hierarchy.
# A subclass of either -- and a library can hand back one -- finds its parent
# before it finds `object`.
_CLAUSE_SHAPES: dict[type, Callable[[Any, dict[str, Any], str], tuple[str, dict[str, Any]]]] = {
    NoneType: _no_clause,
    str: _refuse_written_sql,
    dict: _built_clause,
    object: _refuse_a_clause,
}


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
    reader = [_CLAUSE_SHAPES[kind] for kind in type_of(where).__mro__ if kind in _CLAUSE_SHAPES]
    return reader[0](where, given, path)


# ---------------------------------------------------------------------------
# ORDER BY, as a table rather than a branch per column
# ---------------------------------------------------------------------------


# WHAT A LEADING MINUS CHANGES, which is two things and both are data: the
# word SQL wants, and how much of the string is the column name.
#
# These were two functions differing by that word and that offset. One
# function over the table is what the shape asked for, and it puts the two
# spellings side by side where they can be compared.
_ORDER_DIRECTION: dict[bool, tuple[int, str]] = {
    True: (1, "DESC"),
    False: (0, "ASC"),
}


def _order_term(col: str) -> str:
    """One ORDER BY term, with its direction and the name it sorts on."""
    skip, direction = _ORDER_DIRECTION[col.startswith("-")]
    return f"{_quote_column(col[skip:])} {direction}"


def _join_sql(join: dict[str, str]) -> str:
    """One JOIN clause. A join that does not name its type is an INNER one."""
    return f'{join.get("type", "INNER")} JOIN "{join["table"]}" ON {join["on"]}'


def _returning(columns: list[str] | None) -> list[str]:
    """The RETURNING clause, or no clause when no columns were asked for."""
    return ["RETURNING " + ", ".join(f'"{c}"' for c in cols) for cols in [columns] if cols]


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
    the_where = resolved(where, None)
    the_params = resolved(params, None)
    the_order_by = resolved(order_by, None)
    the_limit = resolved(limit, None)
    the_offset = resolved(offset, None)
    the_joins = resolved(joins, None)
    the_group_by = resolved(group_by, None)
    the_having = resolved(having, None)
    the_params = the_params or {}

    # NAMING NOTHING SELECTS EVERYTHING, and the join says so on its own: an
    # empty column list joins to the empty string. `_quote_column` passes a
    # lone `*` through, so both spellings of "everything" agree.
    cols_sql = ", ".join(_quote_column(c) for c in columns) or "*"

    # WHERE is rendered before HAVING because each merges into the params the
    # one before it produced.
    where_sql, the_params = _clause(the_where, the_params, "w")
    having_sql, the_params = _clause(the_having, the_params, "h")

    group_sql = ", ".join(_quote_column(c) for c in (the_group_by or []))
    order_sql = ", ".join(_order_term(c) for c in (the_order_by or []))

    # THE STATEMENT IS THE CLAUSES THAT ARE PRESENT, joined. A clause a caller
    # did not ask for contributes nothing rather than being skipped by a
    # branch, and the order they are written in here is the order SQL requires.
    sql_parts = [
        f"SELECT {cols_sql}",
        f'FROM "{from_table}"',
        *(_join_sql(join) for join in (the_joins or [])),
        *[f"WHERE {clause}" for clause in [where_sql] if clause],
        *[f"GROUP BY {group_sql}" for cols in [the_group_by] if cols],
        *[f"HAVING {clause}" for clause in [having_sql] if clause],
        *[f"ORDER BY {order_sql}" for cols in [the_order_by] if cols],
        # `is not None` rather than truthiness: 0 is a real limit and a real
        # offset, and reading either as absent drops a clause the caller asked
        # for.
        *[f"LIMIT {n}" for n in [the_limit] if n is not None],
        *[f"OFFSET {n}" for n in [the_offset] if n is not None],
    ]

    return {
        "sql": " ".join(sql_parts),
        "params": the_params,
        "engine": None,
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


def _refuse_no_rows(_values_list: list[Any]) -> list[Any]:
    """An INSERT with no rows names no columns and binds nothing."""
    raise ValueError("values cannot be empty")


def _the_rows(values_list: list[Any]) -> list[Any]:
    """At least one row, which is what the columns are read from."""
    return values_list


_WHEN_NO_ROWS: dict[bool, Callable[[list[Any]], list[Any]]] = {
    True: _refuse_no_rows,
    False: _the_rows,
}


def _named_binding(columns: list[str], values_list: list[Any]) -> tuple[str, dict[str, Any]]:
    """One row binds by column name, so the SQL reads as the row does."""
    placeholders = ", ".join(f":{c}" for c in columns)
    return f"({placeholders})", values_list[0]


def _numbered_binding(columns: list[str], values_list: list[Any]) -> tuple[str, dict[str, Any]]:
    """Several rows need a name per column per row, so the row number joins it.

    A row that omits a column binds NULL for it. The columns come from the
    first row, so a later row carrying an extra key contributes nothing.
    """
    groups = ", ".join(
        "(" + ", ".join(f":{c}_{i}" for c in columns) + ")" for i, _row in enumerate(values_list)
    )
    params = {f"{c}_{i}": row.get(c) for i, row in enumerate(values_list) for c in columns}
    return groups, params


_BINDING: dict[bool, Callable[[list[str], list[Any]], tuple[str, dict[str, Any]]]] = {
    True: _named_binding,
    False: _numbered_binding,
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
    the_returning = resolved(returning, None)
    the_on_conflict = resolved(on_conflict, None)
    # ONE ROW OR SEVERAL, AND BOTH MEAN SEVERAL. Two spellings of one thing
    # is a vocabulary, resolved here into the single form the rest reads.
    values_list = _ROWS[written_as(dict, values)](values)
    values_list = _WHEN_NO_ROWS[not values_list](values_list)

    # Get columns from first row
    columns = list(values_list[0].keys())
    cols_sql = ", ".join(f'"{c}"' for c in columns)

    values_sql, params = _BINDING[len(values_list) == 1](columns, values_list)

    sql_parts = [
        f'INSERT INTO "{into_table}" ({cols_sql})',
        f"VALUES {values_sql}",
        *[f"ON CONFLICT {clause}" for clause in [the_on_conflict] if clause],
        *_returning(the_returning),
    ]

    return {
        "sql": " ".join(sql_parts),
        "params": params,
        "engine": None,
    }


def _refuse_nothing_to_set(_set_values: dict[str, Any]) -> dict[str, Any]:
    """An UPDATE that sets no column has nothing to say."""
    raise ValueError("set_values cannot be empty")


def _the_set_values(set_values: dict[str, Any]) -> dict[str, Any]:
    """At least one column, which is what the SET clause is built from."""
    return set_values


_WHEN_NOTHING_TO_SET: dict[bool, Callable[[dict[str, Any]], dict[str, Any]]] = {
    True: _refuse_nothing_to_set,
    False: _the_set_values,
}


def _refuse_a_missing_where(statement: str, _where: Any) -> Any:
    """Required on purpose: a statement with no WHERE touches every row, and
    a caller who means that should have to say so."""
    raise ValueError(f"where clause is required for {statement} (use '1=1' for all rows)")


def _the_where(_statement: str, where: Any) -> Any:
    """A clause was given, so it is what the statement narrows on."""
    return where


_WHEN_NO_WHERE: dict[bool, Callable[[str, Any], Any]] = {
    True: _refuse_a_missing_where,
    False: _the_where,
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
    the_params = resolved(params, None)
    the_returning = resolved(returning, None)
    # SET is asked about before WHERE, and a test pins that order.
    set_values = _WHEN_NOTHING_TO_SET[not set_values](set_values)
    where = _WHEN_NO_WHERE[not where]("UPDATE", where)

    # `set_` namespaces the bindings so a SET column and a WHERE parameter can
    # both be called `id`.
    set_sql = ", ".join(f'"{col}" = :set_{col}' for col in set_values)
    all_params: dict[str, Any] = {f"set_{col}": val for col, val in set_values.items()}
    all_params.update(the_params or {})

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
    where_sql, where_params = _clause(where, the_params, "w")
    all_params.update(where_params)

    sql_parts = [
        f'UPDATE "{table}"',
        f"SET {set_sql}",
        f"WHERE {where_sql}",
        *_returning(the_returning),
    ]

    return {
        "sql": " ".join(sql_parts),
        "params": all_params,
        "engine": None,
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
    the_params = resolved(params, None)
    the_returning = resolved(returning, None)
    where = _WHEN_NO_WHERE[not where]("DELETE", where)

    where_sql, the_params = _clause(where, the_params, "w")
    sql_parts = [
        f'DELETE FROM "{from_table}"',
        f"WHERE {where_sql}",
        *_returning(the_returning),
    ]

    return {
        "sql": " ".join(sql_parts),
        "params": the_params,
        "engine": None,
    }


# What tells an EXPRESSION from a column name: parentheses, a comma, or an
# ` AS ` alias. A bare word, even `count`, is a column.
_EXPRESSION = re.compile(r"[(),]|\s+AS\s+", re.IGNORECASE)


def is_expression(col: str) -> bool:
    """Is this a thing to evaluate rather than a name to quote?"""
    return bool(_EXPRESSION.search(col))


def _is_star(col: str) -> bool:
    """A lone `*` names every column, so quoting it would name one called `*`."""
    return col == "*"


# honest-code-allow: L1.21.1 - _the_star and _as_name are already the two rows of the _QUOTING table below, which is the shape this clause asks for. Collapsing them into one function over a table would mean a table whose only column is the whole body, and would leave _QUOTING with nothing to dispatch to
def _the_star(_col: str) -> str:
    """Every column, written the way SQL writes it."""
    return "*"


def _as_written(col: str) -> str:
    """An expression is evaluated by the engine, so it goes through as typed."""
    return col


def _is_qualified(col: str) -> bool:
    """`table.column`, which is two identifiers rather than one."""
    return "." in col


def _as_qualified(col: str) -> str:
    """Quote each half of `table.column`. Only the first dot splits."""
    table, name = col.split(".", 1)
    return f'"{table}"."{name}"'


def _as_name(col: str) -> str:
    """An ordinary column name, quoted whole."""
    return f'"{col}"'


# THE RULES IN ORDER, first match winning, and `_as_name` closing the list so
# every column has an answer without a rule having to guess.
_QUOTING: tuple[tuple[Callable[[str], bool], Callable[[str], str]], ...] = (
    (_is_star, _the_star),
    (is_expression, _as_written),
    (_is_qualified, _as_qualified),
)


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
    matched = [render for applies, render in _QUOTING if applies(col)]
    return [*matched, _as_name][0](col)


# Query composition helpers


# ADDING OR REPLACING A PAGING CLAUSE, one row each. Both split the statement
# on their keyword, kept the half before it, and appended the clause with a new
# value. The keyword was the only thing that differed.
#
# THE SPLIT IS WHAT MAKES IT ADD-OR-REPLACE RATHER THAN APPEND. Calling it
# twice must leave one clause: `... LIMIT 4 LIMIT 1` is a plausible string and
# not a statement. Splitting needs no guard, because a query carrying no such
# clause splits into one piece, which is the query itself.
_PAGING: dict[str, tuple[str, str]] = {
    "with_limit": (
        " LIMIT ",
        "Add or replace LIMIT on a query.\n\n"
        "    Splitting needs no guard: a query that carries none splits into\n"
        "    one piece, which is the query itself.\n    ",
    ),
    "with_offset": (
        " OFFSET ",
        "Add or replace OFFSET on a query.\n\n"
        "    Anything written after the OFFSET value is dropped, which is what\n"
        "    the split has always done.\n    ",
    ),
}


def _pages_by(name: str) -> Callable[[Query, int], Query]:
    """Build the function that sets one paging clause."""

    keyword, purpose = _PAGING[name]

    def page(query: Query, value: int) -> Query:
        return {
            "sql": f"{query['sql'].split(keyword)[0]}{keyword}{value}",
            "params": query["params"],
            "engine": query["engine"],
        }

    page.__name__ = name
    page.__qualname__ = name
    page.__doc__ = purpose
    return page


with_limit = _pages_by("with_limit")
with_offset = _pages_by("with_offset")


def with_params(query: Query, **new_params: Any) -> Query:
    """Add or replace parameters on a query."""
    return {
        "sql": query["sql"],
        "params": {**query["params"], **new_params},
        "engine": query["engine"],
    }
