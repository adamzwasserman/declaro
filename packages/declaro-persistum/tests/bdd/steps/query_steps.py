"""
Query-related BDD step definitions.
"""

import pytest
from pytest_bdd import given, when, then, parsers

from declaro_persistum.query.table import table
from declaro_persistum.query import count_, sum_, avg_, min_, max_


# =============================================================================
# SELECT Steps
# =============================================================================


@when("I build a select query for all columns")
def when_select_all(bdd_context):
    """Build SELECT * query."""
    bdd_context.query = bdd_context.table_proxy.select()
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I select columns: {columns}"))
def when_select_columns(bdd_context, columns: str):
    """Build SELECT with specific columns."""
    column_names = [c.strip() for c in columns.split(",")]
    cols = [getattr(bdd_context.table_proxy, col) for col in column_names]
    bdd_context.query = bdd_context.table_proxy.select(*cols)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when("I select all columns")
def when_select_all_columns(bdd_context):
    """Build SELECT * query (alias for consistency)."""
    bdd_context.query = bdd_context.table_proxy.select()


@when(parsers.parse('I add a WHERE condition: {column} equals "{value}"'))
def when_add_where_equals(bdd_context, column: str, value: str):
    """Add WHERE column = value condition."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.query.where(col == value)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I add a WHERE condition: {column} > "{value}"'))
def when_add_where_gt(bdd_context, column: str, value: str):
    """Add WHERE column > value condition."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.query.where(col > value)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I add a WHERE condition: {column} < "{value}"'))
def when_add_where_lt(bdd_context, column: str, value: str):
    """Add WHERE column < value condition."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.query.where(col < value)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I add ORDER BY {column} {direction}'))
def when_add_order_by(bdd_context, column: str, direction: str):
    """Add ORDER BY clause."""
    col = getattr(bdd_context.table_proxy, column)
    if direction.lower() == "desc":
        bdd_context.query = bdd_context.query.order_by(col.desc())
    else:
        bdd_context.query = bdd_context.query.order_by(col.asc())
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I add LIMIT {limit:d}"))
def when_add_limit(bdd_context, limit: int):
    """Add LIMIT clause."""
    bdd_context.query = bdd_context.query.limit(limit)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I add OFFSET {offset:d}"))
def when_add_offset(bdd_context, offset: int):
    """Add OFFSET clause."""
    bdd_context.query = bdd_context.query.offset(offset)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# INSERT Steps
# =============================================================================


@when(parsers.parse('I build an INSERT with {column} = "{value}"'))
def when_insert_single(bdd_context, column: str, value: str):
    """Build INSERT with single column."""
    bdd_context.query = bdd_context.table_proxy.insert(**{column: value})
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when("I build an INSERT with multiple values")
def when_insert_multiple(bdd_context):
    """Build INSERT with multiple columns (uses test data)."""
    bdd_context.query = bdd_context.table_proxy.insert(
        title="Test Todo",
        completed=False,
    )
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when("I add RETURNING clause for id")
def when_add_returning_id(bdd_context):
    """Add RETURNING id clause."""
    bdd_context.query = bdd_context.query.returning(bdd_context.table_proxy.id)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# UPDATE Steps
# =============================================================================


@when(parsers.parse('I build an UPDATE setting {column} = "{value}"'))
def when_update_single(bdd_context, column: str, value: str):
    """Build UPDATE with single SET clause."""
    bdd_context.query = bdd_context.table_proxy.update(**{column: value})
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I add WHERE {column} = "{value}"'))
def when_update_where(bdd_context, column: str, value: str):
    """Add WHERE clause to UPDATE."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.query.where(col == value)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# DELETE Steps
# =============================================================================


@when("I build a DELETE query")
def when_delete_all(bdd_context):
    """Build DELETE query."""
    bdd_context.query = bdd_context.table_proxy.delete()
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I build a DELETE WHERE {column} = "{value}"'))
def when_delete_where(bdd_context, column: str, value: str):
    """Build DELETE with WHERE clause."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.table_proxy.delete().where(col == value)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# JOIN Steps
# =============================================================================


@when(parsers.parse('I join with {other_table} on {join_condition}'))
def when_join_tables(bdd_context, other_table: str, join_condition: str):
    """Add JOIN clause."""
    other = table(other_table, bdd_context.schema)
    # Parse simple join condition like "users.id = orders.user_id"
    parts = join_condition.split("=")
    left_parts = parts[0].strip().split(".")
    right_parts = parts[1].strip().split(".")

    left_col = getattr(table(left_parts[0], bdd_context.schema), left_parts[1])
    right_col = getattr(table(right_parts[0], bdd_context.schema), right_parts[1])

    bdd_context.query = bdd_context.query.join(other, on=left_col == right_col)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I left join with {other_table} on {join_condition}'))
def when_left_join(bdd_context, other_table: str, join_condition: str):
    """Add LEFT JOIN clause."""
    other = table(other_table, bdd_context.schema)
    parts = join_condition.split("=")
    left_parts = parts[0].strip().split(".")
    right_parts = parts[1].strip().split(".")

    left_col = getattr(table(left_parts[0], bdd_context.schema), left_parts[1])
    right_col = getattr(table(right_parts[0], bdd_context.schema), right_parts[1])

    bdd_context.query = bdd_context.query.join(other, on=left_col == right_col, type="left")
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# Aggregation Steps
# =============================================================================


@when("I select COUNT(*)")
def when_select_count_star(bdd_context):
    """Build SELECT COUNT(*) query."""
    bdd_context.query = bdd_context.table_proxy.select(count_("*"))
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I select COUNT({column})"))
def when_select_count_col(bdd_context, column: str):
    """Build SELECT COUNT(column) query."""
    if column == "*":
        # Handle COUNT(*) specially
        bdd_context.query = bdd_context.table_proxy.select(count_("*"))
    else:
        col = getattr(bdd_context.table_proxy, column)
        bdd_context.query = bdd_context.table_proxy.select(count_(col))
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I select SUM({column})"))
def when_select_sum(bdd_context, column: str):
    """Build SELECT SUM(column) query."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.table_proxy.select(sum_(col))
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I select AVG({column})"))
def when_select_avg(bdd_context, column: str):
    """Build SELECT AVG(column) query."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.table_proxy.select(avg_(col))
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse("I add GROUP BY {column}"))
def when_add_group_by(bdd_context, column: str):
    """Add GROUP BY clause."""
    col = getattr(bdd_context.table_proxy, column)
    bdd_context.query = bdd_context.query.group_by(col)
    sql, params = bdd_context.query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params
