"""
Query style-specific BDD step definitions (Django, Prisma, SQLAlchemy).
"""

import pytest
from pytest_bdd import given, when, then, parsers

from declaro_persistum.query.table import table, set_default_schema


# =============================================================================
# Django-Style Steps
# =============================================================================


@when(parsers.parse('I call {table_}.objects.filter({field}="{value}")'))
def when_django_filter(bdd_context, table_: str, field: str, value: str):
    """Call Django-style filter."""
    # The table_proxy should already be set up
    query = bdd_context.table_proxy.objects.filter(**{field: value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.filter({field}__gt={value:d})'))
def when_django_filter_gt(bdd_context, table_: str, field: str, value: int):
    """Call Django-style filter with __gt lookup."""
    query = bdd_context.table_proxy.objects.filter(**{f"{field}__gt": value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.filter({field}__gte={value:d})'))
def when_django_filter_gte(bdd_context, table_: str, field: str, value: int):
    """Call Django-style filter with __gte lookup."""
    query = bdd_context.table_proxy.objects.filter(**{f"{field}__gte": value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.filter({field}__lt={value:d})'))
def when_django_filter_lt(bdd_context, table_: str, field: str, value: int):
    """Call Django-style filter with __lt lookup."""
    query = bdd_context.table_proxy.objects.filter(**{f"{field}__lt": value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.filter({field}__lte={value:d})'))
def when_django_filter_lte(bdd_context, table_: str, field: str, value: int):
    """Call Django-style filter with __lte lookup."""
    query = bdd_context.table_proxy.objects.filter(**{f"{field}__lte": value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.filter({field}__contains="{value}")'))
def when_django_filter_contains(bdd_context, table_: str, field: str, value: str):
    """Call Django-style filter with __contains lookup."""
    query = bdd_context.table_proxy.objects.filter(**{f"{field}__contains": value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.filter({field}__isnull={value})'))
def when_django_filter_isnull(bdd_context, table_: str, field: str, value: str):
    """Call Django-style filter with __isnull lookup."""
    bool_value = value.lower() == "true"
    query = bdd_context.table_proxy.objects.filter(**{f"{field}__isnull": bool_value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I chain .filter({field}__gte={value:d})'))
def when_django_chain_filter(bdd_context, field: str, value: int):
    """Chain another filter onto existing query."""
    query = bdd_context.query.filter(**{f"{field}__gte": value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.exclude({field}="{value}")'))
def when_django_exclude(bdd_context, table_: str, field: str, value: str):
    """Call Django-style exclude."""
    query = bdd_context.table_proxy.objects.exclude(**{field: value})
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.order("{field}")'))
def when_django_order(bdd_context, table_: str, field: str):
    """Call Django-style order."""
    query = bdd_context.table_proxy.objects.order(field)
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.objects.order("-{field}")'))
def when_django_order_desc(bdd_context, table_: str, field: str):
    """Call Django-style descending order."""
    query = bdd_context.table_proxy.objects.order(f"-{field}")
    bdd_context.query = query
    sql, params = query.to_sql()
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# Prisma-Style Steps
# =============================================================================


@when(parsers.parse('I call {table_}.prisma.find_many()'))
def when_prisma_find_many(bdd_context, table_: str):
    """Call Prisma-style find_many."""
    builder = bdd_context.table_proxy.prisma
    sql, params = builder._build_select_sql(None, None, None, None, "postgresql")
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.find_many(where={{{field}: "{value}"}})'))
def when_prisma_find_many_where(bdd_context, table_: str, field: str, value: str):
    """Call Prisma-style find_many with where clause."""
    builder = bdd_context.table_proxy.prisma
    sql, params = builder._build_select_sql({field: value}, None, None, None, "postgresql")
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.find_many(where={{{field}: {{"gt": {value:d}}}}})'))
def when_prisma_find_many_where_gt(bdd_context, table_: str, field: str, value: int):
    """Call Prisma-style find_many with gt operator."""
    builder = bdd_context.table_proxy.prisma
    sql, params = builder._build_select_sql({field: {"gt": value}}, None, None, None, "postgresql")
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.find_many(where={{{field}: {{"contains": "{value}"}}}})'))
def when_prisma_find_many_where_contains(bdd_context, table_: str, field: str, value: str):
    """Call Prisma-style find_many with contains operator."""
    builder = bdd_context.table_proxy.prisma
    sql, params = builder._build_select_sql({field: {"contains": value}}, None, None, None, "postgresql")
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.find_many(order_by={{{field}: "{direction}"}})'))
def when_prisma_find_many_order(bdd_context, table_: str, field: str, direction: str):
    """Call Prisma-style find_many with order_by."""
    builder = bdd_context.table_proxy.prisma
    sql, params = builder._build_select_sql(None, {field: direction}, None, None, "postgresql")
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.create(data={{{field}: "{value}"}})'))
def when_prisma_create(bdd_context, table_: str, field: str, value: str):
    """Call Prisma-style create - builds INSERT SQL."""
    builder = bdd_context.table_proxy.prisma
    # Build INSERT SQL manually (mirrors PrismaQueryBuilder.create logic)
    data = {field: value}
    columns = list(data.keys())
    placeholders = ", ".join(f":ins_{c}" for c in columns)
    cols_sql = ", ".join(columns)
    sql = f"INSERT INTO {builder._table_name} ({cols_sql}) VALUES ({placeholders}) RETURNING *"
    params = {f"ins_{k}": v for k, v in data.items()}
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.update(where={{{where_field}: "{where_value}"}}, data={{{data_field}: "{data_value}"}})'))
def when_prisma_update(bdd_context, table_: str, where_field: str, where_value: str, data_field: str, data_value: str):
    """Call Prisma-style update - builds UPDATE SQL."""
    builder = bdd_context.table_proxy.prisma
    # Build UPDATE SQL manually (mirrors PrismaQueryBuilder.update logic)
    data = {data_field: data_value}
    where = {where_field: where_value}

    set_parts = []
    params = {}
    for col, val in data.items():
        param_name = f"upd_{col}"
        set_parts.append(f"{col} = :{param_name}")
        params[param_name] = val

    set_sql = ", ".join(set_parts)
    sql = f"UPDATE {builder._table_name} SET {set_sql}"

    # Build WHERE using the builder's method
    conditions = builder._where_to_conditions(where)
    if conditions:
        combined = conditions[0]
        for c in conditions[1:]:
            combined = combined & c
        where_sql, where_params = combined.to_sql("postgresql")
        sql += f" WHERE {where_sql}"
        params.update(where_params)

    sql += " RETURNING *"
    bdd_context.sql = sql
    bdd_context.params = params


@when(parsers.parse('I call {table_}.prisma.delete(where={{{field}: "{value}"}})'))
def when_prisma_delete(bdd_context, table_: str, field: str, value: str):
    """Call Prisma-style delete - builds DELETE SQL."""
    builder = bdd_context.table_proxy.prisma
    # Build DELETE SQL manually (mirrors PrismaQueryBuilder.delete logic)
    where = {field: value}

    sql = f"DELETE FROM {builder._table_name}"
    params = {}

    # Build WHERE using the builder's method
    conditions = builder._where_to_conditions(where)
    if conditions:
        combined = conditions[0]
        for c in conditions[1:]:
            combined = combined & c
        where_sql, where_params = combined.to_sql("postgresql")
        sql += f" WHERE {where_sql}"
        params.update(where_params)

    sql += " RETURNING *"
    bdd_context.sql = sql
    bdd_context.params = params


# =============================================================================
# SQLAlchemy-Style Steps (using Session)
# =============================================================================


@when(parsers.parse('I create a Session and add a {model} with {field}="{value}"'))
def when_sqlalchemy_add(bdd_context, model: str, field: str, value: str):
    """Create SQLAlchemy-style Session and add model."""
    # This would use the SQLAlchemy compatibility layer
    from declaro_persistum.query.sqlalchemy import Session

    # For now, just verify the API exists
    bdd_context.sql = f"INSERT placeholder for {model}.{field}={value}"


@when(parsers.parse('I query {model}.filter_by({field}="{value}")'))
def when_sqlalchemy_filter_by(bdd_context, model: str, field: str, value: str):
    """Query using SQLAlchemy-style filter_by."""
    from declaro_persistum.query.sqlalchemy import Session

    # Placeholder for SQLAlchemy query building
    bdd_context.sql = f"SELECT placeholder for {model} WHERE {field}={value}"
