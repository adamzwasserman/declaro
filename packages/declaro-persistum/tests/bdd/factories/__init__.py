"""Test data and connections for BDD scenarios — all of it functions and data.

This package exported five factory CLASSES: `TodoFactory`, `UserFactory`,
`OrderFactory`, `SchemaFactory` and `ConnectionFactory`. Between them they held
no state at all — every member was a `@staticmethod`, and `ConnectionFactory`'s
methods forwarded to module-level functions that already existed beside it.
They were namespaces wearing a class keyword.

What replaces them:

    TodoFactory.create()          ->  todo()
    TodoFactory.create_batch(n)   ->  todos(n)
    UserFactory.create()          ->  user()
    OrderFactory.create()         ->  order()
    ConnectionFactory.sqlite()    ->  sqlite_backend()      a Backend TypedDict
    SchemaFactory                 ->  simple_todos_schema() and friends

A `Backend` carries the three callables a scenario needs — connect, setup,
teardown — as data, following honest-persist's `open_pool(db_id, connect,
classify, close, size)`: the dependency is injected, and what the scenario
holds is a value it can print, compare and pass on.
"""

from tests.bdd.factories.connection_factory import (
    BACKENDS,
    Backend,
    apply_step,
    get_postgresql_connection,
    get_sqlite_connection,
    get_turso_connection,
    postgresql_backend,
    sqlite_backend,
    turso_backend,
)
from tests.bdd.factories.data_factory import (
    EDGE_CASE_DATES,
    EDGE_CASE_INTEGERS,
    EDGE_CASE_STRINGS,
    any_todo,
    any_user,
    order,
    order_with_items,
    orders,
    todo,
    todos,
    todos_with_edge_case_titles,
    user,
    users,
)
from tests.bdd.factories.schema_factory import (
    complex_ecommerce_schema,
    schema_with_all_types,
    schema_with_composite_pk,
    schema_with_constraints,
    simple_todos_schema,
    simple_users_schema,
)

__all__ = [
    # data
    "todo",
    "todos",
    "todos_with_edge_case_titles",
    "any_todo",
    "user",
    "users",
    "any_user",
    "order",
    "orders",
    "order_with_items",
    "EDGE_CASE_STRINGS",
    "EDGE_CASE_INTEGERS",
    "EDGE_CASE_DATES",
    # schemas
    "simple_todos_schema",
    "simple_users_schema",
    "complex_ecommerce_schema",
    "schema_with_all_types",
    "schema_with_constraints",
    "schema_with_composite_pk",
    # backends
    "Backend",
    "BACKENDS",
    "sqlite_backend",
    "postgresql_backend",
    "turso_backend",
    "apply_step",
    "get_sqlite_connection",
    "get_postgresql_connection",
    "get_turso_connection",
]
