"""Diffing the one extended schema object an applier can execute: the view.

THIS FILE HELD FOUR DIFFS AND THREE WERE UNREACHABLE IN BOTH DIRECTIONS.
`diff_enums`, `diff_triggers` and `diff_procedures` emitted eight operation
kinds between them — create_enum, drop_enum, add_enum_value, recreate_enum,
create_trigger, drop_trigger, create_function, drop_function — and the
applier's dispatch table handles none of them. Nothing in the package called
the three functions either; only BDD steps did, which is how they stayed alive
through two passes of deleting code no entry point reached.

So they produced operations that could not be applied, for callers that did
not exist. Deleted 2026-08-14 with their steps.

Enums are not lost with them: a `Literal` column becomes a real lookup table
and a foreign key through `abstractions.enums.expand_schema_enums`, which the
apply path calls and which works on every backend. That is the mechanism the
documentation describes, and it never went through `diff_enums`.

`diff_views` stays because it is the opposite case: its two operations,
`create_view` and `drop_view`, ARE in the applier's table, and the inspectors
return views when asked. Only the wiring was missing, so a view declared in a
models file never reached the applier and `usage.md` claimed a chain that did
not connect. `migrations.apply_migrations_async` now calls this.
"""

from typing import Any

from declaro_persistum.types import View


def diff_views(
    old_views: dict[str, View],
    new_views: dict[str, View],
) -> list[dict[str, Any]]:
    """
    Detect changes between view definitions.

    Args:
        old_views: Current view definitions
        new_views: Desired view definitions

    Returns:
        List of operations to apply
    """
    operations: list[dict[str, Any]] = []

    old_names = set(old_views.keys())
    new_names = set(new_views.keys())

    # New views
    for name in new_names - old_names:
        operations.append(
            {
                "op": "create_view",
                "table": "_views",
                "details": new_views[name],
            }
        )

    # Dropped views
    for name in old_names - new_names:
        operations.append(
            {
                "op": "drop_view",
                "table": "_views",
                "details": {
                    "name": name,
                    "materialized": old_views[name].get("materialized", False),
                },
            }
        )

    # Changed views
    for name in old_names & new_names:
        old = old_views[name]
        new = new_views[name]

        old_materialized = old.get("materialized", False)
        new_materialized = new.get("materialized", False)

        # If materialized status changed, need drop + create
        if old_materialized != new_materialized:
            operations.append(
                {
                    "op": "drop_view",
                    "table": "_views",
                    "details": {"name": name, "materialized": old_materialized},
                }
            )
            operations.append(
                {
                    "op": "create_view",
                    "table": "_views",
                    "details": new,
                }
            )
        elif old.get("query") != new.get("query"):
            # DROP THEN CREATE, ALWAYS. This emitted a bare `create_view` for a
            # regular view on the reasoning that CREATE OR REPLACE would handle
            # it. Nothing generates CREATE OR REPLACE: `create_view_sql` emits
            # `CREATE VIEW IF NOT EXISTS`, which is a no-op against a view that
            # already exists. So editing a view's query changed nothing and
            # said it had. Measured 2026-08-14: the database went on serving
            # the old definition after a migration that reported success.
            operations.append(
                {
                    "op": "drop_view",
                    "table": "_views",
                    "details": {
                        "name": name,
                        "materialized": new_materialized,
                    },
                }
            )
            operations.append(
                {
                    "op": "create_view",
                    "table": "_views",
                    "details": new,
                }
            )

    return operations
