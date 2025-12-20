"""
Schema diff detection for extended schema objects.

Detects changes between old and new schema definitions for enums,
triggers, procedures, and views.
"""

from typing import Any

from declaro_persistum.types import Enum, Procedure, Trigger, View


def diff_enums(
    old_enums: dict[str, Enum],
    new_enums: dict[str, Enum],
) -> list[dict[str, Any]]:
    """
    Detect changes between enum definitions.

    Args:
        old_enums: Current enum definitions
        new_enums: Desired enum definitions

    Returns:
        List of operations to apply
    """
    operations: list[dict[str, Any]] = []

    old_names = set(old_enums.keys())
    new_names = set(new_enums.keys())

    # New enums
    for name in new_names - old_names:
        operations.append(
            {
                "op": "create_enum",
                "table": "_enums",
                "details": {"enum": new_enums[name]},
            }
        )

    # Dropped enums
    for name in old_names - new_names:
        operations.append(
            {
                "op": "drop_enum",
                "table": "_enums",
                "details": {"name": name},
            }
        )

    # Changed enums (check for added values)
    for name in old_names & new_names:
        old_values = set(old_enums[name].get("values", []))
        new_values = set(new_enums[name].get("values", []))

        # New values added
        for value in new_values - old_values:
            operations.append(
                {
                    "op": "add_enum_value",
                    "table": "_enums",
                    "details": {"name": name, "value": value},
                }
            )

        # Values removed - PostgreSQL doesn't support this easily
        # Would need to recreate the enum type
        removed = old_values - new_values
        if removed:
            # For now, note this as a limitation
            operations.append(
                {
                    "op": "recreate_enum",
                    "table": "_enums",
                    "details": {
                        "name": name,
                        "enum": new_enums[name],
                        "removed_values": list(removed),
                    },
                }
            )

    return operations


def diff_triggers(
    table: str,
    old_triggers: dict[str, Trigger],
    new_triggers: dict[str, Trigger],
) -> list[dict[str, Any]]:
    """
    Detect changes between trigger definitions.

    Args:
        table: Table name
        old_triggers: Current trigger definitions
        new_triggers: Desired trigger definitions

    Returns:
        List of operations to apply
    """
    operations: list[dict[str, Any]] = []

    old_names = set(old_triggers.keys())
    new_names = set(new_triggers.keys())

    # New triggers
    for name in new_names - old_names:
        operations.append(
            {
                "op": "create_trigger",
                "table": table,
                "details": {"trigger": new_triggers[name]},
            }
        )

    # Dropped triggers
    for name in old_names - new_names:
        operations.append(
            {
                "op": "drop_trigger",
                "table": table,
                "details": {"name": name},
            }
        )

    # Changed triggers - compare body, timing, events
    for name in old_names & new_names:
        old = old_triggers[name]
        new = new_triggers[name]

        # Check for changes
        changed = (
            old.get("body") != new.get("body")
            or old.get("timing") != new.get("timing")
            or old.get("event") != new.get("event")
            or old.get("when") != new.get("when")
            or old.get("execute") != new.get("execute")
        )

        if changed:
            # Drop and recreate
            operations.append(
                {
                    "op": "drop_trigger",
                    "table": table,
                    "details": {"name": name},
                }
            )
            operations.append(
                {
                    "op": "create_trigger",
                    "table": table,
                    "details": {"trigger": new},
                }
            )

    return operations


def diff_procedures(
    old_procedures: dict[str, Procedure],
    new_procedures: dict[str, Procedure],
) -> list[dict[str, Any]]:
    """
    Detect changes between procedure definitions.

    Args:
        old_procedures: Current procedure definitions
        new_procedures: Desired procedure definitions

    Returns:
        List of operations to apply
    """
    operations: list[dict[str, Any]] = []

    old_names = set(old_procedures.keys())
    new_names = set(new_procedures.keys())

    # New procedures
    for name in new_names - old_names:
        operations.append(
            {
                "op": "create_function",
                "table": "_procedures",
                "details": {"procedure": new_procedures[name]},
            }
        )

    # Dropped procedures
    for name in old_names - new_names:
        operations.append(
            {
                "op": "drop_function",
                "table": "_procedures",
                "details": {"procedure": old_procedures[name]},
            }
        )

    # Changed procedures - CREATE OR REPLACE handles updates
    for name in old_names & new_names:
        old = old_procedures[name]
        new = new_procedures[name]

        # Check for any changes
        changed = (
            old.get("body") != new.get("body")
            or old.get("returns") != new.get("returns")
            or old.get("language") != new.get("language")
            or old.get("parameters") != new.get("parameters")
        )

        if changed:
            # CREATE OR REPLACE handles this
            operations.append(
                {
                    "op": "create_function",
                    "table": "_procedures",
                    "details": {"procedure": new},
                }
            )

    return operations


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
            # Query changed - CREATE OR REPLACE handles regular views
            # Materialized views need drop + create
            if new_materialized:
                operations.append(
                    {
                        "op": "drop_view",
                        "table": "_views",
                        "details": {"name": name, "materialized": True},
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
