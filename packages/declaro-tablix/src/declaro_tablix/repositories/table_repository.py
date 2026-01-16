"""Table repository for declaro-tablix.

Provides Persistum-backed CRUD operations for table configurations
and user preferences using the Prisma-style async API.
"""

from typing import Any

from declaro_tablix.models import (
    TableConfigModel,
    UserFilterPreference,
)


async def get_config(
    connection: Any,
    table_name: str,
) -> TableConfigModel | None:
    """Get table configuration by name.

    Args:
        connection: Persistum database connection with Prisma-style API
        table_name: Name of the table to retrieve config for

    Returns:
        TableConfigModel if found, None otherwise
    """
    result = await connection.find_first(
        where={"table_name": table_name, "is_active": True}
    )

    if result is None:
        return None

    return TableConfigModel(**result)


async def save_config(
    connection: Any,
    config: TableConfigModel,
) -> TableConfigModel:
    """Save or update table configuration.

    Uses upsert semantics - creates if not exists, updates if exists.

    Args:
        connection: Persistum database connection with Prisma-style API
        config: TableConfigModel to save

    Returns:
        Saved TableConfigModel
    """
    # Check if config exists
    existing = await connection.find_first(
        where={"table_name": config.table_name}
    )

    if existing:
        # Update existing config
        result = await connection.update(
            where={"id": existing["id"]},
            data=config.model_dump(exclude_unset=True),
        )
    else:
        # Create new config
        result = await connection.create(
            data=config.model_dump(),
        )

    return TableConfigModel(**result)


async def get_user_preferences(
    connection: Any,
    user_id: str,
    table_id: str,
) -> list[UserFilterPreference]:
    """Get user filter preferences for a table.

    Args:
        connection: Persistum database connection with Prisma-style API
        user_id: User ID to get preferences for
        table_id: Table ID to get preferences for

    Returns:
        List of UserFilterPreference objects
    """
    results = await connection.find_many(
        where={"user_id": user_id, "table_id": table_id}
    )

    return [UserFilterPreference(**row) for row in results]
