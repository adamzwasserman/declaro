"""Repository layer for declaro-tablix.

Provides Persistum-backed data access for table configurations and user preferences.
"""

from declaro_tablix.repositories.table_repository import (
    get_config,
    get_user_preferences,
    save_config,
)

__all__ = [
    "get_config",
    "get_user_preferences",
    "save_config",
]
