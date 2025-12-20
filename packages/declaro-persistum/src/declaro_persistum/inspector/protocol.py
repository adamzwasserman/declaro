"""
Database inspector protocol definition.

Defines the interface that all dialect-specific inspectors must implement.
"""

from typing import Any, Protocol

from declaro_persistum.types import Schema


class DatabaseInspector(Protocol):
    """
    Protocol for database schema introspection.

    Implementations exist for each supported dialect:
    - PostgreSQL (pg_inspector.py)
    - SQLite (sqlite_inspector.py)
    - Turso (turso_inspector.py)
    """

    def get_dialect(self) -> str:
        """
        Return dialect identifier.

        Returns:
            One of: "postgresql", "sqlite", "turso"
        """
        ...

    async def introspect(
        self,
        connection: Any,
        *,
        schema_name: str = "public",
    ) -> Schema:
        """
        Introspect database and return current schema state.

        This is a pure function that queries the database metadata
        and returns a Schema dict representing the current state.

        Args:
            connection: Database connection object (dialect-specific)
            schema_name: Database schema to introspect (PostgreSQL only,
                        defaults to "public")

        Returns:
            Schema dict representing current database state.
            Empty dict if no tables exist.

        Raises:
            ConnectionError: If database connection fails during introspection
            DeclaroError: If schema cannot be read due to permissions or other issues

        Example:
            >>> inspector = PostgreSQLInspector()
            >>> schema = await inspector.introspect(conn)
            >>> print(schema.keys())
            dict_keys(['users', 'orders', 'products'])
        """
        ...

    async def table_exists(
        self,
        connection: Any,
        table_name: str,
        *,
        schema_name: str = "public",
    ) -> bool:
        """
        Check if a table exists in the database.

        Args:
            connection: Database connection object
            table_name: Name of the table to check
            schema_name: Database schema (PostgreSQL only)

        Returns:
            True if table exists, False otherwise
        """
        ...

    async def get_table_columns(
        self,
        connection: Any,
        table_name: str,
        *,
        schema_name: str = "public",
    ) -> dict[str, Any]:
        """
        Get column definitions for a specific table.

        Args:
            connection: Database connection object
            table_name: Name of the table
            schema_name: Database schema (PostgreSQL only)

        Returns:
            Dict mapping column names to Column definitions

        Raises:
            DeclaroError: If table doesn't exist
        """
        ...


def create_inspector(dialect: str) -> DatabaseInspector:
    """
    Factory function to create the appropriate inspector for a dialect.

    Args:
        dialect: One of "postgresql", "sqlite", "turso"

    Returns:
        DatabaseInspector implementation for the specified dialect

    Raises:
        ValueError: If dialect is not supported
    """
    if dialect == "postgresql":
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        return PostgreSQLInspector()  # type: ignore[return-value]
    elif dialect == "sqlite":
        from declaro_persistum.inspector.sqlite import SQLiteInspector

        return SQLiteInspector()  # type: ignore[return-value]
    elif dialect == "turso":
        from declaro_persistum.inspector.turso import TursoInspector

        return TursoInspector()  # type: ignore[return-value]
    else:
        raise ValueError(
            f"Unsupported dialect: {dialect}. Supported dialects: postgresql, sqlite, turso"
        )
