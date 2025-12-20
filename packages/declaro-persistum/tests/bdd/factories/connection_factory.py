"""
Connection factories for managing database connections in tests.
"""

import os
import tempfile
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from declaro_persistum.types import Schema


# =============================================================================
# Connection URLs
# =============================================================================

# Use a temp file for SQLite tests so tables persist across connections
_SQLITE_TEMP_DB = os.path.join(tempfile.gettempdir(), "declaro_test.db")


def get_sqlite_url() -> str:
    """Get SQLite connection URL (file path).

    Uses a temp file so tables persist across multiple connections
    within the same test session.
    """
    return os.environ.get("TEST_SQLITE_URL", _SQLITE_TEMP_DB)


def get_postgresql_url() -> str:
    """Get PostgreSQL connection URL."""
    return os.environ.get(
        "TEST_POSTGRESQL_URL",
        "postgresql://postgres:postgres@localhost/declarotodo"
    )


def get_turso_url() -> str:
    """Get Turso connection URL."""
    return os.environ.get("TEST_TURSO_URL", "")


def get_turso_auth_token() -> str:
    """Get Turso auth token."""
    return os.environ.get("TURSO_AUTH_TOKEN", "")


# =============================================================================
# SQLite Connection
# =============================================================================

@asynccontextmanager
async def get_sqlite_connection() -> AsyncGenerator[Any, None]:
    """Get an SQLite connection."""
    import aiosqlite

    url = get_sqlite_url()
    async with aiosqlite.connect(url) as conn:
        conn.row_factory = aiosqlite.Row
        yield conn


async def setup_sqlite_schema(conn: Any, schema: Schema) -> None:
    """Create tables in SQLite from schema."""
    for table_name, table_def in schema.items():
        columns = table_def.get("columns", {})
        col_defs = []

        for col_name, col_def in columns.items():
            col_type = col_def.get("type", "text")
            # Map PostgreSQL types to SQLite
            type_mapping = {
                "uuid": "TEXT",
                "timestamptz": "TEXT",
                "timestamp": "TEXT",
                "boolean": "INTEGER",
                "serial": "INTEGER",
                "bigserial": "INTEGER",
                "jsonb": "TEXT",
                "json": "TEXT",
                "numeric": "REAL",
                "real": "REAL",
                "float": "REAL",
            }
            sqlite_type = type_mapping.get(col_type.lower().split("(")[0], col_type.upper())

            parts = [col_name, sqlite_type]

            if col_def.get("primary_key"):
                parts.append("PRIMARY KEY")
            if col_def.get("nullable") is False:
                parts.append("NOT NULL")
            if col_def.get("unique"):
                parts.append("UNIQUE")
            if "default" in col_def:
                default = col_def["default"]
                # Translate PostgreSQL defaults to SQLite
                if default == "now()":
                    default = "(datetime('now'))"
                elif default == "gen_random_uuid()":
                    default = "(lower(hex(randomblob(16))))"
                parts.append(f"DEFAULT {default}")

            col_defs.append(" ".join(parts))

        # Handle composite primary key
        if "primary_key" in table_def:
            pk_cols = ", ".join(table_def["primary_key"])
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        await conn.execute(sql)

    await conn.commit()


async def teardown_sqlite_schema(conn: Any, schema: Schema) -> None:
    """Drop tables in SQLite."""
    for table_name in schema.keys():
        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    await conn.commit()


# =============================================================================
# Turso (libsql) Connection - Sync API
# =============================================================================

def setup_turso_schema(conn: Any, schema: Schema) -> None:
    """Create tables in Turso/libsql from schema (sync API)."""
    for table_name, table_def in schema.items():
        columns = table_def.get("columns", {})
        col_defs = []

        for col_name, col_def in columns.items():
            col_type = col_def.get("type", "text")
            # Map PostgreSQL types to SQLite
            type_mapping = {
                "uuid": "TEXT",
                "timestamptz": "TEXT",
                "timestamp": "TEXT",
                "boolean": "INTEGER",
                "serial": "INTEGER",
                "bigserial": "INTEGER",
                "jsonb": "TEXT",
                "json": "TEXT",
                "numeric": "REAL",
                "real": "REAL",
                "float": "REAL",
            }
            sqlite_type = type_mapping.get(col_type.lower().split("(")[0], col_type.upper())

            parts = [col_name, sqlite_type]

            if col_def.get("primary_key"):
                parts.append("PRIMARY KEY")
            if col_def.get("nullable") is False:
                parts.append("NOT NULL")
            if col_def.get("unique"):
                parts.append("UNIQUE")
            if "default" in col_def:
                default = col_def["default"]
                if default == "now()":
                    default = "(datetime('now'))"
                elif default == "gen_random_uuid()":
                    default = "(lower(hex(randomblob(16))))"
                parts.append(f"DEFAULT {default}")

            col_defs.append(" ".join(parts))

        if "primary_key" in table_def:
            pk_cols = ", ".join(table_def["primary_key"])
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        conn.execute(sql)

    conn.commit()


def teardown_turso_schema(conn: Any, schema: Schema) -> None:
    """Drop tables in Turso/libsql (sync API)."""
    for table_name in schema.keys():
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.commit()


# =============================================================================
# PostgreSQL Connection
# =============================================================================

@asynccontextmanager
async def get_postgresql_connection() -> AsyncGenerator[Any, None]:
    """Get a PostgreSQL connection."""
    import asyncpg

    url = get_postgresql_url()
    conn = await asyncpg.connect(url)
    try:
        yield conn
    finally:
        await conn.close()


async def setup_postgresql_schema(conn: Any, schema: Schema) -> None:
    """Create tables in PostgreSQL from schema."""
    for table_name, table_def in schema.items():
        columns = table_def.get("columns", {})
        col_defs = []

        for col_name, col_def in columns.items():
            col_type = col_def.get("type", "text")
            parts = [col_name, col_type]

            if col_def.get("primary_key"):
                parts.append("PRIMARY KEY")
            if col_def.get("nullable") is False:
                parts.append("NOT NULL")
            if col_def.get("unique"):
                parts.append("UNIQUE")
            if "default" in col_def:
                parts.append(f"DEFAULT {col_def['default']}")
            if "references" in col_def:
                ref = col_def["references"]
                on_delete = col_def.get("on_delete", "NO ACTION")
                parts.append(f"REFERENCES {ref.replace('.', '(')}){' ON DELETE ' + on_delete.upper()}")

            col_defs.append(" ".join(parts))

        # Handle composite primary key
        if "primary_key" in table_def:
            pk_cols = ", ".join(table_def["primary_key"])
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        await conn.execute(sql)


async def teardown_postgresql_schema(conn: Any, schema: Schema) -> None:
    """Drop tables in PostgreSQL."""
    # Drop in reverse order to handle foreign keys
    for table_name in reversed(list(schema.keys())):
        await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")


# =============================================================================
# Connection Factory
# =============================================================================

class ConnectionFactory:
    """Factory for creating and managing database connections."""

    def __init__(self, dialect: str = "sqlite"):
        self.dialect = dialect

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Any, None]:
        """Get a connection for the configured dialect."""
        if self.dialect == "sqlite":
            async with get_sqlite_connection() as conn:
                yield conn
        elif self.dialect == "postgresql":
            async with get_postgresql_connection() as conn:
                yield conn
        elif self.dialect == "turso":
            # Turso uses sync API
            import libsql_experimental as libsql
            conn = libsql.connect(get_turso_url(), auth_token=get_turso_auth_token())
            try:
                yield conn
            finally:
                conn.close()
        else:
            raise ValueError(f"Unknown dialect: {self.dialect}")

    async def setup_schema(self, conn: Any, schema: Schema) -> None:
        """Create tables from schema."""
        if self.dialect == "sqlite":
            await setup_sqlite_schema(conn, schema)
        elif self.dialect == "postgresql":
            await setup_postgresql_schema(conn, schema)
        elif self.dialect == "turso":
            # Turso uses sync libsql API with SQLite-like syntax
            setup_turso_schema(conn, schema)

    async def teardown_schema(self, conn: Any, schema: Schema) -> None:
        """Drop tables from schema."""
        if self.dialect == "sqlite":
            await teardown_sqlite_schema(conn, schema)
        elif self.dialect == "postgresql":
            await teardown_postgresql_schema(conn, schema)
        elif self.dialect == "turso":
            # Turso uses sync libsql API
            teardown_turso_schema(conn, schema)

    @classmethod
    def sqlite(cls) -> "ConnectionFactory":
        """Create factory for SQLite."""
        return cls("sqlite")

    @classmethod
    def postgresql(cls) -> "ConnectionFactory":
        """Create factory for PostgreSQL."""
        return cls("postgresql")

    @classmethod
    def turso(cls) -> "ConnectionFactory":
        """Create factory for Turso."""
        return cls("turso")
