"""
Database connection module - handles multiple database backends.

This module abstracts database connections so the main app logic
works identically across SQLite, PostgreSQL, Turso Cloud, and Turso Embedded.

The app code doesn't change - only this module handles the differences.
"""

import os
from pathlib import Path
from typing import Any, AsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import dataclass

# Current database configuration
_current_db: "DatabaseConfig | None" = None


@dataclass
class DatabaseConfig:
    """Database configuration."""
    dialect: str  # "sqlite", "postgresql", "turso_cloud", or "turso_embedded"
    connection_string: str
    display_name: str


# Default database paths
DEFAULT_SQLITE_PATH = str(Path(__file__).parent / "todos.db")
DEFAULT_TURSO_EMBEDDED_PATH = str(Path(__file__).parent / "todos_turso.db")


def get_sqlite_config(database_path: str | None = None) -> DatabaseConfig:
    """Create SQLite configuration."""
    path = database_path or DEFAULT_SQLITE_PATH
    return DatabaseConfig(
        dialect="sqlite",
        connection_string=path,
        display_name=f"SQLite ({Path(path).name})"
    )


def get_turso_embedded_config(database_path: str | None = None) -> DatabaseConfig:
    """Create Turso Embedded configuration (pyturso)."""
    path = database_path or DEFAULT_TURSO_EMBEDDED_PATH
    return DatabaseConfig(
        dialect="turso_embedded",
        connection_string=path,
        display_name=f"Turso Embedded ({Path(path).name})"
    )


# Default configs for backward compatibility
SQLITE_CONFIG = get_sqlite_config()
TURSO_EMBEDDED_CONFIG = get_turso_embedded_config()


def get_postgres_config(host: str, port: int, user: str, password: str, database: str) -> DatabaseConfig:
    """Create PostgreSQL configuration."""
    return DatabaseConfig(
        dialect="postgresql",
        connection_string=f"postgresql://{user}:{password}@{host}:{port}/{database}",
        display_name=f"PostgreSQL ({host}:{port}/{database})"
    )


def get_turso_cloud_config(url: str, auth_token: str) -> DatabaseConfig:
    """Create Turso Cloud configuration (libsql)."""
    return DatabaseConfig(
        dialect="turso_cloud",
        connection_string=url,
        display_name=f"Turso Cloud ({url.split('//')[1].split('.')[0]})"
    )


# Alias for backward compatibility
def get_turso_config(url: str, auth_token: str) -> DatabaseConfig:
    """Create Turso Cloud configuration (alias for get_turso_cloud_config)."""
    return get_turso_cloud_config(url, auth_token)


def set_database(config: DatabaseConfig) -> None:
    """Set the current database configuration."""
    global _current_db
    _current_db = config


def get_database() -> DatabaseConfig:
    """Get the current database configuration."""
    if _current_db is None:
        # Default to SQLite
        return SQLITE_CONFIG
    return _current_db


@asynccontextmanager
async def get_connection():
    """
    Get a database connection for the current configuration.

    Usage:
        async with get_connection() as conn:
            results = await query.execute(conn)

    This is the ONLY place where database-specific code lives.
    The rest of the app uses this uniformly.
    """
    config = get_database()

    if config.dialect == "sqlite":
        import aiosqlite
        async with aiosqlite.connect(config.connection_string) as conn:
            yield conn

    elif config.dialect == "postgresql":
        import asyncpg
        conn = await asyncpg.connect(config.connection_string)
        try:
            yield conn
        finally:
            await conn.close()

    elif config.dialect == "turso_cloud":
        import libsql_experimental as libsql
        conn = libsql.connect(
            config.connection_string,
            auth_token=os.environ.get("TURSO_AUTH_TOKEN", "")
        )
        try:
            yield conn
        finally:
            conn.close()

    elif config.dialect == "turso_embedded":
        import turso
        conn = turso.connect(config.connection_string)
        try:
            yield conn
        finally:
            conn.__exit__(None, None, None)

    else:
        raise ValueError(f"Unknown dialect: {config.dialect}")


async def ensure_postgres_database(host: str, port: int, user: str, password: str, database: str) -> tuple[bool, str]:
    """
    Ensure PostgreSQL database exists, creating it if necessary.

    Returns:
        (created, message) tuple - created is True if database was created
    """
    import asyncpg

    # Connect to default 'postgres' database to check/create target database
    try:
        conn = await asyncpg.connect(
            host=host, port=port, user=user, password=password, database="postgres"
        )
        try:
            # Check if database exists
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1", database
            )
            if not exists:
                # CREATE DATABASE cannot run in a transaction
                await conn.execute(f'CREATE DATABASE "{database}"')
                return True, f"Created database '{database}'"
            return False, f"Database '{database}' already exists"
        finally:
            await conn.close()
    except Exception as e:
        return False, f"Could not create database: {e}"


async def test_connection(config: DatabaseConfig) -> tuple[bool, str]:
    """
    Test if a database connection works.

    For PostgreSQL, automatically creates the database if it doesn't exist.

    Returns:
        (success, message) tuple
    """
    try:
        old_config = _current_db

        # For PostgreSQL, try to create database first if it doesn't exist
        if config.dialect == "postgresql":
            import asyncpg
            from urllib.parse import urlparse
            parsed = urlparse(config.connection_string)

            try:
                created, create_msg = await ensure_postgres_database(
                    host=parsed.hostname or "localhost",
                    port=parsed.port or 5432,
                    user=parsed.username or "postgres",
                    password=parsed.password or "",
                    database=parsed.path.lstrip("/") or "declarotodo"
                )
            except asyncpg.InvalidCatalogNameError:
                # Database doesn't exist, try to create it
                created, create_msg = await ensure_postgres_database(
                    host=parsed.hostname or "localhost",
                    port=parsed.port or 5432,
                    user=parsed.username or "postgres",
                    password=parsed.password or "",
                    database=parsed.path.lstrip("/") or "declarotodo"
                )

        set_database(config)

        async with get_connection() as conn:
            if config.dialect == "sqlite":
                await conn.execute("SELECT 1")
            elif config.dialect == "postgresql":
                await conn.execute("SELECT 1")
            elif config.dialect == "turso_cloud":
                conn.execute("SELECT 1")
            elif config.dialect == "turso_embedded":
                conn.execute("SELECT 1")

        set_database(old_config) if old_config else None

        if config.dialect == "postgresql":
            return True, f"Connection successful. {create_msg}"
        return True, "Connection successful"

    except ImportError as e:
        return False, f"Driver not installed: {e}"
    except Exception as e:
        return False, f"Connection failed: {e}"


async def init_schema(config: DatabaseConfig) -> None:
    """
    Initialize the database schema.

    Creates the todos table if it doesn't exist.
    The SQL is dialect-aware for minor syntax differences.
    """
    set_database(config)

    if config.dialect == "sqlite":
        sql = """
            CREATE TABLE IF NOT EXISTS todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """
    elif config.dialect == "postgresql":
        sql = """
            CREATE TABLE IF NOT EXISTS todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now()
            )
        """
    elif config.dialect in ("turso_cloud", "turso_embedded"):
        sql = """
            CREATE TABLE IF NOT EXISTS todos (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                completed INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """
    else:
        raise ValueError(f"Unknown dialect: {config.dialect}")

    async with get_connection() as conn:
        if config.dialect == "sqlite":
            await conn.execute(sql)
            await conn.commit()
        elif config.dialect == "postgresql":
            await conn.execute(sql)
        elif config.dialect in ("turso_cloud", "turso_embedded"):
            conn.execute(sql)
            conn.commit()


def check_driver_available(dialect: str) -> tuple[bool, str]:
    """Check if the database driver is installed."""
    try:
        if dialect == "sqlite":
            import aiosqlite
            return True, "aiosqlite installed"
        elif dialect == "postgresql":
            import asyncpg
            return True, "asyncpg installed"
        elif dialect == "turso_cloud":
            import libsql_experimental
            return True, "libsql installed"
        elif dialect == "turso_embedded":
            import turso
            return True, "pyturso installed"
        else:
            return False, f"Unknown dialect: {dialect}"
    except ImportError as e:
        return False, str(e)
