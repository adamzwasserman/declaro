"""
declaro_persistum - Pure functional SQL library with declarative schema migrations.

A replacement for SQLAlchemy ORM and Alembic that uses:
- Schema as Data: Pydantic models with @table decorator
- State Diffing: Migrations computed by diffing desired state vs actual database state
- Pure Functions: No sessions, no identity maps, no hidden state
- Branch-Friendly: No linear revision chain; each branch carries its own schema state
- Enum Abstraction: Literal types auto-generate lookup tables with FK constraints
"""

from declaro_persistum.exceptions import (
    AmbiguityError,
    ConnectionError,
    CycleError,
    DeclaroError,
    DriftError,
    MigrationError,
    PoolClosedError,
    PoolConnectionError,
    PoolError,
    PoolExhaustedError,
    RollbackError,
    SchemaError,
)
from declaro_persistum.pool import ConnectionPool, MirrorPool, SyncConnectionPool, TursoCloudManager
from declaro_persistum.types import (
    Ambiguity,
    ApplyResult,
    Column,
    DiffResult,
    Index,
    Operation,
    Schema,
    Table,
)
from declaro_persistum.pydantic_loader import (
    load_schema_from_models,
    load_models_from_module,
    is_literal_type,
    extract_literal_values,
    get_literal_columns,
)

__version__ = "0.1.0"

__all__ = [
    # Types
    "Column",
    "Index",
    "Table",
    "Schema",
    "Operation",
    "DiffResult",
    "Ambiguity",
    "ApplyResult",
    # Connection Pool
    "ConnectionPool",
    "SyncConnectionPool",
    "MirrorPool",
    "TursoCloudManager",
    # Pydantic Loader
    "load_schema_from_models",
    "load_models_from_module",
    "is_literal_type",
    "extract_literal_values",
    "get_literal_columns",
    # Exceptions
    "DeclaroError",
    "SchemaError",
    "AmbiguityError",
    "CycleError",
    "DriftError",
    "ConnectionError",
    "MigrationError",
    "RollbackError",
    "PoolError",
    "PoolClosedError",
    "PoolExhaustedError",
    "PoolConnectionError",
    # Version
    "__version__",
]
