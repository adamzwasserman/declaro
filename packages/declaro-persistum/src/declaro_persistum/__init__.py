"""
declaro_persistum - Pure functional SQL library with declarative schema migrations.

A replacement for SQLAlchemy ORM and Alembic that uses:
- Schema as Data: Pydantic models with @table decorator
- State Diffing: Migrations computed by diffing desired state vs actual database state
- Pure Functions: No sessions, no identity maps, no hidden state
- Branch-Friendly: No linear revision chain; each branch carries its own schema state
- Enum Abstraction: Literal types auto-generate lookup tables with FK constraints
"""

# __version__ is declared before submodule imports so callers in this
# package (e.g. migrations.apply_migrations_async, which passes the
# current version into _compute_schema_hash) can import it without a
# circular dependency through __init__.
__version__ = "0.1.6"

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
    TransferError,
    WriteQueueError,
)
from declaro_persistum.instrumentation import LatencyRecord
from declaro_persistum.write_queue import WriteQueue, PendingEntry
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
from declaro_persistum.migrations import (
    apply_migrations_async,
)
from declaro_persistum.transfer import (
    bulk_transfer,
    BulkTransferResult,
)
from declaro_persistum.cutover import begin_cutover
from declaro_persistum.query.hooks import (
    PostHook,
    PreHook,
    QueryMeta,
    table_factory,
)
from declaro_persistum.query.update import Increment, increment

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
    # Migrations
    "apply_migrations_async",
    # Transfer
    "bulk_transfer",
    "BulkTransferResult",
    "begin_cutover",
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
    "TransferError",
    "WriteQueueError",
    # Write Queue
    "WriteQueue",
    "PendingEntry",
    # Instrumentation
    "LatencyRecord",
    # Query hooks
    "PreHook",
    "PostHook",
    "QueryMeta",
    "table_factory",
    # Atomic increment helper
    "Increment",
    "increment",
    # Version
    "__version__",
]
