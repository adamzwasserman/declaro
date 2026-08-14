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
#
# It is read from installed package metadata rather than written as a
# literal here. A literal is a second source of truth that the release
# process has to remember to bump, and when it drifts it does so silently:
# it sat at "0.1.6" through the 0.1.7 and 0.1.8 releases, which meant the
# version mixed into the schema hash never changed and the skip-if-clean
# cache was never invalidated on upgrade — the exact propagation those
# releases depended on. Derived from pyproject, it cannot drift.
from importlib.metadata import version as _pkg_version

__version__ = _pkg_version("declaro-persistum")

from declaro_persistum.exceptions import (  # noqa: I001 - must follow __version__, see above
    ConnectionError,
    CycleError,
    DeclaroError,
    DriftError,
    MigrationError,
    DatabaseClosedError,
    ConnectionFailedError,
    DatabaseError,
    ConnectionsExhaustedError,
    RollbackError,
    SchemaError,
    TransferError,
)
from declaro_persistum.instrumentation import LatencyRecord
from declaro_persistum.write_queue import (
    PendingWrite,
    Receipt,
    Room,
    collect,
    deposit,
    drain,
    new_room,
)
# TursoCloudManager — four classes, now deleted. `database.py` replaces them
# The query API. This package was UNREACHABLE from here after the query
# rewrite — the reachability ratchet caught it, which is what it exists for.
# Nobody could `from declaro_persistum import select`.
from declaro_persistum.query import (
    OPERATORS,
    execute,
    execute_many,
    execute_one,
    execute_scalar,
    Query,
    case_,
    count_,
    delete,
    insert,
    raw,
    render_condition,
    select,
    subquery,
    table,
    update,
)
# Opening a Turso database, and the crew that writes to a local one
# concurrently. The reachability ratchet caught both of these unexported.
from declaro_persistum.crew import Crew, start_crew, stop_crew
from declaro_persistum.mirror import (
    Divergence,
    Mirror,
    detach,
    mirror,
    parallel_write,
    promote,
)
from declaro_persistum.turso_database import migrating, open_turso
from declaro_persistum.database import (
    Database,
    close,
    flush,
    is_replicated,
    new_database,
    reading,
    refresh,
    replicate,
    writing,
)
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
from declaro_persistum.writers import WRITERS
from declaro_persistum.migrations import (
    apply_migrations_async,
)
from declaro_persistum.transfer import (
    bulk_transfer,
    BulkTransferResult,
)
from declaro_persistum.cutover import begin_cutover
# `query/hooks.py` and `query/update.py` exported PreHook, PostHook,
# QueryMeta, table_factory, Increment and increment. All six went with the
# query builder classes and return with Group A of the map.

__all__ = [
    "WRITERS",
    # databases, the crew, the cutover mirror
    "Mirror",
    "Divergence",
    "mirror",
    "promote",
    "detach",
    "parallel_write",
    "open_turso",
    "migrating",
    "start_crew",
    "stop_crew",
    "Crew",
    # query — functions over data
    "select",
    "execute",
    "execute_one",
    "execute_scalar",
    "execute_many",
    "insert",
    "update",
    "delete",
    "raw",
    "Query",
    "render_condition",
    "OPERATORS",
    "table",
    "count_",
    "case_",
    "subquery",
    # Types
    "Column",
    "Index",
    "Table",
    "Schema",
    "Operation",
    "DiffResult",
    "Ambiguity",
    "ApplyResult",
    # Databases and connections
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
    "CycleError",
    "DriftError",
    "ConnectionError",
    "MigrationError",
    "RollbackError",
    "DatabaseError",
    "DatabaseClosedError",
    "ConnectionsExhaustedError",
    "ConnectionFailedError",
    "TransferError",
    # Instrumentation
    "LatencyRecord",
    # Write queue — a waiting room in front of the WAL
    "PendingWrite",
    "Receipt",
    "Room",
    "new_room",
    "deposit",
    "collect",
    "drain",
    # Query hooks
    # Atomic increment helper
    # Version
    "__version__",
]
