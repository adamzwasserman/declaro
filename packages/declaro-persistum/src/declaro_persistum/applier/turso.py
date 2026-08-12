"""
Turso migration applier implementation.

Turso is SQLite-compatible, so the SQL generation is shared with SQLite
via applier.shared module.
Connection handling uses TursoAsyncConnection (async wrapper over pyturso).

Uses per-operation transactions: each operation gets its own BEGIN/COMMIT.
Failed operations are logged and skipped so that one unsupported operation
(e.g. ADD FOREIGN KEY) does not block valid ones (e.g. ADD COLUMN).
"""

import logging
from typing import Any, Literal

from declaro_persistum.applier.shared import (
    apply_reconstruction_changes,
    columns_from_pragma_rows,
    dry_run_preview,
    enum_population_sql,
    generate_operation_sql,
    generate_sql,
    requires_reconstruction,
)
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Operation

logger = logging.getLogger(__name__)




# Turso uses identical view generation to SQLite
from declaro_persistum.applier.sqlite import generate_create_view, generate_drop_view

__all__ = ["TursoApplier", "generate_create_view", "generate_drop_view"]
