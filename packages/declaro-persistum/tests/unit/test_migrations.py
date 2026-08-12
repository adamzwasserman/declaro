"""
Unit tests for schema migration skip-if-clean optimization.

Tests the SHA-256 hash-based dirty flag that skips introspection
when the schema file hasn't changed since the last successful migration.
"""

import asyncio
import pytest
import tempfile
from pathlib import Path

from declaro_persistum.migrations import (
    META_TABLE,
    _compute_schema_hash,
    _ensure_meta_table,
    _get_stored_hash,
    _schema_is_clean,
    _store_hash,
    apply_migrations_async,
)
from declaro_persistum.pool import ConnectionPool








