"""
Live cutover workflow for database migration.

Orchestrates a 4-phase cutover from a local database to a remote database:

    Phase 1: bulk_transfer(local → remote)
    Phase 2: MirrorPool(primary=local, mirror=remote) — dual-write verification
    Phase 3: promote_mirror() — remote becomes primary
    Phase 4: detach_mirror() — run on remote alone

begin_cutover() handles Phase 1 + 2 setup. Phase 3 + 4 are manual calls
because the verification window has indeterminate duration — the caller
decides when confidence is sufficient.

Usage:
    mirror_pool = await begin_cutover(
        source_pool, target_pool,
        "sqlite", "turso",
        schema_path="/path/to/models.py",
    )

    # Phase 2: dual-write verification (use mirror_pool for all operations)
    # ... run your app, verify reads match ...

    # Phase 3: promote remote to primary
    mirror_pool.promote_mirror()

    # Phase 4: detach local, run on remote alone
    old_local = mirror_pool.detach_mirror()
    await old_local.close()
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Union

from declaro_persistum.pool import MirrorPool
from declaro_persistum.transfer import (
    BulkTransferResult,
    TableTransferProgress,
    bulk_transfer,
)

logger = logging.getLogger(__name__)


async def begin_cutover(
    source_pool: Any,
    target_pool: Any,
    source_dialect: str,
    target_dialect: str,
    schema_path: Union[str, Path],
    *,
    batch_size: int = 1000,
    tables: list[str] | None = None,
    expand_enums: bool = True,
    on_progress: Callable[[TableTransferProgress], None] | None = None,
    resume: bool = True,
    fail_open: bool = True,
    compare_on_read: bool = True,
) -> tuple[MirrorPool, BulkTransferResult]:
    """
    Begin a live cutover from source to target database.

    Performs Phase 1 (bulk transfer) and sets up Phase 2 (dual-write
    verification via MirrorPool).

    Args:
        source_pool: Source (local) database pool — becomes MirrorPool primary
        target_pool: Target (remote) database pool — becomes MirrorPool mirror
        source_dialect: Source database dialect
        target_dialect: Target database dialect
        schema_path: Path to Python module with Pydantic models
        batch_size: Rows per batch during transfer
        tables: Specific tables to transfer (None = all)
        expand_enums: Expand Literal types to enum lookup tables
        on_progress: Callback for per-table progress updates
        resume: Skip completed tables on retry
        fail_open: MirrorPool fail_open setting (default True)
        compare_on_read: MirrorPool compare_on_read setting (default True)

    Returns:
        Tuple of (MirrorPool, BulkTransferResult).
        The MirrorPool has source as primary and target as mirror.
        Use promote_mirror() for Phase 3 and detach_mirror() for Phase 4.

    Raises:
        TransferError: If bulk transfer fails critically
    """
    # Phase 1: Bulk transfer
    logger.info("Phase 1: Starting bulk data transfer")
    result = await bulk_transfer(
        source_pool,
        target_pool,
        source_dialect,
        target_dialect,
        schema_path,
        batch_size=batch_size,
        tables=tables,
        expand_enums=expand_enums,
        on_progress=on_progress,
        resume=resume,
    )
    logger.info(
        f"Phase 1 complete: {result['tables_transferred']} tables, "
        f"{result['total_rows']} rows in {result['duration_seconds']:.1f}s"
    )

    # Phase 2: Set up MirrorPool for dual-write verification
    logger.info("Phase 2: Setting up MirrorPool for dual-write verification")
    mirror_pool = MirrorPool(
        source_pool,
        target_pool,
        fail_open=fail_open,
        compare_on_read=compare_on_read,
    )
    logger.info(
        "MirrorPool ready. Use promote_mirror() for Phase 3, "
        "detach_mirror() for Phase 4."
    )

    return mirror_pool, result
