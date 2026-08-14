"""
Live cutover workflow for database migration.

Orchestrates a 4-phase cutover from a local database to a remote database:

    Phase 1: bulk_transfer(local → remote)
    Phase 2: mirror(primary=local, replica=remote) — dual-write verification
    Phase 3: promote_mirror() — remote becomes primary
    Phase 4: detach_mirror() — run on remote alone

begin_cutover() handles Phase 1 + 2 setup. Phase 3 + 4 are manual calls
because the verification window has indeterminate duration — the caller
decides when confidence is sufficient.

Usage:
    m = await begin_cutover(source, target, ...)
    # verify through the mirror, then:
    m = promote(m)
    old_local = detach(m)
    await old_local.close()
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Union

from declaro_persistum.database import Database
from declaro_persistum.mirror import Mirror, mirror
from declaro_persistum.transfer import (
    BulkTransferResult,
    TableTransferProgress,
    bulk_transfer,
)

logger = logging.getLogger(__name__)


async def begin_cutover(
    source: Database,
    target: Database,
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
) -> tuple[Mirror, BulkTransferResult]:
    """
    Begin a live cutover from source to target database.

    Performs Phase 1 (bulk transfer) and sets up Phase 2 (dual-write
    verification via the mirror).

    Args:
        source: Source (local) database — becomes the mirror's primary
        target: Target (remote) database — becomes the mirror's replica
        source_dialect: Source database dialect
        target_dialect: Target database dialect
        schema_path: Path to Python module with Pydantic models
        batch_size: Rows per batch during transfer
        tables: Specific tables to transfer (None = all)
        expand_enums: Expand Literal types to enum lookup tables
        on_progress: Callback for per-table progress updates
        resume: Skip completed tables on retry
        fail_open: what a REPLICA failure means; True keeps serving from primary
        compare_on_read: run reads against both and record disagreement

    Returns:
        Tuple of (Mirror, BulkTransferResult).
        The Mirror has source as primary and target as replica.
        Use promote() for Phase 3 and detach() for Phase 4.

    Raises:
        TransferError: If bulk transfer fails critically
    """
    # Phase 1: Bulk transfer
    logger.info("Phase 1: Starting bulk data transfer")
    result = await bulk_transfer(
        source,
        target,
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

    # Phase 2: dual-write verification. A mirror is DATA — two databases and
    # two policies — and the phases are functions over it.
    logger.info("Phase 2: setting up the mirror for dual-write verification")
    m = mirror(
        primary=source,
        replica=target,
        fail_open=fail_open,
        compare_on_read=compare_on_read,
    )
    logger.info("Mirror ready. promote() is Phase 3, detach() is Phase 4.")

    return m, result
