"""
CLI command implementations.

Each command is an async function that returns an exit code.
"""

import sys
from datetime import UTC
from pathlib import Path
from typing import Any

from declaro_persistum.exceptions import AmbiguityError
from declaro_persistum.types import Ambiguity, Decision, DiffResult, Schema


async def cmd_diff(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: str,
    interactive: bool,
    unattended: bool,
    force: bool,
    verbose: bool,
) -> int:
    """
    Compare target schema to database and show proposed operations.

    Returns:
        0 if no differences or diff shown successfully
        1 if there are unresolved ambiguities (unattended mode)
        2 if there's drift and --force not specified
    """
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector.protocol import create_inspector
    from declaro_persistum.loader import load_decisions, load_schema, load_snapshot

    # Determine interactive mode
    is_tty = sys.stdin.isatty() and sys.stdout.isatty()
    if not interactive and not unattended:
        interactive = is_tty
        unattended = not is_tty

    if verbose:
        print(f"Loading schema from {schema_dir}...")

    # Load target schema from TOML files
    target = load_schema(schema_dir)

    if verbose:
        print(f"Loaded {len(target)} tables from schema files")

    # Connect and introspect database
    connection = await _connect(connection_string, dialect)
    try:
        inspector = create_inspector(dialect)
        actual = await inspector.introspect(connection)

        if verbose:
            print(f"Introspected {len(actual)} tables from database")

        # Load expected snapshot and check for drift
        snapshot_path = Path(schema_dir) / "snapshot.toml"
        if snapshot_path.exists() and not force:
            expected = load_snapshot(schema_dir)
            drift = _check_drift(actual, expected)
            if drift:
                _print_drift_error(drift)
                return 2

        # Load any pre-made decisions
        decisions = load_decisions(schema_dir)

        # Compute diff
        result = diff(actual, target, decisions=decisions)

        # Handle ambiguities
        if result["ambiguities"]:
            if unattended:
                raise AmbiguityError(result["ambiguities"])

            if interactive:
                decisions = await _resolve_ambiguities_interactive(
                    result["ambiguities"], schema_dir
                )
                # Re-run diff with decisions
                result = diff(actual, target, decisions=decisions)

        # Display results
        _print_diff_result(result, verbose)

        return 0

    finally:
        await _disconnect(connection, dialect)


async def cmd_apply(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: str,
    interactive: bool,
    unattended: bool,
    dry_run: bool,
    force: bool,
    verbose: bool,
) -> int:
    """
    Apply pending migrations to database.

    Returns:
        0 if migrations applied successfully
        1 if there are errors
    """
    from declaro_persistum.applier.protocol import create_applier
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector.protocol import create_inspector
    from declaro_persistum.loader import load_decisions, load_schema, load_snapshot, save_snapshot

    # Determine interactive mode
    is_tty = sys.stdin.isatty() and sys.stdout.isatty()
    if not interactive and not unattended:
        interactive = is_tty
        unattended = not is_tty

    # Load schema and connect
    target = load_schema(schema_dir)
    connection = await _connect(connection_string, dialect)

    try:
        inspector = create_inspector(dialect)
        actual = await inspector.introspect(connection)

        # Check drift
        snapshot_path = Path(schema_dir) / "snapshot.toml"
        if snapshot_path.exists() and not force:
            expected = load_snapshot(schema_dir)
            drift = _check_drift(actual, expected)
            if drift:
                _print_drift_error(drift)
                return 2

        # Compute diff
        decisions = load_decisions(schema_dir)
        result = diff(actual, target, decisions=decisions)

        # Handle ambiguities
        if result["ambiguities"]:
            if unattended:
                raise AmbiguityError(result["ambiguities"])
            if interactive:
                decisions = await _resolve_ambiguities_interactive(
                    result["ambiguities"], schema_dir
                )
                result = diff(actual, target, decisions=decisions)

        if not result["operations"]:
            print("No changes to apply.")
            return 0

        # Show what will be applied
        _print_diff_result(result, verbose)

        if dry_run:
            print("\n(dry run - no changes applied)")
            return 0

        # Confirm if interactive
        if interactive and not unattended:
            confirm = input("\nApply these changes? [y/N] ")
            if confirm.lower() != "y":
                print("Aborted.")
                return 1

        # Apply migrations
        applier = create_applier(dialect)
        apply_result = await applier.apply(
            connection,
            result["operations"],
            result["execution_order"],
        )

        if apply_result["success"]:
            print(f"\n✓ Applied {apply_result['operations_applied']} operations successfully")

            # Update snapshot
            save_snapshot(schema_dir, target, dialect)
            if verbose:
                print(f"Updated snapshot at {schema_dir}/snapshot.toml")

            return 0
        else:
            print(f"\n✗ Migration failed: {apply_result['error']}")
            return 1

    finally:
        await _disconnect(connection, dialect)


async def cmd_snapshot(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: str,
    force: bool,
    verbose: bool,
) -> int:
    """
    Update schema snapshot from current database.

    Returns:
        0 if snapshot updated successfully
        1 if error or user declined
    """
    from declaro_persistum.inspector.protocol import create_inspector
    from declaro_persistum.loader import save_snapshot

    snapshot_path = Path(schema_dir) / "snapshot.toml"

    if snapshot_path.exists() and not force:
        confirm = input(f"Overwrite existing snapshot at {snapshot_path}? [y/N] ")
        if confirm.lower() != "y":
            print("Aborted.")
            return 1

    connection = await _connect(connection_string, dialect)
    try:
        inspector = create_inspector(dialect)
        actual = await inspector.introspect(connection)

        save_snapshot(schema_dir, actual, dialect)
        print(f"✓ Snapshot saved to {snapshot_path}")

        if verbose:
            print(f"  Tables: {len(actual)}")
            for table in sorted(actual.keys()):
                cols = len(actual[table].get("columns", {}))
                print(f"    {table}: {cols} columns")

        return 0

    finally:
        await _disconnect(connection, dialect)


def cmd_validate(
    *,
    schema_dir: str,
    strict: bool,
    verbose: bool,
) -> int:
    """
    Validate schema files without database connection.

    Returns:
        0 if valid
        1 if errors
        2 if warnings and --strict
    """
    from declaro_persistum.loader import load_schema
    from declaro_persistum.validator import validate_schema

    try:
        schema = load_schema(schema_dir)
    except Exception as e:
        print(f"✗ Failed to load schema: {e}")
        return 1

    warnings, errors = validate_schema(schema)

    if errors:
        print(f"✗ Schema validation failed with {len(errors)} error(s):")
        for error in errors:
            print(f"  - {error}")
        return 1

    if warnings:
        print(f"⚠ Schema has {len(warnings)} warning(s):")
        for warning in warnings:
            print(f"  - {warning}")
        if strict:
            return 2

    print(f"✓ Schema valid ({len(schema)} tables)")
    if verbose:
        for table in sorted(schema.keys()):
            cols = len(schema[table].get("columns", {}))
            print(f"  {table}: {cols} columns")

    return 0


async def cmd_migrate_remote(
    *,
    remote_url: str,
    auth_token: str | None,
    schema_path: str,
    dialect: str,
    expand_enums: bool,
    init: bool,
    dry_run: bool,
    no_fks: bool,
    verbose: bool,
) -> int:
    """
    Apply schema migrations directly to a remote Turso cloud DB.

    Bypasses the embedded replica sync engine (which cannot replicate DDL).
    Uses a temp local file with turso.aio.sync to pull/push cloud state.

    When --no-fks is set, FK constraints are stripped from the target schema
    before diffing.  This creates cloud tables without FK enforcement, avoiding
    sync engine replay-order violations.  FKs remain on local replicas.

    Safety: if the cloud DB appears empty (0 tables introspected) and the
    diff produces create_table ops, the --init flag is required.  This
    prevents accidental data loss when pull fails to sync cloud state.

    Returns:
        0 if migrations applied successfully (or no changes needed)
        1 if errors
    """
    from pathlib import Path

    from declaro_persistum.abstractions.enums import expand_schema_enums
    from declaro_persistum.applier.protocol import create_applier
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector.protocol import create_inspector
    from declaro_persistum.pydantic_loader import load_models_from_module

    schema_file = Path(schema_path)
    if not schema_file.exists():
        print(f"Error: Schema file not found: {schema_file}", file=sys.stderr)
        return 1

    # Load target schema from Pydantic models
    target_schema = load_models_from_module(schema_file)
    if not target_schema:
        print("No @table decorated models found in schema file.", file=sys.stderr)
        return 1

    if expand_enums:
        target_schema = expand_schema_enums(target_schema)

    if no_fks:
        from declaro_persistum.fk_ordering import strip_foreign_keys
        target_schema = strip_foreign_keys(target_schema)
        if verbose:
            print("Stripped FK constraints from target schema (--no-fks)")

    if verbose:
        print(f"Loaded {len(target_schema)} tables from {schema_file}")

    # Connect via sync driver with a temp local file.
    # pyturso has no direct-to-cloud connection — turso.aio.connect() is
    # local-only.  turso.aio.sync.connect() requires a local path but
    # syncs with cloud via push/pull.
    import os
    import tempfile

    import turso.aio.sync

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "migrate_remote.db")
        conn = await turso.aio.sync.connect(
            local_path,
            remote_url=remote_url,
            auth_token=auth_token,
        )

        try:
            # Pull current cloud state into temp file — try both methods
            try:
                await conn.sync()
            except Exception:
                await conn.pull()

            if verbose:
                print("Synced current schema from cloud")

            # Introspect cloud DB (via local replica)
            inspector = create_inspector(dialect)
            current_schema = await inspector.introspect(conn)

            if verbose:
                print(f"Introspected {len(current_schema)} tables from cloud DB")

            # Diff
            diff_result = diff(current_schema, target_schema)

            if not diff_result["operations"]:
                print("Cloud schema is up to date — no changes needed.")
                return 0

            # Safety guard: detect when pull failed silently
            create_ops = [
                op for op in diff_result["operations"]
                if op["op"] == "create_table"
            ]
            if len(current_schema) == 0 and create_ops:
                if not init:
                    print(
                        f"ABORT: Cloud DB appears empty (0 tables) but schema "
                        f"defines {len(target_schema)} tables.\n"
                        f"This would create all tables from scratch.\n\n"
                        f"If the cloud DB is genuinely empty (first-time setup), "
                        f"re-run with --init.\n"
                        f"If the cloud DB should have tables, the sync/pull may "
                        f"have failed — do NOT use --init or you will lose data.",
                        file=sys.stderr,
                    )
                    return 1
                print(f"--init: creating {len(create_ops)} tables on empty cloud DB")

            print(f"Found {len(diff_result['operations'])} schema differences:")
            for op in diff_result["operations"]:
                print(f"  - {op['op']} on {op.get('table', 'N/A')}")

            if diff_result.get("ambiguities"):
                print(f"\nAmbiguous changes detected: {diff_result['ambiguities']}")
                print("Aborting — resolve ambiguities first.")
                return 1

            if dry_run:
                print("\n(dry run — no changes applied)")
                return 0

            # Apply DDL locally
            applier = create_applier(dialect)
            result = await applier.apply(
                conn, diff_result["operations"], diff_result["execution_order"]
            )

            if result["success"]:
                # Push DDL changes to cloud
                await conn.push()
                print(f"\nApplied {result['operations_applied']} operations to cloud DB")
                if result.get("error"):
                    print(f"Warnings: {result['error']}")
                return 0
            else:
                print(f"\nMigration failed: {result.get('error', 'Unknown error')}", file=sys.stderr)
                return 1

        finally:
            await conn.close()


async def cmd_generate(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: str,
    output: str | None,
    force: bool,
) -> int:
    """
    Generate SQL without executing.

    Returns:
        0 if SQL generated successfully
        1 if errors
    """
    from declaro_persistum.applier.protocol import create_applier
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector.protocol import create_inspector
    from declaro_persistum.loader import load_decisions, load_schema, load_snapshot

    target = load_schema(schema_dir)
    connection = await _connect(connection_string, dialect)

    try:
        inspector = create_inspector(dialect)
        actual = await inspector.introspect(connection)

        # Check drift
        snapshot_path = Path(schema_dir) / "snapshot.toml"
        if snapshot_path.exists() and not force:
            expected = load_snapshot(schema_dir)
            drift = _check_drift(actual, expected)
            if drift:
                _print_drift_error(drift)
                return 2

        decisions = load_decisions(schema_dir)
        result = diff(actual, target, decisions=decisions)

        if result["ambiguities"]:
            raise AmbiguityError(result["ambiguities"])

        if not result["operations"]:
            print("-- No changes needed", file=sys.stderr)
            return 0

        # Generate SQL
        applier = create_applier(dialect)
        sql_statements = applier.generate_sql(
            result["operations"],
            result["execution_order"],
        )

        # Output
        sql_output = "\n".join(sql_statements) + "\n"

        if output:
            Path(output).write_text(sql_output)
            print(f"✓ SQL written to {output}", file=sys.stderr)
        else:
            print(sql_output)

        return 0

    finally:
        await _disconnect(connection, dialect)


# Helper functions


async def _connect(connection_string: str, dialect: str) -> Any:
    """Create database connection."""
    if dialect == "postgresql":
        import asyncpg

        return await asyncpg.connect(connection_string)
    elif dialect == "sqlite":
        import aiosqlite

        # Extract path from sqlite:///path
        path = connection_string.replace("sqlite:///", "")
        return await aiosqlite.connect(path)
    elif dialect == "turso":
        import turso.aio

        return await turso.aio.connect(connection_string)
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")


async def _disconnect(connection: Any, dialect: str) -> None:
    """Close database connection."""
    if dialect == "postgresql" or dialect == "sqlite" or dialect == "turso":
        await connection.close()


def _check_drift(actual: Schema, expected: Schema) -> list[dict[str, str]]:
    """Check for drift between actual and expected schemas."""
    differences: list[dict[str, str]] = []

    actual_tables = set(actual.keys())
    expected_tables = set(expected.keys())

    # Tables in DB but not in snapshot
    for table in actual_tables - expected_tables:
        differences.append(
            {
                "symbol": "+",
                "description": f"Table '{table}' exists in DB but not in snapshot",
            }
        )

    # Tables in snapshot but not in DB
    for table in expected_tables - actual_tables:
        differences.append(
            {
                "symbol": "-",
                "description": f"Table '{table}' in snapshot but not in DB",
            }
        )

    # Column differences in common tables
    for table in actual_tables & expected_tables:
        actual_cols = set(actual[table].get("columns", {}).keys())
        expected_cols = set(expected[table].get("columns", {}).keys())

        for col in actual_cols - expected_cols:
            differences.append(
                {
                    "symbol": "+",
                    "description": f"Column '{table}.{col}' exists in DB but not in snapshot",
                }
            )

        for col in expected_cols - actual_cols:
            differences.append(
                {
                    "symbol": "-",
                    "description": f"Column '{table}.{col}' in snapshot but not in DB",
                }
            )

    return differences


def _print_drift_error(differences: list[dict[str, str]]) -> None:
    """Print drift error message."""
    print("⚠ Database schema has drifted from expected state\n")
    print("  Differences detected:\n")
    for diff in differences:
        print(f"    {diff['symbol']} {diff['description']}")
    print("\n  Options:")
    print("    1. Run 'declaro snapshot' to update snapshot to current DB state")
    print("    2. Run with --force to proceed anyway")
    print("    3. Manually reconcile the differences")


def _print_diff_result(result: DiffResult, verbose: bool) -> None:
    """Print diff result to console."""
    ops = result["operations"]
    order = result["execution_order"]

    if not ops:
        print("No changes needed.")
        return

    print(f"\nProposed changes ({len(ops)} operations):\n")

    for i, op_idx in enumerate(order):
        op = ops[op_idx]
        op_type = op["op"]
        table = op["table"]
        details = op["details"]

        # Format operation nicely
        if op_type == "create_table":
            cols = details.get("columns", {})
            print(f"  {i + 1}. CREATE TABLE {table} ({len(cols)} columns)")
        elif op_type == "drop_table":
            print(f"  {i + 1}. DROP TABLE {table}")
        elif op_type == "rename_table":
            print(f"  {i + 1}. RENAME TABLE {table} -> {details['new_name']}")
        elif op_type == "add_column":
            col = details["column"]
            col_def = details["definition"]
            print(f"  {i + 1}. ADD COLUMN {table}.{col} ({col_def.get('type', '?')})")
        elif op_type == "drop_column":
            print(f"  {i + 1}. DROP COLUMN {table}.{details['column']}")
        elif op_type == "rename_column":
            print(
                f"  {i + 1}. RENAME COLUMN {table}.{details['from_column']} -> {details['to_column']}"
            )
        elif op_type == "alter_column":
            changes = details.get("changes", {})
            change_desc = ", ".join(changes.keys())
            print(f"  {i + 1}. ALTER COLUMN {table}.{details['column']} ({change_desc})")
        elif op_type == "add_index":
            idx = details["index"]
            cols = details["definition"].get("columns", [])
            print(f"  {i + 1}. CREATE INDEX {idx} ON {table} ({', '.join(cols)})")
        elif op_type == "drop_index":
            print(f"  {i + 1}. DROP INDEX {details['index']}")
        elif op_type == "add_foreign_key":
            col = details["column"]
            ref = details["references"]
            print(f"  {i + 1}. ADD FOREIGN KEY {table}.{col} -> {ref}")
        elif op_type == "drop_foreign_key":
            col = details["column"]
            print(f"  {i + 1}. DROP FOREIGN KEY {table}.{col}")
        else:
            print(f"  {i + 1}. {op_type.upper()} on {table}")

        if verbose and details:
            for key, value in details.items():
                if key not in ("column", "index", "constraint", "new_name"):
                    print(f"       {key}: {value}")


async def _resolve_ambiguities_interactive(
    ambiguities: list[Ambiguity],
    schema_dir: str,
) -> dict[str, Decision]:
    """Interactively resolve ambiguities and save decisions."""
    from declaro_persistum.loader import save_decisions

    decisions: dict[str, Decision] = {}

    print(f"\n{len(ambiguities)} ambiguous change(s) detected:\n")

    for i, amb in enumerate(ambiguities):
        print(f"{i + 1}. {amb['message']}")

        if amb["type"] == "possible_rename":
            print("   [1] Rename (preserves data)")
            print("   [2] Drop + Add (loses data)")

            choice = input("   Choice [1/2]: ").strip()

            from datetime import datetime

            key = f"{amb['table']}_{amb.get('from_column', '')}"
            now = datetime.now(UTC).isoformat()
            if choice == "1":
                decisions[key] = {
                    "type": "rename",
                    "table": amb["table"],
                    "from_column": amb.get("from_column"),
                    "to_column": amb.get("to_column"),
                    "column": None,
                    "decided_at": now,
                }
            else:
                decisions[key] = {
                    "type": "drop",
                    "table": amb["table"],
                    "column": amb.get("from_column"),
                    "from_column": None,
                    "to_column": None,
                    "decided_at": now,
                }

        elif amb["type"] == "type_change":
            print("   [1] Proceed with type change")
            print("   [2] Abort")

            choice = input("   Choice [1/2]: ").strip()

            if choice == "2":
                raise KeyboardInterrupt("User aborted")

        print()

    # Save decisions
    if decisions:
        save_decisions(schema_dir, decisions)
        print(f"Decisions saved to {schema_dir}/migrations/pending.toml")

    return decisions
