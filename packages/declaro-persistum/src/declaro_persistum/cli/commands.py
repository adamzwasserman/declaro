"""
CLI command implementations.

Each command is an async function that returns an exit code.
"""

import sys
from pathlib import Path

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.cli.parts import (
    _ANSWER_AMBIGUITIES as _ANSWER_AMBIGUITIES,
)
from declaro_persistum.cli.parts import (
    _DECIDE as _DECIDE,
)
from declaro_persistum.cli.parts import (
    _DESCRIBE as _DESCRIBE,
)
from declaro_persistum.cli.parts import (
    _RENAME_OR_DROP as _RENAME_OR_DROP,
)
from declaro_persistum.cli.parts import (
    _WHEN_A_SQLITE_URL as _WHEN_A_SQLITE_URL,
)
from declaro_persistum.cli.parts import (
    _WHEN_A_TERMINAL as _WHEN_A_TERMINAL,
)
from declaro_persistum.cli.parts import (
    _WHEN_ABORTED as _WHEN_ABORTED,
)
from declaro_persistum.cli.parts import (
    _WHEN_ANY_DECISION as _WHEN_ANY_DECISION,
)
from declaro_persistum.cli.parts import (
    _WHEN_ANY_OPERATION as _WHEN_ANY_OPERATION,
)
from declaro_persistum.cli.parts import (
    _WHEN_ASKING as _WHEN_ASKING,
)
from declaro_persistum.cli.parts import (
    _WHEN_CONFIRMED as _WHEN_CONFIRMED,
)
from declaro_persistum.cli.parts import (
    _WHEN_DRIFTED as _WHEN_DRIFTED,
)
from declaro_persistum.cli.parts import (
    _WHEN_ENUMS_EXPANDED as _WHEN_ENUMS_EXPANDED,
)
from declaro_persistum.cli.parts import (
    _WHEN_ERRORS as _WHEN_ERRORS,
)
from declaro_persistum.cli.parts import (
    _WHEN_FKS_STRIPPED as _WHEN_FKS_STRIPPED,
)
from declaro_persistum.cli.parts import (
    _WHEN_MODE_GIVEN as _WHEN_MODE_GIVEN,
)
from declaro_persistum.cli.parts import (
    _WHEN_OUTPUT_NAMED as _WHEN_OUTPUT_NAMED,
)
from declaro_persistum.cli.parts import (
    _WHEN_OVERWRITING as _WHEN_OVERWRITING,
)
from declaro_persistum.cli.parts import (
    _WHEN_PUSHED as _WHEN_PUSHED,
)
from declaro_persistum.cli.parts import (
    _WHEN_SETTLED as _WHEN_SETTLED,
)
from declaro_persistum.cli.parts import (
    _WHEN_SNAPSHOT_READ as _WHEN_SNAPSHOT_READ,
)
from declaro_persistum.cli.parts import (
    _WHEN_UNANSWERABLE as _WHEN_UNANSWERABLE,
)
from declaro_persistum.cli.parts import (
    _WHEN_WANTED as _WHEN_WANTED,
)
from declaro_persistum.cli.parts import (
    _WHEN_WARNINGS as _WHEN_WARNINGS,
)
from declaro_persistum.cli.parts import (
    _WHEN_WARNINGS_ARE_FATAL as _WHEN_WARNINGS_ARE_FATAL,
)

# The pieces these six commands are built from. They live in
# `parts.py` and are re-exported here so no caller had to change.
from declaro_persistum.cli.parts import (  # noqa: F401
    CONNECTORS as CONNECTORS,
)
from declaro_persistum.cli.parts import (
    SQLITE_ABSOLUTE as SQLITE_ABSOLUTE,
)
from declaro_persistum.cli.parts import (
    SQLITE_RELATIVE as SQLITE_RELATIVE,
)
from declaro_persistum.cli.parts import (
    Rest as Rest,
)
from declaro_persistum.cli.parts import (
    Settle as Settle,
)
from declaro_persistum.cli.parts import (
    _a_drop as _a_drop,
)
from declaro_persistum.cli.parts import (
    _a_rename as _a_rename,
)
from declaro_persistum.cli.parts import (
    _abort_the_run as _abort_the_run,
)
from declaro_persistum.cli.parts import (
    _aborted as _aborted,
)
from declaro_persistum.cli.parts import (
    _ask_about as _ask_about,
)
from declaro_persistum.cli.parts import (
    _ask_about_overwriting as _ask_about_overwriting,
)
from declaro_persistum.cli.parts import (
    _ask_before_applying as _ask_before_applying,
)
from declaro_persistum.cli.parts import (
    _ask_the_operator as _ask_the_operator,
)
from declaro_persistum.cli.parts import (
    _carry_on as _carry_on,
)
from declaro_persistum.cli.parts import (
    _check_drift as _check_drift,
)
from declaro_persistum.cli.parts import (
    _column_drift as _column_drift,
)
from declaro_persistum.cli.parts import (
    _confirm_the_overwrite as _confirm_the_overwrite,
)
from declaro_persistum.cli.parts import (
    _confirmed as _confirmed,
)
from declaro_persistum.cli.parts import (
    _connect as _connect,
)
from declaro_persistum.cli.parts import (
    _connect_postgresql as _connect_postgresql,
)
from declaro_persistum.cli.parts import (
    _connect_sqlite as _connect_sqlite,
)
from declaro_persistum.cli.parts import (
    _connect_turso as _connect_turso,
)
from declaro_persistum.cli.parts import (
    _decide_a_rename as _decide_a_rename,
)
from declaro_persistum.cli.parts import (
    _decide_a_type_change as _decide_a_type_change,
)
from declaro_persistum.cli.parts import (
    _describe as _describe,
)
from declaro_persistum.cli.parts import (
    _disconnect as _disconnect,
)
from declaro_persistum.cli.parts import (
    _drift as _drift,
)
from declaro_persistum.cli.parts import (
    _drift_against_the_snapshot as _drift_against_the_snapshot,
)
from declaro_persistum.cli.parts import (
    _go_ahead_with_the_change as _go_ahead_with_the_change,
)
from declaro_persistum.cli.parts import (
    _leave_them_alone as _leave_them_alone,
)
from declaro_persistum.cli.parts import (
    _modes as _modes,
)
from declaro_persistum.cli.parts import (
    _no_snapshot_to_compare as _no_snapshot_to_compare,
)
from declaro_persistum.cli.parts import (
    _not_a_sqlite_url as _not_a_sqlite_url,
)
from declaro_persistum.cli.parts import (
    _not_pushed_to_the_cloud as _not_pushed_to_the_cloud,
)
from declaro_persistum.cli.parts import (
    _note as _note,
)
from declaro_persistum.cli.parts import (
    _nothing_to_overwrite as _nothing_to_overwrite,
)
from declaro_persistum.cli.parts import (
    _nothing_unanswerable as _nothing_unanswerable,
)
from declaro_persistum.cli.parts import (
    _operation_lines as _operation_lines,
)
from declaro_persistum.cli.parts import (
    _past_the_drift_check as _past_the_drift_check,
)
from declaro_persistum.cli.parts import (
    _print_diff_result as _print_diff_result,
)
from declaro_persistum.cli.parts import (
    _print_drift_error as _print_drift_error,
)
from declaro_persistum.cli.parts import (
    _print_it as _print_it,
)
from declaro_persistum.cli.parts import (
    _print_no_changes as _print_no_changes,
)
from declaro_persistum.cli.parts import (
    _print_no_warnings as _print_no_warnings,
)
from declaro_persistum.cli.parts import (
    _print_nothing as _print_nothing,
)
from declaro_persistum.cli.parts import (
    _print_the_operations as _print_the_operations,
)
from declaro_persistum.cli.parts import (
    _print_the_warnings as _print_the_warnings,
)
from declaro_persistum.cli.parts import (
    _pushed_to_the_cloud as _pushed_to_the_cloud,
)
from declaro_persistum.cli.parts import (
    _refusal_lines as _refusal_lines,
)
from declaro_persistum.cli.parts import (
    _refuse_the_ambiguities as _refuse_the_ambiguities,
)
from declaro_persistum.cli.parts import (
    _refuse_the_unanswerable as _refuse_the_unanswerable,
)
from declaro_persistum.cli.parts import (
    _refuse_without_a_terminal as _refuse_without_a_terminal,
)
from declaro_persistum.cli.parts import (
    _report_the_errors as _report_the_errors,
)
from declaro_persistum.cli.parts import (
    _report_the_warnings as _report_the_warnings,
)
from declaro_persistum.cli.parts import (
    _resolve_ambiguities_interactive as _resolve_ambiguities_interactive,
)
from declaro_persistum.cli.parts import (
    _save_nothing as _save_nothing,
)
from declaro_persistum.cli.parts import (
    _save_the_decisions as _save_the_decisions,
)
from declaro_persistum.cli.parts import (
    _schema_as_written as _schema_as_written,
)
from declaro_persistum.cli.parts import (
    _schema_keeping_its_foreign_keys as _schema_keeping_its_foreign_keys,
)
from declaro_persistum.cli.parts import (
    _schema_passes as _schema_passes,
)
from declaro_persistum.cli.parts import (
    _schema_with_enums_expanded as _schema_with_enums_expanded,
)
from declaro_persistum.cli.parts import (
    _schema_without_foreign_keys as _schema_without_foreign_keys,
)
from declaro_persistum.cli.parts import (
    _settled as _settled,
)
from declaro_persistum.cli.parts import (
    _sql_to_a_file as _sql_to_a_file,
)
from declaro_persistum.cli.parts import (
    _sql_to_stdout as _sql_to_stdout,
)
from declaro_persistum.cli.parts import (
    _stop_at_drift as _stop_at_drift,
)
from declaro_persistum.cli.parts import (
    _that_is_the_answer as _that_is_the_answer,
)
from declaro_persistum.cli.parts import (
    _the_filename as _the_filename,
)
from declaro_persistum.cli.parts import (
    _the_modes_asked_for as _the_modes_asked_for,
)
from declaro_persistum.cli.parts import (
    _the_modes_the_terminal_implies as _the_modes_the_terminal_implies,
)
from declaro_persistum.cli.parts import (
    _warnings_are_fatal as _warnings_are_fatal,
)
from declaro_persistum.cli.parts import (
    sqlite_path as sqlite_path,
)
from declaro_persistum.types import (
    NOTHING,
    Dialect,
    Schema,
)


@helper
async def cmd_diff(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: Dialect,
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
    from declaro_persistum.inspector import introspect
    from declaro_persistum.loader import load_decisions, load_schema

    # Determine interactive mode
    interactive, unattended = _modes(
        interactive, unattended, sys.stdin.isatty() and sys.stdout.isatty()
    )

    _note(verbose, f"Loading schema from {schema_dir}...", sys.stdout)

    # Load target schema from TOML files
    target = load_schema(schema_dir)

    _note(verbose, f"Loaded {len(target)} tables from schema files", sys.stdout)

    # Connect and introspect database
    connection = await _connect(connection_string, dialect)
    try:
        actual = await introspect(connection, dialect)

        _note(verbose, f"Introspected {len(actual)} tables from database", sys.stdout)

        @helper
        async def show_the_diff() -> int:
            """Diff, settle anything ambiguous, and print what would happen."""
            result = diff(actual, target, dialect=dialect, decisions=load_decisions(schema_dir))
            _print_diff_result(
                await _settled(
                    result,
                    schema_dir,
                    unattended,
                    interactive,
                    lambda made: diff(actual, target, dialect=dialect, decisions=made),
                ),
                verbose,
            )
            return 0

        # Load the expected snapshot and check for drift
        drift = _drift(actual, schema_dir, force)
        return await _WHEN_DRIFTED[bool(drift)](drift, show_the_diff)

    finally:
        await _disconnect(connection, dialect)


@boundary
async def cmd_apply(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: Dialect,
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
    from declaro_persistum.applier import apply as apply_operations
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector import introspect
    from declaro_persistum.loader import (
        clear_decisions,
        load_decisions,
        load_schema,
        save_snapshot,
    )
    from declaro_persistum.validator import validate_schema

    # Determine interactive mode
    interactive, unattended = _modes(
        interactive, unattended, sys.stdin.isatty() and sys.stdout.isatty()
    )

    # Load schema and connect
    _note(verbose, f"Loading schema from {schema_dir}...", sys.stderr)

    target = load_schema(schema_dir)

    # THE VALIDATOR IS ASKED BEFORE ANY DDL IS WRITTEN, and was asked nowhere
    # in this command before. `validate_schema` was called from `cmd_validate`
    # and from no other CLI path, so `declaro apply` wrote DDL to a live
    # database without consulting a single rule.
    #
    # `apply_migrations_async` was given this check the same day and the CLI
    # does not go through it: it diffs and applies directly. So the command an
    # operator runs against production was the one still unchecked.
    #
    # A foreign key pointing at a table nobody declared is the clearest case.
    # The validator catches it by comparing the schema against itself; SQLite
    # accepts the DDL because it does not check a reference at creation time,
    # so the command printed its operations, applied them, exited 0, and left
    # a column pointing at nothing.
    #
    # Errors only. An unknown type and a circular reference are warnings,
    # because the first may be a custom type or an enum and the second may
    # want deferrable constraints, and refusing those would reject schemas
    # that work.
    invalid = validate_schema(target)["errors"]

    @boundary
    async def against_the_database() -> int:
        """The schema passed the validator, so the database is opened."""
        _note(verbose, f"Loaded {len(target)} tables from schema files", sys.stderr)

        connection = await _connect(connection_string, dialect)

        try:
            actual = await introspect(connection, dialect)

            _note(
                verbose,
                f"Introspected {len(actual)} tables from database",
                sys.stderr,
            )

            @boundary
            async def apply_the_diff() -> int:
                """Diff, settle anything ambiguous, and run what comes out."""
                result = await _settled(
                    diff(
                        actual,
                        target,
                        dialect=dialect,
                        decisions=load_decisions(schema_dir),
                    ),
                    schema_dir,
                    unattended,
                    interactive,
                    lambda made: diff(actual, target, dialect=dialect, decisions=made),
                )

                @boundary
                async def run_the_operations() -> int:
                    """Write the DDL, then say what happened."""
                    apply_result = await apply_operations(
                        connection,
                        result["operations"],
                        result["execution_order"],
                        dialect,
                        dry_run=False,
                    )

                    @boundary
                    def applied() -> int:
                        """Every operation ran, so the record is brought up to date."""
                        print(
                            f"\n✓ Applied {apply_result['operations_applied']} "
                            f"operations successfully"
                        )

                        # Update snapshot
                        save_snapshot(schema_dir, target, dialect, applied_by=NOTHING)
                        _note(
                            verbose,
                            f"Updated snapshot at {schema_dir}/snapshot.toml",
                            sys.stdout,
                        )

                        # SPEND THE DECISIONS. They answered questions this
                        # apply asked, and keeping them means the next apply
                        # never asks again: a decision keyed on a column name
                        # goes on answering for any later rename that happens
                        # to match. Nothing cleared them before, so
                        # `pending.toml` survived every successful migration.
                        clear_decisions(schema_dir)
                        _note(verbose, "Cleared pending decisions", sys.stdout)

                        return 0

                    @boundary
                    def not_applied() -> int:
                        """Something refused, and the applier said what."""
                        print(f"\n✗ Migration failed: {apply_result['error']}")
                        return 1

                    # Whether the applier got through the whole plan.
                    return {True: applied, False: not_applied}[apply_result["success"]]()

                @helper
                async def show_then_run() -> int:
                    """Show the plan, then dry-run, ask or run it."""
                    _print_diff_result(result, verbose)
                    return await _WHEN_SETTLED[dry_run](
                        "\n(dry run - no changes applied)",
                        sys.stdout,
                        0,
                        lambda: _WHEN_ASKING[interactive and not unattended](run_the_operations),
                    )

                return await _WHEN_SETTLED[not result["operations"]](
                    "No changes to apply.", sys.stdout, 0, show_then_run
                )

            # Check drift
            drift = _drift(actual, schema_dir, force)
            return await _WHEN_DRIFTED[bool(drift)](drift, apply_the_diff)

        finally:
            await _disconnect(connection, dialect)

    return await _WHEN_SETTLED[bool(invalid)](
        _refusal_lines("Schema is not valid, and nothing has been applied:", invalid),
        sys.stderr,
        1,
        against_the_database,
    )


@boundary
async def cmd_snapshot(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: Dialect,
    force: bool,
    verbose: bool,
) -> int:
    """
    Update schema snapshot from current database.

    Returns:
        0 if snapshot updated successfully
        1 if error or user declined
    """
    from declaro_persistum.inspector import introspect
    from declaro_persistum.loader import save_snapshot

    snapshot_path = Path(schema_dir) / "snapshot.toml"

    @boundary
    async def write_the_snapshot() -> int:
        """Read the database and write down what is there."""
        connection = await _connect(connection_string, dialect)
        try:
            actual = await introspect(connection, dialect)

            save_snapshot(schema_dir, actual, dialect, applied_by=NOTHING)
            print(f"✓ Snapshot saved to {snapshot_path}")

            _note(
                verbose,
                "\n".join(
                    [f"  Tables: {len(actual)}"]
                    + [
                        f"    {table}: {len(actual[table].get('columns', {}))} columns"
                        for table in sorted(actual.keys())
                    ]
                ),
                sys.stdout,
            )

            return 0

        finally:
            await _disconnect(connection, dialect)

    # BOTH READS HAPPEN HERE, at the command, which is the edge. Whether a
    # snapshot is in the way and whether anybody is at the terminal are both
    # facts about the world, and the dispatcher below only decides.
    return await _WHEN_OVERWRITING[snapshot_path.exists() and not force](
        snapshot_path, write_the_snapshot, sys.stdin.isatty()
    )


@boundary
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

    result = validate_schema(schema)
    warnings, errors = result["warnings"], result["errors"]

    return _WHEN_ERRORS[bool(errors)](schema, errors, warnings, strict, verbose)


@boundary
async def cmd_migrate_remote(
    *,
    remote_url: str,
    auth_token: str | None,
    schema_path: str,
    dialect: Dialect,
    expand_enums: bool,
    init: bool,
    dry_run: bool,
    no_fks: bool,
    verbose: bool,
) -> int:
    """
    Apply schema migrations directly to a remote Turso cloud DB.

    Bypasses the embedded replica replication engine (which cannot replicate DDL).
    Uses a temp local file with turso.aio.sync to pull/push cloud state.

    When --no-fks is set, FK constraints are stripped from the target schema
    before diffing.  This creates cloud tables without FK enforcement, avoiding
    replication engine replay-order violations.  FKs remain on local replicas.

    Safety: if the cloud DB appears empty (0 tables introspected) and the
    diff produces create_table ops, the --init flag is required.  This
    prevents accidental data loss when pull fails to sync cloud state.

    Returns:
        0 if migrations applied successfully (or no changes needed)
        1 if errors
    """
    from pathlib import Path

    from declaro_persistum.applier import apply as apply_operations
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector import introspect
    from declaro_persistum.pydantic_loader import load_models_from_module

    schema_file = Path(schema_path)

    # ASKED BEFORE THE NETWORK, WHICH IS THE POINT OF PUTTING IT HERE. This
    # command writes to a production cloud database, and it reached that
    # database before checking the schema at all: a dangling reference was
    # answered by `sync engine operation failed ... status=404`, after an auth
    # token had been used, rather than by the validator on the developer's
    # machine.
    #
    # It is the last of the three paths that migrate. `apply_migrations_async`
    # and `cmd_apply` were given the same check the same day.
    #
    # AFTER the enum expansion and the FK strip on purpose: `--no-fks` removes
    # references, so validating before it would refuse a schema the command
    # was told not to send those references for.
    #
    # Errors only, as elsewhere. An unknown type is a warning because it may
    # be a custom type or an enum.
    from declaro_persistum.validator import validate_schema

    @boundary
    async def with_the_models(loaded: Schema) -> int:
        """The file held tables, so they are prepared and then checked."""
        target_schema = _WHEN_FKS_STRIPPED[no_fks](
            _WHEN_ENUMS_EXPANDED[expand_enums](loaded), verbose
        )
        invalid = validate_schema(target_schema)["errors"]

        @boundary
        async def against_the_cloud() -> int:
            """The schema passed the validator, so the primary is reached."""
            _note(
                verbose,
                f"Loaded {len(target_schema)} tables from {schema_file}",
                sys.stdout,
            )

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
                    # PULL UNTIL THE COPY IS LEVEL, not once, and never via
                    # `sync`.
                    #
                    # This was `try: await conn.sync() except Exception: await
                    # conn.pull()`, which had two defects in four lines.
                    #
                    # `ConnectionSync.sync` does not exist on pyturso. Measured
                    # 2026-08-17: `hasattr(ConnectionSync, "sync")` is False, so
                    # the first call raised AttributeError on every run and the
                    # `except` did the real work. An AttributeError means WE
                    # called something wrongly, and it was being used as feature
                    # detection.
                    #
                    # And one `pull()` fetches ONE batch. `_pull_until_level`
                    # exists because of that: a replica showed nothing after
                    # connect, gained the missing table on pull #1, and reported
                    # empty on pull #2. Pulling once here left the copy possibly
                    # behind, so the schema this command introspects could be
                    # missing tables the primary has, and the migration would
                    # propose creating them.
                    from declaro_persistum.turso_database import _pull_until_level

                    await _pull_until_level(conn)

                    _note(verbose, "Replicated current schema from cloud", sys.stdout)

                    # Introspect cloud DB (via local replica)
                    current_schema = await introspect(conn, dialect)

                    _note(
                        verbose,
                        f"Introspected {len(current_schema)} tables from cloud DB",
                        sys.stdout,
                    )

                    # Diff
                    diff_result = diff(
                        current_schema,
                        target_schema,
                        dialect=dialect,
                        decisions=NOTHING,
                    )

                    # Safety guard: detect when pull failed silently
                    create_ops = [
                        op for op in diff_result["operations"] if op["op"] == "create_table"
                    ]
                    # A cloud database that reads as empty while the schema
                    # declares tables is the shape a failed pull leaves behind,
                    # so creating them all is either the first deploy or data
                    # loss, and --init is how the operator says which.
                    from_scratch = len(current_schema) == 0 and bool(create_ops)

                    @helper
                    async def apply_and_push() -> int:
                        """Run the DDL on the local copy, then send it up."""
                        result = await apply_operations(
                            conn,
                            diff_result["operations"],
                            diff_result["execution_order"],
                            dialect,
                            dry_run=False,
                        )
                        return await _WHEN_PUSHED[result["success"]](conn, result)

                    @boundary
                    async def show_the_differences() -> int:
                        """List what will change, then refuse, rehearse or run."""
                        _note(
                            from_scratch and init,
                            f"--init: creating {len(create_ops)} tables on empty cloud DB",
                            sys.stdout,
                        )
                        print(f"Found {len(diff_result['operations'])} schema differences:")
                        print(
                            "\n".join(
                                f"  - {op['op']} on {op.get('table', 'N/A')}"
                                for op in diff_result["operations"]
                            )
                        )
                        return await _WHEN_SETTLED[bool(diff_result.get("ambiguities"))](
                            f"\nAmbiguous changes detected: "
                            f"{diff_result.get('ambiguities')}\n"
                            f"Aborting — resolve ambiguities first.",
                            sys.stdout,
                            1,
                            lambda: _WHEN_SETTLED[dry_run](
                                "\n(dry run — no changes applied)",
                                sys.stdout,
                                0,
                                apply_and_push,
                            ),
                        )

                    @helper
                    async def migrate_them() -> int:
                        """There are operations, so the guard is asked first."""
                        return await _WHEN_SETTLED[from_scratch and not init](
                            f"ABORT: Cloud DB appears empty (0 tables) but schema "
                            f"defines {len(target_schema)} tables.\n"
                            f"This would create all tables from scratch.\n\n"
                            f"If the cloud DB is genuinely empty (first-time "
                            f"setup), re-run with --init.\n"
                            f"If the cloud DB should have tables, the sync/pull "
                            f"may have failed — do NOT use --init or you will "
                            f"lose data.",
                            sys.stderr,
                            1,
                            show_the_differences,
                        )

                    return await _WHEN_SETTLED[not diff_result["operations"]](
                        "Cloud schema is up to date — no changes needed.",
                        sys.stdout,
                        0,
                        migrate_them,
                    )

                finally:
                    await conn.close()

        return await _WHEN_SETTLED[bool(invalid)](
            _refusal_lines(
                "Schema is not valid, and nothing has been sent to the remote:",
                invalid,
            ),
            sys.stderr,
            1,
            against_the_cloud,
        )

    @helper
    async def with_the_schema_file() -> int:
        """The file is there, so its @table models are read off it.

        The read sits behind the existence check because a missing file has to
        be reported rather than raised.
        """
        loaded = load_models_from_module(schema_file)
        return await _WHEN_SETTLED[not loaded](
            "No @table decorated models found in schema file.",
            sys.stderr,
            1,
            lambda: with_the_models(loaded),
        )

    return await _WHEN_SETTLED[not schema_file.exists()](
        f"Error: Schema file not found: {schema_file}",
        sys.stderr,
        1,
        with_the_schema_file,
    )


@helper
async def cmd_generate(
    *,
    connection_string: str,
    schema_dir: str,
    dialect: Dialect,
    output: str | None,
    force: bool,
    verbose: bool,
) -> int:
    """
    Generate SQL without executing.

    Returns:
        0 if SQL generated successfully
        1 if errors
    """
    from declaro_persistum.applier import generate_sql
    from declaro_persistum.differ import diff
    from declaro_persistum.inspector import introspect
    from declaro_persistum.loader import load_decisions, load_schema

    _note(verbose, f"Loading schema from {schema_dir}...", sys.stderr)

    target = load_schema(schema_dir)

    _note(verbose, f"Loaded {len(target)} tables from schema files", sys.stderr)

    connection = await _connect(connection_string, dialect)

    try:
        actual = await introspect(connection, dialect)

        _note(verbose, f"Introspected {len(actual)} tables from database", sys.stderr)

        @helper
        async def write_the_sql() -> int:
            """Diff, then render the operations as statements."""
            result = diff(actual, target, dialect=dialect, decisions=load_decisions(schema_dir))

            # Nothing here can answer an ambiguity, so the command refuses.
            _WHEN_UNANSWERABLE[bool(result["ambiguities"])](result["ambiguities"])

            @helper
            async def render() -> int:
                """There are operations, so they become SQL."""
                sql_statements = generate_sql(
                    result["operations"],
                    result["execution_order"],
                    dialect,
                )

                # Output
                _note(
                    verbose,
                    f"Generated {len(sql_statements)} statements",
                    sys.stderr,
                )

                return _WHEN_OUTPUT_NAMED[bool(output)]("\n".join(sql_statements) + "\n", output)

            return await _WHEN_SETTLED[not result["operations"]](
                "-- No changes needed", sys.stderr, 0, render
            )

        # Check drift
        drift = _drift(actual, schema_dir, force)
        return await _WHEN_DRIFTED[bool(drift)](drift, write_the_sql)

    finally:
        await _disconnect(connection, dialect)
