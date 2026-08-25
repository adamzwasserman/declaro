"""
CLI entry point for declaro_persistum.

Usage:
    declaro <command> [options]

Commands:
    diff        Compare target schema to database, show proposed operations
    apply       Apply pending migrations to database
    snapshot    Update schema snapshot from current database
    validate    Validate schema files without database connection
    generate    Generate SQL without executing
"""

import argparse
import asyncio
import os
import sys
from collections.abc import Callable, Sequence
from typing import cast

from declaro_persistum import __version__
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.cli.commands import (
    cmd_apply,
    cmd_diff,
    cmd_generate,
    cmd_migrate_remote,
    cmd_snapshot,
    cmd_validate,
)
from declaro_persistum.types import NOTHING, Dialect, Nothing, resolved


@helper
def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="declaro",
        description="Pure functional SQL library with declarative schema migrations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Interactive diff and apply
    declaro diff -c postgresql://localhost/mydb
    declaro apply -c postgresql://localhost/mydb

    # CI pipeline (unattended)
    declaro diff --unattended -c $DATABASE_URL

    # Generate SQL for review
    declaro generate -c postgresql://localhost/mydb > migration.sql

    # Validate schema files
    declaro validate -s ./schema
""",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"declaro_persistum {__version__}",
    )

    # Global options
    parser.add_argument(
        "-c",
        "--connection",
        metavar="URL",
        help="Database connection string (or DECLARO_DATABASE_URL env)",
    )
    parser.add_argument(
        "-s",
        "--schema-dir",
        metavar="DIR",
        default="./schema",
        help="Schema directory (default: ./schema)",
    )
    parser.add_argument(
        "-d",
        "--dialect",
        choices=["postgresql", "sqlite", "turso"],
        help="Force dialect (auto-detected from connection if not specified)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )

    # Subcommands
    subparsers = parser.add_subparsers(
        dest="command",
        title="commands",
        metavar="<command>",
    )

    # diff command
    diff_parser = subparsers.add_parser(
        "diff",
        help="Compare target schema to database, show proposed operations",
    )
    diff_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Prompt for ambiguity resolution (default for TTY)",
    )
    diff_parser.add_argument(
        "-u",
        "--unattended",
        action="store_true",
        help="Fail on ambiguities (default for non-TTY, CI)",
    )
    diff_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip drift detection warning",
    )

    # apply command
    apply_parser = subparsers.add_parser(
        "apply",
        help="Apply pending migrations to database",
    )
    apply_parser.add_argument(
        "-i",
        "--interactive",
        action="store_true",
        help="Prompt for confirmation (default for TTY)",
    )
    apply_parser.add_argument(
        "-u",
        "--unattended",
        action="store_true",
        help="Apply without confirmation (for CI)",
    )
    apply_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show operations without executing",
    )
    apply_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip drift detection warning",
    )

    # snapshot command
    snapshot_parser = subparsers.add_parser(
        "snapshot",
        help="Update schema snapshot from current database",
    )
    snapshot_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing snapshot without confirmation",
    )

    # validate command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate schema files without database connection",
    )
    validate_parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail on warnings (not just errors)",
    )

    # migrate-remote command
    remote_parser = subparsers.add_parser(
        "migrate-remote",
        help="Apply schema migrations directly to a remote Turso cloud DB",
    )
    remote_parser.add_argument(
        "--remote",
        required=True,
        metavar="URL",
        help="Turso cloud URL (libsql://your-db.turso.io)",
    )
    remote_parser.add_argument(
        "--token",
        metavar="TOKEN",
        help="Auth token (or TURSO_AUTH_TOKEN env)",
    )
    remote_parser.add_argument(
        "--schema",
        required=True,
        metavar="FILE",
        help="Path to Python file with @table Pydantic models",
    )
    remote_parser.add_argument(
        "--no-enums",
        action="store_true",
        help="Skip Literal type expansion to lookup tables",
    )
    remote_parser.add_argument(
        "--init",
        action="store_true",
        help="Allow creating all tables from scratch (required when cloud DB is empty)",
    )
    remote_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show operations without executing",
    )
    remote_parser.add_argument(
        "--no-fks",
        action="store_true",
        help="Strip foreign key constraints from cloud schema (avoids replication engine FK violations)",
    )

    # generate command
    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate SQL without executing",
    )
    generate_parser.add_argument(
        "-o",
        "--output",
        metavar="FILE",
        help="Output file (default: stdout)",
    )
    generate_parser.add_argument(
        "--force",
        action="store_true",
        help="Skip drift detection warning",
    )

    return parser


@boundary
def get_connection_string(args: argparse.Namespace) -> str | None:
    """Get connection string from args or environment."""
    return str(args.connection) if args.connection else os.environ.get("DECLARO_DATABASE_URL")


# Which URL prefix names which engine. A table rather than a chain of tests,
# so the vocabulary is one list a reader can see all of.
#
# `sqlite:///` takes THREE SLASHES, not two. `sqlite://foo.db` used to be
# accepted here and could not be opened by `_connect`, so the CLI said yes at
# the front door and failed at the back one with an error about the database
# rather than about the URL.
_DIALECT_BY_PREFIX: dict[str, str] = {
    "postgresql://": "postgresql",
    "postgres://": "postgresql",
    "sqlite:///": "sqlite",
    "libsql://": "turso",
    "https://": "turso",
}


@helper
def _the_named_dialect(named: list[str]) -> str:
    """The connection string carries a prefix this package knows."""
    return named[0]


@helper
def _refuse_unknown_prefix(_named: list[str]) -> str:
    """No prefix matched, so nothing here can say which engine is meant."""
    raise ValueError(
        "Cannot detect dialect from connection string. Use --dialect to specify explicitly."
    )


_NAMES_A_DIALECT: dict[bool, Callable[[list[str]], str]] = {
    True: _the_named_dialect,
    False: _refuse_unknown_prefix,
}


@helper
def detect_dialect(connection_string: str) -> str:
    """Detect dialect from connection string."""
    named = [
        dialect
        for prefix, dialect in _DIALECT_BY_PREFIX.items()
        if connection_string.startswith(prefix)
    ]
    return _NAMES_A_DIALECT[bool(named)](named)


# ---------------------------------------------------------------------------
# The commands, as a table. One entry per subcommand argparse offers, keyed by
# the name the operator types, and read with `[]`: a name the table does not
# hold is a name argparse never accepts, so there is nothing to fall back to.
#
# Every entry takes the same three arguments, and the ones a command does not
# use are named with a leading underscore rather than left out, so the table
# stays one shape.
# ---------------------------------------------------------------------------

Runner = Callable[[argparse.Namespace, "str | None", "Dialect | None"], int]

COMMANDS_NEEDING_CONNECTION = frozenset({"diff", "apply", "snapshot", "generate"})


@helper
def _run_diff(args: argparse.Namespace, connection_string: str | None, dialect: Dialect | None) -> int:
    assert connection_string is not None
    assert dialect is not None
    return asyncio.run(
        cmd_diff(
            connection_string=connection_string,
            schema_dir=args.schema_dir,
            dialect=dialect,
            interactive=args.interactive,
            unattended=args.unattended,
            force=args.force,
            verbose=args.verbose,
        )
    )


@helper
def _run_apply(args: argparse.Namespace, connection_string: str | None, dialect: Dialect | None) -> int:
    assert connection_string is not None
    assert dialect is not None
    return asyncio.run(
        cmd_apply(
            connection_string=connection_string,
            schema_dir=args.schema_dir,
            dialect=dialect,
            interactive=args.interactive,
            unattended=args.unattended,
            dry_run=args.dry_run,
            force=args.force,
            verbose=args.verbose,
        )
    )


@helper
def _run_snapshot(args: argparse.Namespace, connection_string: str | None, dialect: Dialect | None) -> int:
    assert connection_string is not None
    assert dialect is not None
    return asyncio.run(
        cmd_snapshot(
            connection_string=connection_string,
            schema_dir=args.schema_dir,
            dialect=dialect,
            force=args.force,
            verbose=args.verbose,
        )
    )


@boundary
@boundary
def _run_migrate_remote(
    args: argparse.Namespace, _connection_string: str | None, _dialect: Dialect | None
) -> int:
    token = getattr(args, "token", None) or os.environ.get("TURSO_AUTH_TOKEN")
    return asyncio.run(
        cmd_migrate_remote(
            remote_url=args.remote,
            auth_token=token,
            schema_path=args.schema,
            dialect="turso",
            expand_enums=not getattr(args, "no_enums", False),
            init=getattr(args, "init", False),
            dry_run=getattr(args, "dry_run", False),
            no_fks=getattr(args, "no_fks", False),
            verbose=args.verbose,
        )
    )


@helper
def _run_validate(
    args: argparse.Namespace, _connection_string: str | None, _dialect: Dialect | None
) -> int:
    return cmd_validate(
        schema_dir=args.schema_dir,
        strict=args.strict,
        verbose=args.verbose,
    )


@helper
def _run_generate(args: argparse.Namespace, connection_string: str | None, dialect: Dialect | None) -> int:
    assert connection_string is not None
    assert dialect is not None
    return asyncio.run(
        cmd_generate(
            connection_string=connection_string,
            schema_dir=args.schema_dir,
            dialect=dialect,
            output=args.output,
            force=args.force,
            verbose=args.verbose,
        )
    )


COMMANDS: dict[str, Runner] = {
    "diff": _run_diff,
    "apply": _run_apply,
    "snapshot": _run_snapshot,
    "migrate-remote": _run_migrate_remote,
    "validate": _run_validate,
    "generate": _run_generate,
}


@helper
def _print_traceback() -> int:
    """The operator asked for detail, so the stack goes to stderr."""
    import traceback

    traceback.print_exc()
    return 1


@helper
def _keep_the_traceback() -> int:
    """Without -v the message alone is the report."""
    return 0


_TRACEBACK: dict[bool, Callable[[], int]] = {
    True: _print_traceback,
    False: _keep_the_traceback,
}


@boundary
def _dispatch(
    args: argparse.Namespace, connection_string: str | None, dialect: Dialect | None
) -> int:
    """Run the named command and turn what it raises into an exit code."""
    try:
        return COMMANDS[args.command](args, connection_string, dialect)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        _TRACEBACK[bool(args.verbose)]()
        return 1


@helper
def _keep_given_dialect(given: Dialect | None, _connection_string: str | None) -> Dialect | None:
    """The operator passed --dialect, so nothing is guessed from the URL."""
    return given


@helper
def _detect_from_url(_given: Dialect | None, connection_string: str | None) -> Dialect | None:
    """No --dialect and a URL to read, so the URL says which engine."""
    return cast("Dialect", detect_dialect(cast("str", connection_string)))


_DIALECT_SOURCE: dict[bool, Callable[[Dialect | None, str | None], Dialect | None]] = {
    True: _detect_from_url,
    False: _keep_given_dialect,
}


def _chosen_dialect(args: argparse.Namespace, connection_string: str | None) -> Dialect | None:
    """THE ONE PLACE A DIALECT ENTERS FROM OUTSIDE, so the one place the
    vocabulary is enforced. argparse already restricts `--dialect` to
    `choices=["postgresql", "sqlite", "turso"]` and `detect_dialect` returns
    only those three, so the narrowing is declared here rather than
    re-checked in each of the 44 signatures downstream (Rule 13).
    """
    given: Dialect | None = cast("Dialect | None", args.dialect)
    return _DIALECT_SOURCE[bool(connection_string) and not given](given, connection_string)


@boundary
def _with_connection(args: argparse.Namespace, connection_string: str | None) -> int:
    try:
        dialect = _chosen_dialect(args, connection_string)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return _dispatch(args, connection_string, dialect)


@boundary
def _refuse_missing_connection(
    _args: argparse.Namespace, _connection_string: str | None
) -> int:
    print(
        "Error: Database connection required. "
        "Use -c/--connection or set DECLARO_DATABASE_URL environment variable.",
        file=sys.stderr,
    )
    return 1


_HAS_CONNECTION: dict[bool, Callable[[argparse.Namespace, "str | None"], int]] = {
    True: _with_connection,
    False: _refuse_missing_connection,
}


@helper
def _run_command(_parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:
    """An operator named a command. Find the connection it needs, then run it."""
    connection_string = get_connection_string(args)
    satisfied = bool(connection_string) or args.command not in COMMANDS_NEEDING_CONNECTION
    return _HAS_CONNECTION[satisfied](args, connection_string)


@helper
def _show_help(parser: argparse.ArgumentParser, _args: argparse.Namespace) -> int:
    """No command was named, so the list of them is the whole answer."""
    parser.print_help()
    return 1


_HAS_COMMAND: dict[bool, Callable[[argparse.ArgumentParser, argparse.Namespace], int]] = {
    True: _run_command,
    False: _show_help,
}


@helper
def main(argv: Sequence[str] | None | Nothing) -> int:
    """Main entry point for the CLI."""
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    argv = resolved(argv, None)
    parser = create_parser()
    args = parser.parse_args(argv)
    return _HAS_COMMAND[bool(args.command)](parser, args)


@helper
def _run_as_a_program() -> int:
    """Started with `python -m`, so the CLI runs and its status is the exit code."""
    sys.exit(main(argv=NOTHING))


@helper
def _do_not_run() -> int:
    """Imported, so this module defines its functions and runs none of them."""
    return 0


_ENTRY: dict[bool, Callable[[], int]] = {
    True: _run_as_a_program,
    False: _do_not_run,
}

_ENTRY[__name__ == "__main__"]()
