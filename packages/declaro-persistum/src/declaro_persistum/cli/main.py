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
from collections.abc import Sequence

from declaro_persistum import __version__
from declaro_persistum.cli.commands import (
    cmd_apply,
    cmd_diff,
    cmd_generate,
    cmd_snapshot,
    cmd_validate,
)


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


def get_connection_string(args: argparse.Namespace) -> str | None:
    """Get connection string from args or environment."""
    if args.connection:
        return str(args.connection)
    return os.environ.get("DECLARO_DATABASE_URL")


def detect_dialect(connection_string: str) -> str:
    """Detect dialect from connection string."""
    if connection_string.startswith("postgresql://") or connection_string.startswith("postgres://"):
        return "postgresql"
    elif connection_string.startswith("sqlite://"):
        return "sqlite"
    elif connection_string.startswith("libsql://") or connection_string.startswith("https://"):
        return "turso"
    else:
        raise ValueError(
            "Cannot detect dialect from connection string. Use --dialect to specify explicitly."
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    # Get connection string for commands that need it
    connection_string = get_connection_string(args)
    commands_needing_connection = {"diff", "apply", "snapshot", "generate"}

    if args.command in commands_needing_connection and not connection_string:
        print(
            "Error: Database connection required. "
            "Use -c/--connection or set DECLARO_DATABASE_URL environment variable.",
            file=sys.stderr,
        )
        return 1

    # Detect or use specified dialect
    dialect: str | None = args.dialect
    if connection_string and not dialect:
        try:
            dialect = detect_dialect(connection_string)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

    # Run the appropriate command
    try:
        if args.command == "diff":
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
        elif args.command == "apply":
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
        elif args.command == "snapshot":
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
        elif args.command == "validate":
            return cmd_validate(
                schema_dir=args.schema_dir,
                strict=args.strict,
                verbose=args.verbose,
            )
        elif args.command == "generate":
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
        else:
            parser.print_help()
            return 1

    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
