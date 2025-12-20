"""Command-line interface for declaro-ximinez."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import load_config
from .checker import check_file
from .errors import (
    format_violations_inquisition,
    format_violations_quiet,
    format_violations_machine,
    format_comfy_chair,
    format_parse_error,
    format_schema_not_found,
)
from .types import XiminezConfig, Violation


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser.

    Returns:
        Configured ArgumentParser.
    """
    parser = argparse.ArgumentParser(
        prog="ximinez",
        description="Nobody expects the Ximínez Inquisition! Type enforcement for Python.",
    )

    parser.add_argument(
        "files",
        nargs="*",
        help="Python files to check",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Minimal output - count and locations only",
    )

    parser.add_argument(
        "--machine",
        action="store_true",
        help="CI-friendly plain format",
    )

    parser.add_argument(
        "--comfy-chair",
        action="store_true",
        dest="comfy_chair",
        help="Lenient mode - violations as warnings, never fails",
    )

    parser.add_argument(
        "--rack",
        action="store_true",
        help="Strict mode - promotes warnings to errors",
    )

    parser.add_argument(
        "--declaro-schema",
        dest="declaro_schema",
        metavar="PATH",
        help="Path to TOML schema directory for model validation",
    )

    parser.add_argument(
        "--no-declaro",
        action="store_true",
        dest="no_declaro",
        help="Disable Declaro model validation",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0",
    )

    return parser


def run_checks(
    files: list[Path],
    config: XiminezConfig,
) -> list[Violation]:
    """Run ximinez checks on a list of files.

    Args:
        files: List of file paths to check.
        config: Ximinez configuration.

    Returns:
        List of all violations found.
    """
    all_violations: list[Violation] = []

    for file_path in files:
        if not file_path.exists():
            all_violations.append({
                "file": str(file_path),
                "line": 0,
                "col": 0,
                "message": f"file not found: {file_path}",
                "code": "XI000",
            })
            continue

        if not file_path.suffix == ".py":
            continue

        result = check_file(file_path, config)
        all_violations.extend(result["violations"])

    return all_violations


def collect_files(paths: list[str], config: XiminezConfig) -> list[Path]:
    """Collect Python files from paths.

    Args:
        paths: List of file or directory paths.
        config: Ximinez configuration.

    Returns:
        List of Python file paths.
    """
    files: list[Path] = []

    for path_str in paths:
        path = Path(path_str)

        if path.is_file():
            if path.suffix == ".py":
                files.append(path)
        elif path.is_dir():
            files.extend(path.rglob("*.py"))

    return files


def format_output(
    violations: list[Violation],
    quiet: bool = False,
    machine: bool = False,
    comfy_chair: bool = False,
) -> str:
    """Format violations for output.

    Args:
        violations: List of violations to format.
        quiet: Use quiet mode.
        machine: Use machine-readable mode.
        comfy_chair: Use comfy chair (warning) mode.

    Returns:
        Formatted output string.
    """
    if machine:
        return format_violations_machine(violations)

    if quiet:
        return format_violations_quiet(violations)

    if comfy_chair:
        return format_comfy_chair(violations)

    # Separate type and model violations
    type_violations = [v for v in violations if not v["code"].startswith("XIM")]
    model_violations = [v for v in violations if v["code"].startswith("XIM")]

    parts = []

    if type_violations:
        parts.append(format_violations_inquisition(type_violations, "type"))

    if model_violations:
        if parts:
            parts.append("")  # Blank line between sections
        parts.append(format_violations_inquisition(model_violations, "model"))
        parts.append("")
        parts.append("The Inquisition has examined your models and found them... heretical.")

    if not violations:
        parts.append("Dismissed! The accused is free to go.")

    return "\n".join(parts)


def main(argv: list[str] | None = None) -> int:
    """Main entry point.

    Args:
        argv: Command-line arguments (defaults to sys.argv[1:]).

    Returns:
        Exit code (0 = success, 1 = violations, 2 = error).
    """
    parser = create_parser()
    args = parser.parse_args(argv)

    # Load configuration
    config = load_config()

    # Apply CLI overrides
    if args.declaro_schema:
        config["declaro_enabled"] = True
        config["declaro_schema_paths"] = [args.declaro_schema]

    if args.no_declaro:
        config["declaro_enabled"] = False

    # Collect files
    paths = args.files if args.files else config.get("paths", ["."])
    files = collect_files(paths, config)

    if not files:
        print("No Python files to check.", file=sys.stderr)
        return 0

    # Run checks
    try:
        violations = run_checks(files, config)
    except FileNotFoundError as e:
        print(format_schema_not_found(str(e)), file=sys.stderr)
        return 2
    except SyntaxError as e:
        print(format_parse_error(str(e.filename), e.lineno or 1, str(e.msg)), file=sys.stderr)
        return 2

    # Format and print output
    output = format_output(
        violations,
        quiet=args.quiet,
        machine=args.machine,
        comfy_chair=args.comfy_chair,
    )
    print(output)

    # Determine exit code
    if args.comfy_chair:
        return 0  # Comfy chair never fails

    if violations:
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
