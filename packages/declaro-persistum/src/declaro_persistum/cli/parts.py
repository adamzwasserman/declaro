"""The pieces every command is built from.

`commands.py` reached 1619 lines holding two things: six commands, and the
sixty-odd small functions and dispatch tables they are assembled from. This
is the second of those. Every name here is re-exported from `commands`, so
nothing that imports from there had to change.
"""


import sys
from collections.abc import Awaitable, Callable
from datetime import UTC
from pathlib import Path
from typing import Any

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import AmbiguityError
from declaro_persistum.types import (
    Ambiguity,
    ApplyResult,
    Decision,
    Dialect,
    DiffResult,
    Operation,
    Schema,
    for_dialect,
)

# =============================================================================
# The pieces every command is built from
# =============================================================================
#
# THIS FILE HELD 69 CONDITIONALS, more than any other in the package. `umbra
# audit` on 2026-08-21 counted 80 here and 73 branches nothing had ever taken,
# which is what put the file at the top of the list.
#
# Nearly all of them were one of three shapes repeated across six commands:
# print this line only when --verbose, stop here with an exit code, or choose
# between two ways of finishing. Each shape is written once below and read as a
# table, so the six commands share one copy instead of six.

# What the remainder of a command does, once an earlier step has let it run.
Rest = Callable[[], Awaitable[int]]


@boundary
def _print_it(message: str, stream: Any) -> int:
    """This line was asked for, so it is printed."""
    print(message, file=stream)
    return 1


@boundary
def _print_nothing(_message: str, _stream: Any) -> int:
    """This line was not asked for, so nothing is printed."""
    return 0


# Whether the line is wanted.
# How many lines printed: one or none. Both halves answer, so neither is a
# function with an empty body.
_WHEN_WANTED: dict[bool, Callable[[str, Any], int]] = {
    True: _print_it,
    False: _print_nothing,
}


def _note(wanted: bool, message: str, stream: Any) -> None:
    """Print `message` on `stream` when `wanted`, and otherwise say nothing.

    Almost every caller passes `verbose`, which is the twenty-odd `if verbose:`
    blocks this file used to carry. Two pass something else: the warning line
    that appears only when the applier produced one, and the `--init` line that
    appears only when the guard it belongs to was crossed.
    """
    _WHEN_WANTED[wanted](message, stream)


@boundary
async def _that_is_the_answer(message: str, stream: Any, code: int, _rest: Rest) -> int:
    """This step settles the command, so it says why and hands back its code."""
    print(message, file=stream)
    return code


@boundary
async def _carry_on(_message: str, _stream: Any, _code: int, rest: Rest) -> int:
    """This step had nothing to say, so the rest of the command runs."""
    return await rest()


# Whether a step has already decided how the command ends.
_WHEN_SETTLED: dict[bool, Callable[[str, Any, int, Rest], Awaitable[int]]] = {
    True: _that_is_the_answer,
    False: _carry_on,
}


@boundary
async def _stop_at_drift(drift: list[dict[str, str]], _rest: Rest) -> int:
    """The database no longer matches its snapshot, so the command stops."""
    _print_drift_error(drift)
    return 2


@boundary
async def _past_the_drift_check(_drift: list[dict[str, str]], rest: Rest) -> int:
    """Nothing has drifted, so the rest of the command runs."""
    return await rest()


# Whether the drift check found anything.
_WHEN_DRIFTED: dict[bool, Callable[[list[dict[str, str]], Rest], Awaitable[int]]] = {
    True: _stop_at_drift,
    False: _past_the_drift_check,
}


@boundary
def _drift_against_the_snapshot(actual: Schema, schema_dir: str) -> list[dict[str, str]]:
    """The snapshot is there and is being read, so compare the two."""
    from declaro_persistum.loader import load_snapshot

    return _check_drift(actual, load_snapshot(schema_dir))


@boundary
def _no_snapshot_to_compare(_actual: Schema, _schema_dir: str) -> list[dict[str, str]]:
    """No snapshot, or --force, so there is nothing to have drifted from."""
    return []


# Whether a snapshot is going to be read at all.
_WHEN_SNAPSHOT_READ: dict[bool, Callable[[Schema, str], list[dict[str, str]]]] = {
    True: _drift_against_the_snapshot,
    False: _no_snapshot_to_compare,
}


@boundary
def _drift(actual: Schema, schema_dir: str, force: bool) -> list[dict[str, str]]:
    """How the database differs from the snapshot, when there is one to read.

    `declaro diff`, `declaro apply` and `declaro generate` all asked this the
    same way, in three copies of the same eight lines.
    """
    snapshot_path = Path(schema_dir) / "snapshot.toml"
    return _WHEN_SNAPSHOT_READ[snapshot_path.exists() and not force](actual, schema_dir)


@helper
def _the_modes_asked_for(
    interactive: bool, unattended: bool, _is_tty: bool
) -> tuple[bool, bool]:
    """The operator named a mode on the command line, so it stands."""
    return interactive, unattended


@helper
def _the_modes_the_terminal_implies(
    _interactive: bool, _unattended: bool, is_tty: bool
) -> tuple[bool, bool]:
    """Neither flag was given: a terminal means ask, a pipe means do not."""
    return is_tty, not is_tty


# Whether the operator named a mode.
_WHEN_MODE_GIVEN: dict[bool, Callable[[bool, bool, bool], tuple[bool, bool]]] = {
    True: _the_modes_asked_for,
    False: _the_modes_the_terminal_implies,
}


@helper
def _modes(interactive: bool, unattended: bool, is_tty: bool) -> tuple[bool, bool]:
    """Which of interactive and unattended this run is in."""
    return _WHEN_MODE_GIVEN[interactive or unattended](interactive, unattended, is_tty)


# What settles a diff that came back with ambiguities in it.
Settle = Callable[
    [DiffResult, str, Callable[[dict[str, Decision]], DiffResult]],
    Awaitable[DiffResult],
]


@boundary
async def _refuse_the_ambiguities(
    result: DiffResult,
    _schema_dir: str,
    _rerun: Callable[[dict[str, Decision]], DiffResult],
) -> DiffResult:
    """Nobody is at the terminal to answer, so the command refuses."""
    raise AmbiguityError(result["ambiguities"])


@boundary
async def _ask_the_operator(
    result: DiffResult,
    schema_dir: str,
    rerun: Callable[[dict[str, Decision]], DiffResult],
) -> DiffResult:
    """Put each ambiguity to the operator, then diff again with the answers."""
    return rerun(await _resolve_ambiguities_interactive(result["ambiguities"], schema_dir))


@boundary
async def _leave_them_alone(
    result: DiffResult,
    _schema_dir: str,
    _rerun: Callable[[dict[str, Decision]], DiffResult],
) -> DiffResult:
    """Nothing to answer, or nothing that could answer it, so the diff stands."""
    return result


# WHO ANSWERS AN AMBIGUITY, over the whole of (any ambiguity, unattended,
# interactive). Written out rather than nested, because the case that had never
# been thought about was the last one: a run that is neither unattended nor
# interactive kept the diff it already had, and that is now a line rather than a
# gap between two `if`s.
_ANSWER_AMBIGUITIES: dict[tuple[bool, bool, bool], Settle] = {
    (False, False, False): _leave_them_alone,
    (False, False, True): _leave_them_alone,
    (False, True, False): _leave_them_alone,
    (False, True, True): _leave_them_alone,
    (True, True, True): _refuse_the_ambiguities,
    (True, True, False): _refuse_the_ambiguities,
    (True, False, True): _ask_the_operator,
    (True, False, False): _leave_them_alone,
}


@helper
async def _settled(
    result: DiffResult,
    schema_dir: str,
    unattended: bool,
    interactive: bool,
    rerun: Callable[[dict[str, Decision]], DiffResult],
) -> DiffResult:
    """The diff, once whatever can answer its ambiguities has answered them."""
    return await _ANSWER_AMBIGUITIES[
        (bool(result["ambiguities"]), unattended, interactive)
    ](result, schema_dir, rerun)


@boundary
async def _aborted(_rest: Rest) -> int:
    """The operator said no."""
    print("Aborted.")
    return 1


@boundary
async def _confirmed(rest: Rest) -> int:
    """The operator said yes, so the rest of the command runs."""
    return await rest()


# Whether the operator typed y.
_WHEN_CONFIRMED: dict[bool, Callable[[Rest], Awaitable[int]]] = {
    True: _confirmed,
    False: _aborted,
}


@boundary
async def _ask_before_applying(rest: Rest) -> int:
    """Somebody is at the terminal, so they are asked before anything is written."""
    confirm = input("\nApply these changes? [y/N] ")
    return await _WHEN_CONFIRMED[confirm.lower() == "y"](rest)


# Whether there is anybody to ask before the migration runs.
_WHEN_ASKING: dict[bool, Callable[[Rest], Awaitable[int]]] = {
    True: _ask_before_applying,
    False: _confirmed,
}


@helper
def _refusal_lines(headline: str, messages: list[str]) -> str:
    """A headline and one indented line per message, as one block of text."""
    return "\n".join([headline] + [f"  {message}" for message in messages])


@boundary
async def _refuse_without_a_terminal(snapshot_path: Path, _rest: Rest) -> int:
    """NO TERMINAL MEANS NO PROMPT.

    `input()` raises EOFError on a pipe, in CI, in cron and in a container, and
    taking a snapshot after a deploy is exactly the automated case. Measured
    2026-08-17:

        $ declaro ... snapshot < /dev/null
        Error: EOF when reading a line

    `cmd_diff` and `cmd_apply` already derive their behaviour from
    `sys.stdin.isatty()`; this command was the one that did not. It refuses
    rather than overwriting, and names the flag that proceeds, because a
    snapshot is the record of what the database looked like and replacing it
    unasked is how drift detection goes quiet.
    """
    print(
        f"Error: a snapshot already exists at {snapshot_path} and "
        f"there is no terminal to confirm overwriting it. "
        f"Pass --force to overwrite.",
        file=sys.stderr,
    )
    return 1


@boundary
async def _ask_about_overwriting(snapshot_path: Path, rest: Rest) -> int:
    """Somebody is at the terminal, so they are asked."""
    confirm = input(f"Overwrite existing snapshot at {snapshot_path}? [y/N] ")
    return await _WHEN_CONFIRMED[confirm.lower() == "y"](rest)


# Whether there is a terminal to put the question to.
_WHEN_A_TERMINAL: dict[bool, Callable[[Path, Rest], Awaitable[int]]] = {
    True: _ask_about_overwriting,
    False: _refuse_without_a_terminal,
}


@boundary
async def _confirm_the_overwrite(snapshot_path: Path, rest: Rest) -> int:
    """A snapshot is already there, so overwriting it has to be agreed to."""
    return await _WHEN_A_TERMINAL[sys.stdin.isatty()](snapshot_path, rest)


@boundary
async def _nothing_to_overwrite(_snapshot_path: Path, rest: Rest) -> int:
    """No snapshot there, or --force given, so nothing needs agreeing to."""
    return await rest()


# Whether an existing snapshot stands in the way.
_WHEN_OVERWRITING: dict[bool, Callable[[Path, Rest], Awaitable[int]]] = {
    True: _confirm_the_overwrite,
    False: _nothing_to_overwrite,
}


@boundary
def _print_the_warnings(warnings: list[str]) -> int:
    """The schema drew warnings, so each one is named."""
    print(
        "\n".join(
            [f"⚠ Schema has {len(warnings)} warning(s):"]
            + [f"  - {warning}" for warning in warnings]
        )
    )
    return 1


@boundary
def _print_no_warnings(_warnings: list[str]) -> int:
    """Nothing to warn about."""
    return 0


# Whether the validator raised any warning.
# How many warnings printed. Both halves answer, so neither is a
# function with an empty body.
_WHEN_WARNINGS: dict[bool, Callable[[list[str]], int]] = {
    True: _print_the_warnings,
    False: _print_no_warnings,
}


@boundary
def _warnings_are_fatal(_schema: Schema, _verbose: bool) -> int:
    """--strict was given and the schema drew warnings, so it does not pass."""
    return 2


@boundary
def _schema_passes(schema: Schema, verbose: bool) -> int:
    """Nothing fatal, so the schema is good and --verbose lists it."""
    print(f"✓ Schema valid ({len(schema)} tables)")
    listing = [
        f"  {table}: {len(schema[table].get('columns', {}))} columns"
        for table in sorted(schema.keys())
    ]
    _note(verbose and bool(listing), "\n".join(listing), sys.stdout)
    return 0


# Whether a warning is enough to fail this run.
_WHEN_WARNINGS_ARE_FATAL: dict[bool, Callable[[Schema, bool], int]] = {
    True: _warnings_are_fatal,
    False: _schema_passes,
}


@boundary
def _report_the_errors(
    _schema: Schema,
    errors: list[str],
    _warnings: list[str],
    _strict: bool,
    _verbose: bool,
) -> int:
    """The schema breaks a rule, so the rules it breaks are named."""
    print(
        "\n".join(
            [f"✗ Schema validation failed with {len(errors)} error(s):"]
            + [f"  - {error}" for error in errors]
        )
    )
    return 1


@boundary
def _report_the_warnings(
    schema: Schema,
    _errors: list[str],
    warnings: list[str],
    strict: bool,
    verbose: bool,
) -> int:
    """No errors, so what is left is warnings and how strictly they are read."""
    _WHEN_WARNINGS[bool(warnings)](warnings)
    return _WHEN_WARNINGS_ARE_FATAL[bool(warnings) and strict](schema, verbose)


# Whether the validator found anything it calls an error.
_WHEN_ERRORS: dict[bool, Callable[[Schema, list[str], list[str], bool, bool], int]] = {
    True: _report_the_errors,
    False: _report_the_warnings,
}


@helper
def _schema_with_enums_expanded(schema: Schema) -> Schema:
    """--expand-enums was given, so each Literal gains its lookup table."""
    from declaro_persistum.abstractions.enums import expand_schema_enums

    return expand_schema_enums(schema)


@helper
def _schema_as_written(schema: Schema) -> Schema:
    """--expand-enums was not given, so the schema goes on as it came."""
    return schema


# Whether the enum lookup tables are being added to the target schema.
_WHEN_ENUMS_EXPANDED: dict[bool, Callable[[Schema], Schema]] = {
    True: _schema_with_enums_expanded,
    False: _schema_as_written,
}


@boundary
def _schema_without_foreign_keys(schema: Schema, verbose: bool) -> Schema:
    """--no-fks was given, so the references come off before the diff."""
    from declaro_persistum.fk_ordering import strip_foreign_keys

    stripped = strip_foreign_keys(schema)
    _note(verbose, "Stripped FK constraints from target schema (--no-fks)", sys.stdout)
    return stripped


@boundary
def _schema_keeping_its_foreign_keys(schema: Schema, _verbose: bool) -> Schema:
    """--no-fks was not given, so the references stay."""
    return schema


# Whether the foreign keys are being stripped before the diff.
_WHEN_FKS_STRIPPED: dict[bool, Callable[[Schema, bool], Schema]] = {
    True: _schema_without_foreign_keys,
    False: _schema_keeping_its_foreign_keys,
}


@boundary
async def _pushed_to_the_cloud(conn: Any, result: ApplyResult) -> int:
    """The DDL ran on the local copy, so it goes up to the primary."""
    await conn.push()
    print(f"\nApplied {result['operations_applied']} operations to cloud DB")
    _note(bool(result.get("error")), f"Warnings: {result.get('error')}", sys.stdout)
    return 0


@boundary
async def _not_pushed_to_the_cloud(_conn: Any, result: ApplyResult) -> int:
    """The applier refused, so nothing is sent and it says why."""
    print(
        f"\nMigration failed: {result.get('error', 'Unknown error')}", file=sys.stderr
    )
    return 1


# Whether the applier got through the whole plan.
_WHEN_PUSHED: dict[bool, Callable[[Any, ApplyResult], Awaitable[int]]] = {
    True: _pushed_to_the_cloud,
    False: _not_pushed_to_the_cloud,
}


@helper
def _refuse_the_unanswerable(ambiguities: list[Ambiguity]) -> int:
    """`declaro generate` asks nobody and reads no decisions, so it refuses."""
    raise AmbiguityError(ambiguities)
    return 1


@helper
def _nothing_unanswerable(_ambiguities: list[Ambiguity]) -> int:
    """The diff came back clear, so there is nothing to refuse."""
    return 0


# Whether the diff produced an ambiguity this command cannot settle.
# How many ambiguities refused. Both halves answer, so neither is a
# function with an empty body.
_WHEN_UNANSWERABLE: dict[bool, Callable[[list[Ambiguity]], int]] = {
    True: _refuse_the_unanswerable,
    False: _nothing_unanswerable,
}


@boundary
def _sql_to_a_file(sql: str, output: str | None) -> int:
    """--output named a file, so the statements go there and stdout stays clean."""
    Path(str(output)).write_text(sql)
    print(f"✓ SQL written to {output}", file=sys.stderr)
    return 0


@boundary
def _sql_to_stdout(sql: str, _output: str | None) -> int:
    """No file was named, so the statements go to stdout to be piped."""
    print(sql)
    return 0


# Whether --output named a file. `str(output)` is safe on the entry that reads
# it, because the key is the very question of whether there is a name there.
_WHEN_OUTPUT_NAMED: dict[bool, Callable[[str, str | None], int]] = {
    True: _sql_to_a_file,
    False: _sql_to_stdout,
}


# Helper functions


SQLITE_RELATIVE = "sqlite:///"
SQLITE_ABSOLUTE = "sqlite:////"


@boundary
def sqlite_path(connection_string: str) -> str:
    """The filename a `sqlite://` URL names, or a refusal that shows the forms.

    THREE SLASHES IS RELATIVE, FOUR IS ABSOLUTE, which is the convention the
    documented examples use (`sqlite:///./data/app.db`).

    THIS WAS `connection_string.replace("sqlite:///", "")`, and two things were
    wrong with it. A replace is not a prefix strip, so it also ate the sequence
    out of the middle of a path. And it did nothing at all to a two-slash URL,
    which `detect_dialect` accepted as SQLite, so aiosqlite was handed
    `sqlite://foo.db` as a FILENAME and answered

        sqlite3.OperationalError: unable to open database file

    A user who typed a URL this CLI called valid is sent to look at disk
    permissions. Measured 2026-08-17.
    """
    # A four-slash URL starts with both prefixes, so the absolute form is
    # listed first and the first match wins, exactly as the two `if`s did.
    cut = [
        keep
        for prefix, keep in (
            (SQLITE_ABSOLUTE, len(SQLITE_ABSOLUTE) - 1),
            (SQLITE_RELATIVE, len(SQLITE_RELATIVE)),
        )
        if connection_string.startswith(prefix)
    ]
    return _WHEN_A_SQLITE_URL[bool(cut)](connection_string, cut)


@helper
def _the_filename(connection_string: str, cut: list[int]) -> str:
    """The URL names a file, so the prefix comes off and the path is left."""
    return connection_string[cut[0] :]


@helper
def _not_a_sqlite_url(connection_string: str, _cut: list[int]) -> str:
    """Neither form matched, so the two that work are named."""
    raise ValueError(
        f"{connection_string!r} is not a SQLite URL this CLI can open. Use "
        f"{SQLITE_RELATIVE}relative/path.db or {SQLITE_ABSOLUTE}absolute/path.db"
    )


# Whether the string is one of the two SQLite URL forms.
_WHEN_A_SQLITE_URL: dict[bool, Callable[[str, list[int]], str]] = {
    True: _the_filename,
    False: _not_a_sqlite_url,
}


@boundary
async def _connect_postgresql(connection_string: str) -> Any:
    import asyncpg

    return await asyncpg.connect(connection_string)


@boundary
async def _connect_sqlite(connection_string: str) -> Any:
    import aiosqlite

    return await aiosqlite.connect(sqlite_path(connection_string))


@boundary
async def _connect_turso(connection_string: str) -> Any:
    import turso.aio

    return await turso.aio.connect(connection_string)


CONNECTORS: dict[str, Any] = {
    "postgresql": _connect_postgresql,
    "sqlite": _connect_sqlite,
    "turso": _connect_turso,
}


@helper
async def _connect(connection_string: str, dialect: Dialect) -> Any:
    """Open a connection for `dialect`."""
    connector = for_dialect(CONNECTORS, dialect, "Connectors")
    return await connector(connection_string)


@boundary
async def _disconnect(connection: Any, dialect: Dialect) -> None:
    """Close a connection.

    The chain here read `if dialect == "postgresql" or dialect == "sqlite" or
    dialect == "turso"`, which is every dialect there is, so it was an `if`
    that is always true guarding the one line it contains -- and silence for
    any engine added later. `for_dialect` keeps the check and makes the
    silence impossible.
    """
    for_dialect(CONNECTORS, dialect, "Connectors")
    await connection.close()


@helper
def _check_drift(actual: Schema, expected: Schema) -> list[dict[str, str]]:
    """Check for drift between actual and expected schemas."""
    actual_tables = set(actual.keys())
    expected_tables = set(expected.keys())

    # Tables in DB but not in snapshot
    added_tables = [
        {
            "symbol": "+",
            "description": f"Table '{table}' exists in DB but not in snapshot",
        }
        for table in actual_tables - expected_tables
    ]

    # Tables in snapshot but not in DB
    removed_tables = [
        {
            "symbol": "-",
            "description": f"Table '{table}' in snapshot but not in DB",
        }
        for table in expected_tables - actual_tables
    ]

    # Column differences in common tables, still table by table so that the
    # order an operator reads is the order the loops produced.
    column_differences = [
        difference
        for table in actual_tables & expected_tables
        for difference in _column_drift(
            table,
            set(actual[table].get("columns", {}).keys()),
            set(expected[table].get("columns", {}).keys()),
        )
    ]

    return added_tables + removed_tables + column_differences


@helper
def _column_drift(
    table: str, actual_cols: set[str], expected_cols: set[str]
) -> list[dict[str, str]]:
    """The columns one side of the comparison has and the other does not."""
    return [
        {
            "symbol": "+",
            "description": f"Column '{table}.{col}' exists in DB but not in snapshot",
        }
        for col in actual_cols - expected_cols
    ] + [
        {
            "symbol": "-",
            "description": f"Column '{table}.{col}' in snapshot but not in DB",
        }
        for col in expected_cols - actual_cols
    ]


@boundary
def _print_drift_error(differences: list[dict[str, str]]) -> None:
    """Print drift error message."""
    print("⚠ Database schema has drifted from expected state\n")
    print("  Differences detected:\n")
    [print(f"    {diff['symbol']} {diff['description']}") for diff in differences]
    print("\n  Options:")
    print("    1. Run 'declaro snapshot' to update snapshot to current DB state")
    print("    2. Run with --force to proceed anyway")
    print("    3. Manually reconcile the differences")



# HOW EACH OPERATION READS, one entry per kind. This was an if/elif chain of
# eleven branches, which is dispatch on a value written the long way: a kind
# added to the differ needed a branch here, and one removed left a branch
# nobody could reach.
#
# READ BY SUBSCRIPT WITH A DECLARED FALLBACK. `_describe` asks the table and
# falls back to naming the kind, because an operation this CLI has not learned
# to phrase must still be PRINTED. That is a display default and not a
# behavioural one: nothing is chosen for the operator, and the unknown kind
# appears in the output rather than being skipped.
_DESCRIBE: dict[str, Callable[[str, dict[str, Any]], str]] = {
    "create_table": lambda table, d: (
        f"CREATE TABLE {table} ({len(d.get('columns', {}))} columns)"
    ),
    "drop_table": lambda table, _d: f"DROP TABLE {table}",
    "rename_table": lambda table, d: f"RENAME TABLE {table} -> {d['new_name']}",
    "add_column": lambda table, d: (
        f"ADD COLUMN {table}.{d['column']} ({d['definition'].get('type', '?')})"
    ),
    "drop_column": lambda table, d: f"DROP COLUMN {table}.{d['column']}",
    "rename_column": lambda table, d: (
        f"RENAME COLUMN {table}.{d['from_column']} -> {d['to_column']}"
    ),
    "alter_column": lambda table, d: (
        f"ALTER COLUMN {table}.{d['column']} "
        f"({', '.join(d.get('changes', {}).keys())})"
    ),
    "add_index": lambda table, d: (
        f"CREATE INDEX {d['index']} ON {table} "
        f"({', '.join(d['definition'].get('columns', []))})"
    ),
    "drop_index": lambda _table, d: f"DROP INDEX {d['index']}",
    "add_foreign_key": lambda table, d: (
        f"ADD FOREIGN KEY {table}.{d['column']} -> {d['references']}"
    ),
    "drop_foreign_key": lambda table, d: f"DROP FOREIGN KEY {table}.{d['column']}",
}


@helper
def _describe(op_type: str, table: str, details: dict[str, Any]) -> str:
    """One operation as a line an operator reads."""
    phrase = _DESCRIBE.get(op_type)
    return phrase(table, details) if phrase else f"{op_type.upper()} on {table}"


@helper
def _operation_lines(position: int, op: Operation, verbose: bool) -> str:
    """One operation, and under --verbose the details worth spelling out."""
    details = op["details"]
    return "\n".join(
        [f"  {position + 1}. {_describe(op['op'], op['table'], details)}"]
        + [
            f"       {key}: {value}"
            for key, value in details.items()
            if verbose and key not in ("column", "index", "constraint", "new_name")
        ]
    )


@boundary
def _print_no_changes(_ops: list[Operation], _order: list[int], _verbose: bool) -> None:
    """The diff is empty, so there is nothing to list."""
    print("No changes needed.")


@boundary
def _print_the_operations(
    ops: list[Operation], order: list[int], verbose: bool
) -> None:
    """Every operation in the order it will run."""
    print(f"\nProposed changes ({len(ops)} operations):\n")
    [
        print(_operation_lines(position, ops[op_idx], verbose))
        for position, op_idx in enumerate(order)
    ]


# Whether the diff produced anything at all.
_WHEN_ANY_OPERATION: dict[bool, Callable[[list[Operation], list[int], bool], None]] = {
    True: _print_the_operations,
    False: _print_no_changes,
}


@helper
def _print_diff_result(result: DiffResult, verbose: bool) -> None:
    """Print diff result to console."""
    ops = result["operations"]
    _WHEN_ANY_OPERATION[bool(ops)](ops, result["execution_order"], verbose)


@helper
def _a_rename(amb: Ambiguity, now: str) -> Decision:
    """The operator chose [1], so the column keeps its data under a new name."""
    return {
        "type": "rename",
        "table": amb["table"],
        "from_column": amb.get("from_column"),
        "to_column": amb.get("to_column"),
        "column": None,
        "decided_at": now,
    }


@helper
def _a_drop(amb: Ambiguity, now: str) -> Decision:
    """Anything other than [1], so the old column goes and the new one arrives."""
    return {
        "type": "drop",
        "table": amb["table"],
        "column": amb.get("from_column"),
        "from_column": None,
        "to_column": None,
        "decided_at": now,
    }


# Whether the operator typed 1.
_RENAME_OR_DROP: dict[bool, Callable[[Ambiguity, str], Decision]] = {
    True: _a_rename,
    False: _a_drop,
}


@boundary
def _decide_a_rename(amb: Ambiguity) -> dict[str, Decision]:
    """Ask whether the column was renamed or replaced, and record the answer."""
    from datetime import datetime

    print("   [1] Rename (preserves data)")
    print("   [2] Drop + Add (loses data)")

    choice = input("   Choice [1/2]: ").strip()

    key = f"{amb['table']}_{amb.get('from_column', '')}"
    return {key: _RENAME_OR_DROP[choice == "1"](amb, datetime.now(UTC).isoformat())}


@helper
def _abort_the_run() -> int:
    """The operator chose [2]."""
    raise KeyboardInterrupt("User aborted")
    return 1


@helper
def _go_ahead_with_the_change() -> int:
    """Anything other than [2] proceeds, and records no decision."""
    return 0


# Whether the operator typed 2.
# How many runs aborted. Both halves answer, so neither is a
# function with an empty body.
_WHEN_ABORTED: dict[bool, Callable[[], int]] = {
    True: _abort_the_run,
    False: _go_ahead_with_the_change,
}


@boundary
def _decide_a_type_change(_amb: Ambiguity) -> dict[str, Decision]:
    """Ask whether to go through with a type change; [2] ends the run."""
    print("   [1] Proceed with type change")
    print("   [2] Abort")

    choice = input("   Choice [1/2]: ").strip()

    _WHEN_ABORTED[choice == "2"]()
    return {}


# ONE ENTRY PER KIND OF AMBIGUITY THE DIFFER DECLARES. `Ambiguity["type"]` is a
# Literal of exactly these two, so the table is total and is read with []. The
# chain this replaces ended in an unwritten `else` that printed the message,
# printed a blank line and recorded nothing.
_DECIDE: dict[str, Callable[[Ambiguity], dict[str, Decision]]] = {
    "possible_rename": _decide_a_rename,
    "type_change": _decide_a_type_change,
}


@boundary
def _ask_about(position: int, amb: Ambiguity) -> dict[str, Decision]:
    """Put one ambiguity to the operator and hand back what they decided."""
    print(f"{position + 1}. {amb['message']}")
    made = _DECIDE[amb["type"]](amb)
    print()
    return made


@boundary
def _save_the_decisions(schema_dir: str, decisions: dict[str, Decision]) -> int:
    """Something was decided, so it is written down for the next run."""
    from declaro_persistum.loader import save_decisions

    save_decisions(schema_dir, decisions)
    print(f"Decisions saved to {schema_dir}/migrations/pending.toml")
    return 1


@boundary
def _save_nothing(_schema_dir: str, _decisions: dict[str, Decision]) -> int:
    """Nothing was decided, so there is nothing to write down."""
    return 0


# Whether any decision came out of the questions.
# How many decision files written. Both halves answer, so neither is a
# function with an empty body.
_WHEN_ANY_DECISION: dict[bool, Callable[[str, dict[str, Decision]], int]] = {
    True: _save_the_decisions,
    False: _save_nothing,
}


@boundary
async def _resolve_ambiguities_interactive(
    ambiguities: list[Ambiguity],
    schema_dir: str,
) -> dict[str, Decision]:
    """Interactively resolve ambiguities and save decisions."""
    print(f"\n{len(ambiguities)} ambiguous change(s) detected:\n")

    answered = [_ask_about(position, amb) for position, amb in enumerate(ambiguities)]
    decisions: dict[str, Decision] = {
        key: decision for one in answered for key, decision in one.items()
    }

    _WHEN_ANY_DECISION[bool(decisions)](schema_dir, decisions)

    return decisions
