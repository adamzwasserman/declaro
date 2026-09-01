"""
Exception hierarchy for declaro_persistum.

All exceptions inherit from DeclaroError for easy catching.
Exception messages follow the principle: "A programmer should not have to be
an expert stack tracer to figure out where they went wrong."
"""

# honest: disable HC-P003: Python requires an exception to be a class and to
# inherit from BaseException, and rule 8 asks for typed exceptions precisely
# so a boundary can map a TYPE to a response. `A_BUG_IN_THIS_PACKAGE` is the
# tuple that separates our defect from the engine's, and it can only do that
# because the hierarchy exists. There is no composition available here.

from collections.abc import Callable
from typing import Any

from declaro_persistum.boundary import helper
from declaro_persistum.types import (
    NOTHING,
    Ambiguity,
    Nothing,
    Operation,
    is_nothing,
    resolved,
)


def _given(parts: tuple[str, ...]) -> list[str]:
    """Every part that came out with something in it.

    Six constructors assemble a message out of whichever context they were
    handed, and each asked the same question once per field with its own `if`.
    Each part is written as an expression now, and the empty ones drop out
    here.
    """
    return [part for part in parts if part]


def _shortened(text: str) -> str:
    """SQL cut down for reading. The whole statement stays on the exception."""
    return text if len(text) < 500 else text[:500] + "..."


def _nowhere(_file: Any, _line: Any) -> str:
    """Neither a file nor a line, so there is nowhere to point."""
    return ""


def _line_alone(_file: Any, line: Any) -> str:
    """A line number and no file.

    THIS USED TO PRINT NOTHING. The `if line` sat inside the `if file`, so a
    caller who knew the line and not the file was told neither. A line number
    on its own still narrows the search; dropping it in silence does not.
    """
    return f"\n\n  Line: {line}"


def _file_alone(file: Any, _line: Any) -> str:
    """A file and no line number."""
    return f"\n\n  File: {file}"


def _file_and_line(file: Any, line: Any) -> str:
    """Both, which is the case a reader wants most."""
    return f"\n\n  File: {file}, line {line}"


_LOCATION: dict[tuple[bool, bool], Callable[[Any, Any], str]] = {
    (False, False): _nowhere,
    (False, True): _line_alone,
    (True, False): _file_alone,
    (True, True): _file_and_line,
}


class DeclaroError(Exception):
    """Base exception for all declaro_persistum errors."""

    pass


class SchemaError(DeclaroError):
    """
    Schema definition or validation error.

    Raised when:
    - TOML schema file cannot be parsed
    - Schema contains invalid references
    - Required fields are missing
    """

    def __init__(self, message: str, *, file: str | Nothing, line: int | Nothing) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_file = resolved(file, None)
        the_line = resolved(line, None)
        self.file = the_file
        self.line = the_line
        location = _LOCATION[(bool(the_file), bool(the_line))](the_file, the_line)
        super().__init__(f"{message}{location}")


class AmbiguityError(SchemaError):
    """
    Unresolved ambiguous change detected.

    Raised in unattended mode when the diff contains changes that
    could be interpreted multiple ways (e.g., rename vs drop+add).
    """

    def __init__(self, ambiguities: list[Ambiguity]) -> None:
        self.ambiguities = ambiguities
        ambiguity_list = "\n".join([f"  - {amb['table']}: {amb['message']}" for amb in ambiguities])
        super().__init__(
            f"Unresolved ambiguities detected ({len(ambiguities)} total):\n\n"
            f"{ambiguity_list}\n\n"
            "Run in interactive mode (-i) to resolve, or add migration hints to schema files.",
            file=NOTHING,
            line=NOTHING,
        )


class CycleError(SchemaError):
    """
    Circular dependency in operations.

    Raised when the diff produces operations with circular dependencies,
    making it impossible to determine a valid execution order.
    """

    def __init__(self, cycle: list[str], tables: set[str]) -> None:
        self.cycle = cycle
        self.tables = tables
        cycle_display = "\n    ↓ depends on\n".join(
            f"    {i + 1}. {op}" for i, op in enumerate(cycle)
        )
        cycle_display += "\n    ↓ depends on\n    1. (cycle back)"
        tables_list = ", ".join(sorted(tables))
        super().__init__(
            f"Circular dependency detected in migration operations\n\n"
            f"  The following operations form a cycle:\n\n"
            f"{cycle_display}\n\n"
            f"  This usually indicates a circular reference in your schema.\n\n"
            f"  To resolve:\n"
            f"    - Remove one of the foreign keys\n"
            f"    - Or use a deferred constraint (add manually after migration)\n\n"
            f"  Tables involved: {tables_list}",
            file=NOTHING,
            line=NOTHING,
        )


class DriftError(DeclaroError):
    """
    Database state differs from expected snapshot.

    Raised when the actual database schema doesn't match the last-applied
    snapshot, indicating someone modified the database directly.
    """

    def __init__(
        self,
        differences: list[dict[str, str]],
        *,
        last_snapshot: str | Nothing,
        current_time: str | Nothing,
    ) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_last_snapshot = resolved(last_snapshot, None)
        the_current_time = resolved(current_time, None)
        self.differences = differences
        self.last_snapshot = the_last_snapshot
        self.current_time = the_current_time

        diff_display = "\n".join(
            [
                f"    {diff.get('symbol', '~')} {diff.get('description', 'Unknown')}"
                for diff in differences
            ]
        )

        # EACH TIMESTAMP STANDS ALONE. This read `if last_snapshot and
        # current_time`, so passing one without the other dropped BOTH, in
        # silence. "The database drifted, and I will not tell you when the
        # snapshot was taken because I do not know what time it is now" is not
        # a trade a caller would make.
        stamped = _given(
            (
                f"  Last snapshot: {the_last_snapshot}" if the_last_snapshot else "",
                f"  Current time:  {the_current_time}" if the_current_time else "",
            )
        )
        timestamps = "\n\n" + "\n".join(stamped) if stamped else ""

        super().__init__(
            f"Database schema has drifted from expected state\n\n"
            f"  The database does not match the last applied migration snapshot.\n"
            f"  Someone may have modified the database directly.\n\n"
            f"  Differences detected:\n\n"
            f"{diff_display}\n\n"
            f"  Options:\n"
            f"    1. Run 'declaro snapshot' to update snapshot to current DB state\n"
            f"    2. Run 'declaro apply --force' to proceed anyway (may cause errors)\n"
            f"    3. Manually reconcile the differences"
            f"{timestamps}"
        )


class ConnectionError(DeclaroError):
    """
    Database connection failure.

    Raised when:
    - Connection string is invalid
    - Database is unreachable
    - Authentication fails
    """

    def __init__(self, message: str, *, engine: str | Nothing) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_engine = resolved(engine, None)
        self.engine = the_engine
        engine_hint = f"\n\n  Engine: {the_engine}" if the_engine else ""
        super().__init__(f"{message}{engine_hint}")


class MigrationError(DeclaroError):
    """
    Migration execution failure.

    Raised when a DDL operation fails during migration execution.
    Contains the failing operation and SQL for debugging.
    """

    # honest-code-allow: L1.21.1 - this and ValidationError.__init__ both resolve three optional fields and store them, and that half is real duplication. Collapsing it needs setattr, which mypy cannot follow, so error.operation, error.sql, error.table and error.column would every one lose its type. The half duplicated for nothing is already gone: RollbackError resolved the three its parent owns and handed them back to a constructor that resolves them again, which cost four mypy errors
    def __init__(
        self,
        message: str,
        *,
        operation: Operation | Nothing,
        sql: str | Nothing,
        original_error: Exception | Nothing,
    ) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_operation = resolved(operation, None)
        the_sql = resolved(sql, None)
        the_original_error = resolved(original_error, None)
        self.operation = the_operation
        self.sql = the_sql
        self.original_error = the_original_error

        details = _given(
            (
                f"  Operation: {the_operation['op']} on {the_operation['table']}"
                if the_operation
                else "",
                f"  SQL: {_shortened(the_sql)}" if the_sql else "",
                f"  Original error: {type(the_original_error).__name__}: {the_original_error}"  # honest: ignore HC-P005: the class name IS the message
                if the_original_error
                else "",
            )
        )
        details_str = "\n\n" + "\n".join(details) if details else ""

        super().__init__(f"{message}{details_str}")


class RollbackError(MigrationError):
    """
    Rollback after failure also failed.

    This is a critical error state - the database may be in an
    inconsistent state and requires manual intervention.
    """

    def __init__(
        self,
        message: str,
        *,
        operation: Operation | Nothing,
        sql: str | Nothing,
        original_error: Exception | Nothing,
        rollback_error: Exception | Nothing,
    ) -> None:
        the_rollback_error = resolved(rollback_error, None)
        self.rollback_error = the_rollback_error
        rollback_msg = (
            (
                f"\n\n  CRITICAL: Rollback also failed!\n"
                f"  Rollback error: {type(the_rollback_error).__name__}: {the_rollback_error}\n"  # honest: ignore HC-P005: the class name IS the message
                f"  Manual database inspection required."
            )
            if the_rollback_error
            else ""
        )
        # THE PARENT RESOLVES THE THREE IT OWNS. This resolved them and then
        # handed the resolved values back to a constructor that resolves them
        # again, which is the same three lines written twice and the second
        # copy doing nothing. Only `rollback_error` is this class's own.
        super().__init__(
            f"{message}{rollback_msg}",
            operation=operation,
            sql=sql,
            original_error=original_error,
        )


class ValidationError(SchemaError):
    """
    Schema validation error.

    Raised when schema references are invalid:
    - Foreign key references non-existent table/column
    - Index references non-existent column
    - Circular table dependencies (without deferrable)
    """

    def __init__(
        self,
        message: str,
        *,
        table: str | Nothing,
        column: str | Nothing,
        reference: str | Nothing,
    ) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_table = resolved(table, None)
        the_column = resolved(column, None)
        the_reference = resolved(reference, None)
        self.table = the_table
        self.column = the_column
        self.reference = the_reference

        context = _given(
            (
                f"Table: {the_table}" if the_table else "",
                f"Column: {the_column}" if the_column else "",
                f"Reference: {the_reference}" if the_reference else "",
            )
        )
        context_str = "\n  " + "\n  ".join(context) if context else ""

        super().__init__(f"{message}{context_str}", file=NOTHING, line=NOTHING)


class LoaderError(SchemaError):
    """
    Schema file loading error.

    Raised when:
    - TOML file has syntax errors
    - File cannot be read
    - Directory structure is invalid
    """

    def __init__(self, message: str, *, path: str | Nothing) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_path = resolved(path, None)
        self.path = the_path
        path_str = f"\n\n  Path: {the_path}" if the_path else ""
        super().__init__(f"{message}{path_str}", file=NOTHING, line=NOTHING)


# =============================================================================
# Database and connection exceptions
# =============================================================================


class DatabaseError(DeclaroError):
    """Base exception for database and connection errors."""

    pass


class DatabaseClosedError(DatabaseError):
    """
    The database has been closed.

    Raised when attempting to acquire a connection from a database
    that has already been closed.
    """

    pass


class ConnectionsExhaustedError(DatabaseError):
    """
    No connections available (acquire timeout).

    Raised when the database cannot provide a connection within
    the configured acquire_timeout period.
    """

    pass


class ConnectionFailedError(DatabaseError):
    """
    Failed to create a connection.

    Raised when:
    - The database is unreachable
    - Authentication fails
    - The connection string is invalid
    """

    pass


# =============================================================================
# Transfer Exceptions
# =============================================================================


class TransferError(DeclaroError):
    """
    Bulk data transfer failure.

    Raised when:
    - Schema migration on target fails
    - Critical setup (progress table, FK disable) fails
    - Transfer cannot proceed

    Individual table failures are logged but do not raise this
    exception — they are recorded in BulkTransferResult.errors.
    """

    def __init__(
        self,
        message: str,
        *,
        phase: str | Nothing,
        table: str | Nothing,
        original_error: Exception | Nothing,
    ) -> None:
        # A default in the signature would decide this where no reader
        # sees it happen and no test reaches it.
        the_phase = resolved(phase, None)
        the_table = resolved(table, None)
        the_original_error = resolved(original_error, None)
        self.phase = the_phase
        self.table = the_table
        self.original_error = the_original_error

        details = _given(
            (
                f"Phase: {the_phase}" if the_phase else "",
                f"Table: {the_table}" if the_table else "",
                f"Original error: {type(the_original_error).__name__}: {the_original_error}"  # honest: ignore HC-P005: the class name IS the message
                if the_original_error
                else "",
            )
        )
        details_str = "\n\n  " + "\n  ".join(details) if details else ""

        super().__init__(f"{message}{details_str}")


class NotSupportedError(DeclaroError):
    """Raised when a feature is not supported by the target database.

    `alternatives` is required: a caller says either what to do instead, or
    explicitly that there is nothing. The list is folded into the message and
    not retained, because nothing outside this class ever read it back.
    """

    def __init__(self, message: str, alternatives: list[str]) -> None:
        options = (
            "\n\nOptions:\n"
            + "\n".join([f"  {i + 1}. {alt}" for i, alt in enumerate(alternatives)])
            if alternatives
            else ""
        )
        super().__init__(f"{message}{options}")


A_BUG_IN_THIS_PACKAGE = (
    AttributeError,
    IndexError,
    KeyError,
    NameError,
    TypeError,
)
"""The exception types that mean WE called something wrongly.

Not that a database refused anything. Python raises these when code reaches for
an attribute that is not there, indexes past the end, looks up a key that does
not exist, uses a name it never bound, or calls something with the wrong
arguments. A driver refusing a write raises its own type instead.

CATCH THIS AND RE-RAISE IT AHEAD OF ANY BROAD HANDLER. A per-item loop that
absorbs `Exception` is right for one item the engine refuses and wrong for a
bug of ours, because ours fails on EVERY item. Measured 2026-08-17 in
`bulk_transfer`: a `TypeError` from `_transfer_table` was recorded against all
four tables, and the caller was handed "4 tables failed" with none of the four
at fault.

    try:
        ...
    except A_BUG_IN_THIS_PACKAGE:
        raise
    except Exception as exc:
        ...  # the engine refused this one item

THE TYPE IS THE SIGNAL, NEVER THE MESSAGE. Sorting on message text is what made
a contention check retry a `KeyError('busy')`: the word matched and the cause
was ours. A type is decidable; a string is a guess about somebody else's
wording.
"""


# =============================================================================
# A failure inside a teardown
# =============================================================================


@helper
def _note_the_teardown(going_out: BaseException, doing: str, failure: Any) -> BaseException:
    """The teardown failed too, so the exception on its way out carries it."""
    going_out.add_note(
        f"{doing} ALSO failed: {type(failure).__name__}: {failure}. That work was NOT done."
    )
    return going_out


@helper
def _teardown_did_its_work(going_out: BaseException, _doing: str, _failure: Any) -> BaseException:
    """The teardown succeeded, so the exception goes out as it arrived."""
    return going_out


# WHETHER THE TEARDOWN HAS A FAILURE TO REPORT. Both entries return the
# exception, so neither is a function with an empty body and the caller reads
# one value either way.
_ALSO_FAILED: dict[bool, Callable[[BaseException, str, Any], BaseException]] = {
    True: _note_the_teardown,
    False: _teardown_did_its_work,
}


@helper
def also_failed(
    going_out: BaseException, doing: str, failure: Exception | Nothing
) -> BaseException:
    """Carry a teardown's failure out on the exception already in flight.

    THIS IS WHAT A TEARDOWN DOES INSTEAD OF LOGGING. A restore step runs in a
    `finally` or an `except` and must not raise, because its own failure would
    replace the one already travelling and the caller would never learn what
    actually went wrong. So four of them caught, wrote an error to a log and
    returned a count. The log was a channel no caller can read, and every one
    of those four callers threw the count away, so a connection left with
    foreign key enforcement OFF was reported to nobody at all.

    A note goes on the exception object the caller already receives. It reaches
    whoever catches it, prints in the traceback, and cannot be filtered out by
    logging configuration. The original failure still leads; this rides along.

    `doing` names the work in the words an operator needs, because by the time
    anyone reads it the connection is already in the state being described.
    """
    return _ALSO_FAILED[not is_nothing(failure)](going_out, doing, failure)
