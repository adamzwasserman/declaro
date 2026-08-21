"""`bulk_loader.py` declared thirty database handles as `Any`.

A function annotated `Any` has no contract. Every caller and every reader has
to work out what may be passed by reading the body, and the compiler cannot
help either of them. This module moves rows between databases, so a wrong
value here loses rows rather than raising, which makes it the least defensible
place in the package to leave the contract unwritten.

The driver was never in doubt. `postgres_read_rows` is only ever handed an
asyncpg connection and `dbapi_read_rows` only ever aiosqlite or pyturso. The
names say so. `Any` was not describing uncertainty; it was the declaration
nobody wrote.

The three read functions also carried `offset: int = 0` and
`limit: int | None = None`. Rule 14: a default makes "the caller chose 0"
indistinguishable from "the caller forgot", and nothing inside the function
can tell them apart.

THE SILENCE INDEX WILL NOT MOVE FOR THE CONNECTIONS, and that is not the
point. Umbra looks for empty, boundary and absent cases in a declared type,
and an object type has none, so `conn: Any` and `conn: FetchingConnection`
look the same to it. Typing the parameter is worth doing because the function
should say what it takes.
"""

import ast
import pathlib

import pytest

_MODULE = (
    pathlib.Path(__file__).resolve().parents[2]
    / "src"
    / "declaro_persistum"
    / "bulk_loader.py"
)


def _functions() -> dict[str, ast.FunctionDef | ast.AsyncFunctionDef]:
    tree = ast.parse(_MODULE.read_text())
    return {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _annotation(node, parameter: str) -> str | None:
    arguments = node.args
    for candidate in arguments.posonlyargs + arguments.args + arguments.kwonlyargs:
        if candidate.arg == parameter:
            return ast.unparse(candidate.annotation) if candidate.annotation else None
    return None


def test_the_scan_still_finds_the_functions() -> None:
    """The floor. A scan that matches nothing passes everything below it."""
    found = _functions()

    assert "postgres_read_rows" in found
    assert "dbapi_read_rows" in found
    assert len(found) > 20, f"only {len(found)} functions found"


@pytest.mark.parametrize(
    "name",
    sorted(n for n in _functions() if n.startswith("postgres_")),
)
def test_a_postgres_function_declares_an_asyncpg_connection(name: str) -> None:
    """These are handed an asyncpg connection and nothing else."""
    node = _functions()[name]

    for parameter in ("conn", "_conn"):
        declared = _annotation(node, parameter)
        if declared is None:
            continue
        assert declared == "FetchingConnection", (
            f"{name}({parameter}: {declared}) -- asyncpg answers fetch, "
            f"fetchrow, fetchval and transaction"
        )


@pytest.mark.parametrize(
    "name",
    sorted(n for n in _functions() if n.startswith("dbapi_")),
)
def test_a_dbapi_function_declares_a_cursor_connection(name: str) -> None:
    """These are handed aiosqlite or pyturso."""
    node = _functions()[name]

    declared = _annotation(node, "conn")
    assert declared == "DbapiConnection", (
        f"{name}(conn: {declared}) -- aiosqlite and pyturso return a cursor "
        f"from execute and take explicit commits"
    )


def test_no_connection_anywhere_is_still_any() -> None:
    """Including the callable Protocols, which accept either driver.

    A Protocol saying `conn: Any` describes nothing, so a loader that took a
    file handle would satisfy it.
    """
    offenders: list[str] = []
    for name, node in _functions().items():
        arguments = node.args
        for parameter in (
            arguments.posonlyargs + arguments.args + arguments.kwonlyargs
        ):
            if "conn" not in parameter.arg:
                continue
            declared = (
                ast.unparse(parameter.annotation) if parameter.annotation else "UNTYPED"
            )
            if declared.startswith("Any") or declared == "UNTYPED":
                offenders.append(f"{name}({parameter.arg}: {declared})")

    assert not offenders, f"{len(offenders)} undeclared connections: {offenders}"


def test_reading_a_page_of_rows_takes_no_implicit_default() -> None:
    """`offset` and `limit` must be passed, so a forgotten one is visible.

    A transfer that silently starts at row zero because a caller omitted the
    offset reads exactly like one that was asked to start there.
    """
    offenders: list[str] = []
    for name, node in _functions().items():
        arguments = node.args
        defaulted = {
            parameter.arg
            for parameter, default in zip(
                arguments.args[len(arguments.args) - len(arguments.defaults) :],
                arguments.defaults,
                strict=False,
            )
        } | {
            parameter.arg
            for parameter, default in zip(
                arguments.kwonlyargs, arguments.kw_defaults, strict=False
            )
            if default is not None
        }
        offenders += [f"{name}({p})" for p in sorted(defaulted & {"offset", "limit"})]

    assert not offenders, f"{len(offenders)} implicit defaults: {offenders}"
