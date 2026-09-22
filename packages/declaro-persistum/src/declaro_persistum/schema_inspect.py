"""What the database currently holds, asked once and compared afterwards.

WHY IT EXISTS. The models say what should exist. This says what does exist.
`fn difference` compares the two and works out the operations that close the
gap, and `orchestrator fn session_connect` reaches this on every open. Without
it there is no migration, because nothing knows what is already there.

WHAT IT MAY NOT DO, AND THIS IS THE WHOLE OF IT. `docs/architecture.md`, under
"Engines are data, not branches": introspection tells you what is, and it cannot
tell you what should be. So nothing here decides what belongs. It reads, it
answers, and `fn schema_from_declared` builds the target out of the models with
no parameter a live schema could arrive through. A table this function finds and
no model declares reaches `fn difference` as something to drop, never as
something to adopt.

THREE ENGINES ARE A TABLE. PostgreSQL keeps its description in
`information_schema` and `pg_catalog`; SQLite and Turso keep theirs in pragmas
and in the text of the CREATE TABLE. That is two shapes of reading, and
`_READERS` below has a row per engine rather than a branch per engine. An engine
with no row comes back as a refusal naming what arrived.

TURSO IS NOT A libSQL FORK, so every pragma this file runs was run against
pyturso before it was written down. Measured 2026-09-22: `table_info`,
`foreign_key_list`, `index_list`, `index_info` and a read of `sqlite_master` all
answer, and `foreign_key_list` reports `ON DELETE CASCADE` in the same column
SQLite does. One difference bites: Turso folds a stored table name to lower
case, so a table created as `Par"ent` is listed as `par"ent`, while the CREATE
TABLE text it kept still spells the name the way it was written. This function
reports the name the engine lists, which is the name every later statement has
to use.

THE QUOTING IS WHAT BITES. A pragma takes an identifier in its text and not as a
bound value, so the name is quoted on the way in and a double quote inside it is
doubled. Get that wrong and half the names come back broken and the differ
rebuilds a table on every deploy.

A DECLARED TYPE COMES BACK IN LOWER CASE. A model writes `text` and the engines
echo `TEXT` and `text`, and `fn difference` compares the declared type as a
string, so a column that never changed would look changed on one engine and not
on the other.

A RULE COMES BACK AS A CONDITION, NEVER AS A STRING. A CHECK is data once it is
read, so `Constraint.rule` holds a `Condition`. What this can read is one
comparison between a column and a literal, which is the shape the models
produce. A rule outside that shape comes back as a refusal naming the table and
the text, because a rule silently dropped reads to the differ as a rule the
database has not got, and the next migration writes it again.

TWO THINGS IT DOES NOT READ, said out loud rather than left to be found.
A live view is not read, so `Schema.views` is always empty here: a `View` carries
a `Declaration`, and a declaration cannot be recovered from the text of a view.
A column's default is not read either, so a changed default is invisible to the
comparison. Neither is a decision; both are work nobody has done.

A FAILURE COMES BACK IN THE RETURN. A driver that refuses a pragma, a server
that drops the connection, a rule nobody can read: each one is a `Fault` the
caller is handed, never an exception and never a log line.
"""

from __future__ import annotations

import itertools
import re
from collections.abc import Callable, Coroutine
from typing import Any

from declaro_persistum.types import (
    Column,
    Comparison,
    Condition,
    Connection,
    Constraint,
    Database,
    Engine,
    Fault,
    Index,
    OnDelete,
    Operator,
    Reference,
    Schema,
    Table,
    Value,
)

# What a reader hands back: the tables it read, and one sentence for every
# thing it could not read. Both travel together so a partial answer can never
# be mistaken for a whole one.
Reading = tuple[list[Table], list[str]]

# A value nothing else can be. Compared by identity, so no literal is it.
UNREADABLE = object()


# --- the smallest pieces ----------------------------------------------------


def _quoted(name: str) -> str:
    """One identifier, as an engine reads it inside a statement's text."""
    doubled = name.replace('"', '""')
    return f'"{doubled}"'


def _bare(name: str) -> str:
    """The name inside an identifier an engine quoted back at us."""
    stripped = name.strip()
    inner = stripped[1:-1].replace('""', '"')
    return {True: inner, False: stripped}[stripped.startswith('"') and stripped.endswith('"')]


# --- a rule, read out of the text an engine kept ----------------------------
#
# WHY IT IS PARSED AT ALL. An engine keeps a CHECK as the words somebody wrote,
# and `Constraint.rule` is a `Condition`, which is data. Something has to turn
# one into the other, and this is the only place that holds the words.

_COMPARISONS: dict[str, Operator] = {
    "<=": "lte",
    ">=": "gte",
    "<>": "ne",
    "!=": "ne",
    "=": "eq",
    "<": "lt",
    ">": "gt",
}

# Longest first, so `<=` is never read as `<` followed by `=`.
_IN_ORDER = sorted(_COMPARISONS, key=len, reverse=True)


_NESTING = {"(": 1, ")": -1}


def _changes_depth(character: str) -> int:
    """A parenthesis, which opens or closes one level."""
    return _NESTING[character]


def _changes_nothing(_character: str) -> int:
    """Every other character. That is a rule about them, not a gap in the table
    above, so it is a row of its own rather than a default on the read."""
    return 0


_DEEPER_OR_NOT: dict[bool, Callable[[str], int]] = {True: _changes_depth, False: _changes_nothing}


def _closes_at(text: str) -> int:
    """Where the parenthesis opening this text is closed, or -1 if it is not."""
    depths = itertools.accumulate(
        _DEEPER_OR_NOT[character in _NESTING](character) for character in text
    )
    return next(
        (
            place
            for place, (character, depth) in enumerate(zip(text, depths, strict=True))
            if character == ")" and depth == 0
        ),
        -1,
    )


def _unwrapped(text: str) -> str:
    """The same rule with the parentheses wrapping the whole of it taken off.

    PostgreSQL hands back `((score > 0))` for a rule written `(score > 0)`, and
    this is what makes both spellings the same rule.
    """
    settled = text.strip()
    while settled.startswith("(") and _closes_at(settled) == len(settled) - 1:
        settled = settled[1:-1].strip()
    return settled


def _split_at(rule: str) -> tuple[str, str, str] | None:
    """The rule as its left side, its operator and its right side."""
    found = [
        (place, spelling)
        for place in range(len(rule))
        for spelling in _IN_ORDER
        if rule.startswith(spelling, place)
    ]
    return next(
        ((rule[:place], spelling, rule[place + len(spelling) :]) for place, spelling in found[:1]),
        None,
    )


def _a_string(text: str) -> Value:
    """A quoted literal, with the doubled quote inside it undoubled."""
    return {True: text[1:-1].replace("''", "'")}[
        len(text) >= 2 and text.startswith("'") and text.endswith("'")
    ]


def _a_truth(text: str) -> Value:
    return {"true": True, "false": False}[text.lower()]


# EACH ROW IS ONE KIND OF LITERAL A RULE CAN COMPARE AGAINST, tried in turn. A
# number reads itself, so `int` and `float` are the rows rather than two wrappers
# whose only content was the word inside them.
_THE_LITERALS: tuple[Callable[[str], Value], ...] = (int, float, _a_truth, _a_string)


def _literal(text: str) -> Any:
    """The value a rule compares against, or `UNREADABLE`."""
    settled = _unwrapped(text)
    for read in _THE_LITERALS:
        try:
            return read(settled)
        except (ValueError, KeyError):
            continue
    return UNREADABLE


def _nothing_to_read(_parts: Any) -> Any:
    """No operator anywhere in the rule, so there is no comparison in it."""
    return UNREADABLE


def _one_comparison(parts: tuple[str, str, str]) -> Any:
    """The rule as a condition, where both sides read as a column and a value."""
    left, spelling, right = parts
    column = _bare(_unwrapped(left))
    value = _literal(right)
    return {
        True: UNREADABLE,
        False: Condition(
            each={
                column: Comparison(operator=_COMPARISONS[spelling], value=value, ignore_case=False)
            },
            any_of=[],
            none_of=[],
        ),
    }[value is UNREADABLE or not column.isidentifier()]


_READ_THE_PARTS: dict[bool, Callable[[Any], Any]] = {False: _one_comparison, True: _nothing_to_read}


def _condition_from(rule: str) -> Any:
    """One CHECK, as the condition it states, or `UNREADABLE`.

    WHAT IT READS: one column compared with one literal, which is the rule a
    model declares. Anything wider, such as a cast, a function call or two
    comparisons joined, is handed back as unreadable rather than as an
    approximation, because an approximate rule is one the differ keeps
    rewriting.
    """
    parts = _split_at(_unwrapped(rule))
    return _READ_THE_PARTS[parts is None](parts)


def _a_complaint(table: str, name: str, rule: str, _read: Any) -> Any:
    """Why one rule could not be turned into a condition."""
    return (
        f"{table} declares the rule {name or '(unnamed)'} as {rule!r}, and this "
        f"reads one column compared with one literal and nothing wider"
    )


def _a_constraint(_table: str, name: str, _rule: str, read: Condition) -> Any:
    """One rule the database holds, as the constraint it is."""
    return Constraint(name=name, kind="check", columns=list(read["each"]), rule=read)


_A_RULE: dict[bool, Callable[[str, str, str, Any], Any]] = {
    False: _a_constraint,
    True: _a_complaint,
}


def _rules_read(table: str, declared: list[tuple[str, str]]) -> tuple[list[Constraint], list[str]]:
    """Every CHECK a table declares, as constraints and as complaints."""
    answers = [
        _A_RULE[read is UNREADABLE](table, name, rule, read)
        for name, rule in declared
        for read in [_condition_from(rule)]
    ]
    return (
        [answer for answer in answers if not isinstance(answer, str)],
        [answer for answer in answers if isinstance(answer, str)],
    )


# --- SQLite and Turso: pragmas, and the text the engine kept ----------------

_THE_TABLES = (
    "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
)

# `foreign_key_list` spells what a delete does in the engine's own words.
_PRAGMA_ON_DELETE: dict[str, OnDelete] = {
    "NO ACTION": "refuse",
    "RESTRICT": "refuse",
    "CASCADE": "cascade",
    "SET NULL": "set_null",
}

# `CHECK (` and, where somebody named it, the `CONSTRAINT <name>` before it.
_A_CHECK = re.compile(r"\bCHECK\s*\(", re.IGNORECASE)
_A_NAME = re.compile(r'\bCONSTRAINT\s+("(?:[^"]|"")*"|[A-Za-z_][A-Za-z_0-9]*)\s*$', re.IGNORECASE)


def _the_name_found(found: re.Match[str]) -> str:
    return _bare(found.group(1))


def _unnamed(_found: Any) -> str:
    """SQLite keeps no name for a CHECK nobody named, so neither does this."""
    return ""


_NAMED: dict[bool, Callable[[Any], str]] = {False: _the_name_found, True: _unnamed}


def _checks_in(ddl: str) -> list[tuple[str, str]]:
    """Every CHECK the kept CREATE TABLE declares, as name and rule."""
    found = [
        (match.end() - 1, _A_NAME.search(ddl[: match.start()]))
        for match in _A_CHECK.finditer(ddl or "")
    ]
    return [
        (_NAMED[named is None](named), ddl[opens + 1 : opens + closes])
        for opens, named in found
        for closes in [_closes_at(ddl[opens:])]
        if closes > 0
    ]


def _the_key_found(pointing: dict[Any, Reference], where: Any) -> Reference | None:
    """The column points at a parent, and this is where."""
    return pointing[where]


def _points_at_nothing(_pointing: dict[Any, Reference], _where: Any) -> Reference | None:
    """The column points at nothing, which most columns do. That is a rule about
    them rather than a gap in the keys a table declares, so it is a row."""
    return None


_POINTING: dict[bool, Callable[[dict[Any, Reference], Any], Reference | None]] = {
    True: _the_key_found,
    False: _points_at_nothing,
}


def _rows(connection: Connection, statement: str) -> list[Any]:
    """Whatever the driver hands back for one statement, as a list."""
    return list(connection.execute(statement).fetchall())


def _pragma_columns(connection: Connection, name: str) -> list[Column]:
    """Every column a table holds, as the pragma describes it."""
    described = _rows(connection, f"PRAGMA table_info({_quoted(name)})")
    pointing = {
        row[3]: Reference(
            table=row[2],
            column=row[4],
            on_delete=_PRAGMA_ON_DELETE[str(row[6]).upper()],
        )
        for row in _rows(connection, f"PRAGMA foreign_key_list({_quoted(name)})")
    }
    return [
        Column(
            name=row[1],
            declared_type=str(row[2]).lower(),
            renamed_from=None,
            references=_POINTING[row[1] in pointing](pointing, row[1]),
            facets={"nullable": row[3] == 0, "primary_key": row[5] > 0},
        )
        for row in described
    ]


def _pragma_indexes(connection: Connection, name: str) -> list[Index]:
    """Every index on a table, less the one the engine made to hold its key."""
    listed = [
        row for row in _rows(connection, f"PRAGMA index_list({_quoted(name)})") if row[3] != "pk"
    ]
    return [
        Index(
            name=row[1],
            columns=[
                part[2]
                for part in sorted(_rows(connection, f"PRAGMA index_info({_quoted(row[1])})"))
                if part[2] is not None
            ],
            unique=row[2] == 1,
        )
        for row in listed
    ]


async def _read_pragmas(connection: Connection) -> Reading:
    """SQLite and Turso, which describe themselves the same way."""
    kept = {row[0]: row[1] or "" for row in _rows(connection, _THE_TABLES)}
    read = {name: _rules_read(name, _checks_in(ddl)) for name, ddl in kept.items()}
    return (
        [
            Table(
                name=name,
                columns=_pragma_columns(connection, name),
                indexes=_pragma_indexes(connection, name),
                constraints=read[name][0],
            )
            for name in kept
        ],
        [complaint for name in kept for complaint in read[name][1]],
    )


# --- PostgreSQL: information_schema and pg_catalog --------------------------

_PG_TABLES = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
ORDER BY table_name
"""

_PG_COLUMNS = """
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = current_schema()
ORDER BY table_name, ordinal_position
"""

_PG_KEYS = """
SELECT cl.relname AS child, att.attname AS child_column,
       parent.relname AS parent, patt.attname AS parent_column,
       con.confdeltype AS on_delete
FROM pg_constraint con
JOIN pg_class cl ON cl.oid = con.conrelid
JOIN pg_class parent ON parent.oid = con.confrelid
JOIN unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
JOIN unnest(con.confkey) WITH ORDINALITY AS f(attnum, ord) ON f.ord = k.ord
JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
JOIN pg_attribute patt ON patt.attrelid = con.confrelid AND patt.attnum = f.attnum
WHERE con.contype = 'f' AND cl.relnamespace = current_schema()::regnamespace
"""

_PG_RULES = """
SELECT cl.relname AS table_name, con.conname AS name, con.contype AS kind,
       pg_get_constraintdef(con.oid) AS rule,
       (SELECT array_agg(a.attname ORDER BY k.ord)
          FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum) AS columns
FROM pg_constraint con
JOIN pg_class cl ON cl.oid = con.conrelid
WHERE cl.relnamespace = current_schema()::regnamespace AND con.contype IN ('c', 'p')
"""

_PG_INDEXES = """
SELECT cl.relname AS table_name, icl.relname AS name, ix.indisunique AS unique,
       (SELECT array_agg(a.attname ORDER BY k.ord)
          FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum) AS columns
FROM pg_index ix
JOIN pg_class cl ON cl.oid = ix.indrelid
JOIN pg_class icl ON icl.oid = ix.indexrelid
WHERE cl.relnamespace = current_schema()::regnamespace AND NOT ix.indisprimary
"""

# `pg_constraint.confdeltype`, one letter per thing a delete does to the child.
_PG_ON_DELETE: dict[str, OnDelete] = {"a": "refuse", "r": "refuse", "c": "cascade", "n": "set_null"}

# The rule kinds this reads: a CHECK becomes a constraint, a primary key becomes
# a facet on each column it covers.
_PG_CHECK = "c"
_PG_PRIMARY_KEY = "p"


# asyncpg hands back `"char"` as bytes and other drivers as a string, so each
# row reads the letter its own way rather than one wrapper doing both.
_THE_LETTER: dict[bool, Callable[[Any], str]] = {True: bytes.decode, False: str}


def _letter(value: Any) -> str:
    """One catalog letter, whichever of bytes or str the driver hands back."""
    return _THE_LETTER[isinstance(value, bytes)](value)


async def _read_catalogs(connection: Connection) -> Reading:
    """PostgreSQL, which keeps its description in tables of its own."""
    names = [row["table_name"] for row in await connection.fetch(_PG_TABLES)]
    described = await connection.fetch(_PG_COLUMNS)
    keys = await connection.fetch(_PG_KEYS)
    rules = await connection.fetch(_PG_RULES)
    indexes = await connection.fetch(_PG_INDEXES)

    pointing = {
        (row["child"], row["child_column"]): Reference(
            table=row["parent"],
            column=row["parent_column"],
            on_delete=_PG_ON_DELETE[_letter(row["on_delete"])],
        )
        for row in keys
    }
    in_the_key = {
        (row["table_name"], column)
        for row in rules
        if _letter(row["kind"]) == _PG_PRIMARY_KEY
        for column in (row["columns"] or [])
    }
    read = {
        name: _rules_read(
            name,
            [
                (row["name"], row["rule"][len("CHECK") :])
                for row in rules
                if _letter(row["kind"]) == _PG_CHECK and row["table_name"] == name
            ],
        )
        for name in names
    }
    return (
        [
            Table(
                name=name,
                columns=[
                    Column(
                        name=row["column_name"],
                        declared_type=str(row["data_type"]).lower(),
                        renamed_from=None,
                        references=_POINTING[(name, row["column_name"]) in pointing](
                            pointing, (name, row["column_name"])
                        ),
                        facets={
                            "nullable": row["is_nullable"] == "YES",
                            "primary_key": (name, row["column_name"]) in in_the_key,
                        },
                    )
                    for row in described
                    if row["table_name"] == name
                ],
                indexes=[
                    Index(
                        name=row["name"],
                        columns=list(row["columns"] or []),
                        unique=bool(row["unique"]),
                    )
                    for row in indexes
                    if row["table_name"] == name
                ],
                constraints=read[name][0],
            )
            for name in names
        ],
        [complaint for name in names for complaint in read[name][1]],
    )


# --- one row per engine, and no fallback behind it --------------------------

_READERS: dict[Engine, Callable[[Connection], Coroutine[Any, Any, Reading]]] = {
    "sqlite": _read_pragmas,
    "turso": _read_pragmas,
    "postgresql": _read_catalogs,
}


def _described(tables: list[Table], _complaints: list[str]) -> Schema | Fault:
    """Everything read, and nothing it could not read."""
    return Schema(tables=tables, views=[])


def _refuse_the_rules(_tables: list[Table], complaints: list[str]) -> Schema | Fault:
    """A rule that came back as words nobody can turn into a condition."""
    return Fault(
        kind="refused",
        what_went_wrong="The database holds rules this cannot read: " + "; ".join(complaints) + ".",
        what_to_do_instead=(
            "Write each rule as one column compared with one literal, such as "
            "CHECK (score > 0), or take it off the table and let a model declare it."
        ),
    )


_ANSWER: dict[bool, Callable[[list[Table], list[str]], Schema | Fault]] = {
    False: _described,
    True: _refuse_the_rules,
}


def _no_such_engine(engine: Any) -> Fault:
    """A word with no row in the table above, named back at the caller."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A database arrived saying its engine is {engine!r}, and nothing here "
            f"knows how to ask that engine what it holds."
        ),
        what_to_do_instead=(
            f"Open the database as one of {', '.join(sorted(_READERS))}, or add a row "
            f"for {engine!r} beside the others."
        ),
    )


def _refuse_the_driver(engine: Any, refused: BaseException) -> Fault:
    """The engine, or the driver in front of it, would not answer."""
    return Fault(
        kind="cut_short",
        what_went_wrong=(
            f"The {engine} database was asked what it holds and answered "
            f"{type(refused).__name__}: {refused}."
        ),
        what_to_do_instead=(
            "Check the connection is open and that this user may read the "
            "database's own description, then ask again."
        ),
    )


async def schema_inspect(db: Database) -> Schema | Fault:
    """Ask one database what it currently holds.

    The engine is on the `Database` already, so the connection is handed to the
    driver and never asked what it is. Every failure — an engine with no row, a
    driver that refuses, a rule nobody can read — comes back as the return
    value.
    """
    engine = db["engine"]
    try:
        read = _READERS[engine]
    except KeyError:
        return _no_such_engine(engine)

    try:
        tables, complaints = await read(db["connection"])
    except Exception as refused:  # noqa: BLE001
        return _refuse_the_driver(engine, refused)

    return _ANSWER[bool(complaints)](tables, complaints)
