"""`spell_operation`: one schema change into the statements that carry it out.

THE SCHEMA HALF'S COMPILER, AND ONE OF THE FEW PLACES A `Engine` IS HANDED IN.
An operation says what must change and names no engine. This is where the
change becomes SQL, so this is where PostgreSQL, SQLite and Turso may be named
at all. Everything upstream -- the models, the difference between two schemas,
the order the operations run in -- is engine-free, which is what lets one
declaration serve three engines.

WHY IT HANDS BACK A LIST. One change is often several statements. SQLite cannot
alter a column in place, so the change is a new table, the rows copied into it,
the old table gone and the new one renamed into its place. That is one operation
and five statements, and they come back in the order they have to run.

WHY IT REFUSES RATHER THAN GUESSES. A refusal is returned, never raised and
never logged. Five of them: an engine nobody declared, a kind of change nobody
declared, a change missing something its spelling needs, a declared type this
package has no storage class for, and a word out of one of the small
vocabularies this engine has no row for. Each one names what arrived and what to
write instead.

WHY THERE ARE NO BRANCHES. Three engines and thirteen kinds of change are a
table, and every row is present. A key with no row is a fault naming the key,
never a fallback answering for an input nobody wrote a rule for. The three
constraint kinds and the three deletion rules are tables for the same reason.

A VALUE IS BOUND AND AN IDENTIFIER IS QUOTED. A table or a column name is
written into the statement text, quoted for the engine, because no driver binds
one. Everything else travels beside the statement in `parameters` for the driver
to bind. The one exception is a DEFAULT clause and a CHECK clause, which no
engine will take a placeholder in: those carry a literal, quoted here, and they
carry what the author declared rather than anything a caller typed.

WHERE A REBUILD GETS THE TABLE FROM, SINCE 2026-09-21. `Operation` carries
`columns`, `indexes`, `constraints` and `copying`, so the new table is built
from the columns and constraints the operation hands over, the rows are copied
in the columns `copying` names, and the indexes are made again after the rename
because SQLite drops an index with the table it is on. Until that day this was
spelled from two strings the caller supplied, `definition` and `keeping`, which
this function invented because the type was short. Both are gone: a structure
squeezed into a string is a structure nothing can check.

WHAT THE WIDER TYPES BOUGHT, SINCE 2026-09-21. Three things this function could
not say are now rows in tables here.

A TABLE-LEVEL PRIMARY KEY. `Constraint` carries a `kind`, so a rule that names
columns is spelled as `PRIMARY KEY (a, b)` on the table and a rule that reads
comparisons is spelled as a CHECK. Which kind it is arrives with the
constraint rather than being guessed from its shape.

A RULE ABOUT WHAT HAPPENS TO A CHILD ROW. `Column.references` is a `Reference`,
which names the table, the column and what a delete does, so ON DELETE CASCADE
is written from what the declaration said rather than from a string this
function took apart.

TAKING A VALUE OUT OF A LOOKUP. `remove_enum_values` is a word `OperationKind`
carries, and it is the same function that puts one in with a different
statement. A value a row still holds is refused by the foreign key on the
column, in the engine, where the rows are.

WHAT STILL CANNOT BE SPELLED, named here because a silent gap is worse than a
loud one.

A KEY ON MORE THAN ONE COLUMN, DECLARED AS A FACET. `primary_key` is also a
facet of a `Column`, so two columns can each claim to be the key, and
`column_definition` writes an inline PRIMARY KEY for each of them. The table
level constraint is the shape that works, and nothing here refuses the other
one.

THE PRAGMA THAT MAKES A REBUILD POSSIBLE BELONGS TO WHOEVER COMMITS. Dropping a
table another table points at is refused while foreign keys are enforced, so the
applier turns enforcement off around a rebuild and checks the keys before it
commits. `PRAGMA foreign_keys` is a no-op inside a transaction, so it cannot be
one of the statements returned here, and this function emits none.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args

from declaro_persistum.types import (
    NOTHING,
    Column,
    Constraint,
    ConstraintKind,
    Engine,
    Fault,
    Index,
    OnDelete,
    Operation,
    OperationKind,
    Reference,
    Statement,
    Value,
)

THE_ENGINES = get_args(Engine)
THE_KINDS = get_args(OperationKind)

# The name a rebuild copies rows into, and the name orphan recovery looks for.
# It carries no random tail: this function reads no random source, so a rebuild
# drops a leftover temporary table before it makes one instead of telling two of
# them apart.
TEMPORARY_PREFIX = "_declaro_tmp_"

# The library's own tables all start with an underscore, and the engines' own
# start with their name. The user's tables are what is left.


# --- refusals ----------------------------------------------------------------


def _refused(went_wrong: str, do_instead: str) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": went_wrong,
        "what_to_do_instead": do_instead,
    }


def _no_such_engine(engine: str) -> Fault:
    return _refused(
        f"You asked for {engine}, and this package ships {', '.join(THE_ENGINES)}.",
        f"Write the call again naming one of {', '.join(THE_ENGINES)}.",
    )


def _no_such_type(declared: str, engine: str) -> Fault:
    return _refused(
        f"A column is declared {declared!r}, and this package has no {engine} "
        f"storage class for it.",
        f"Declare the column as one of {', '.join(sorted(SQLITE_TYPES))}.",
    )


def _is_fault(piece: object) -> bool:
    """Whether a piece came back refused rather than spelled."""
    return isinstance(piece, dict) and set(piece) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


def _the_first_fault_or[Built](pieces: list[Any], build: Callable[[], Built]) -> Built | Fault:
    """A refusal anywhere in the pieces is the answer; otherwise build one.

    A piece is a type this package could not spell, a default it could not
    write, or a column whose facets it does not know. Any one of them is the
    answer, because the statement it would have gone into cannot be written.
    """
    faults = cast("list[Fault]", list(filter(_is_fault, pieces)))
    answer: dict[bool, Callable[[], Built | Fault]] = {True: lambda: faults[0], False: build}
    return answer[faults != []]()


def _for_engine(engine: str, known: Callable[[], Any]) -> Any:
    """Answer for an engine this package ships, and refuse any other."""
    return {True: known, False: lambda: _no_such_engine(engine)}[engine in THE_ENGINES]()


def _one_of(found: list[Value], absent: Value) -> Value:
    """The value found, or the named absence when nothing was found."""
    return ([absent] + found)[len(found)]


def _present(detail: dict[str, Value], key: str) -> list[Value]:
    """What a key holds, as a list, so an absent key is an empty one."""
    return [detail[name] for name in filter(detail.__contains__, [key])]


# --- identifiers -------------------------------------------------------------


def _quoted(name: str) -> str:
    """A name, quoted so a name carrying a quote cannot end the identifier."""
    return '"' + name.replace('"', '""') + '"'


# ONE ROW PER ENGINE, THOUGH ALL THREE SPELL IT THE SAME WAY TODAY. Folding
# them into one row would make the next difference between them an edit to the
# shape of this table rather than an edit to one row.
QUOTE: dict[Engine, Callable[[str], str]] = {
    "sqlite": _quoted,
    "postgresql": _quoted,
    "turso": _quoted,
}


# --- naming a type -----------------------------------------------------------
#
# WHAT THE AUTHOR DECLARES IS ON THE LEFT, AND WHAT THE ENGINE STORES IT IN IS
# ON THE RIGHT. The two tables carry the same keys, so a declared type either
# has a spelling on all three engines or has none anywhere.

SQLITE_TYPES: dict[str, str] = {
    "uuid": "TEXT",
    "text": "TEXT",
    "json": "TEXT",
    "date": "TEXT",
    "time": "TEXT",
    "datetime": "TEXT",
    "decimal": "TEXT",
    "integer": "INTEGER",
    "boolean": "INTEGER",
    "float": "REAL",
    "bytes": "BLOB",
}

POSTGRESQL_TYPES: dict[str, str] = {
    "uuid": "uuid",
    "text": "text",
    "json": "jsonb",
    "date": "date",
    "time": "time",
    "datetime": "timestamptz",
    "decimal": "numeric",
    "integer": "integer",
    "boolean": "boolean",
    "float": "double precision",
    "bytes": "bytea",
}


# TURSO STORES WHAT SQLITE STORES, AND KEEPS A ROW OF ITS OWN, so the next
# difference between the two is an edit to one row rather than to this shape.
TYPES: dict[Engine, dict[str, str]] = {
    "sqlite": SQLITE_TYPES,
    "postgresql": POSTGRESQL_TYPES,
    "turso": SQLITE_TYPES,
}


def type_spelled(declared: str, engine: str) -> str | Fault:
    """A declared type in one engine's words: `uuid` is `TEXT` on SQLite.

    ONE FUNCTION AND A TABLE, NOT ONE FUNCTION PER ENGINE. A speller per engine
    is the same five lines three times, and the only thing that differs between
    them is which table they read.
    """

    def looked_up() -> str | Fault:
        table = TYPES[cast(Engine, engine)]
        wanted = declared.lower()
        answer: dict[bool, Callable[[], str | Fault]] = {
            True: lambda: _no_such_type(declared, engine),
            False: lambda: table[wanted],
        }
        return answer[wanted not in table]()

    return cast("str | Fault", _for_engine(engine, looked_up))


# --- naming a value a statement cannot bind ----------------------------------
#
# A DEFAULT AND A CHECK ARE THE TWO PLACES A LITERAL HAS TO REACH THE TEXT. No
# engine takes a placeholder in either, and what goes there is what the author
# declared rather than anything a caller typed.

BOOLEAN_LITERAL: dict[Engine, dict[bool, str]] = {
    "sqlite": {True: "1", False: "0"},
    "postgresql": {True: "TRUE", False: "FALSE"},
    "turso": {True: "1", False: "0"},
}


def _text_literal(value: Value, _engine: Engine) -> str:
    """A declared string, quoted, with any quote of its own doubled.

    A string wrapped in parentheses is an expression the author wrote --
    `(now())` -- and goes in as it stands.
    """
    text = cast(str, value)
    written: dict[bool, Callable[[], str]] = {
        True: lambda: text,
        False: lambda: "'" + text.replace("'", "''") + "'",
    }
    return written[text.startswith("(") and text.endswith(")")]()


LITERAL: dict[type, Callable[[Value, Engine], str]] = {
    str: _text_literal,
    int: lambda value, _engine: str(value),
    float: lambda value, _engine: repr(value),
    bool: lambda value, engine: BOOLEAN_LITERAL[engine][cast(bool, value)],
    type(None): lambda _value, _engine: "NULL",
}


def _literal(value: Value, engine: Engine) -> str | Fault:
    """A declared value as the text of a clause nothing can bind."""
    answer: dict[bool, Callable[[], str | Fault]] = {
        True: lambda: _refused(
            f"A default of {value!r} is a {type(value).__name__}, which this "
            f"package cannot write into a {engine} clause.",
            "Declare the default as text, a number, a boolean or nothing.",
        ),
        False: lambda: LITERAL[type(value)](value, engine),
    }
    return answer[type(value) not in LITERAL]()


def default_clause(value: Value, engine: str) -> str | Fault:
    """The text that follows DEFAULT in one engine's column definition.

    ONE FUNCTION AND A TABLE, NOT ONE PER ENGINE. What a SQLite default and a
    PostgreSQL default differ by is how each writes a boolean, which is a row in
    the table above.
    """

    def spelled() -> str | Fault:
        literal = _literal(value, cast(Engine, engine))
        return cast("str | Fault", _the_first_fault_or([literal], lambda: f"DEFAULT {literal}"))

    return cast("str | Fault", _for_engine(engine, spelled))


# --- one column, and one constraint ------------------------------------------

FACET_CLAUSE: dict[str, Callable[[Value, Engine], str | Fault]] = {
    "primary_key": lambda value, _engine: {True: "PRIMARY KEY", False: ""}[value is True],
    "nullable": lambda value, _engine: {True: "", False: "NOT NULL"}[value is True],
    "unique": lambda value, _engine: {True: "UNIQUE", False: ""}[value is True],
    "default": default_clause,
}

# The order a CREATE TABLE reads them in.
FACET_ORDER = ("primary_key", "nullable", "unique", "default")


# WHAT EACH ENGINE WRITES FOR A DECLARED DELETION RULE. `refuse` is what a
# declaration means when it says nothing about the parent: the delete stops and
# the child rows stay, which SQL spells NO ACTION.
#
# ALL THREE WRITE THE STANDARD WORDS TODAY AND EACH KEEPS A ROW, so the next
# engine that spells one of them differently is an edit to one row rather than
# to the shape of this table.
STANDARD_ON_DELETE: dict[OnDelete, str] = {
    "refuse": "ON DELETE NO ACTION",
    "cascade": "ON DELETE CASCADE",
    "set_null": "ON DELETE SET NULL",
}

ON_DELETE: dict[Engine, dict[OnDelete, str]] = {
    "sqlite": STANDARD_ON_DELETE,
    "postgresql": STANDARD_ON_DELETE,
    "turso": STANDARD_ON_DELETE,
}


def _reference_clause(reference: Reference, engine: Engine) -> str | Fault:
    """`REFERENCES "authors"("id") ON DELETE CASCADE`, from what a column points at.

    THE RULE ABOUT THE CHILD ROW IS CARRIED, NOT PARSED. A `Reference` names the
    table, the column and what a delete does, so nothing here splits a string
    apart to find out where the key points.
    """
    quote = QUOTE[engine]
    rules = ON_DELETE[engine]
    declared = reference["on_delete"]
    written: dict[bool, Callable[[], str | Fault]] = {
        True: lambda: _refused(
            f"A column points at {reference['table']}.{reference['column']} and "
            f"declares {declared!r}, which this package cannot write into a "
            f"{engine} definition.",
            f"Declare the deletion rule as one of {', '.join(rules)}.",
        ),
        False: lambda: f"REFERENCES {quote(reference['table'])}"
        f"({quote(reference['column'])}) {rules[declared]}",
    }
    return written[declared not in rules]()


def column_definition(column: Column, engine: str) -> str | Fault:
    """One column as a CREATE TABLE carries it: its name, its type, its facets."""

    def spelled() -> str | Fault:
        known = cast(Engine, engine)
        unknown = list(filter(lambda facet: facet not in FACET_CLAUSE, column["facets"]))
        return {
            True: lambda: _refused(
                f"The column {column['name']!r} declares {', '.join(unknown)}, "
                f"which this package cannot write into a {engine} definition.",
                f"Declare only {', '.join(FACET_ORDER)} on a column.",
            ),
            False: lambda: _definition(column, known),
        }[unknown != []]()

    return cast("str | Fault", _for_engine(engine, spelled))


def _definition(column: Column, engine: Engine) -> str | Fault:
    facets = column["facets"]
    pieces: list[Any] = [
        QUOTE[engine](column["name"]),
        type_spelled(column["declared_type"], engine),
        *[
            FACET_CLAUSE[facet](facets[facet], engine)
            for facet in filter(facets.__contains__, FACET_ORDER)
        ],
        *[
            _reference_clause(reference, engine)
            for reference in filter(None, [column["references"]])
        ],
    ]
    return _the_first_fault_or(pieces, lambda: " ".join(filter(None, pieces)))


COMPARISON_TEXT: dict[str, str] = {
    "eq": "=",
    "ne": "<>",
    "lt": "<",
    "lte": "<=",
    "gt": ">",
    "gte": ">=",
}


def _comparison(column: str, operator: str, value: Value, engine: Engine) -> str | Fault:
    """`"id" > 0`, with the value written out because a CHECK binds nothing."""
    literal = _literal(value, engine)
    return {
        True: lambda: _refused(
            f"A constraint compares {column!r} with {operator!r}, and a CHECK "
            f"clause on {engine} is written with {', '.join(sorted(COMPARISON_TEXT))}.",
            f"Write the rule with one of {', '.join(sorted(COMPARISON_TEXT))}.",
        ),
        False: lambda: _the_first_fault_or(
            [literal], lambda: f"{QUOTE[engine](column)} {COMPARISON_TEXT[operator]} {literal}"
        ),
    }[operator not in COMPARISON_TEXT]()


def _flat_check(constraint: Constraint, engine: Engine) -> str | Fault:
    comparisons: list[Any] = [
        _comparison(column, comparison["operator"], cast(Value, comparison["value"]), engine)
        for column, comparison in constraint["rule"]["each"].items()
    ]
    return _the_first_fault_or(
        comparisons,
        lambda: f"CONSTRAINT {QUOTE[engine](constraint['name'])} CHECK "
        f"({' AND '.join(comparisons)})",
    )


def _check(constraint: Constraint, engine: Engine) -> str | Fault:
    """A rule that reads its comparisons, which is the only kind that reads them."""
    rule = constraint["rule"]
    nested = rule["any_of"] + rule["none_of"]
    return {
        True: lambda: _refused(
            f"The constraint {constraint['name']!r} groups its comparisons "
            f"with any_of or none_of, and this package writes a flat CHECK.",
            "Write the rule as one comparison per column.",
        ),
        False: lambda: _flat_check(constraint, engine),
    }[nested != []]()


def _over_its_columns(word: str) -> Callable[[Constraint, Engine], str | Fault]:
    """A rule that reads the columns it names: a key, or a promise of uniqueness.

    ONE FUNCTION AND TWO ROWS. `PRIMARY KEY ("a", "b")` and `UNIQUE ("a", "b")`
    differ by the word in front of the columns, so the word is the argument and
    the rest is written once. Neither reads the rule, which is the condition
    that narrows by nothing.
    """

    def spelled(constraint: Constraint, engine: Engine) -> str | Fault:
        quote = QUOTE[engine]
        named = [quote(column) for column in constraint["columns"]]
        written: dict[bool, Callable[[], str | Fault]] = {
            True: lambda: _refused(
                f"The constraint {constraint['name']!r} is a {constraint['kind']} "
                f"and names no column, and {word} () is a statement no engine takes.",
                f"Send the constraint again naming the columns the {word} is over.",
            ),
            False: lambda: f"CONSTRAINT {quote(constraint['name'])} {word} ({', '.join(named)})",
        }
        return written[named == []]()

    return spelled


# WHICH KIND OF RULE IT IS SAYS HOW IT IS SPELLED. Before `Constraint` carried a
# kind, every constraint was read as a CHECK, so a key spanning two columns was
# two inline PRIMARY KEY clauses that no engine accepts.
CONSTRAINT_CLAUSE: dict[ConstraintKind, Callable[[Constraint, Engine], str | Fault]] = {
    "check": _check,
    "primary_key": _over_its_columns("PRIMARY KEY"),
    "unique": _over_its_columns("UNIQUE"),
}


def _no_such_constraint_kind(constraint: Constraint, engine: str) -> Fault:
    return _refused(
        f"The constraint {constraint['name']!r} says it is a "
        f"{constraint['kind']!r}, and this package writes "
        f"{', '.join(CONSTRAINT_CLAUSE)} into a {engine} definition.",
        f"Declare the constraint as one of {', '.join(CONSTRAINT_CLAUSE)}.",
    )


def constraint_clause(constraint: Constraint, engine: str) -> str | Fault:
    """One declared constraint as a CREATE TABLE carries it."""

    def spelled() -> str | Fault:
        known = cast(Engine, engine)
        kind = constraint["kind"]
        return {
            True: lambda: CONSTRAINT_CLAUSE[kind](constraint, known),
            False: lambda: _no_such_constraint_kind(constraint, engine),
        }[kind in CONSTRAINT_CLAUSE]()

    return cast("str | Fault", _for_engine(engine, spelled))


# --- the mark a driver reads -------------------------------------------------

PLACEHOLDER: dict[Engine, Callable[[int], str]] = {
    # asyncpg numbers its placeholders and reads nothing else.
    "postgresql": lambda position: f"${position}",
    # aiosqlite and pyturso take a question mark per value, in order.
    "sqlite": lambda _position: "?",
    "turso": lambda _position: "?",
}


def _marks(engine: Engine, count: int) -> list[str]:
    return [PLACEHOLDER[engine](position) for position in range(1, count + 1)]


def _statement(text: str, values: list[Value]) -> Statement:
    """A statement and the values that travel beside it, in the order they bind."""
    return {
        "text": text,
        "parameters": {f"p{position}": value for position, value in enumerate(values, start=1)},
    }


# --- the two statements a migration asks of every engine ---------------------


def temporary_name(table: str) -> str:
    """The name a rebuild copies rows into, for one table."""
    return f"{TEMPORARY_PREFIX}{table}"


# --- one change, one engine, the statements that carry it out ----------------


def _missing(operation: Operation, engine: Engine, wanted: tuple[str, ...]) -> Fault:
    absent = list(filter(lambda key: key not in operation["detail"], wanted))
    return _refused(
        f"A {operation['kind']} on {operation['table']} was handed to {engine} "
        f"without {', '.join(absent)}.",
        f"Send the operation again carrying {', '.join(absent)}.",
    )


def _needing(
    operation: Operation,
    engine: Engine,
    wanted: tuple[str, ...],
    build: Callable[[], list[Statement] | Fault],
) -> list[Statement] | Fault:
    """Spell the change when its detail carries what the spelling reads."""
    absent = list(filter(lambda key: key not in operation["detail"], wanted))
    return {
        True: lambda: _missing(operation, engine, wanted),
        False: build,
    }[absent != []]()


def _word(operation: Operation, key: str) -> str:
    """One piece of an operation's detail, which is a name or a clause."""
    return cast(str, operation["detail"][key])


def _empty_handed(operation: Operation, engine: Engine, field: str) -> Fault:
    return _refused(
        f"A {operation['kind']} on {operation['table']} was handed to {engine} "
        f"with an empty {field}.",
        f"Send the operation again carrying the {field} it acts on.",
    )


def _carrying(
    operation: Operation,
    engine: Engine,
    field: str,
    build: Callable[[], list[Statement] | Fault],
) -> list[Statement] | Fault:
    """Spell the change when the field its spelling reads has something in it.

    AN EMPTY LIST IS NOT AN ABSENCE HERE. Every field is present on every
    operation, so what a missing column list looks like is `[]`, and a table
    with no columns is a statement no engine takes.
    """
    held = cast("dict[str, list[Any]]", operation)[field]
    return {True: lambda: _empty_handed(operation, engine, field), False: build}[held == []]()


def _named_column(operation: Operation, engine: Engine) -> Column | Fault:
    """The column an add or an alter names, out of the columns it carries.

    An `add_column` carries the one column it adds. An `alter_column` carries
    the whole table as it will be afterwards, and `detail` says which of them
    changed, so one lookup answers for both.
    """
    wanted = _one_of(_present(operation["detail"], "column"), NOTHING)
    found = [column for column in operation["columns"] if column["name"] == wanted]
    answer: dict[bool, Callable[[], Column | Fault]] = {
        True: lambda: found[0],
        False: lambda: _refused(
            f"A {operation['kind']} on {operation['table']} names the column "
            f"{wanted!r}, and the columns it was handed to {engine} with are "
            f"{[column['name'] for column in operation['columns']]}.",
            f"Send the operation again with {wanted!r} among its columns.",
        ),
    }
    return answer[found != []]()


# A KEY ON ONE COLUMN IS THE COLUMN'S OWN BUSINESS; A KEY ON SEVERAL IS THE
# TABLE'S. SQL lets a single column say PRIMARY KEY beside its own type, and
# requires PRIMARY KEY (a, b) at the table level for more than one. Two columns
# each claiming the facet spells two inline clauses, which no engine parses.
#
# REFUSED RATHER THAN SPELLED, added 2026-09-21. Before this it came back as SQL
# and the engine rejected it, which tells a reader about a statement they did
# not write. The refusal names the columns and says where the key goes instead.
def _keyed_inline(columns: list[Column]) -> list[str]:
    return [column["name"] for column in columns if column["facets"].get("primary_key") is True]


def _too_many_keys(named: list[str], table: str) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"{table} declares {', '.join(named)} as the primary key, and a key "
            f"on more than one column cannot be written beside each of them."
        ),
        "what_to_do_instead": (
            f"Take primary_key off those columns and declare one constraint of "
            f"kind primary_key naming {', '.join(named)} together."
        ),
    }


def _spelled_body(operation: Operation, engine: Engine) -> str | Fault:
    pieces: list[Any] = [
        *[column_definition(column, engine) for column in operation["columns"]],
        *[constraint_clause(constraint, engine) for constraint in operation["constraints"]],
    ]
    return cast("str | Fault", _the_first_fault_or(pieces, lambda: ", ".join(pieces)))


_A_BODY_OR_A_REFUSAL: dict[bool, Callable[[Operation, Engine, list[str]], str | Fault]] = {
    False: lambda operation, engine, _named: _spelled_body(operation, engine),
    True: lambda operation, _engine, named: _too_many_keys(named, operation["table"]),
}


def _table_body(operation: Operation, engine: Engine) -> str | Fault:
    """What goes between the parentheses of a CREATE TABLE: every column, then
    every constraint, in the order the operation carries them."""
    named = _keyed_inline(operation["columns"])
    return _A_BODY_OR_A_REFUSAL[len(named) > 1](operation, engine, named)


def _create_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    def spelled() -> list[Statement] | Fault:
        body = _table_body(operation, engine)
        return _the_first_fault_or(
            [body],
            lambda: [_statement(f"CREATE TABLE {QUOTE[engine](operation['table'])} ({body})", [])],
        )

    return _carrying(operation, engine, "columns", spelled)


def _drop_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return [_statement(f"DROP TABLE {QUOTE[engine](operation['table'])}", [])]


def _add_column(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    def spelled() -> list[Statement] | Fault:
        column = _named_column(operation, engine)
        definition = _the_first_fault_or(
            [column], lambda: column_definition(cast(Column, column), engine)
        )
        return _the_first_fault_or(
            [column, definition],
            lambda: [
                _statement(
                    f"ALTER TABLE {QUOTE[engine](operation['table'])} ADD COLUMN {definition}",
                    [],
                )
            ],
        )

    return _carrying(operation, engine, "columns", spelled)


def _drop_column_directly(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    quote = QUOTE[engine]
    return _needing(
        operation,
        engine,
        ("column",),
        lambda: [
            _statement(
                f"ALTER TABLE {quote(operation['table'])} DROP COLUMN "
                f"{quote(_word(operation, 'column'))}",
                [],
            )
        ],
    )


def _copy(operation: Operation, engine: Engine) -> list[Statement]:
    """The one statement that carries the rows across, or none at all.

    A TABLE WITH NO COLUMN ON BOTH SIDES COPIES NOTHING, and `INSERT INTO t ()
    SELECT FROM u` is not a statement, so the rebuild is four moves rather than
    five. Nothing is lost that `copying` did not already say goes.
    """
    quote = QUOTE[engine]
    carried = ", ".join(quote(column) for column in operation["copying"])
    temporary = quote(temporary_name(operation["table"]))
    table = quote(operation["table"])
    return {
        True: lambda: [],
        False: lambda: [
            _statement(f"INSERT INTO {temporary} ({carried}) SELECT {carried} FROM {table}", [])
        ],
    }[operation["copying"] == []]()


def _rebuild(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """Make a new table, copy the rows into it, drop the old, rename the new,
    and make its indexes again.

    SQLITE AND TURSO CANNOT ALTER A COLUMN IN PLACE, so every alter and every
    drop of a column is this. The first statement clears a temporary table a
    previous attempt left behind, which is what lets the name be the same every
    time and this function read no random source.

    THE INDEXES COME LAST BECAUSE THE DROP TOOK THEM. An index belongs to the
    table it is on, so dropping the old table drops every index on it, and the
    new table carries none until these run.
    """

    def spelled() -> list[Statement] | Fault:
        quote = QUOTE[engine]
        table = quote(operation["table"])
        temporary = quote(temporary_name(operation["table"]))
        body = _table_body(operation, engine)
        return _the_first_fault_or(
            [body],
            lambda: [
                _statement(f"DROP TABLE IF EXISTS {temporary}", []),
                _statement(f"CREATE TABLE {temporary} ({body})", []),
                *_copy(operation, engine),
                _statement(f"DROP TABLE {table}", []),
                _statement(f"ALTER TABLE {temporary} RENAME TO {table}", []),
                *[
                    _index_statement(index, operation["table"], engine)
                    for index in operation["indexes"]
                ],
            ],
        )

    return _carrying(operation, engine, "columns", spelled)


# WHAT ONE ALTERED COLUMN ASKS OF AN ENGINE THAT ALTERS IN PLACE, read off the
# column rather than off `detail`. The column is the declaration, so the alter
# makes the live column match it.
ALTERATION: dict[str, Callable[[Column, Engine], str | Fault]] = {
    "declared_type": lambda column, engine: _the_first_fault_or(
        [type_spelled(column["declared_type"], engine)],
        lambda: f"TYPE {type_spelled(column['declared_type'], engine)}",
    ),
    "nullable": lambda column, _engine: {True: "DROP NOT NULL", False: "SET NOT NULL"}[
        column["facets"]["nullable"] is True
    ],
    "default": lambda column, engine: _the_first_fault_or(
        [default_clause(column["facets"]["default"], engine)],
        lambda: f"SET {default_clause(column['facets']['default'], engine)}",
    ),
}

# The order a migration reads them in, so two changes to one column always run
# the same way round.
ALTERATION_ORDER = ("declared_type", "nullable", "default")

# A COLUMN ALWAYS NAMES ITS TYPE AND MAY NAME THE OTHER TWO. `declared_type` is
# a field of `Column`, so every alter sets the type; `nullable` and `default`
# are facets, and a column that declares neither is altered without them.
DECLARED_ON_EVERY_COLUMN = ("declared_type",)


def _alter_column_directly(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """PostgreSQL alters a column in place, one statement per thing it names."""

    def spelled() -> list[Statement] | Fault:
        column = _named_column(operation, engine)
        return _the_first_fault_or(
            [column], lambda: _in_place(cast(Column, column), operation, engine)
        )

    return _carrying(operation, engine, "columns", spelled)


def _in_place(column: Column, operation: Operation, engine: Engine) -> list[Statement] | Fault:
    changes = [
        change
        for change in ALTERATION_ORDER
        if change in DECLARED_ON_EVERY_COLUMN or change in column["facets"]
    ]
    clauses: list[Any] = [ALTERATION[change](column, engine) for change in changes]
    quote = QUOTE[engine]
    return _the_first_fault_or(
        clauses,
        lambda: [
            _statement(
                f"ALTER TABLE {quote(operation['table'])} ALTER COLUMN "
                f"{quote(column['name'])} {clause}",
                [],
            )
            for clause in clauses
        ],
    )


def _rename_column(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    quote = QUOTE[engine]
    return _needing(
        operation,
        engine,
        ("column", "to"),
        lambda: [
            _statement(
                f"ALTER TABLE {quote(operation['table'])} RENAME COLUMN "
                f"{quote(_word(operation, 'column'))} TO {quote(_word(operation, 'to'))}",
                [],
            )
        ],
    )


def _rename_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    quote = QUOTE[engine]
    return _needing(
        operation,
        engine,
        ("to",),
        lambda: [
            _statement(
                f"ALTER TABLE {quote(operation['table'])} RENAME TO "
                f"{quote(_word(operation, 'to'))}",
                [],
            )
        ],
    )


UNIQUENESS: dict[bool, str] = {True: "UNIQUE ", False: ""}


def _index_statement(index: Index, table: str, engine: Engine) -> Statement:
    """One declared index, on the table it is on.

    THE REBUILD AND `create_index` SPELL AN INDEX THE SAME WAY, so they spell it
    here. An index made when a table is created and an index made again after a
    rebuild that dropped it are the same statement about the same `Index`.
    """
    quote = QUOTE[engine]
    columns = ", ".join(quote(column) for column in index["columns"])
    return _statement(
        f"CREATE {UNIQUENESS[index['unique'] is True]}INDEX {quote(index['name'])} "
        f"ON {quote(table)} ({columns})",
        [],
    )


def _create_index(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _carrying(
        operation,
        engine,
        "indexes",
        lambda: [
            _index_statement(index, operation["table"], engine) for index in operation["indexes"]
        ],
    )


def _drop_index(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _needing(
        operation,
        engine,
        ("name",),
        lambda: [_statement(f"DROP INDEX {QUOTE[engine](_word(operation, 'name'))}", [])],
    )


def _create_view(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _needing(
        operation,
        engine,
        ("name", "select"),
        lambda: [
            _statement(
                f"CREATE VIEW {QUOTE[engine](_word(operation, 'name'))} AS "
                f"{_word(operation, 'select')}",
                [],
            )
        ],
    )


def _drop_view(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _needing(
        operation,
        engine,
        ("name",),
        lambda: [_statement(f"DROP VIEW {QUOTE[engine](_word(operation, 'name'))}", [])],
    )


def _lookup_table(table: str, column: str) -> str:
    """The table a declared `Literal` becomes, one per column that declares one."""
    return f"_dp_enum_{table}_{column}"


def _one_value_at_a_time(
    spelling: Callable[[str, str, str], str],
) -> Callable[[Operation, Engine], list[Statement] | Fault]:
    """A change to the lookup the column's foreign key points at, value by value.

    PUTTING A VALUE IN AND TAKING ONE OUT ARE ONE FUNCTION. Both read the same
    column, name the same lookup and bind the same value, and they differ by the
    statement, so the statement is the argument.

    Each value is bound. It is a declared value rather than a caller's, and it
    could still carry a quote, so it travels beside the statement like every
    other value this package sends.

    A VALUE A ROW STILL HOLDS IS REFUSED BY THE ENGINE AND NOT BY THIS FUNCTION.
    The column points at the lookup, so taking away a value somebody is using
    breaks that key, and the engine says so where it can see the rows.
    """

    def spelled(operation: Operation, engine: Engine) -> list[Statement] | Fault:
        quote = QUOTE[engine]

        def written() -> list[Statement] | Fault:
            lookup = quote(_lookup_table(operation["table"], _word(operation, "column")))
            (mark,) = _marks(engine, 1)
            return [
                _statement(spelling(lookup, quote("value"), mark), [value])
                for value in operation["values"]
            ]

        return _needing(
            operation, engine, ("column",), lambda: _carrying(operation, engine, "values", written)
        )

    return spelled


_insert_enum_values = _one_value_at_a_time(
    lambda lookup, column, mark: f"INSERT INTO {lookup} ({column}) VALUES ({mark})"
)

_remove_enum_values = _one_value_at_a_time(
    lambda lookup, column, mark: f"DELETE FROM {lookup} WHERE {column} = {mark}"
)


Speller = Callable[[Operation, Engine], "list[Statement] | Fault"]

# THE ELEVEN CHANGES EVERY ENGINE SPELLS THE SAME WAY. Each still asks its
# engine for the quoting, the type and the placeholder, so "the same way" is the
# shape and not the text.
SHARED: dict[OperationKind, Speller] = {
    "create_table": _create_table,
    "drop_table": _drop_table,
    "add_column": _add_column,
    "rename_column": _rename_column,
    "rename_table": _rename_table,
    "create_index": _create_index,
    "drop_index": _drop_index,
    "create_view": _create_view,
    "drop_view": _drop_view,
    "insert_enum_values": _insert_enum_values,
    "remove_enum_values": _remove_enum_values,
}

# AND THE TWO THEY DO NOT. PostgreSQL alters and drops a column in place; SQLite
# and Turso rebuild the table around the change.
SQLITE_SPELLING: dict[OperationKind, Speller] = SHARED | {
    "drop_column": _rebuild,
    "alter_column": _rebuild,
}

POSTGRESQL_SPELLING: dict[OperationKind, Speller] = SHARED | {
    "drop_column": _drop_column_directly,
    "alter_column": _alter_column_directly,
}

SPELLING: dict[Engine, dict[OperationKind, Speller]] = {
    "sqlite": SQLITE_SPELLING,
    "postgresql": POSTGRESQL_SPELLING,
    "turso": SQLITE_SPELLING,
}


def _refuse_the_engine(_operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _no_such_engine(engine)


def _refuse_the_operation(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _refused(
        f"A {operation['kind']} on {operation['table']} was handed to {engine}, "
        f"and {engine} has no spelling for a {operation['kind']}.",
        f"Send one of {', '.join(THE_KINDS)}, or declare {operation['kind']} in "
        "docs/design/persistum.hd and give every engine a row for it.",
    )


def _spell(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return SPELLING[engine][operation["kind"]](operation, engine)


# The engine is a word this package ships, the kind is a word the declaration
# carries, and the four answers are all four combinations.
ANSWER: dict[tuple[bool, bool], Speller] = {
    (False, False): _refuse_the_engine,
    (False, True): _refuse_the_engine,
    (True, False): _refuse_the_operation,
    (True, True): _spell,
}


def spell_operation(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """Turn one schema change into the statements that carry it out on one engine.

    Reads the tables above and nothing else. It opens no file, holds no
    connection and asks nothing of the clock or of a random source, so every
    statement it writes can be read before anything runs it.
    """
    return ANSWER[(engine in THE_ENGINES, operation["kind"] in THE_KINDS)](operation, engine)
