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
never logged. Four of them: an engine nobody declared, a kind of change nobody
declared, a change missing something its spelling needs, and a declared type
this package has no storage class for. Each one names what arrived and what to
write instead.

WHY THERE ARE NO BRANCHES. Three engines and twelve kinds of change are a
table, and every row is present. A key with no row is a fault naming the key,
never a fallback answering for an input nobody wrote a rule for.

A VALUE IS BOUND AND AN IDENTIFIER IS QUOTED. A table or a column name is
written into the statement text, quoted for the engine, because no driver binds
one. Everything else travels beside the statement in `parameters` for the driver
to bind. The one exception is a DEFAULT clause and a CHECK clause, which no
engine will take a placeholder in: those carry a literal, quoted here, and they
carry what the author declared rather than anything a caller typed.

TWO THINGS THE DECLARATION CANNOT CARRY YET, named here because a silent gap is
worse than a loud one.

A REBUILD NEEDS THE TABLE AND IS HANDED AN `Operation`. Preserving the rows,
the indexes, the unique constraints, the composite primary key and the foreign
keys means knowing all of them, and an `Operation` carries a kind, a table name,
`detail: dict<str, Value>` and a flag. A `Value` is a scalar, so a column list
cannot travel in one. Until `fn spell_operation` is handed the `Table` it
rewrites, the rebuild below is spelled from two strings the caller supplies:
`definition`, the new table's column clause, and `keeping`, the columns to copy.
Both are a structure squeezed into a string, and both should be a `Table`.

THERE IS NO WORD FOR TAKING A VALUE OUT OF A LOOKUP. `OperationKind` carries
`insert_enum_values` and nothing that removes one, so a declared value that goes
away cannot be spelled at all.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args

from declaro_persistum.types import (
    Column,
    Constraint,
    Engine,
    Fault,
    Operation,
    OperationKind,
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
THE_LIBRARY_PREFIX = "_"
THE_STAMP_TABLE = "_declaro_meta"


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
    "string": "TEXT",
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
    "string": "text",
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


def same_type(declared: str, live: str, engine: str) -> bool | Fault:
    """Whether a declared type and a live column's type are one type here.

    A column declared `uuid` is a `TEXT` column on SQLite, so the two spellings
    have to compare equal or every open would migrate a database that already
    matches its models.
    """

    def compared() -> bool | Fault:
        spelled = type_spelled(declared, engine)
        return _the_first_fault_or(
            [spelled], lambda: cast(str, spelled).casefold() == live.casefold()
        )

    return cast("bool | Fault", _for_engine(engine, compared))


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


def _reference_clause(reference: Value, engine: Engine) -> str:
    """`REFERENCES "authors"("id")`, from the `table.column` a column declares."""
    table, _, column = cast(str, reference).rpartition(".")
    quote = QUOTE[engine]
    return f"REFERENCES {quote(table)}({quote(column)})"


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


def constraint_clause(constraint: Constraint, engine: str) -> str | Fault:
    """One declared constraint as a CREATE TABLE carries it."""

    def spelled() -> str | Fault:
        known = cast(Engine, engine)
        rule = constraint["rule"]
        nested = rule["any_of"] + rule["none_of"]
        return {
            True: lambda: _refused(
                f"The constraint {constraint['name']!r} groups its comparisons "
                f"with any_of or none_of, and this package writes a flat CHECK.",
                "Write the rule as one comparison per column.",
            ),
            False: lambda: _check(constraint, known),
        }[nested != []]()

    return cast("str | Fault", _for_engine(engine, spelled))


def _check(constraint: Constraint, engine: Engine) -> str | Fault:
    comparisons: list[Any] = [
        _comparison(column, comparison["operator"], cast(Value, comparison["value"]), engine)
        for column, comparison in constraint["rule"]["each"].items()
    ]
    return _the_first_fault_or(
        comparisons,
        lambda: f"CONSTRAINT {QUOTE[engine](constraint['name'])} CHECK "
        f"({' AND '.join(comparisons)})",
    )


# --- the mark a driver reads -------------------------------------------------

PLACEHOLDER: dict[Engine, Callable[[int], str]] = {
    # asyncpg numbers its placeholders and reads nothing else.
    "postgresql": lambda position: f"${position}",
    # aiosqlite and pyturso take a question mark per value, in order.
    "sqlite": lambda _position: "?",
    "turso": lambda _position: "?",
}


def placeholder(engine: str) -> str | Fault:
    """The mark this engine reads where the first value goes."""
    return cast(
        "str | Fault",
        _for_engine(engine, lambda: PLACEHOLDER[cast(Engine, engine)](1)),
    )


def _marks(engine: Engine, count: int) -> list[str]:
    return [PLACEHOLDER[engine](position) for position in range(1, count + 1)]


def _statement(text: str, values: list[Value]) -> Statement:
    """A statement and the values that travel beside it, in the order they bind."""
    return {
        "text": text,
        "parameters": {f"p{position}": value for position, value in enumerate(values, start=1)},
    }


# --- the two statements a migration asks of every engine ---------------------


def _sqlite_stamp(engine: Engine, key: str, schema_hash: str) -> Statement:
    quote = QUOTE[engine]
    first, second = _marks(engine, 2)
    return _statement(
        f"INSERT INTO {quote(THE_STAMP_TABLE)} ({quote('key')}, {quote('value')}) "
        f"VALUES ({first}, {second}) "
        f"ON CONFLICT ({quote('key')}) DO UPDATE SET "
        f"{quote('value')} = excluded.{quote('value')}",
        [key, schema_hash],
    )


STAMPING: dict[Engine, Callable[[Engine, str, str], Statement]] = {
    "sqlite": _sqlite_stamp,
    "turso": _sqlite_stamp,
    "postgresql": _sqlite_stamp,
}


def stamping_the_schema_hash(engine: str, key: str, schema_hash: str) -> Statement | Fault:
    """Write the schema's hash down, over whatever was written there before."""
    return cast(
        "Statement | Fault",
        _for_engine(
            engine,
            lambda: STAMPING[cast(Engine, engine)](cast(Engine, engine), key, schema_hash),
        ),
    )


def _sqlite_user_tables(engine: Engine) -> Statement:
    quote = QUOTE[engine]
    kind, ours, theirs = _marks(engine, 3)
    return _statement(
        f"SELECT {quote('name')} FROM {quote('sqlite_master')} "
        f"WHERE {quote('type')} = {kind} "
        f"AND substr({quote('name')}, 1, 1) <> {ours} "
        f"AND {quote('name')} NOT LIKE {theirs}",
        ["table", THE_LIBRARY_PREFIX, "sqlite%"],
    )


def _postgresql_user_tables(engine: Engine) -> Statement:
    quote = QUOTE[engine]
    schema, ours = _marks(engine, 2)
    return _statement(
        f"SELECT {quote('tablename')} FROM {quote('pg_tables')} "
        f"WHERE {quote('schemaname')} = {schema} "
        f"AND substr({quote('tablename')}, 1, 1) <> {ours}",
        ["public", THE_LIBRARY_PREFIX],
    )


LISTING: dict[Engine, Callable[[Engine], Statement]] = {
    "sqlite": _sqlite_user_tables,
    "turso": _sqlite_user_tables,
    "postgresql": _postgresql_user_tables,
}


def listing_the_user_tables(engine: str) -> Statement | Fault:
    """Name the tables a user made, and none of the ones this library keeps."""
    return cast(
        "Statement | Fault",
        _for_engine(engine, lambda: LISTING[cast(Engine, engine)](cast(Engine, engine))),
    )


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


def _column_from(operation: Operation) -> Column:
    """The column an add or an alter declares, as a `Column`."""
    detail = operation["detail"]
    return {
        "name": _word(operation, "column"),
        "declared_type": _word(operation, "declared_type"),
        "renamed_from": None,
        "references": cast("str | None", _one_of(_present(detail, "references"), None)),
        "facets": {
            "nullable": detail["nullable"],
            **{key: detail[key] for key in filter(detail.__contains__, ("default", "unique"))},
        },
    }


def _create_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _needing(
        operation,
        engine,
        ("definition",),
        lambda: [
            _statement(
                f"CREATE TABLE {QUOTE[engine](operation['table'])} "
                f"({_word(operation, 'definition')})",
                [],
            )
        ],
    )


def _drop_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return [_statement(f"DROP TABLE {QUOTE[engine](operation['table'])}", [])]


def _add_column(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    def spelled() -> list[Statement] | Fault:
        definition = column_definition(_column_from(operation), engine)
        return _the_first_fault_or(
            [definition],
            lambda: [
                _statement(
                    f"ALTER TABLE {QUOTE[engine](operation['table'])} ADD COLUMN {definition}",
                    [],
                )
            ],
        )

    return _needing(operation, engine, ("column", "declared_type", "nullable"), spelled)


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


def _rebuild(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """Make a new table, copy the rows into it, drop the old, rename the new.

    SQLITE AND TURSO CANNOT ALTER A COLUMN IN PLACE, so every alter and every
    drop of a column is this. The first statement clears a temporary table a
    previous attempt left behind, which is what lets the name be the same every
    time and this function read no random source.
    """

    def spelled() -> list[Statement]:
        quote = QUOTE[engine]
        table = quote(operation["table"])
        temporary = quote(temporary_name(operation["table"]))
        keeping = ", ".join(
            quote(column.strip()) for column in _word(operation, "keeping").split(",")
        )
        return [
            _statement(f"DROP TABLE IF EXISTS {temporary}", []),
            _statement(f"CREATE TABLE {temporary} ({_word(operation, 'definition')})", []),
            _statement(f"INSERT INTO {temporary} ({keeping}) SELECT {keeping} FROM {table}", []),
            _statement(f"DROP TABLE {table}", []),
            _statement(f"ALTER TABLE {temporary} RENAME TO {table}", []),
        ]

    return _needing(operation, engine, ("definition", "keeping"), spelled)


ALTERATION: dict[str, Callable[[Operation, Engine], str | Fault]] = {
    "declared_type": lambda operation, engine: _the_first_fault_or(
        [type_spelled(_word(operation, "declared_type"), engine)],
        lambda: f"TYPE {type_spelled(_word(operation, 'declared_type'), engine)}",
    ),
    "nullable": lambda operation, _engine: {True: "DROP NOT NULL", False: "SET NOT NULL"}[
        operation["detail"]["nullable"] is True
    ],
    "default": lambda operation, engine: _the_first_fault_or(
        [default_clause(operation["detail"]["default"], engine)],
        lambda: f"SET {default_clause(operation['detail']['default'], engine)}",
    ),
}

# The order a migration reads them in, so two changes to one column always run
# the same way round.
ALTERATION_ORDER = ("declared_type", "nullable", "default")


def _alter_column_directly(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """PostgreSQL alters a column in place, one statement per thing changed."""

    def spelled() -> list[Statement] | Fault:
        detail = operation["detail"]
        changes = list(filter(detail.__contains__, ALTERATION_ORDER))
        clauses: list[Any] = [ALTERATION[change](operation, engine) for change in changes]
        quote = QUOTE[engine]
        return {
            True: lambda: _refused(
                f"An alter_column on {operation['table']} names no change to "
                f"{_word(operation, 'column')}.",
                f"Send the operation again carrying one of {', '.join(ALTERATION_ORDER)}.",
            ),
            False: lambda: _the_first_fault_or(
                clauses,
                lambda: [
                    _statement(
                        f"ALTER TABLE {quote(operation['table'])} ALTER COLUMN "
                        f"{quote(_word(operation, 'column'))} {clause}",
                        [],
                    )
                    for clause in clauses
                ],
            ),
        }[changes == []]()

    return _needing(operation, engine, ("column",), spelled)


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


def _create_index(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    quote = QUOTE[engine]

    def spelled() -> list[Statement]:
        columns = ", ".join(
            quote(column.strip()) for column in _word(operation, "columns").split(",")
        )
        return [
            _statement(
                f"CREATE {UNIQUENESS[operation['detail']['unique'] is True]}INDEX "
                f"{quote(_word(operation, 'name'))} ON {quote(operation['table'])} ({columns})",
                [],
            )
        ]

    return _needing(operation, engine, ("name", "columns", "unique"), spelled)


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


def _insert_enum_values(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """Put one declared value in the lookup the column's foreign key points at.

    The value is bound. It is a declared value rather than a caller's, and it
    could still carry a quote, so it travels beside the statement like every
    other value this package sends.
    """
    quote = QUOTE[engine]

    def spelled() -> list[Statement]:
        lookup = _lookup_table(operation["table"], _word(operation, "column"))
        (mark,) = _marks(engine, 1)
        return [
            _statement(
                f"INSERT INTO {quote(lookup)} ({quote('value')}) VALUES ({mark})",
                [operation["detail"]["value"]],
            )
        ]

    return _needing(operation, engine, ("column", "value"), spelled)


Speller = Callable[[Operation, Engine], "list[Statement] | Fault"]

# THE NINE CHANGES EVERY ENGINE SPELLS THE SAME WAY. Each still asks its engine
# for the quoting, the type and the placeholder, so "the same way" is the shape
# and not the text.
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
