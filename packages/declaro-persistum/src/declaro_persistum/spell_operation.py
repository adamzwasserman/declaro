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

THE PRAGMA THAT MAKES A REBUILD POSSIBLE BELONGS TO WHOEVER COMMITS. Dropping a
table another table points at is refused while foreign keys are enforced, so the
applier turns enforcement off around a rebuild and checks the keys before it
commits. `PRAGMA foreign_keys` is a no-op inside a transaction, so it cannot be
one of the statements returned here, and this function emits none.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args

from declaro_persistum.how_to_spell import how_to_spell
from declaro_persistum.spell_view import spell_view
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
    View,
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
    write, or a rule of a kind nothing has a clause for. Any one of them is the
    answer, because the statement it would have gone into cannot be written.
    """
    faults = cast("list[Fault]", list(filter(_is_fault, pieces)))
    answer: dict[bool, Callable[[], Built | Fault]] = {True: lambda: faults[0], False: build}
    return answer[faults != []]()


def _for_engine(engine: str, known: Callable[[], Any]) -> Any:
    """Answer for an engine this package ships, and refuse any other."""
    return {True: known, False: lambda: _no_such_engine(engine)}[engine in THE_ENGINES]()


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
#
# WHERE EACH KIND GOES IS SQL'S DECISION RATHER THAN THIS PACKAGE'S, and the two
# clause tables below are where it is written down. A kind with a row in
# `COLUMN_CLAUSE` is spelled beside its column; every other kind is spelled over
# the table. A DEFAULT has no table-level form in any engine, so it goes beside
# the column or nowhere. NOT NULL has a table-level spelling as a CHECK and is
# still written inline, because inline is the form a schema read back reports as
# nullability.

# The order a column definition reads its own rules in.
COLUMN_CLAUSE_ORDER: tuple[ConstraintKind, ...] = ("not_null", "default")


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


def column_definition(column: Column, constraints: list[Constraint], engine: str) -> str | Fault:
    """One column as a CREATE TABLE carries it: its name, its type, its own rules.

    IT TAKES THE WHOLE TABLE'S CONSTRAINTS AND READS THE ONES THAT NAME IT. Until
    2026-09-23 a column carried a bag of facets, and this function had to hunt
    for keys nobody wrote a clause for before it could write anything. A rule is
    a `Constraint` now, `WHERE_A_KIND_GOES` says which kinds are written beside a
    column, and a kind with no clause never reaches here.
    """

    def spelled() -> str | Fault:
        return _definition(column, constraints, cast(Engine, engine))

    return cast("str | Fault", _for_engine(engine, spelled))


def _its_own(
    column: Column, constraints: list[Constraint], kind: ConstraintKind
) -> list[Constraint]:
    """The rules of one kind that name this column."""
    return [
        constraint
        for constraint in constraints
        if constraint["kind"] == kind and column["name"] in constraint["columns"]
    ]


def _definition(column: Column, constraints: list[Constraint], engine: Engine) -> str | Fault:
    pieces: list[Any] = [
        QUOTE[engine](column["name"]),
        type_spelled(column["declared_type"], engine),
        *[
            COLUMN_CLAUSE[kind](constraint, engine)
            for kind in COLUMN_CLAUSE_ORDER
            for constraint in _its_own(column, constraints, kind)
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


def _length(constraint: Constraint, engine: Engine) -> str | Fault:
    """`CHECK (length("code") <= 8)`, over every column the rule names.

    A CHECK RATHER THAN A TYPE MODIFIER, because `VARCHAR(8)` is enforced by
    PostgreSQL and ignored by SQLite, so the same declaration would mean two
    different things. Measured 2026-09-23: SQLite and pyturso both take this
    CHECK and both raise IntegrityError on a ninth character.

    THE LENGTH IS A WHOLE NUMBER OR IT IS A REFUSAL. `value` is a `Value`, so
    `"eight"` is expressible, and an engine handed `length("code") <= 'eight'`
    compares a number against text and answers something nobody asked for.
    """
    allowed = constraint["value"]
    quote = QUOTE[engine]
    named = [f"length({quote(column)}) <= {allowed}" for column in constraint["columns"]]
    written: dict[bool, Callable[[], str | Fault]] = {
        True: lambda: _refused(
            f"The constraint {constraint['name']!r} allows a length of "
            f"{allowed!r}, which is a {type(allowed).__name__} rather than a "
            "count of characters.",
            "Declare the length as a whole number of at least one.",
        ),
        False: lambda: f"CONSTRAINT {quote(constraint['name'])} CHECK ({' AND '.join(named)})",
    }
    return written[not isinstance(allowed, int) or isinstance(allowed, bool) or allowed < 1]()


# WHICH KIND OF RULE IT IS SAYS HOW IT IS SPELLED. Before `Constraint` carried a
# kind, every constraint was read as a CHECK, so a key spanning two columns was
# two inline PRIMARY KEY clauses that no engine accepts.
#
# A KIND IS IN EXACTLY ONE OF THESE TWO TABLES, and `WHERE_A_KIND_GOES` says
# which. A kind in neither is refused by name rather than spelled as silence,
# which is what a bag keyed by strings used to do.
CONSTRAINT_CLAUSE: dict[ConstraintKind, Callable[[Constraint, Engine], str | Fault]] = {
    "check": _check,
    "primary_key": _over_its_columns("PRIMARY KEY"),
    "unique": _over_its_columns("UNIQUE"),
    "length": _length,
}

COLUMN_CLAUSE: dict[ConstraintKind, Callable[[Constraint, Engine], str | Fault]] = {
    "not_null": lambda _constraint, _engine: "NOT NULL",
    "default": lambda constraint, engine: default_clause(constraint["value"], engine),
}


_EVERY_KIND_WITH_A_CLAUSE = sorted(set(CONSTRAINT_CLAUSE) | set(COLUMN_CLAUSE))


def _no_such_constraint_kind(constraint: Constraint, engine: str) -> Fault:
    return _refused(
        f"The constraint {constraint['name']!r} says it is a "
        f"{constraint['kind']!r}, and this package writes "
        f"{', '.join(_EVERY_KIND_WITH_A_CLAUSE)} into a {engine} definition.",
        f"Declare the constraint as one of {', '.join(_EVERY_KIND_WITH_A_CLAUSE)}.",
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


def _missing(operation: Operation, engine: Engine, absent: list[str]) -> Fault:
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
    """Spell the change when the operation carries what the spelling reads.

    EVERY FIELD IS PRESENT ON EVERY OPERATION, so what an absent one looks like
    is the named absence rather than a key nobody put in. Until 2026-09-23 these
    lived in a bag called `detail` and the question was whether a key was there
    at all, which is how `Operation` came to declare four fields nobody filled
    and carry four nobody declared.
    """
    absent = [name for name in wanted if _FIELD_A_SPELLING_READS[name](operation) is NOTHING]
    return {
        True: lambda: _missing(operation, engine, absent),
        False: build,
    }[absent != []]()


# WHAT A SPELLING ASKS FOR -> WHERE THE OPERATION KEEPS IT. A rename's target is
# `renamed_to`. A dropped index is the one index the operation carries. A view's
# name is on the view, which carries a declaration rather than a SELECT anybody
# wrote out. Only the column has a field of its own.
_FIELD_A_SPELLING_READS: dict[str, Callable[[Operation], Any]] = {
    "column": lambda operation: _or_nothing(operation["column"]),
    "to": lambda operation: _or_nothing(operation["renamed_to"]),
    "index": lambda operation: ([NOTHING, *operation["indexes"]])[len(operation["indexes"][:1])],
    "view": lambda operation: _or_nothing(operation["view"]),
}


def _or_nothing[Held](held: Held | None) -> Held | str:
    """A field's value, or the named absence when the field holds none."""
    return ([NOTHING, held])[held is not None]  # type: ignore[return-value]


def _word(operation: Operation, key: str) -> str:
    """One name a spelling reads, out of the field the operation keeps it in."""
    return cast(str, _FIELD_A_SPELLING_READS[key](operation))


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
    the whole table as it will be afterwards, and `column` says which of them
    changed, so one lookup answers for both.
    """
    wanted = _or_nothing(operation["column"])
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


# A PRIMARY KEY GOES AT THE TABLE LEVEL WHETHER IT COVERS ONE COLUMN OR THREE,
# and until 2026-09-23 that was not true. A column carried a `primary_key` facet,
# so a key over two columns spelled two inline PRIMARY KEY clauses that no engine
# parses, and this function had to count them and refuse. A key is a `Constraint`
# naming its columns now, `PRIMARY KEY ("id")` is what one column spells, and
# there is nothing left to count. Measured 2026-09-23: SQLite, PostgreSQL and
# pyturso all take the table-level form over a single column.


def _table_body(operation: Operation, engine: Engine) -> str | Fault:
    """What goes between the parentheses of a CREATE TABLE.

    Every column with the rules written beside it, then every rule written over
    the table, in the order the operation carries them.
    """
    constraints = operation["constraints"]
    pieces: list[Any] = [
        *[column_definition(column, constraints, engine) for column in operation["columns"]],
        *[
            constraint_clause(constraint, engine)
            for constraint in constraints
            if constraint["kind"] not in COLUMN_CLAUSE
        ],
    ]
    return cast("str | Fault", _the_first_fault_or(pieces, lambda: ", ".join(pieces)))


def _create_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """The table, and then one statement per index it carries.

    THE INDEXES WERE LOST UNTIL 2026-10-07. A new table arrives here carrying the
    columns, the rules and the indexes it is meant to end with, and this spelled
    only the table. `fn what_an_operation_carries` said `create_table` carries
    indexes and this returned one statement, so the indexes went nowhere and the
    deployment got a table, no index and a success report. The same defect shipped
    in 0.1.11, where the differ packed them in and the applier never read them,
    and another project lost four indexes in production to it.

    AN INDEX CANNOT GO INSIDE THE CREATE TABLE, so this has to be more than one
    statement, which is why `fn spell_operation` hands back a list.
    """

    def spelled() -> list[Statement] | Fault:
        body = _table_body(operation, engine)
        return _the_first_fault_or(
            [body],
            lambda: [
                _statement(f"CREATE TABLE {QUOTE[engine](operation['table'])} ({body})", []),
                *[
                    _index_statement(index, operation["table"], engine)
                    for index in operation["indexes"]
                ],
            ],
        )

    return _carrying(operation, engine, "columns", spelled)


def _drop_table(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return [_statement(f"DROP TABLE {QUOTE[engine](operation['table'])}", [])]


def _add_column(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    def spelled() -> list[Statement] | Fault:
        column = _named_column(operation, engine)
        definition = _the_first_fault_or(
            [column],
            lambda: column_definition(cast(Column, column), operation["constraints"], engine),
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


def _set_or_drop(word: str) -> Callable[[list[Constraint], Engine], str | Fault]:
    """SET it when a rule names the column, DROP it when none does.

    ABSENCE IS A DECLARATION HERE, WHICH IS WHAT CHANGED ON 2026-09-23. The
    operation carries the target table's whole constraint list, so a column with
    no not_null rule among them is a column the declaration says may be empty,
    and a column with no default rule is one that starts at nothing. The alter
    makes the live column match the declaration, so it says both ways round.
    """

    def clause(_rules: list[Constraint], _engine: Engine) -> str | Fault:
        return f"DROP {word}"

    def named(rules: list[Constraint], engine: Engine) -> str | Fault:
        written = _WRITTEN_AFTER_SET[word](rules[0], engine)
        return _the_first_fault_or([written], lambda: f"SET {written}")

    def either(rules: list[Constraint], engine: Engine) -> str | Fault:
        return {True: named, False: clause}[rules != []](rules, engine)

    return either


_WRITTEN_AFTER_SET: dict[str, Callable[[Constraint, Engine], str | Fault]] = {
    "NOT NULL": lambda _constraint, _engine: "NOT NULL",
    "DEFAULT": lambda constraint, engine: default_clause(constraint["value"], engine),
}


# WHAT ONE ALTERED COLUMN ASKS OF AN ENGINE THAT ALTERS IN PLACE. The column and
# the rules that name it are the declaration, so the alter makes the live column
# match them. Each row takes the rules of its own kind and nothing else.
ALTERATION: dict[str, Callable[[Column, list[Constraint], Engine], str | Fault]] = {
    "declared_type": lambda column, _rules, engine: _the_first_fault_or(
        [type_spelled(column["declared_type"], engine)],
        lambda: f"TYPE {type_spelled(column['declared_type'], engine)}",
    ),
    "not_null": lambda _column, rules, engine: _set_or_drop("NOT NULL")(rules, engine),
    "default": lambda _column, rules, engine: _set_or_drop("DEFAULT")(rules, engine),
}

# The order a migration reads them in, so two changes to one column always run
# the same way round. `declared_type` reads no constraint and the other two read
# the rules of their own kind, which is why the key is the kind's own word.
ALTERATION_ORDER: tuple[str, ...] = ("declared_type", "not_null", "default")


def _alter_column_directly(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """PostgreSQL alters a column in place, one statement per thing it names."""

    def spelled() -> list[Statement] | Fault:
        column = _named_column(operation, engine)
        return _the_first_fault_or(
            [column], lambda: _in_place(cast(Column, column), operation, engine)
        )

    return _carrying(operation, engine, "columns", spelled)


def _in_place(column: Column, operation: Operation, engine: Engine) -> list[Statement] | Fault:
    clauses: list[Any] = [
        ALTERATION[change](
            column, _its_own(column, operation["constraints"], cast(ConstraintKind, change)), engine
        )
        for change in ALTERATION_ORDER
    ]
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


# --- the same two changes, on a database that has a primary ------------------
#
# A RENAME RUNS ON THE LOCAL COPY AND SENDS NOTHING. Measured 2026-10-10 against
# a real Turso primary: `ALTER TABLE ... RENAME COLUMN` and
# `ALTER TABLE ... RENAME TO` both report success and reach no primary, so a
# replica that renamed a column and a primary that did not go on serving two
# different schemas with nothing to say so. Worse, the next write naming the new
# column raised on its first push, reported success on its second, and never
# arrived.
#
# SO BOTH ARE SPELLED WITH STATEMENTS THAT DO REPLICATE: an add, a copy and a
# drop. `fn how_to_spell` says they are committed on their own, which is also
# measured: all three in one transaction failed the push and left the literal
# string 'None' on the primary.


def _the_renamed_column(operation: Operation, engine: Engine) -> Column | Fault:
    """The column a rename moves, under whichever of its two names it carries.

    A RENAME DOES NOT CHANGE THE TYPE, so the column arriving under the new name
    is the column that was there under the old one. A target schema carries it
    under the new name and a live one under the old, and either settles the
    definition, which is why the lookup takes both.
    """
    names = {str(operation["column"]), str(operation["renamed_to"])}
    found = [column for column in operation["columns"] if column["name"] in names]
    answer: dict[bool, Callable[[], Column | Fault]] = {
        True: lambda: found[0],
        False: lambda: _refused(
            f"A rename on {operation['table']} moves {operation['column']!r} to "
            f"{operation['renamed_to']!r}, and {engine} has a primary, so the "
            f"column has to be added under its new name before the rows move "
            f"across. The columns it was handed are "
            f"{[column['name'] for column in operation['columns']]}, and neither "
            f"name is among them.",
            f"Send the operation again carrying the column under "
            f"{operation['renamed_to']!r} or under {operation['column']!r}, so "
            f"its declared type can be written into the ADD COLUMN.",
        ),
    }
    return answer[found != []]()


def _it_must_hold_something(
    moved: Column, operation: Operation, engine: Engine
) -> list[Statement] | Fault:
    """A column declared NOT NULL cannot be added beside rows that have none."""
    return _refused(
        f"A rename on {operation['table']} moves {operation['column']!r} to "
        f"{moved['name']!r}, which is declared to hold something. On {engine} "
        f"with a primary the move is an ADD COLUMN, a copy and a DROP COLUMN, "
        f"and a column that must hold something cannot be added to a table that "
        f"already has rows.",
        f"Give {moved['name']!r} a default in the model, or declare the move as "
        f"a new column and a dropping line and fill it yourself.",
    )


def _added_filled_and_dropped(
    moved: Column, operation: Operation, engine: Engine
) -> list[Statement] | Fault:
    """Add the column under its new name, copy the rows across, drop the old one."""
    quote = QUOTE[engine]
    table = quote(operation["table"])
    was = quote(_word(operation, "column"))
    definition = column_definition(moved, operation["constraints"], engine)
    return _the_first_fault_or(
        [definition],
        lambda: [
            _statement(f"ALTER TABLE {table} ADD COLUMN {definition}", []),
            _statement(f"UPDATE {table} SET {quote(moved['name'])} = {was}", []),
            _statement(f"ALTER TABLE {table} DROP COLUMN {was}", []),
        ],
    )


_WHETHER_IT_MAY_ARRIVE_EMPTY: dict[
    bool, Callable[[Column, Operation, Engine], list[Statement] | Fault]
] = {
    True: _added_filled_and_dropped,
    False: _it_must_hold_something,
}


def _rename_column_by_copying(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """Three statements and no rename, for a column on a database with a primary."""

    def spelled() -> list[Statement] | Fault:
        moved = _the_renamed_column(operation, engine)

        def by_copying() -> list[Statement] | Fault:
            arriving = cast(
                Column, {**cast("dict[str, Any]", moved), "name": _word(operation, "to")}
            )
            must_hold = _its_own(arriving, operation["constraints"], "not_null")
            return _WHETHER_IT_MAY_ARRIVE_EMPTY[must_hold == []](arriving, operation, engine)

        return _the_first_fault_or([moved], by_copying)

    return _needing(operation, engine, ("column", "to"), spelled)


def _rename_table_by_copying(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """A rebuild without the rename: make it under the new name, copy, drop the old.

    THE SAME THREE MOVES AS A COLUMN, one level up. A rebuild would end by
    renaming the temporary table into place, which is the statement that cannot
    leave the machine, so the new name goes on the CREATE TABLE instead and
    nothing is renamed at all.
    """

    def spelled() -> list[Statement] | Fault:
        quote = QUOTE[engine]
        was = quote(operation["table"])
        now = quote(_word(operation, "to"))
        body = _table_body(operation, engine)
        carried = ", ".join(quote(column["name"]) for column in operation["columns"])
        return _the_first_fault_or(
            [body],
            lambda: [
                _statement(f"CREATE TABLE {now} ({body})", []),
                _statement(f"INSERT INTO {now} ({carried}) SELECT {carried} FROM {was}", []),
                _statement(f"DROP TABLE {was}", []),
            ],
        )

    return _needing(
        operation, engine, ("to",), lambda: _carrying(operation, engine, "columns", spelled)
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
    """The one index the operation carries, dropped by its own name."""
    return _needing(
        operation,
        engine,
        ("index",),
        lambda: [_statement(f"DROP INDEX {QUOTE[engine](operation['indexes'][0]['name'])}", [])],
    )


def _create_view(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """The view the operation carries, with `fn spell_view` writing its SELECT.

    A VIEW HOLDS A DECLARATION, NEVER A SELECT. Until 2026-09-23 this read a
    finished SELECT out of `detail`, which meant whoever built the operation
    wrote SQL, and `fn spell_view` was already written and reached by nothing.
    A declaration is what a caller can be refused on and what a second engine
    can be spelled from; a string is neither.
    """

    def spelled() -> list[Statement] | Fault:
        view = cast(View, operation["view"])
        select = spell_view(view, engine)
        return _the_first_fault_or(
            [select],
            lambda: [
                _statement(
                    f"CREATE VIEW {QUOTE[engine](view['name'])} AS "
                    f"{cast(Statement, select)['text']}",
                    list(cast(Statement, select)["parameters"].values()),
                )
            ],
        )

    return _needing(operation, engine, ("view",), spelled)


def _drop_view(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    return _needing(
        operation,
        engine,
        ("view",),
        lambda: [
            _statement(f"DROP VIEW {QUOTE[engine](cast(View, operation['view'])['name'])}", [])
        ],
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


def _it_cannot_reach_the_primary(operation: Operation, engine: Engine) -> list[Statement] | Fault:
    """A change this engine can only make locally, in the generators table's words.

    `fn how_to_spell` refuses this pair before anything reaches here, so this row
    exists to keep the table below complete rather than to be read. A table with
    a hole in it is a table read with a fallback.
    """
    return cast(Fault, how_to_spell(operation["kind"], engine, True))


# AND WHAT A REPLICATED TURSO WRITES INSTEAD OF ALL THREE. A drop replicates on
# its own, so it is written plainly rather than rebuilt around. Both renames
# become an add, a copy and a drop. An alter is refused, because Turso has no
# ALTER COLUMN and its rebuild cannot leave the machine.
TURSO_REPLICATED_SPELLING: dict[OperationKind, Speller] = SHARED | {
    "drop_column": _drop_column_directly,
    "alter_column": _it_cannot_reach_the_primary,
    "rename_column": _rename_column_by_copying,
    "rename_table": _rename_table_by_copying,
}

# WHETHER THE DATABASE HAS A PRIMARY PICKS THE TABLE, AND THE ENGINE PICKS THE
# ROW. Neither SQLite nor PostgreSQL replicates, so each reads the same spellings
# either way and still keeps a row, because a read with no row is a read with a
# fallback.
SPELLING: dict[bool, dict[Engine, dict[OperationKind, Speller]]] = {
    False: {
        "sqlite": SQLITE_SPELLING,
        "postgresql": POSTGRESQL_SPELLING,
        "turso": SQLITE_SPELLING,
    },
    True: {
        "sqlite": SQLITE_SPELLING,
        "postgresql": POSTGRESQL_SPELLING,
        "turso": TURSO_REPLICATED_SPELLING,
    },
}


def _refuse_the_engine(
    _operation: Operation, engine: Engine, _replicating: bool
) -> list[Statement] | Fault:
    return _no_such_engine(engine)


def _refuse_the_operation(
    operation: Operation, engine: Engine, _replicating: bool
) -> list[Statement] | Fault:
    return _refused(
        f"A {operation['kind']} on {operation['table']} was handed to {engine}, "
        f"and {engine} has no spelling for a {operation['kind']}.",
        f"Send one of {', '.join(THE_KINDS)}, or declare {operation['kind']} in "
        "docs/design/persistum.hd and give every engine a row for it.",
    )


def _spell(operation: Operation, engine: Engine, replicating: bool) -> list[Statement] | Fault:
    return SPELLING[replicating][engine][operation["kind"]](operation, engine)


# The engine is a word this package ships, the kind is a word the declaration
# carries, and the four answers are all four combinations.
ANSWER: dict[tuple[bool, bool], Callable[[Operation, Engine, bool], list[Statement] | Fault]] = {
    (False, False): _refuse_the_engine,
    (False, True): _refuse_the_engine,
    (True, False): _refuse_the_operation,
    (True, True): _spell,
}


def spell_operation(
    operation: Operation, engine: Engine, replicating: bool
) -> list[Statement] | Fault:
    """Turn one schema change into the statements that carry it out on one engine.

    Reads the tables above and nothing else. It opens no file, holds no
    connection and asks nothing of the clock or of a random source, so every
    statement it writes can be read before anything runs it.

    WHETHER THIS PAIR HAS A RULE AT ALL IS ASKED OF `fn how_to_spell`, which is
    the generators table `docs/architecture.md:62` names. A pair that table has
    no row for is refused there and the refusal travels out of here unchanged,
    so a kind of change added to the vocabulary and not to the table fails on
    the first attempt to spell it rather than being written as whatever this
    file happened to fall through to.

    `replicating` SAYS WHETHER THE DATABASE HAS A PRIMARY, since 2026-10-12, and
    a replicated database is handed a different spelling for the same change. A
    statement that runs on the local copy is not a statement that reaches the
    primary, so what comes back here is what reaches the primary rather than
    what runs on the copy.
    """
    written = how_to_spell(operation["kind"], engine, replicating)
    return _OR_THE_REFUSAL[_is_a_refusal(written)](written, operation, engine, replicating)


def _is_a_refusal(written: object) -> bool:
    """Whether the generators table refused this pair rather than answering."""
    return isinstance(written, dict) and "what_went_wrong" in written


def _the_table_refused(
    written: object, _operation: Operation, _engine: Engine, _replicating: bool
) -> Any:
    """It leaves in the words the table wrote, unreworded."""
    return written


def _spelled(_written: object, operation: Operation, engine: Engine, replicating: bool) -> Any:
    """The table has a rule, so the statements are written."""
    return ANSWER[(engine in THE_ENGINES, operation["kind"] in THE_KINDS)](
        operation, engine, replicating
    )


# Whether the generators table had a rule for this pair. Both rows are written
# out, so a pair with no rule is answered by a row rather than by a check
# falling through to a speller written for a different kind.
_OR_THE_REFUSAL: dict[bool, Callable[..., Any]] = {
    True: _the_table_refused,
    False: _spelled,
}
