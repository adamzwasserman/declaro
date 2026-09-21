"""Every shape `docs/design/persistum.hd` declares, and nothing else.

WHY THIS FILE IS FIRST. Thirty-seven pure functions name these in their
signatures, so none of them can be written until the shapes exist. It is the
one module the rewrite builds before anything fans out.

WHY IT IS ONLY DATA. A TypedDict is a shape a dict has, not an object with
behaviour, so nothing here can carry a method that quietly becomes a second
place where a decision is made. The decisions are the functions.

`NOTHING` IS ABSENCE, AND `None` IS NOT ALLOWED TO MEAN IT. `docs/architecture.md`
line 120: a row that is not there is an answer you have to look at, and `None`
is also what a nullable column holds, so the two stop being distinguishable
exactly where it matters. Every field that may be absent says so in its type.

A HANDLE IS HELD AND NEVER READ. `Connection` is the driver's own object. The
declaration types it `handle`, which says it may be passed to a boundary and may
not be opened up; Python has no word for that, so it is `Any` here and the rule
lives in the declaration and in the reviews.
"""

from typing import Any, Literal, TypedDict

# --- the words, each one closed -------------------------------------------

Engine = Literal["sqlite", "postgresql", "turso"]
Shutdown = Literal["exit_immediately", "replicate_then_exit"]
Grammar = Literal["pseudo_sql", "django", "prisma", "sqlalchemy"]
Direction = Literal["ascending", "descending"]
Freshness = Literal["always current", "current when read", "refreshed when asked"]
FaultKind = Literal["conflict", "refused", "budget_spent", "cut_short"]
Feature = Literal["materialized_view", "check_constraint", "partial_index", "enum", "range_type"]
Operator = Literal[
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "in",
    "not_in",
    "between",
    "like",
    "contains",
    "startswith",
    "endswith",
    "is_null",
    "is_not_null",
]
OperationKind = Literal[
    "create_table",
    "drop_table",
    "add_column",
    "drop_column",
    "alter_column",
    "rename_column",
    "rename_table",
    "create_index",
    "drop_index",
    "create_view",
    "drop_view",
    "insert_enum_values",
]

# The asking vocabulary, in the two halves that travel differently: a reading
# word runs now and answers now, a writing word is deposited and drained later.
Reading = Literal[
    "all",
    "one",
    "first",
    "page",
    "distinct",
    "exists",
    "count",
    "sum",
    "average",
    "smallest",
    "largest",
    "count_distinct",
]
Writing = Literal["add", "change", "change_all", "remove", "remove_all", "replace"]
Wanted = Reading | Writing

# --- the smallest values ---------------------------------------------------

NOTHING = None
"""The named absence. Bound to `None` because Python has one empty value, and
named because a reader has to see which of the two meanings is meant."""

Value = str | int | float | bool | None
Row = dict[str, Value]
Rows = list[Row]
Connection = Any
Ticket = str

# --- what a caller says ----------------------------------------------------


class Comparison(TypedDict):
    operator: Operator
    # `between` TAKES TWO VALUES AND `in` TAKES A LIST, SO ONE VALUE IS NOT
    # ENOUGH. Widened 2026-09-21 to match the declaration:
    # `{"age": {"between": [18, 30]}}` built a comparison this type did not
    # describe, and every builder carried it through untouched rather than say
    # so. A list here is still bound by the driver, one parameter per member,
    # and still never pasted into the text.
    value: Value | list[Value]
    ignore_case: bool


class Condition(TypedDict):
    each: dict[str, Comparison]
    any_of: list["Condition"]
    none_of: list["Condition"]


# A NARROWING IS WIDER THAN A CONDITION. A `Condition` is what this library
# holds: every comparison spelled out, `any_of` and `none_of` present whether or
# not anybody used them. A `Narrowing` is what a caller wrote, which is a bare
# value where they meant eq, `or` and `not` where they meant `any_of` and
# `none_of`, and a named operator where they wanted one.
#
# ADDED 2026-09-21, BECAUSE FOUR BUILDERS WERE HANDED SOMETHING THIS TYPE DID
# NOT ADMIT. `{"age": {"gt": 18}}` and `{"or": [...]}` are neither a `Row` nor a
# `Condition`, so every builder was taking data the declaration said could not
# arrive. `condition_from_narrowing` is the one place a narrowing becomes a
# condition.
# THE NESTING IS A RULE A READER KEEPS, NOT A SHAPE THIS FILE CAN WRITE DOWN
# TODAY. The declaration says `Narrowing = dict<str, AskedValue>` and
# `AskedValue` names `Narrowing` back, because `or` and `not` hold more
# narrowings. Python 3.11, which this package still supports, has no lazy type
# alias, so that circle has no spelling here.
#
# WHY THE OBVIOUS FIX DOES NOT WORK, MEASURED TWICE ON 2026-09-21 so nobody
# spends a third afternoon on it. `Narrowing = dict[str, "AskedValue"]` builds a
# `ForwardRef` carrying no module. It resolves against whichever module calls
# `get_type_hints`, and the four grammar builders import `Asked` without
# importing `AskedValue`, so the call raises `NameError: name 'AskedValue' is
# not defined`. A single-module test of the same spelling passes, which is why
# it looks like it works: the names are in scope there and nowhere else.
#
# So the value inside a narrowing is `Any` here, and the recursion lives in the
# declaration, beside `handle` and `NOTHING`, which are already kept that way.
# Raising the floor to 3.12 would let `type Narrowing = dict[str, AskedValue]`
# say it properly, and that is a packaging decision rather than this file's.
Narrowing = dict[str, Any]
AskedValue = (
    Value
    | Row
    | Rows
    | Condition
    | Narrowing
    | list[Narrowing]
    | list[str]
    | list[Value]
    | dict[str, Direction]
)
Asked = dict[str, AskedValue]


class Join(TypedDict):
    model: str
    by_column: str
    to_column: str
    condition: Condition


class Declaration(TypedDict):
    model: str
    wanted: Wanted
    columns: list[str]
    condition: Condition
    order: dict[str, Direction]
    grouping: list[str]
    having: Condition
    paging: dict[str, int] | None
    rows: Rows | None
    across: list[Join] | None


class Statement(TypedDict):
    text: str
    parameters: dict[str, Value]


class Fault(TypedDict):
    kind: FaultKind
    what_went_wrong: str
    what_to_do_instead: str


Answer = Rows | int | bool | Ticket | None

# --- what the code declares ------------------------------------------------


class Column(TypedDict):
    name: str
    declared_type: str
    renamed_from: str | None
    references: str | None
    facets: dict[str, Value]


class Index(TypedDict):
    name: str
    columns: list[str]
    unique: bool


class Constraint(TypedDict):
    name: str
    columns: list[str]
    rule: Condition


class Model(TypedDict):
    table: str
    columns: list[Column]
    dropping: list[str]
    indexes: list[Index]
    constraints: list[Constraint]


class View(TypedDict):
    name: str
    declaration: Declaration
    freshness: Freshness | None


class Declared(TypedDict):
    models: list[Model]
    views: list[View]
    dropping_tables: list[str]
    dropping_views: list[str]


# --- what a database holds, and what changes it ----------------------------


class Table(TypedDict):
    name: str
    columns: list[Column]
    indexes: list[Index]
    constraints: list[Constraint]


class Schema(TypedDict):
    tables: list[Table]
    views: list[View]


class Operation(TypedDict):
    kind: OperationKind
    table: str
    detail: dict[str, Value]
    destroys: bool


class Plan(TypedDict):
    operations: list[Operation]
    destroys: list[str]


class Report(TypedDict):
    plan: Plan
    applied: int


class Capability(TypedDict):
    native: bool
    built_from: str


# --- the write queue -------------------------------------------------------


class Receipt(TypedDict):
    id: str
    ok: bool
    attempts: int
    failure: Fault | None


class Room(TypedDict):
    arrivals: list[Statement]
    waiting: list[Ticket]


# --- the handle a caller keeps ---------------------------------------------


class Database(TypedDict):
    engine: Engine
    connection: Connection
    writers: list[Connection]
    room: Room
    shutdown: Shutdown
    primary: str | None


# --- what each orchestrator carries while it works -------------------------


class Connecting(TypedDict):
    target: str
    shutdown: Shutdown
    connections: int | None
    engine: Engine
    settings: dict[str, Value]
    permitted: int
    crew: int
    writers: list[Connection]
    connection: Connection
    declared: Declared
    live: Schema
    wanted_schema: Schema
    needs_migration: bool
    migration: Report
    database: Database


class Answering(TypedDict):
    db: Database
    model: Model
    grammar: Grammar
    asked: Asked
    declaration: Declaration
    statement: Statement
    rows: Rows
    answer: Answer


class Collecting(TypedDict):
    db: Database
    ticket: Ticket
    receipt: Receipt


class CatchingUp(TypedDict):
    db: Database
    writer: Connection
    statement: Statement | None
    writer_waiting: bool
    attempts: int


class ShuttingDown(TypedDict):
    databases: list[Database]
    started_at: float
    attempts: int
