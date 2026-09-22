"""`session_connect`: everything that has to be true before a handle goes back.

    orchestrator fn session_connect : (connecting: Connecting) -> Connecting | Fault

WHAT AN ORCHESTRATOR DOES HERE. It sequences the twenty-eight functions the
declaration lists and decides which path runs. It does no work of its own that a
pure function could do, and it reaches outside the process only through the
boundaries it calls. What it keeps is what each step returned, in the record the
declaration gives it.

THE MIGRATION RUNS BEFORE THE HANDLE COMES BACK, and that is why this function
is the largest one here. The models the code has and the schema the database has
meet in this chain. A handle handed back while the database is still behind its
models is a handle whose first question fails on a column the models declare,
and it fails in a caller's request rather than at open, where somebody is
watching.

A DESTRUCTIVE CHANGE NOBODY DECLARED STOPS THE OPEN. `fn refuse_undeclared_loss`
reads the plan and what the models declared, and a loss nobody asked for by name
refuses the whole migration. Nobody is standing at a deploy to approve one, so
the permission is a declaration in the code rather than a prompt.

A FAILURE COMES BACK IN THE RETURN AND TRAVELS OUT UNCHANGED. Any of the
twenty-eight can hand back a `Fault`. The chain below stops at the first one and
carries it out to the caller in the words the function that found it wrote. This
function never rewrites one, because a refusal reworded twice is a refusal whose
author nobody can find.

NOT ONE BRANCH, AND NOT ONE DEFAULT. Every decision is a lookup in a table keyed
by a value somebody already decided: whether the last step failed, whether the
copy is cold, whether a migration is needed, whether the view a change names is
declared. Each table holds every key its question can produce, so no arm can go
untaken and unseen, and no lookup carries a fallback.

IT KNOWS NO ENGINE BY NAME. It is handed a `Connecting`, works the engine out
once with `fn engine_of`, and hands that word to every function that needs it.
There is no engine spelled anywhere in this module and no SQL in any string.

IT IS ASYNCHRONOUS BECAUSE THE BOUNDARIES IT CALLS ARE. aiosqlite, asyncpg and
pyturso are all async-only, and nothing in pyturso is natively synchronous.
Calling one of them without awaiting raises nothing and does nothing, because a
coroutine is truthy, so every outward call below is awaited.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from declaro_persistum.capability_of import capability_of
from declaro_persistum.crew_size import crew_size
from declaro_persistum.difference import difference
from declaro_persistum.engine_of import engine_of
from declaro_persistum.lifecycle_install_handler import lifecycle_install_handler
from declaro_persistum.lifecycle_register import lifecycle_register
from declaro_persistum.log_write import log_write
from declaro_persistum.mechanism_for import mechanism_for
from declaro_persistum.migration_needed import migration_needed
from declaro_persistum.migration_report import migration_report
from declaro_persistum.name_what_destroys import name_what_destroys
from declaro_persistum.name_what_is_ambiguous import name_what_is_ambiguous
from declaro_persistum.order_operations import order_operations
from declaro_persistum.permitted_connections import permitted_connections
from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.refuse_owned_names import refuse_owned_names
from declaro_persistum.refuse_undeclared_loss import refuse_undeclared_loss
from declaro_persistum.refuse_unusable_settings import refuse_unusable_settings
from declaro_persistum.replicates import replicates
from declaro_persistum.room_make import room_make
from declaro_persistum.schema_from_declared import schema_from_declared
from declaro_persistum.schema_inspect import schema_inspect
from declaro_persistum.schema_read_declared import schema_read_declared
from declaro_persistum.session_open_connection import session_open_connection
from declaro_persistum.session_open_writer import session_open_writer
from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.spell_view import spell_view
from declaro_persistum.turso_pull import turso_pull
from declaro_persistum.types import (
    NOTHING,
    Connecting,
    Database,
    Engine,
    Fault,
    Operation,
    Plan,
    Schema,
    Statement,
    Value,
    View,
)

# =============================================================================
# telling a failure from an answer, once
# =============================================================================
#
# A `Fault` IS THE THREE FIELDS AND NOTHING ELSE, so what came back is read by
# its shape rather than by a flag somebody remembered to set. Every other record
# in this package carries different names, so no answer here can read as one.

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_fault(answer: object) -> bool:
    """Whether what came back is a failure on its way to the caller."""
    return isinstance(answer, dict) and frozenset(answer) == THE_FIELDS_OF_A_FAULT


def _the_fault(answer: Any, _keep: Any) -> Any:
    """It failed, so it leaves in the words it arrived in."""
    return answer


def _kept(answer: Any, keep: Callable[[Any], Any]) -> Any:
    """It worked, so the next step is handed what it produced."""
    return keep(answer)


# Whether the last call failed. Both keys are here because a bool has two.
WHETHER_IT_FAILED: dict[bool, Callable[[Any, Any], Any]] = {True: _the_fault, False: _kept}


def _or_fault(answer: Any, keep: Callable[[Any], Any]) -> Any:
    """Hand `answer` on to `keep`, unless it is a failure, which travels out."""
    return WHETHER_IT_FAILED[_is_fault(answer)](answer, keep)


def _the_first_one(faults: list[Any], _build: Any) -> Any:
    return faults[0]


def _what_they_build(_faults: Any, build: Callable[[], Any]) -> Any:
    return build()


WHETHER_ANY_FAILED: dict[bool, Callable[[Any, Any], Any]] = {
    True: _the_first_one,
    False: _what_they_build,
}


def _first_fault_or(pieces: list[Any], build: Callable[[], Any]) -> Any:
    """The first failure among several answers, or what they build together."""
    faults = [piece for piece in pieces if _is_fault(piece)]
    return WHETHER_ANY_FAILED[bool(faults)](faults, build)


# =============================================================================
# the chain, and how it stops
# =============================================================================
#
# EVERY STEP TAKES THE RECORD AND HANDS BACK THE RECORD. That is what lets the
# order be a tuple a reader can follow top to bottom, and what lets the stop be
# one table read once per step instead of a check written twenty times.

type Step = Callable[[Connecting], Awaitable[Any]]


async def _skipped(_step: Step, stopped: Any) -> Any:
    """Something already failed, so this step does not run and does no I/O."""
    return stopped


async def _taken(step: Step, carried: Any) -> Any:
    return await step(carried)


WHETHER_TO_TAKE_IT: dict[bool, Callable[[Step, Any], Awaitable[Any]]] = {
    True: _skipped,
    False: _taken,
}


async def _through(steps: tuple[Step, ...], carried: Any) -> Any:
    """Run the steps in order, and stop carrying anything once one fails."""
    for step in steps:
        carried = await WHETHER_TO_TAKE_IT[_is_fault(carried)](step, carried)
    return carried


async def _stop_here(_step: Any, carried: Any, _item: Any) -> Any:
    return carried


async def _go_on(step: Any, carried: Any, item: Any) -> Any:
    return await step(carried, item)


WHETHER_TO_STOP: dict[bool, Callable[[Any, Any, Any], Awaitable[Any]]] = {
    True: _stop_here,
    False: _go_on,
}


async def _each_in_turn(
    items: list[Any],
    carried: Any,
    step: Callable[[Any, Any], Awaitable[Any]],
    stopped: Callable[[Any], bool],
) -> Any:
    """Fold `step` over `items`, and stop asking once `stopped` says so.

    The migration runs one operation at a time and one statement at a time, and
    both have to stop where the engine refused rather than carry on against a
    database that is now part way through.
    """
    for item in items:
        carried = await WHETHER_TO_STOP[stopped(carried)](step, carried, item)
    return carried


# =============================================================================
# what the caller said, and what the target names
# =============================================================================


async def _the_engine(carried: Connecting) -> Any:
    """Which engine the target names, decided once and handed to everything."""
    return _or_fault(engine_of(carried["target"]), lambda named: {**carried, "engine": named})


async def _the_settings(carried: Connecting) -> Any:
    """A setting this engine cannot use is refused rather than ignored."""
    return _or_fault(
        refuse_unusable_settings(carried["engine"], carried["settings"]),
        lambda usable: {**carried, "settings": usable},
    )


# WHAT A CALLER WHO NAMED NO PRIMARY MEANS. Written as a row rather than read
# with a fallback, so the absent case is a value in the table beside the
# present one.
NO_PRIMARY_NAMED: dict[str, Value] = {"primary": NOTHING}


async def _the_handle_so_far(carried: Connecting) -> Any:
    """The room, and the three facts about this database that are already known.

    The connection and the writers are not open yet, so they are absent here and
    filled in below. `fn replicates` reads the primary and nothing else, so this
    much of the handle is enough to ask it, and asking it is what settles how
    many connections and how many drainers this database gets.
    """
    return {
        **carried,
        "database": Database(
            engine=carried["engine"],
            connection=NOTHING,
            writers=[],
            room=room_make(),
            shutdown=carried["shutdown"],
            primary={**NO_PRIMARY_NAMED, **carried["settings"]}["primary"],
        ),
    }


async def _the_bound(carried: Connecting) -> Any:
    """How many connections this database may hold."""
    return _or_fault(
        permitted_connections(carried["connections"], replicates(carried["database"])),
        lambda permitted: {**carried, "permitted": permitted},
    )


async def _the_crew(carried: Connecting) -> Any:
    """How many drainers run over this database's room.

    Reached only after the bound, which refuses the one case `fn crew_size`
    cannot answer: a local database whose deployment set no number.
    """
    return {
        **carried,
        "crew": crew_size(carried["connections"], replicates(carried["database"])),
    }


async def _the_writers(carried: Connecting) -> Any:
    """One connection per drainer, opened here and held for the database's life."""
    opened = [
        await session_open_writer(carried["target"], carried["engine"])
        for _ in range(carried["crew"])
    ]
    return _first_fault_or(opened, lambda: {**carried, "writers": opened})


async def _the_connection(carried: Connecting) -> Any:
    """The one connection reads and the migration run on."""
    return _or_fault(
        await session_open_connection(carried["target"], carried["engine"], carried["permitted"]),
        lambda connection: {**carried, "connection": connection},
    )


async def _the_handle(carried: Connecting) -> Any:
    """The six fields together, which is the value every later call reads."""
    return {
        **carried,
        "database": {
            **carried["database"],
            "connection": carried["connection"],
            "writers": carried["writers"],
        },
    }


# =============================================================================
# what opening owes the process
# =============================================================================


async def _the_signal_handler(carried: Connecting) -> Any:
    """The first open is startup, so the first open installs the handler."""
    return _or_fault(lifecycle_install_handler(), lambda _installed: carried)


async def _on_the_shutdown_list(carried: Connecting) -> Any:
    """A database that asked to replicate at exit writes itself down."""
    return _or_fault(lifecycle_register(carried["database"]), lambda _written: carried)


# =============================================================================
# what the code declares, and what the database holds
# =============================================================================


async def _what_the_models_declare(carried: Connecting) -> Any:
    return _or_fault(schema_read_declared(), lambda declared: {**carried, "declared": declared})


async def _no_name_the_library_owns(carried: Connecting) -> Any:
    return _or_fault(refuse_owned_names(carried["declared"]), lambda _clear: carried)


async def _the_wanted_schema(carried: Connecting) -> Any:
    """The models are the desired state, so the target comes from them alone."""
    return _or_fault(
        schema_from_declared(carried["declared"]),
        lambda wanted: {**carried, "wanted_schema": wanted},
    )


async def _the_live_schema(carried: Connecting) -> Any:
    return _or_fault(
        await schema_inspect(carried["database"]), lambda live: {**carried, "live": live}
    )


# A DATABASE THAT HOLDS NOTHING AT ALL. A replica in this state is unusable
# rather than merely stale, which is the one case that puts the sync channel on
# the critical path of an open.
NOTHING_ON_DISK: Schema = {"tables": [], "views": []}


async def _the_pull_failed(pulled: Any, _carried: Connecting) -> Any:
    return pulled


async def _read_the_copy_again(_pulled: Any, carried: Connecting) -> Any:
    """The copy has changed under us, so the live schema is read again."""
    return await _the_live_schema(carried)


WHETHER_THE_PULL_FAILED: dict[bool, Callable[[Any, Connecting], Awaitable[Any]]] = {
    True: _the_pull_failed,
    False: _read_the_copy_again,
}


async def _filled_from_the_primary(carried: Connecting) -> Any:
    """Copy the whole database down before anything else reads it."""
    pulled = await turso_pull(carried["database"])
    return await WHETHER_THE_PULL_FAILED[_is_fault(pulled)](pulled, carried)


async def _already_usable(carried: Connecting) -> Any:
    """Either it holds something, or there is no primary to ask."""
    return carried


# (this database replicates, its copy holds nothing) -> what to do about it.
# All four keys are written out, so the one case that blocks an open is a row a
# reader can find rather than a condition buried in a sentence.
WHETHER_THE_COPY_IS_COLD: dict[tuple[bool, bool], Callable[[Connecting], Awaitable[Any]]] = {
    (True, True): _filled_from_the_primary,
    (True, False): _already_usable,
    (False, True): _already_usable,
    (False, False): _already_usable,
}


async def _a_cold_copy_filled(carried: Connecting) -> Any:
    return await WHETHER_THE_COPY_IS_COLD[
        (replicates(carried["database"]), carried["live"] == NOTHING_ON_DISK)
    ](carried)


async def _whether_a_migration_is_needed(carried: Connecting) -> Any:
    return {
        **carried,
        "needs_migration": migration_needed(carried["live"], carried["wanted_schema"]),
    }


# =============================================================================
# the plan, which is pure, and the run, which is not
# =============================================================================


def _the_plan(carried: Connecting) -> Plan | Fault:
    """Which operations turn the live schema into the one the models want.

    Four functions in a row, each able to refuse: a change that could be a
    rename or a drop and an add, an order no dependency allows, and a loss no
    model asked for.
    """
    return _or_fault(
        name_what_is_ambiguous(
            difference(carried["live"], carried["wanted_schema"]), carried["declared"]
        ),
        lambda operations: _or_fault(
            order_operations(operations),
            lambda ordered: refuse_undeclared_loss(
                name_what_destroys(ordered), carried["declared"]
            ),
        ),
    )


def _no_view_declares_it(operation: Operation, _engine: Engine, _named: Any) -> Fault:
    """A change naming a view the models do not declare."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"The migration would create the view {operation['table']}, and no "
            "model declares a view by that name, so there is no declaration to "
            "write its question from."
        ),
        what_to_do_instead=(
            f"Declare {operation['table']} as a view against the models, or take "
            "it out of the migration."
        ),
    )


def _every_one_spelled(operations: list[Operation], engine: Engine) -> Any:
    """Several operations as one list of statements, or the first refusal."""
    spelled = [spell_operation(operation, engine) for operation in operations]
    return _first_fault_or(spelled, lambda: [statement for one in spelled for statement in one])


def _a_plain_view(view: View, operation: Operation, engine: Engine) -> Any:
    """The library writes the SELECT, and the operation carries it."""
    return _or_fault(
        spell_view(view, engine),
        lambda select: spell_operation(
            {
                **operation,
                "detail": {
                    **operation["detail"],
                    "name": view["name"],
                    "select": select["text"],
                },
            },
            engine,
        ),
    )


def _a_materialized_view(view: View, _operation: Operation, engine: Engine) -> Any:
    """A freshness the caller asked for, and the mechanism that keeps it."""
    return _or_fault(
        mechanism_for(view["freshness"], engine, capability_of(engine, "materialized_view")),
        lambda operations: _every_one_spelled(operations, engine),
    )


# Whether the view the caller declared says how fresh it must be. A plain view
# has no freshness to keep, so the library writes its SELECT and stops there.
HOW_FRESH_THE_VIEW_MUST_BE: dict[bool, Callable[[View, Operation, Engine], Any]] = {
    True: _a_plain_view,
    False: _a_materialized_view,
}


def _the_view_it_names(operation: Operation, engine: Engine, named: list[View]) -> Any:
    view = named[0]
    return HOW_FRESH_THE_VIEW_MUST_BE[view["freshness"] is NOTHING](view, operation, engine)


WHETHER_THE_VIEW_IS_DECLARED: dict[bool, Callable[[Operation, Engine, list[View]], Any]] = {
    True: _the_view_it_names,
    False: _no_view_declares_it,
}


def _a_declared_view(operation: Operation, engine: Engine, wanted: Schema) -> Any:
    """A view's statements, which need the declaration the models hold.

    `fn difference` can say a view has to be created and cannot say what it
    selects, because it compares schemas and a view's declaration lives in the
    models. So the declaration is looked up here and handed to `fn spell_view`.
    """
    named = [view for view in wanted["views"] if view["name"] == operation["table"]]
    return WHETHER_THE_VIEW_IS_DECLARED[bool(named)](operation, engine, named)


def _plainly(operation: Operation, engine: Engine, _wanted: Schema) -> Any:
    """Every other change carries everything its kind names already."""
    return spell_operation(operation, engine)


HOW_THE_CHANGE_IS_SPELLED: dict[bool, Callable[[Operation, Engine, Schema], Any]] = {
    True: _a_declared_view,
    False: _plainly,
}


def _spelled(operation: Operation, engine: Engine, wanted: Schema) -> Any:
    return HOW_THE_CHANGE_IS_SPELLED[operation["kind"] == "create_view"](operation, engine, wanted)


# --- running what was spelled ------------------------------------------------
#
# WHAT IS CARRIED WHILE THE MIGRATION RUNS: the handle to run on, how many
# operations have landed, and the failure that stopped it, which is NOTHING
# while nothing has.


def _the_engine_refused(database: Database, applied: int, answer: Any) -> Any:
    return (database, applied, answer)


def _the_engine_ran_it(database: Database, applied: int, _answer: Any) -> Any:
    return (database, applied, NOTHING)


WHAT_THE_ENGINE_ANSWERED: dict[bool, Callable[[Database, int, Any], Any]] = {
    True: _the_engine_refused,
    False: _the_engine_ran_it,
}


def _it_stopped(running: Any) -> bool:
    return _is_fault(running[2])


async def _one_statement(running: Any, statement: Statement) -> Any:
    database, applied, _so_far = running
    answer = await query_run_statement(database, statement)
    return WHAT_THE_ENGINE_ANSWERED[_is_fault(answer)](database, applied, answer)


def _it_did_not_land(after: Any) -> Any:
    return after


def _it_landed(after: Any) -> Any:
    database, applied, stopped = after
    return (database, applied + 1, stopped)


WHETHER_THE_OPERATION_LANDED: dict[bool, Callable[[Any], Any]] = {
    True: _it_did_not_land,
    False: _it_landed,
}


async def _one_operation(running: Any, statements: list[Statement]) -> Any:
    """One operation lands when every statement it takes ran."""
    database, applied, _so_far = running
    after = await _each_in_turn(
        statements, (database, applied, NOTHING), _one_statement, _it_stopped
    )
    return WHETHER_THE_OPERATION_LANDED[_it_stopped(after)](after)


def _the_migration_stopped(_carried: Connecting, _report: Any, stopped: Any) -> Any:
    """The engine refused an operation, so the open stops in its words."""
    return stopped


def _the_migration_finished(carried: Connecting, report: Any, _stopped: Any) -> Any:
    return {**carried, "migration": report}


WHETHER_THE_MIGRATION_STOPPED: dict[bool, Callable[[Connecting, Any, Any], Any]] = {
    True: _the_migration_stopped,
    False: _the_migration_finished,
}


async def _a_change_this_engine_cannot_write(
    _carried: Connecting, _plan: Plan, _spelled: list[Any], faults: list[Any]
) -> Any:
    """Nothing ran, because a change could not be written for this engine."""
    return faults[0]


async def _run_them(carried: Connecting, plan: Plan, spelled: list[Any], _faults: list[Any]) -> Any:
    _database, applied, stopped = await _each_in_turn(
        spelled, (carried["database"], 0, NOTHING), _one_operation, _it_stopped
    )
    return WHETHER_THE_MIGRATION_STOPPED[_is_fault(stopped)](
        carried, migration_report(plan, applied), stopped
    )


# EVERY OPERATION IS SPELLED BEFORE ANY OF THEM RUNS, so a change this engine
# cannot write stops the migration with the database untouched rather than part
# way through.
WHETHER_ANY_SPELLING_FAILED: dict[
    bool, Callable[[Connecting, Plan, list[Any], list[Any]], Awaitable[Any]]
] = {
    True: _a_change_this_engine_cannot_write,
    False: _run_them,
}


async def _carried_out(carried: Connecting, plan: Plan) -> Any:
    """Spell every operation, then run them in order until one is refused."""
    spelled = [
        _spelled(operation, carried["engine"], carried["wanted_schema"])
        for operation in plan["operations"]
    ]
    faults = [piece for piece in spelled if _is_fault(piece)]
    return await WHETHER_ANY_SPELLING_FAILED[bool(faults)](carried, plan, spelled, faults)


async def _the_plan_was_refused(plan: Any, _carried: Connecting) -> Any:
    return plan


async def _the_plan_runs(plan: Any, carried: Connecting) -> Any:
    return await _carried_out(carried, plan)


WHETHER_THE_PLAN_WAS_REFUSED: dict[bool, Callable[[Any, Connecting], Awaitable[Any]]] = {
    True: _the_plan_was_refused,
    False: _the_plan_runs,
}


async def _the_migration(carried: Connecting) -> Any:
    """Work the plan out, refuse an undeclared loss, and run what is left."""
    plan = _the_plan(carried)
    return await WHETHER_THE_PLAN_WAS_REFUSED[_is_fault(plan)](plan, carried)


# NOTHING TO RUN IS AN ANSWER AND NOT A FAILURE. A database already matching its
# models gets an empty plan and a count of nothing, which is an ordinary report a
# caller can read rather than an absence it has to check for.
NOTHING_TO_RUN: Plan = {"operations": [], "destroys": []}


async def _nothing_to_migrate(carried: Connecting) -> Any:
    return {**carried, "migration": migration_report(NOTHING_TO_RUN, 0)}


WHETHER_A_MIGRATION_RUNS: dict[bool, Callable[[Connecting], Awaitable[Any]]] = {
    True: _the_migration,
    False: _nothing_to_migrate,
}


async def _brought_up_to_the_models(carried: Connecting) -> Any:
    return await WHETHER_A_MIGRATION_RUNS[carried["needs_migration"]](carried)


# =============================================================================
# what an operator reads afterwards
# =============================================================================


async def _said_out_loud(carried: Connecting) -> Any:
    """One line naming what was opened and how much of a migration ran.

    Nobody is watching a deploy, so this is the only account of one anybody will
    read. It runs where the open succeeded; a failure says what happened in the
    fault it hands the caller, and `docs/architecture.md` keeps failures out of
    the log for that reason.
    """
    log_write(
        {
            "opened": carried["target"],
            "engine": carried["engine"],
            "connections": carried["permitted"],
            "drainers": carried["crew"],
            "planned": len(carried["migration"]["plan"]["operations"]),
            "applied": carried["migration"]["applied"],
        }
    )
    return carried


# =============================================================================
# the order, top to bottom
# =============================================================================
#
# THE RECORD FILLS IN THIS ORDER, and the order is the declaration's: the
# caller's three arguments arrive filled, then the engine, the settings, the
# bound, the crew, the writers, the connection, what the models declare, the
# live schema, whether anything has to run, the migration's report, and the
# handle.

THE_CHAIN: tuple[Step, ...] = (
    _the_engine,
    _the_settings,
    _the_handle_so_far,
    _the_bound,
    _the_crew,
    _the_writers,
    _the_connection,
    _the_handle,
    _the_signal_handler,
    _on_the_shutdown_list,
    _what_the_models_declare,
    _no_name_the_library_owns,
    _the_wanted_schema,
    _the_live_schema,
    _a_cold_copy_filled,
    _whether_a_migration_is_needed,
    _brought_up_to_the_models,
    _said_out_loud,
)


async def session_connect(connecting: Connecting) -> Connecting | Fault:
    """Open a database: work out the engine, connect, migrate, hand back the handle.

    `connecting` arrives carrying what the caller said, which is the target, the
    shutdown policy, the number of connections the deployment stated and the
    settings. Everything else in the record is filled here, in the order above,
    and the record that comes back carries the handle every later call reads.

    A failure at any step comes back instead, in the words of the function that
    found it.
    """
    return await _through(THE_CHAIN, connecting)
