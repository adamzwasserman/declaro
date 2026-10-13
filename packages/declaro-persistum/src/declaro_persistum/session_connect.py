"""Everything that has to be true before a handle goes back.

    orchestrator fn session_connect : (connecting: Connecting) -> Connecting | Fault awaited

ONE DOOR REACHES THIS AND NOTHING ELSE DOES. `boundary_in fn session_open` takes
a caller's target and hands over a `Connecting` carrying the caller's own words
and nothing else filled in. This fills the rest, in order, and the last thing it
fills is the handle every later call reads.

THE ORDER IS THE DESIGN, SO IT IS A TUPLE AND NOT A BODY. `THE_ORDER` holds one
entry per step. Each takes the record and hands back the record with its own
field filled, or hands back a fault; the first fault ends the open and every
entry after it hands that fault straight on. Written as thirty lines of body the
order is a detail nobody can read back. Written as a tuple a test can read it.

MOST OF THOSE ENTRIES ARE ROWS RATHER THAN FUNCTIONS. Fourteen steps do the same
thing: ask one function, and write its answer into one field. Written out they
were fourteen bodies differing by a name and a string, which is one function over
a table with the names and strings in the rows. `fn _asking` is that function and
`THE_ORDER` holds the rows. Four more steps choose between two ways to go, and
`fn _either` is that function.

THE MIGRATION IS HERE AND NOT BEHIND A DOOR OF ITS OWN. Nobody is standing at a
deploy to ask for one. A handle handed back while the database is still behind
its models is a handle whose first question fails on a column the models declare,
at request time, in front of somebody.

WHAT A FAILED MIGRATION LEAVES BEHIND IS THE ENGINE'S ANSWER, NOT THIS
FUNCTION'S. `fn how_to_migrate` says whether an engine takes a batch as one
transaction or commits each change as it makes it. All three take the batch, so a
failure on the thirtieth change undoes the twenty-nine before it. The fault says
which happened, because that is what an operator has to know before they decide
what to do. The Turso row said it commits as it goes until 2026-10-10; measured
against pyturso that day, plain `BEGIN` rolls a created table and an added column
all the way back.

THE HANDLE IS BUILT BEFORE THE CREW STARTS. A drainer is handed the database it
drains, so the database has to exist before the first drainer runs. The crew
inside it comes from `fn crew_make` here: a crew a caller had to build is a crew
a caller can leave out, and a database with no crew takes a write and never runs
it.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast

from declaro_persistum.capability_of import capability_of
from declaro_persistum.channel_pull import channel_pull
from declaro_persistum.channel_push import channel_push
from declaro_persistum.copy_on_disk import copy_on_disk
from declaro_persistum.crew_make import crew_make
from declaro_persistum.crew_size import crew_size
from declaro_persistum.crew_start import crew_start
from declaro_persistum.difference import difference
from declaro_persistum.engine_of import engine_of
from declaro_persistum.how_to_inspect import how_to_inspect
from declaro_persistum.how_to_migrate import how_to_migrate
from declaro_persistum.how_to_spell import how_to_spell
from declaro_persistum.how_to_write import how_to_write
from declaro_persistum.lifecycle_install_handler import lifecycle_install_handler
from declaro_persistum.lifecycle_register import lifecycle_register
from declaro_persistum.lifecycle_turn import lifecycle_turn
from declaro_persistum.log_write import log_write
from declaro_persistum.mechanism_for import mechanism_for
from declaro_persistum.migration_needed import migration_needed
from declaro_persistum.name_what_destroys import name_what_destroys
from declaro_persistum.name_what_is_ambiguous import name_what_is_ambiguous
from declaro_persistum.order_operations import order_operations
from declaro_persistum.permitted_connections import permitted_connections
from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.query_run_together import query_run_together
from declaro_persistum.refuse_owned_names import refuse_owned_names
from declaro_persistum.refuse_undeclared_loss import refuse_undeclared_loss
from declaro_persistum.refuse_unusable_settings import refuse_unusable_settings
from declaro_persistum.replicates import replicates
from declaro_persistum.schema_from_declared import schema_from_declared
from declaro_persistum.schema_inspect import schema_inspect
from declaro_persistum.schema_read_declared import schema_read_declared
from declaro_persistum.session_close_connection import session_close_connection
from declaro_persistum.session_open_channel import session_open_channel
from declaro_persistum.session_open_connection import session_open_connection
from declaro_persistum.session_open_writer import session_open_writer
from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.types import (
    NOTHING,
    Connecting,
    Database,
    Engine,
    Fault,
    MigrationMode,
    Operation,
    Plan,
    Report,
    Shutdown,
    Statement,
    Value,
    View,
)
from declaro_persistum.what_an_operation_carries import what_an_operation_carries

# THE THREE FACTS A FAULT CARRIES. Read as a whole set rather than by one key, so
# a record that happens to have a `kind` is not taken for a refusal.
THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# A step is a function from the record to the record, or to the refusal that
# ended the open.
Step = Callable[[Connecting], Awaitable["Connecting | Fault"]]


def _is_fault(piece: object) -> bool:
    return isinstance(piece, dict) and frozenset(piece) == THE_THREE_FACTS_IN_A_FAULT


def _the_first_refusal(refused: list[Any]) -> Fault | None:
    """Whichever refused first, or nothing when none did."""
    return cast("Fault | None", ([*refused[:1], None])[0])


# --- asking one function and writing its answer into one field ----------------
#
# FOURTEEN STEPS DO EXACTLY THIS AND THEY ARE ROWS, NOT BODIES. A row says which
# field it fills, which function answers, which words that function is handed,
# what goes into the field, and whether the answer has to be awaited. Written as
# fourteen functions they differed by a name and a string, and two of them could
# drift apart without anything noticing.


async def _await_it(ask: Callable[..., Any], words: tuple[Any, ...]) -> Any:
    """The answer is a promise, so the caller waits on it."""
    return await ask(*words)


async def _call_it(ask: Callable[..., Any], words: tuple[Any, ...]) -> Any:
    """The answer is the answer. It is still returned, so both halves answer."""
    return ask(*words)


_WHETHER_THE_ANSWER_IS_AWAITED: dict[bool, Callable[..., Awaitable[Any]]] = {
    True: _await_it,
    False: _call_it,
}


def _the_refusal(
    _connecting: Connecting, answer: object, _fills: str, _into: Callable[..., Any]
) -> Fault:
    """What came back refuses the open, so it is the answer."""
    return cast("Fault", answer)


def _written_down(
    connecting: Connecting, answer: object, fills: str, into: Callable[..., Any]
) -> Connecting:
    return cast("Connecting", {**connecting, fills: into(answer, connecting)})


_WHETHER_IT_REFUSED: dict[bool, Callable[..., Connecting | Fault]] = {
    True: _the_refusal,
    False: _written_down,
}


def _asking(
    *,
    fills: str,
    ask: Callable[..., Any],
    hand: Callable[[Connecting], tuple[Any, ...]],
    into: Callable[[Any, Connecting], Any],
    awaited: bool,
) -> Step:
    """Build the step that asks one function and writes down one answer."""

    async def step(connecting: Connecting) -> Connecting | Fault:
        answer = await _WHETHER_THE_ANSWER_IS_AWAITED[awaited](ask, hand(connecting))
        return _WHETHER_IT_REFUSED[_is_fault(answer)](connecting, answer, fills, into)

    return step


def _either(*, question: Callable[[Connecting], Any], ways: dict[Any, Step]) -> Step:
    """Build the step that asks the record a question and takes one of two ways."""

    async def step(connecting: Connecting) -> Connecting | Fault:
        return await ways[question(connecting)](connecting)

    return step


# --- what the rows paste in ---------------------------------------------------


def _the_answer_itself(answer: Any, _connecting: Connecting) -> Any:
    return answer


def _in_a_list(answer: Any, _connecting: Connecting) -> list[Any]:
    """`fn session_open_connection` answers with one connection and the record
    holds the list every later question is asked on."""
    return [answer]


def _what_was_already_there(_answer: Any, connecting: Connecting) -> Any:
    """`fn refuse_owned_names` answers with nothing when it allows the names, so
    the field it guards keeps what it had."""
    return connecting["declared"]


def _the_crew_it_started(answer: Any, connecting: Connecting) -> Any:
    return {**connecting["database"], "crew": answer}


def _what_the_caller_set(connecting: Connecting) -> dict[str, Value]:
    """A primary is the one setting a caller hands in that only one engine holds.

    Taking SQLite's and ignoring it would leave a caller believing their
    database replicates, so it goes to `fn refuse_unusable_settings` by name.
    """
    return dict([("primary", connecting["primary"])][: int(connecting["primary"] is not NOTHING)])


# --- the steps that are not one question ---------------------------------------


async def _as_it_stands(connecting: Connecting) -> Connecting | Fault:
    """Nothing to do here, and the record goes on as it is.

    THIS IS THE DO-NOTHING HALF OF FOUR PAIRS AND IT IS ONE FUNCTION. A local
    database has no channel to open and none to pull; a database that exits
    immediately has nothing to listen for; a database already matching its
    models has nothing to run. Each is a real answer rather than a step skipped.
    """
    return connecting


async def _every_writer(target: str, engine: Engine, how_many: int) -> list[Any] | Fault:
    """One connection per drainer, each held for that drainer's whole life.

    A drainer sharing a connection with another drainer would serialise the crew
    down to one writer, which is a crew doing nothing one connection could not.
    """
    opened = [await session_open_writer(target, engine) for _ in range(how_many)]
    refused = [one for one in opened if _is_fault(one)]
    return cast("list[Any] | Fault", ([*refused[:1], opened])[0])


async def _build_the_handle(connecting: Connecting) -> Connecting | Fault:
    """The value every later call reads, built here and by nobody else."""
    return cast(
        "Connecting",
        {
            **connecting,
            "database": Database(
                target=connecting["target"],
                engine=connecting["engine"],
                connections=connecting["connections"],
                writers=connecting["writers"],
                channel=connecting["channel"],
                crew={**crew_make(), "size": connecting["crew_size"]},
                shutdown=connecting["shutdown"],
            ),
        },
    )


async def _say_it_opened(connecting: Connecting) -> Connecting | Fault:
    """One line an operator can read, naming the database and what it took."""
    log_write(
        {
            "event": "database_opened",
            "target": connecting["target"],
            "engine": connecting["engine"],
            "connections": len(connecting["connections"]),
            "writers": len(connecting["writers"]),
            "migrated": connecting["migration"]["applied"],
        }
    )
    return connecting


# --- the channel, and filling a cold copy --------------------------------------


def _whether_it_is_set(field: str) -> Callable[[Connecting], bool]:
    """Build the question "did this field ever get a value?".

    Two steps ask it, of two fields. Written out they were two bodies differing
    by one string, and a change to how absence is spelled would have had to be
    made in both.
    """

    def asked(connecting: Connecting) -> bool:
        return connecting[field] is not NOTHING  # type: ignore[literal-required]

    return asked


def _whether_the_copy_can_be_behind(connecting: Connecting) -> tuple[bool, bool]:
    """Is there a channel, and was a copy already here when the open started?

    Two questions and four rows, because a local database has no channel and a
    machine that started with nothing has nothing to catch up on. Only a
    replicated open on a copy that was already here pulls.
    """
    return (connecting["channel"] is not NOTHING, connecting["copy_was_here"])


_A_CHANNEL_TO_THE_PRIMARY = _asking(
    fills="channel",
    ask=session_open_channel,
    hand=lambda connecting: (
        connecting["target"],
        str(connecting["primary"]),
        str(connecting["token"]),
    ),
    into=_the_answer_itself,
    awaited=True,
)

# WHAT IS LOCAL GOES UP BEFORE ANYTHING COMES DOWN, and this is multicardz's
# production failure written down. A deploy created a table on the local copy.
# The push that would have sent it never landed, and the deploy recorded the
# schema as done anyway. Every open after that pulled, read a copy that matched
# the models, planned no migration and sent nothing, so the primary went without
# the table for weeks while every open reported success.
#
# A PUSH THAT DOES NOT LAND REFUSES THE OPEN, which `fn _asking` gives it: a
# fault from any step is the answer and every step after it hands that fault on.
# A caller handed a handle here would write into a copy whose changes cannot
# leave, and on ephemeral disk those writes die with the process.
#
# MEASURED 2026-10-10, because the order only matters if a pull keeps what is
# local: it does. A replica holding an unpushed CREATE TABLE still holds it after
# a pull, and a push after that pull reaches the primary.
#
# IT IS ALSO THE FIRST CALL ON THE CHANNEL, SO IT DOES TWO MORE THINGS. The
# driver builds its sync half on whichever call comes first, so this push is what
# proves the channel reaches a primary at all: `boundary_out fn
# session_open_channel` touches no network and proved nothing until 2026-10-10,
# when it stopped spending a pull on a proof this call makes anyway. And on a
# copy with nothing on it yet, `bootstrap_if_empty` fills the copy as this push
# runs, measured at twenty-three tables with the pull after it bringing nothing.
_SEND_WHAT_IS_LOCAL = _asking(
    fills="channel",
    ask=channel_push,
    hand=lambda connecting: (connecting["channel"],),
    into=lambda _answer, connecting: connecting["channel"],
    awaited=True,
)

# WHAT THIS PULL IS FOR, AND IT IS NOT THE COLD COPY. The push above fills a copy
# with nothing on it, so this pull brings down what the primary gained from
# somebody else: several services share one primary here, and a copy that was
# already on this machine is the one that can be behind. The migration below
# reads the live schema off this copy, so a copy that is behind plans a change
# the primary already has.
#
# A MACHINE THAT STARTED WITH NOTHING SKIPS IT, which is what `reader fn
# copy_on_disk` decides and why the disk is asked before anything opens. Nobody
# tells this library which kind of start it is: a rebuild wipes the files, a soft
# restart keeps them, a provisioned disk keeps them, and the same open handles all
# three by asking.
_FILL_THE_COPY = _asking(
    fills="channel",
    ask=channel_pull,
    hand=lambda connecting: (connecting["channel"],),
    into=lambda _answer, connecting: connecting["channel"],
    awaited=True,
)

# THE CHANNEL'S THREE STEPS, IN THE ORDER THEY RUN, and the order is the whole of
# what they say. They are a table rather than three entries written into
# `THE_ORDER` so that a test reads them by name: a tuple of nineteen closures
# says nothing about which one is the push.
#
# A LOCAL DATABASE HAS NO CHANNEL AND TAKES ALL THREE DO-NOTHING HALVES. The
# first asks whether the caller named a primary and the other two ask whether a
# channel was opened, which is one question each rather than one answer carried
# between them.
THE_CHANNEL_IN_ORDER: tuple[tuple[str, Step], ...] = (
    (
        "open",
        _either(
            question=_whether_it_is_set("primary"),
            ways={True: _A_CHANNEL_TO_THE_PRIMARY, False: _as_it_stands},
        ),
    ),
    (
        "push",
        _either(
            question=_whether_it_is_set("channel"),
            ways={True: _SEND_WHAT_IS_LOCAL, False: _as_it_stands},
        ),
    ),
    (
        "pull",
        _either(
            question=_whether_the_copy_can_be_behind,
            ways={
                (True, True): _FILL_THE_COPY,
                (True, False): _as_it_stands,
                (False, True): _as_it_stands,
                (False, False): _as_it_stands,
            },
        ),
    ),
)


# --- listening for the platform -------------------------------------------------
#
# TWO QUESTIONS SETTLE IT AND EACH HAS TWO ANSWERS, so the table has four rows
# and no fallback: whether there is a channel to push up, and which word the
# caller opened with.


def _what_happens_at_exit(connecting: Connecting) -> tuple[bool, Shutdown]:
    return (replicates(connecting["database"]), connecting["shutdown"])


def _both_doors(connecting: Connecting) -> None | Fault:
    """Put the handler in front of the platform's signal, and put this database
    on the list that handler reads."""
    installed = lifecycle_install_handler()
    registered = lifecycle_register(connecting["database"])
    return _the_first_refusal([one for one in [installed, registered] if _is_fault(one)])


_LISTEN = _asking(
    fills="database",
    ask=_both_doors,
    hand=lambda connecting: (connecting,),
    into=lambda _answer, connecting: connecting["database"],
    awaited=False,
)


# --- planning the migration ------------------------------------------------------


def _views_needing_a_mechanism(operations: list[Operation]) -> list[View]:
    """The declared views that ask to be kept up to date.

    `fn what_an_operation_carries` says which kinds fill the `view` field, so
    nothing here reads a kind's name. A view with no freshness is an ordinary
    view, and the engine answers each question against it as it is asked.
    """
    return [
        one["view"]
        for one in operations
        if what_an_operation_carries(one["kind"])["view"]
        and one["view"] is not NOTHING
        and one["view"]["freshness"] is not NOTHING
    ]


def _under_their_view_names(views: list[View], built: list[Any]) -> list[Operation]:
    """`fn mechanism_for` is never shown the view, so it leaves the name empty.

    Whoever holds the `View` fills it in, and that is here.
    """
    return [
        cast("Operation", {**one, "table": view["name"]})
        for view, made in zip(views, built, strict=True)
        for one in cast("list[Operation]", made)
    ]


def _a_mechanism_was_refused(
    _operations: list[Operation], _views: list[View], built: list[Any]
) -> Fault:
    return cast("Fault", [one for one in built if _is_fault(one)][0])


def _with_their_mechanisms(
    operations: list[Operation], views: list[View], built: list[Any]
) -> list[Operation]:
    return [*operations, *_under_their_view_names(views, built)]


_WHETHER_EVERY_MECHANISM_CAME_BACK: dict[bool, Callable[..., list[Operation] | Fault]] = {
    True: _a_mechanism_was_refused,
    False: _with_their_mechanisms,
}


def _every_mechanism_a_view_asked_for(
    operations: list[Operation], engine: Engine
) -> list[Operation] | Fault:
    """A materialized view is a mechanism, and which mechanism the engine decides.

    PostgreSQL has a real materialized view and SQLite does not. A caller says
    how fresh the view has to be, and bridging that to something the engine can
    do is this library's work.
    """
    views = _views_needing_a_mechanism(operations)
    built = [
        mechanism_for(
            cast("Any", view["freshness"]), engine, capability_of(engine, "materialized_view")
        )
        for view in views
    ]
    return _WHETHER_EVERY_MECHANISM_CAME_BACK[[one for one in built if _is_fault(one)] != []](
        operations, views, built
    )


def _carry_on(step: Callable[[Any], Any], carried: object) -> object:
    return step(carried)


def _stop_here(_step: Callable[[Any], Any], carried: object) -> object:
    """Something already refused, so this question is never asked."""
    return carried


_WHETHER_TO_TAKE_IT_FURTHER: dict[bool, Callable[..., object]] = {
    False: _carry_on,
    True: _stop_here,
}


def _the_plan(connecting: Connecting) -> Plan | Fault:
    """Five questions in order, and the first refusal is the answer.

    What has to change, whether any of it is a rename nothing can tell from a
    drop, which mechanisms the declared views need, what order the changes go in,
    and whether any of it destroys data no model asked to lose.
    """
    declared = connecting["declared"]
    engine = connecting["engine"]
    carried: object = difference(connecting["live"], connecting["wanted_schema"])
    for step in (
        lambda operations: name_what_is_ambiguous(operations, declared),
        lambda operations: _every_mechanism_a_view_asked_for(operations, engine),
        order_operations,
        name_what_destroys,
        lambda plan: refuse_undeclared_loss(plan, declared),
    ):
        carried = _WHETHER_TO_TAKE_IT_FURTHER[_is_fault(carried)](step, carried)
    return cast("Plan | Fault", carried)


async def _nothing_to_migrate(connecting: Connecting) -> Connecting | Fault:
    """The database already matches its models, so no statement changes it.

    The empty report is written out rather than left as it arrived. What the
    door happened to hand over is the door's business, and a caller reading this
    report has to see a plan of nothing because nothing had to change.
    """
    return cast(
        "Connecting",
        {
            **connecting,
            "needs_migration": False,
            "migration": Report(plan=Plan(operations=[], destroys=[]), applied=0),
        },
    )


async def _work_out_what_changes(connecting: Connecting) -> Connecting | Fault:
    """The database is behind its models, so work out what has to change.

    IT IS NOT A ROW IN `THE_ORDER` BECAUSE IT WRITES TWO FIELDS. Every row
    writes one. This writes the plan and it writes down that a migration is
    needed, and the step below reads that second field to decide whether to run
    anything. Squeezing it into a row left `needs_migration` false with a plan
    of changes sitting beside it, and nothing ran.
    """
    plan = _the_plan(connecting)
    return _WHETHER_IT_REFUSED[_is_fault(plan)](
        cast("Connecting", {**connecting, "needs_migration": True}),
        plan,
        "migration",
        lambda answer, _connecting: Report(plan=answer, applied=0),
    )


def _whether_it_is_behind_its_models(connecting: Connecting) -> bool:
    return migration_needed(connecting["live"], connecting["wanted_schema"])


# --- running it -------------------------------------------------------------------


async def _run_them_as_one(
    connecting: Connecting, per_operation: list[list[Statement]]
) -> tuple[Fault | None, int]:
    """One transaction over the whole batch, so a failure undoes all of it."""
    statements = [one for spelled in per_operation for one in spelled]
    ran = await query_run_together(
        connecting["writers"][0],
        connecting["engine"],
        statements,
        how_to_write(connecting["engine"]),
    )
    refused = [ran][: int(_is_fault(ran))]
    return _the_first_refusal(refused), len(per_operation) * int(refused == [])


async def _stopped_already(
    _connecting: Connecting, _spelled: list[Statement], carried: tuple[Fault | None, int]
) -> tuple[Fault | None, int]:
    """The engine already refused one, so this operation never runs."""
    return carried


async def _run_this_one(
    connecting: Connecting, spelled: list[Statement], carried: tuple[Fault | None, int]
) -> tuple[Fault | None, int]:
    ran = [
        await query_run_statement(connecting["writers"][0], connecting["engine"], one)
        for one in spelled
    ]
    refused = [one for one in ran if _is_fault(one)]
    return _the_first_refusal(refused), carried[1] + int(refused == [])


_WHETHER_IT_IS_STILL_GOING: dict[bool, Callable[..., Awaitable[tuple[Fault | None, int]]]] = {
    True: _run_this_one,
    False: _stopped_already,
}


async def _run_each_on_its_own(
    connecting: Connecting, per_operation: list[list[Statement]]
) -> tuple[Fault | None, int]:
    """Each operation on its own, so whatever already landed stays landed."""
    carried: tuple[Fault | None, int] = (None, 0)
    for spelled in per_operation:
        carried = await _WHETHER_IT_IS_STILL_GOING[carried[0] is None](connecting, spelled, carried)
    return carried


HOW_FAR_BACK_A_FAILURE_GOES: dict[
    MigrationMode, Callable[..., Awaitable[tuple[Fault | None, int]]]
] = {
    "all_or_nothing": _run_them_as_one,
    "per_operation": _run_each_on_its_own,
}


# WHEN A CHANGE HAS TO COMMIT AS IT GOES, THE WHOLE PLAN DOES. Measured
# 2026-10-10: a rename on a replicated Turso is an add, a copy and a drop, and
# all three inside one transaction failed the push and left the literal string
# `'None'` on the primary. The same three in three transactions all arrived.
# `fn how_to_spell` says which changes are like that, so the mode is the
# engine's answer unless this plan holds one of them.
_HOW_FAR_BACK_THIS_PLAN_GOES: dict[bool, Callable[[Connecting], MigrationMode]] = {
    True: lambda _connecting: "per_operation",
    False: lambda connecting: how_to_migrate(connecting["engine"]),
}


def _the_mode_this_plan_runs_in(
    connecting: Connecting, operations: list[Operation]
) -> MigrationMode:
    """How far back a failure goes, for this plan on this database.

    Asked of the plan and not of the engine alone, because a change whose
    statements have to be committed one at a time cannot be wrapped in one
    transaction with the rest, however far back this engine would otherwise roll.
    """
    asked = [
        how_to_spell(one["kind"], connecting["engine"], _whether_it_replicates(connecting))
        for one in operations
    ]
    committing = [one for one in asked if not _is_fault(one) if one["commits_between"]]  # type: ignore[typeddict-item]
    return _HOW_FAR_BACK_THIS_PLAN_GOES[committing != []](connecting)


# WHAT AN OPERATOR FINDS THE NEXT MORNING, said as what survived rather than as
# the name of a mode.
WHAT_A_STOPPED_MIGRATION_LEFT: dict[MigrationMode, str] = {
    "all_or_nothing": (
        "The engine undid every change the migration had made, so the database "
        "is as it was before the deploy started."
    ),
    "per_operation": (
        "This engine commits each change as it makes it, so the database is part "
        "way through its migration. Somebody has to look at the database by hand "
        "and decide what to do with the changes that already landed."
    ),
}


def _the_engine_stopped(
    connecting: Connecting, operations: list[Operation], carried: tuple[Fault | None, int]
) -> Fault:
    """Name the operation it stopped on, what the engine said, and what is left."""
    stopped = operations[carried[1]]
    return Fault(
        kind="cut_short",
        what_went_wrong=(
            f"Opening {connecting['target']} stopped partway through its migration. "
            f"It ran {carried[1]} of {len(operations)} changes and stopped on "
            f"{stopped['kind']} on {stopped['table']!r}. The engine said: "
            + str(cast("Fault", carried[0])["what_went_wrong"])
        ),
        what_to_do_instead=WHAT_A_STOPPED_MIGRATION_LEFT[
            _the_mode_this_plan_runs_in(connecting, operations)
        ],
    )


def _the_migration_ran(
    connecting: Connecting, _operations: list[Operation], carried: tuple[Fault | None, int]
) -> Connecting:
    return cast(
        "Connecting",
        {**connecting, "migration": {**connecting["migration"], "applied": carried[1]}},
    )


_WHETHER_THE_ENGINE_TOOK_IT_ALL: dict[bool, Callable[..., Connecting | Fault]] = {
    True: _the_engine_stopped,
    False: _the_migration_ran,
}


async def _a_change_this_engine_cannot_write(
    _connecting: Connecting, _operations: list[Operation], _spelled: list[Any], refused: list[Any]
) -> Fault:
    return cast("Fault", refused[0])


async def _every_change_spelled_out(
    connecting: Connecting, operations: list[Operation], spelled: list[Any], _refused: list[Any]
) -> Connecting | Fault:
    carried = await HOW_FAR_BACK_A_FAILURE_GOES[
        _the_mode_this_plan_runs_in(connecting, operations)
    ](connecting, cast("list[list[Statement]]", spelled))
    return _WHETHER_THE_ENGINE_TOOK_IT_ALL[carried[0] is not None](connecting, operations, carried)


_WHETHER_EVERY_CHANGE_HAS_A_SPELLING: dict[bool, Callable[..., Awaitable[Connecting | Fault]]] = {
    True: _a_change_this_engine_cannot_write,
    False: _every_change_spelled_out,
}


def _whether_it_replicates(connecting: Connecting) -> bool:
    """Whether this database has a primary, which is whether it holds a channel.

    `fn replicates` asks the same question of a finished `Database`. The handle
    is built before the migration is planned, and what both read is the channel,
    so this reads it off the record being filled rather than waiting for one.
    """
    return connecting["channel"] is not NOTHING


async def _carry_out_the_plan(connecting: Connecting) -> Connecting | Fault:
    operations = connecting["migration"]["plan"]["operations"]
    replicating = _whether_it_replicates(connecting)
    spelled = [spell_operation(one, connecting["engine"], replicating) for one in operations]
    refused = [one for one in spelled if _is_fault(one)]
    return await _WHETHER_EVERY_CHANGE_HAS_A_SPELLING[refused != []](
        connecting, operations, spelled, refused
    )


def _whether_anything_has_to_run(connecting: Connecting) -> bool:
    return connecting["needs_migration"]


# WHAT THE DATABASE HOLDS NOW, asked of the database rather than of a file. It is
# named because the channel steps above have to run before it: on a replicated
# database this reads the local copy, so a copy holding changes that never
# reached the primary reads as a copy that matches its models.
THE_LIVE_SCHEMA_READ = _asking(
    fills="live",
    ask=schema_inspect,
    hand=lambda connecting: (
        connecting["connections"][0],
        connecting["engine"],
        how_to_inspect(connecting["engine"]),
    ),
    into=_the_answer_itself,
    awaited=True,
)


# --- the order, as data ----------------------------------------------------------
#
# A TEST CAN READ IT, AND NOTHING HERE DECIDES IT AT RUN TIME.

THE_ORDER: tuple[Step, ...] = (
    # The target says which engine, so nobody has to name one. It is asked once,
    # before anything opens, because a local Turso database is a bare path and a
    # bare path reads as SQLite.
    _asking(
        fills="engine",
        ask=engine_of,
        hand=lambda connecting: (connecting["target"],),
        into=_the_answer_itself,
        awaited=False,
    ),
    # BEFORE ANYTHING OPENS, BECAUSE OPENING MAKES THE FILE. Measured 2026-10-10:
    # `turso.aio.connect` leaves a database at a path that held nothing a moment
    # earlier, so this question asked after the connections are open answers yes
    # on a machine that started with an empty disk.
    _asking(
        fills="copy_was_here",
        ask=copy_on_disk,
        hand=lambda connecting: (connecting["target"],),
        into=_the_answer_itself,
        awaited=False,
    ),
    _asking(
        fills="settings",
        ask=refuse_unusable_settings,
        hand=lambda connecting: (connecting["engine"], _what_the_caller_set(connecting)),
        into=_the_answer_itself,
        awaited=False,
    ),
    # The connection count and the crew's size are two numbers, not one. They
    # were one field until 2026-09-22, and opening with the crew set to 2 and
    # the connections to 4 opened four writers.
    _asking(
        fills="permitted",
        ask=permitted_connections,
        hand=lambda connecting: (connecting["connections_asked"],),
        into=_the_answer_itself,
        awaited=False,
    ),
    _asking(
        fills="crew_size",
        ask=crew_size,
        hand=lambda connecting: (connecting["crew_asked"],),
        into=_the_answer_itself,
        awaited=False,
    ),
    # THE CHANNEL'S WORK COMES BEFORE ANY CONNECTION TO THE COPY, and the driver
    # insists on it. Measured 2026-10-10: with a reader and two writers already
    # open on the replica, the pull refuses with "sync engine operation failed:
    # database error: Database is busy", so an open that connected first could
    # never bring the primary down. The push goes through either way, which is
    # why this only showed up on a copy that was already here.
    #
    # IT IS ALSO THE RIGHT ORDER TO READ A SCHEMA IN. The copy has to be current
    # before anything asks it what it holds, and the connections are what ask.
    *(step for _word, step in THE_CHANNEL_IN_ORDER),
    _asking(
        fills="connections",
        ask=session_open_connection,
        hand=lambda connecting: (
            connecting["target"],
            connecting["engine"],
            connecting["permitted"],
        ),
        into=_in_a_list,
        awaited=True,
    ),
    _asking(
        fills="writers",
        ask=_every_writer,
        hand=lambda connecting: (
            connecting["target"],
            connecting["engine"],
            connecting["crew_size"],
        ),
        into=_the_answer_itself,
        awaited=True,
    ),
    _build_the_handle,
    # One drainer per writer, each spending its turns at `fn lifecycle_turn`.
    _asking(
        fills="database",
        ask=crew_start,
        hand=lambda connecting: (connecting["database"], lifecycle_turn),
        into=_the_crew_it_started,
        awaited=True,
    ),
    _either(
        question=_what_happens_at_exit,
        ways={
            (True, "replicate_then_exit"): _LISTEN,
            (True, "exit_immediately"): _as_it_stands,
            (False, "replicate_then_exit"): _as_it_stands,
            (False, "exit_immediately"): _as_it_stands,
        },
    ),
    _asking(
        fills="declared",
        ask=schema_read_declared,
        hand=lambda _connecting: (),
        into=_the_answer_itself,
        awaited=False,
    ),
    # A model named after one of this library's own tables would collide with it.
    _asking(
        fills="declared",
        ask=refuse_owned_names,
        hand=lambda connecting: (connecting["declared"],),
        into=_what_was_already_there,
        awaited=False,
    ),
    _asking(
        fills="wanted_schema",
        ask=schema_from_declared,
        hand=lambda connecting: (connecting["declared"],),
        into=_the_answer_itself,
        awaited=False,
    ),
    THE_LIVE_SCHEMA_READ,
    _either(
        question=_whether_it_is_behind_its_models,
        ways={True: _work_out_what_changes, False: _nothing_to_migrate},
    ),
    _either(
        question=_whether_anything_has_to_run,
        ways={True: _carry_out_the_plan, False: _as_it_stands},
    ),
    _say_it_opened,
)


async def _keep_the_refusal(_step: Step, carried: object) -> object:
    """A step already refused, so every step after it hands that refusal on."""
    return carried


async def _take_the_next_step(step: Step, carried: object) -> object:
    return await step(cast("Connecting", carried))


_WHETHER_SOMETHING_ALREADY_REFUSED_IT: dict[bool, Callable[..., Awaitable[object]]] = {
    True: _keep_the_refusal,
    False: _take_the_next_step,
}


async def session_connect(connecting: Connecting) -> Connecting | Fault:
    """Open a database, bring it up to its models, and hand back the handle.

    What comes back is the record this was handed with every field filled in, so
    a reader can see the engine it decided, the numbers it settled, what it
    opened and what the migration did. A refusal at any step is the answer
    instead, and every step after it does nothing.
    """
    carried: object = connecting
    opened: Connecting = connecting
    for step in THE_ORDER:
        carried = await _WHETHER_SOMETHING_ALREADY_REFUSED_IT[_is_fault(carried)](step, carried)
        opened = _WHATEVER_IS_STILL_A_RECORD[_is_fault(carried)](opened, carried)
    closed = await _WHETHER_IT_REFUSED_AFTER_OPENING[_is_fault(carried)](opened)
    log_write(_WHAT_TO_SAY_ABOUT_THE_ANSWER[_is_fault(carried)](opened, carried, closed))
    return cast("Connecting | Fault", carried)


def _the_last_record_before_the_refusal(opened: Connecting, _carried: object) -> Connecting:
    """A refusal replaces the record, so the record before it is the one that
    knows what had been opened by then."""
    return opened


def _the_record_this_step_made(_opened: Connecting, carried: object) -> Connecting:
    return cast("Connecting", carried)


_WHATEVER_IS_STILL_A_RECORD: dict[bool, Callable[[Connecting, object], Connecting]] = {
    True: _the_last_record_before_the_refusal,
    False: _the_record_this_step_made,
}


async def _close_what_was_opened(opened: Connecting) -> int:
    """Every connection this open made, closed because the caller never got them.

    WHY A REFUSED OPEN CLOSES ITS OWN CONNECTIONS. The steps that open come
    before the steps that refuse, so a refusal at `fn refuse_undeclared_loss`
    happens with a reader and every writer already open. The caller gets a fault
    and no handle, so no caller can ever have written through them: there is
    nothing to push and nothing to lose by closing. Leaving them open costs a
    file handle and an operating system thread for the life of the process, and
    the driver's own thread is not a daemon, so a process that was going to exit
    does not. Measured 2026-10-02: three threads outlived every test run.
    """
    held = [*opened["writers"], *opened["connections"]]
    for connection in held:
        await session_close_connection(connection)
    return len(held)


async def _nothing_was_opened(_opened: Connecting) -> int:
    """The open finished, so the caller holds the connections and closes them at
    shutdown. Nothing was closed here, and that is the answer rather than a
    silence: both halves hand back a count the line below reads."""
    return 0


_WHETHER_IT_REFUSED_AFTER_OPENING: dict[bool, Callable[[Connecting], Awaitable[int]]] = {
    True: _close_what_was_opened,
    False: _nothing_was_opened,
}


# WHAT AN OPERATOR READS, EITHER WAY. A refused open wrote nothing to the log
# until 2026-10-03, so a deploy that stopped on a destructive change left the
# person reading the log with no line at all. Both halves build a record, because
# a logging call made for its effect in one branch and skipped in the other is
# the shape that let the refusal go unrecorded.
def _say_it_refused(opened: Connecting, carried: object, closed: int) -> dict[str, object]:
    fault = cast("Fault", carried)
    return {
        "event": "database_refused",
        "level": "error",
        "target": opened["target"],
        "kind": fault["kind"],
        "why": fault["what_went_wrong"],
        "what_to_do_instead": fault["what_to_do_instead"],
        "connections_closed": closed,
    }


def _say_it_opened_at_all(opened: Connecting, _carried: object, closed: int) -> dict[str, object]:
    """The open succeeded, and `fn _say_it_opened` has already said what it took.

    This line is what the shape costs: one record saying the open finished and
    closed nothing, rather than a branch that logs on one path and not the other.
    """
    return {
        "event": "database_open_finished",
        "target": opened["target"],
        "connections_closed": closed,
    }


_WHAT_TO_SAY_ABOUT_THE_ANSWER: dict[
    bool, Callable[[Connecting, object, int], dict[str, object]]
] = {
    True: _say_it_refused,
    False: _say_it_opened_at_all,
}
