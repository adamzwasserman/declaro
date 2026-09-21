"""Bind `tests/bdd/features/core/answer_from_ticket.feature`.

WHAT THE FEATURE ASKS FOR. Two scenarios. The first says a deposited write
answers with its ticket, so the ticket the room handed back has to come out the
other side unchanged. The second says the caller has not waited, which is a
claim about what producing that answer needed, not about what it returned.

HOW THE SECOND ONE IS ANSWERED. Nothing can watch a write fail to run, so the
test asks what the function is able to reach. The declaration gives it one
argument, a ticket, so a database, a room and a connection are not among its
inputs; the test reads the signature to hold that, reads what the code consults
outside its argument, and reads what the module imports. A function that opened
a connection would have to name one of the three, and all three are absent by
construction.

Then it asks whether a caller could be made to wait. `answer_from_ticket` is
not a coroutine function, so there is nothing to await, and a ticket naming a
write no engine has ever seen answers exactly like one that landed.

`docs/architecture.md` line 138: deposit hands back a ticket immediately and
runs nothing. Line 152: a ticket belongs to exactly one caller. This function is
the step that turns that ticket into the one shape a caller reads.
"""

import ast
import inspect
import pathlib
import typing

import pytest
from pytest_bdd import scenarios, then, when

from declaro_persistum import types as declared_types

scenarios("../features/core/answer_from_ticket.feature")


# The three words an implementation would have to say to make the write's run
# matter. None of them is an argument it has, so none of them can be reached
# without going around the declaration.
A_WAY_TO_REACH_THE_WRITE = ("connection", "connect", "room", "deposit", "drain", "execute")

# What a module would import to wait, to run something, or to ask the world.
# The module under test is allowed its own shapes and nothing else.
THE_ONLY_IMPORT_IT_NEEDS = "declaro_persistum.types"


@pytest.fixture
def one_new_person() -> dict:
    """The declaration a caller makes to add a single row.

    `wanted` is a writing word, so this is deposited rather than run, which is
    what puts a ticket in the caller's hand instead of rows.
    """
    return {
        "model": "person",
        "wanted": "add",
        "columns": ["id", "name"],
        "condition": {"each": {}, "any_of": [], "none_of": []},
        "order": {},
        "grouping": [],
        "having": {"each": {}, "any_of": [], "none_of": []},
        "paging": None,
        "rows": [{"id": 1, "name": "Ada"}],
        "across": None,
    }


@pytest.fixture
def a_ticket() -> str:
    """What the room handed back. A Ticket is a str, per the declaration."""
    return "w-0007"


@when("a caller declares one new person")
def _declares_one_new_person(scenario: dict, one_new_person: dict, a_ticket: str) -> None:
    scenario["declaration"] = one_new_person
    scenario["ticket"] = a_ticket


@then("the answer carries the ticket the room handed back")
def _carries_the_ticket(scenario: dict) -> None:
    from declaro_persistum.answer_from_ticket import answer_from_ticket

    answer = answer_from_ticket(scenario["ticket"])

    assert answer == scenario["ticket"], "the caller has to be able to collect on it"

    # A Ticket is one of the arms an Answer may be, which is what lets a read
    # and a write come back through one door.
    assert isinstance(answer, str)
    assert typing.get_args(declared_types.Answer) != (), "Answer is the union the shapes declare"

    # The ticket moves and the answer moves with it. An implementation that
    # returned a constant, or the first ticket it ever saw, passes the line
    # above and fails here.
    tickets = ["w-0001", "w-0007", "another-ticket-entirely", "", "w-0007 "]
    carried = [(t, answer_from_ticket(t)) for t in tickets]
    lost = [seen for seen in carried if seen[0] != seen[1]]
    assert lost == [], f"the ticket did not survive the trip: {lost}"

    # Nothing about the caller's declaration reaches the answer, because the
    # declaration is not one of its arguments.
    assert len(inspect.signature(answer_from_ticket).parameters) == 1


@then("nothing about the answer required the write to have run")
def _required_nothing_to_have_run(a_ticket: str) -> None:
    from declaro_persistum import answer_from_ticket as module
    from declaro_persistum.answer_from_ticket import answer_from_ticket

    # WHAT IT IS GIVEN. One ticket, and no way to say a default for it, so a
    # caller cannot omit it and have the function invent one.
    parameters = inspect.signature(answer_from_ticket).parameters
    assert list(parameters) == ["ticket"], f"the declaration gives it one argument: {parameters}"
    assert parameters["ticket"].default is inspect.Parameter.empty
    assert parameters["ticket"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD

    # NOTHING TO AWAIT. A caller that cannot await it cannot be made to wait on
    # a drainer finishing the write.
    assert not inspect.iscoroutinefunction(answer_from_ticket)
    assert not inspect.isgeneratorfunction(answer_from_ticket)

    # WHAT IT CONSULTS. Every global name the code reads is in `co_names`, so
    # an empty one says the body reads its argument and nothing else. No clock,
    # no connection, no module-level anything.
    consulted = set(answer_from_ticket.__code__.co_names)
    assert consulted == set(), f"it consults something outside its argument: {sorted(consulted)}"
    assert answer_from_ticket.__defaults__ is None
    assert answer_from_ticket.__closure__ is None

    # WHAT THE MODULE CAN REACH. The scan above reads the compiled body; this
    # reads the file, which is where an import sits that a body has not used
    # yet. Both are needed: either alone passes while the other is broken.
    source = pathlib.Path(inspect.getfile(module)).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = sorted(
        name.name if isinstance(node, ast.Import) else f"{node.module}"
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
        for name in node.names
    )
    assert set(imported) <= {THE_ONLY_IMPORT_IT_NEEDS}, (
        f"it can reach further than its shapes: {imported}"
    )

    # WHAT THE CODE NAMES. Identifiers only, so the prose above them can say
    # "connection" while the code may not. A module-level constant holding a
    # room, or a helper defined beside the function, is named here and nowhere
    # in `co_names`.
    named = sorted(
        {
            node.id if isinstance(node, ast.Name) else node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Name | ast.Attribute)
        }
    )
    reachable = [
        name for name in named if any(word in name.lower() for word in A_WAY_TO_REACH_THE_WRITE)
    ]
    assert reachable == [], f"the code names a way to reach the write: {reachable}"

    # THE WRITE NEED NOT HAVE HAPPENED. These tickets name writes no engine has
    # ever seen, and they answer exactly like one that landed.
    never_ran = ["w-9999999", "a-ticket-for-a-write-that-was-refused", "", a_ticket]
    answers = [(t, answer_from_ticket(t)) for t in never_ran]
    withheld = [seen for seen in answers if seen[1] != seen[0]]
    assert withheld == [], f"it answered differently for a write that never ran: {withheld}"

    # Reading it twice reads the same thing, since nothing outside the argument
    # is consulted and nothing is recorded between calls.
    assert answer_from_ticket(a_ticket) == answer_from_ticket(a_ticket)
