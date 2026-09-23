"""Bind `tests/bdd/features/crew/crew_make.feature`, and nothing else.

WHAT THE FEATURE ASKS. A crew with nobody in it holds nothing and nobody is
working, and its bell can be slept on and rung.

WHY THE SECOND SCENARIO EXISTS. The bell is the difference between a worker
sleeping and a worker looking. `write_queue.py` recorded what looking cost
before the rewrite deleted it: a busy loop starves the worker while a caller
waits with no time limit, which hung about one run in six. A crew whose bell
cannot be rung passes the first scenario and is useless.

TWO BELLS, NOT ONE. A deposit rings `arrived`; a shutdown rings `stop`. Sharing
one would make a deposit look like a shutdown, so the scenario checks they are
not the same object.
"""

from __future__ import annotations

import asyncio

from pytest_bdd import scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.types import Crew

scenarios("../features/crew/crew_make.feature")


@when("a crew is made", target_fixture="crew")
def _a_crew_is_made() -> Crew:
    return crew_make()


@then("nothing is waiting and nobody is working")
def _it_holds_nothing(crew: Crew) -> None:
    assert not crew["arrivals"], f"a new crew already has {crew['arrivals']} waiting"
    assert not crew["writes"], f"a new crew already holds {crew['writes']}"
    assert not crew["waiting"], f"a new crew already has callers parked: {crew['waiting']}"
    assert not crew["drainers"], f"a new crew already has workers running: {crew['drainers']}"
    assert crew["size"] == 0, (
        f"a new crew says it is {crew['size']} strong, and nothing has started "
        f"any workers yet, so a caller would believe writes are being drained"
    )


@then("a worker can sleep on its bell and a deposit can ring it")
def _the_bells_work(crew: Crew) -> None:
    assert crew["arrived"] is not crew["stop"], (
        "the arrival bell and the stop bell are the same object, so handing "
        "over one write would end every worker"
    )

    async def _rung_wakes_a_sleeper() -> bool:
        """Sleep on the bell, ring it, and see whether the sleeper woke.

        Bounded, because a bell that cannot be rung hangs the suite rather than
        failing it.
        """
        asyncio.get_running_loop().call_soon(crew["arrived"].set)
        await asyncio.wait_for(crew["arrived"].wait(), 5.0)
        return crew["arrived"].is_set()

    assert asyncio.run(_rung_wakes_a_sleeper()), (
        "a worker slept on the arrival bell and a ring did not wake it, so "
        "every deposit would sit until the idle limit ran out"
    )
