"""A queue is a list of writes that have not happened yet.

Everything here is a plain function over that list. There is no class, no
hidden state, no background supervisor, and no clock. The caller holds the
list and runs the loop, so a queue that is never drained is visible in the
caller's own code rather than sitting empty inside an object nobody can see.

`drain` takes the write function as an argument, so these tests use plain
recording functions. No mocks, no database, no pool.
"""

import pytest

from declaro_persistum.write_queue import DrainFailed, add, drain, remove


def _write(key: str, n: int = 0):
    return {"key": key, "sql": f"INSERT INTO t VALUES ({n})", "params": (n,)}


class TestAdd:
    """add puts a write on the list and returns a new list."""

    def test_add_returns_a_list_containing_the_write(self):
        assert add((), _write("a")) == (_write("a"),)

    def test_add_appends_in_order(self):
        pending = add(add((), _write("a", 1)), _write("b", 2))
        assert [w["key"] for w in pending] == ["a", "b"]

    def test_add_does_not_mutate_the_original(self):
        original = ()
        add(original, _write("a"))
        assert original == ()

    def test_two_writes_to_the_same_row_both_stay(self):
        """Both writes happened; applying only the last would lose one."""
        pending = add(add((), _write("a", 1)), _write("a", 2))
        assert len(pending) == 2


class TestRemove:
    """remove takes a write off the list and returns a new list."""

    def test_remove_drops_the_matching_key(self):
        pending = add(add((), _write("a")), _write("b"))
        assert [w["key"] for w in remove(pending, "a")] == ["b"]

    def test_remove_does_not_mutate_the_original(self):
        pending = add((), _write("a"))
        remove(pending, "a")
        assert len(pending) == 1

    def test_removing_an_absent_key_changes_nothing(self):
        pending = add((), _write("a"))
        assert remove(pending, "zzz") == pending

    def test_remove_drops_every_entry_with_that_key(self):
        pending = add(add((), _write("a", 1)), _write("a", 2))
        assert remove(pending, "a") == ()


class TestDrain:
    """drain executes each write and removes the ones that land."""

    @pytest.mark.asyncio
    async def test_every_write_is_executed_in_order(self):
        seen = []

        async def execute(w):
            seen.append(w["key"])

        await drain(add(add((), _write("a")), _write("b")), execute, attempts=3)
        assert seen == ["a", "b"]

    @pytest.mark.asyncio
    async def test_an_emptied_queue_comes_back_empty(self):
        async def execute(_w):
            return None

        remaining = await drain(add((), _write("a")), execute, attempts=3)
        assert remaining == ()

    @pytest.mark.asyncio
    async def test_the_next_write_waits_for_the_previous_return(self):
        """The return value releases the next one. Nothing overlaps."""
        events = []

        async def execute(w):
            events.append(f"start {w['key']}")
            events.append(f"end {w['key']}")

        await drain(add(add((), _write("a")), _write("b")), execute, attempts=3)
        assert events == ["start a", "end a", "start b", "end b"]

    @pytest.mark.asyncio
    async def test_draining_an_empty_queue_does_nothing(self):
        async def execute(_w):
            raise AssertionError("nothing to execute")

        assert await drain((), execute, attempts=3) == ()


class TestRetry:
    """Three attempts, then raise. The write stays on the list."""

    @pytest.mark.asyncio
    async def test_a_failing_write_is_attempted_three_times(self):
        tries = []

        async def execute(w):
            tries.append(w["key"])
            raise RuntimeError("nope")

        with pytest.raises(DrainFailed):
            await drain(add((), _write("a")), execute, attempts=3)
        assert tries == ["a", "a", "a"]

    @pytest.mark.asyncio
    async def test_a_write_that_succeeds_on_the_third_try_is_removed(self):
        calls = {"n": 0}

        async def execute(_w):
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("not yet")

        remaining = await drain(add((), _write("a")), execute, attempts=3)
        assert calls["n"] == 3
        assert remaining == ()

    @pytest.mark.asyncio
    async def test_the_failed_write_is_still_on_the_returned_list(self):
        """A raise must not lose the queue -- the caller needs it back."""

        async def execute(_w):
            raise RuntimeError("nope")

        with pytest.raises(DrainFailed) as caught:
            await drain(add((), _write("a")), execute, attempts=3)
        assert [w["key"] for w in caught.value.pending] == ["a"]

    @pytest.mark.asyncio
    async def test_writes_that_already_landed_are_not_on_the_returned_list(self):
        """Only the unfinished work comes back, so a retry cannot double-apply."""

        async def execute(w):
            if w["key"] == "b":
                raise RuntimeError("nope")

        with pytest.raises(DrainFailed) as caught:
            await drain(add(add((), _write("a")), _write("b")), execute, attempts=3)
        assert [w["key"] for w in caught.value.pending] == ["b"]

    @pytest.mark.asyncio
    async def test_the_original_error_is_the_cause(self):
        original = RuntimeError("connection refused")

        async def execute(_w):
            raise original

        with pytest.raises(DrainFailed) as caught:
            await drain(add((), _write("a")), execute, attempts=3)
        assert caught.value.__cause__ is original

    @pytest.mark.asyncio
    async def test_the_failed_write_is_named(self):
        async def execute(_w):
            raise RuntimeError("nope")

        with pytest.raises(DrainFailed) as caught:
            await drain(add((), _write("a")), execute, attempts=3)
        assert caught.value.write["key"] == "a"

    @pytest.mark.asyncio
    async def test_one_attempt_means_no_retry(self):
        tries = []

        async def execute(_w):
            tries.append(1)
            raise RuntimeError("nope")

        with pytest.raises(DrainFailed):
            await drain(add((), _write("a")), execute, attempts=1)
        assert len(tries) == 1


class TestTheQueueIsJustData:
    """No state, no clock, no I/O of its own."""

    def test_a_pending_list_survives_json(self):
        """Persistence needs no library function: the list is already data."""
        import json

        pending = add(add((), _write("a", 1)), _write("b", 2))
        restored = tuple(
            {**w, "params": tuple(w["params"])}
            for w in json.loads(json.dumps(list(pending)))
        )
        assert restored == pending
