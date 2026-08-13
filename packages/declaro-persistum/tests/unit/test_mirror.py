"""The mirror: two databases kept in step during a cutover, as data.

`MirrorPool`, `MirrorConnection` and `MirrorCursor` were three classes
wrapping two connections and two booleans. A mirror is two databases and two
policies; what is done with it is functions.

Real local Turso databases on both sides. A fake would let a divergence test
pass by construction, and detecting divergence is the entire reason a cutover
runs in dual-write instead of just switching over.
"""

from __future__ import annotations

import pytest

from declaro_persistum.mirror import (
    compare,
    detach,
    mirror,
    mirror_reading,
    mirror_writing,
    parallel_write,
    promote,
)
from declaro_persistum.turso_database import migrating, open_turso

pytestmark = pytest.mark.turso


async def _pair(tmp_path):
    a = await open_turso(str(tmp_path / "a.db"), shutdown="exit_immediately")
    b = await open_turso(str(tmp_path / "b.db"), shutdown="exit_immediately")
    for db in (a, b):
        conn = await migrating(db)
        await conn.execute("CREATE TABLE t (v INTEGER)")
        await conn.commit()
        await conn.close()
    return a, b


@pytest.mark.asyncio
async def test_a_write_reaches_both(tmp_path):
    a, b = await _pair(tmp_path)
    m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=True)

    await parallel_write(m, "INSERT INTO t VALUES (?)", (1,))

    for db in (a, b):
        from declaro_persistum.database import reading

        async with reading(db) as conn:
            cur = await conn.execute("SELECT count(*) FROM t")
            assert (await cur.fetchone())[0] == 1


@pytest.mark.asyncio
async def test_reads_come_from_the_primary(tmp_path):
    """The replica is not authoritative until promote.

    Reading from it would hide the divergence the cutover exists to find.
    """
    a, b = await _pair(tmp_path)
    m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=False)

    async with mirror_writing(m) as (pa, _pb):
        await pa.execute("INSERT INTO t VALUES (?)", (7,))
        await pa.commit()

    async with mirror_reading(m) as conn:
        cur = await conn.execute("SELECT v FROM t")
        assert (await cur.fetchone())[0] == 7


@pytest.mark.asyncio
async def test_a_divergence_is_recorded_not_raised(tmp_path):
    """The whole point: notice that the two disagree, keep serving."""
    a, b = await _pair(tmp_path)
    m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=True)

    # Write to the primary only, so the two genuinely differ.
    from declaro_persistum.database import writing

    async with writing(a) as conn:
        await conn.execute("INSERT INTO t VALUES (?)", (1,))
        await conn.commit()

    rows, m2 = await compare(m, "SELECT v FROM t", ())

    assert rows == [(1,)], rows
    assert len(m2["divergences"]) == 1, m2["divergences"]
    assert m["divergences"] == [], "the original value was mutated"


@pytest.mark.asyncio
async def test_agreement_records_nothing(tmp_path):
    a, b = await _pair(tmp_path)
    m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=True)

    await parallel_write(m, "INSERT INTO t VALUES (?)", (1,))
    _rows, m2 = await compare(m, "SELECT v FROM t", ())

    assert m2["divergences"] == []


@pytest.mark.asyncio
async def test_compare_off_never_reads_the_replica(tmp_path):
    """`compare_on_read=False` must not pay for a read it was told to skip."""
    a, b = await _pair(tmp_path)
    m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=False)

    from declaro_persistum.database import writing

    async with writing(a) as conn:
        await conn.execute("INSERT INTO t VALUES (?)", (1,))
        await conn.commit()

    _rows, m2 = await compare(m, "SELECT v FROM t", ())
    assert m2["divergences"] == [], "it compared despite being told not to"


class TestThePhasesReturnNewValues:
    """promote and detach return a new Mirror rather than mutating one.

    A caller holding the old value would otherwise be silently talking to a
    different database than it believes.
    """

    @pytest.mark.asyncio
    async def test_promote_swaps_without_mutating(self, tmp_path):
        a, b = await _pair(tmp_path)
        m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=True)

        promoted = promote(m)

        assert promoted["primary"] is b
        assert promoted["replica"] is a
        assert m["primary"] is a, "the original mirror was mutated"

    @pytest.mark.asyncio
    async def test_detach_returns_the_survivor(self, tmp_path):
        a, b = await _pair(tmp_path)
        m = mirror(primary=a, replica=b, fail_open=True, compare_on_read=True)

        assert detach(m) is a
        assert detach(promote(m)) is b


def test_both_policies_are_required():
    """Neither has an obviously safe value, so neither is defaulted (Rule 14).

    A cutover starts fail-open, because the point is to shadow production
    without risking it, and ends fail-closed, because by then the mirror IS
    production. A default would pick one of those for someone.
    """
    import inspect

    params = inspect.signature(mirror).parameters
    for name in ("fail_open", "compare_on_read"):
        assert params[name].default is inspect.Parameter.empty, (
            f"{name} has a default; it is a cutover-phase decision"
        )
