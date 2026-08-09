"""SPIKE — throwaway. How often does a write exceed 50ms?

50ms is the write queue's designed trigger: a write slower than that would
be queued and the caller would return immediately. Nobody has measured how
often that happens, so nobody knows whether the queue is worth having.

This measures the CONSUMER-VISIBLE latency of a write: from asking the pool
for a write connection to control coming back with the row committed. That
is the number the queue would hide.

Two paths are measured, because they cost different amounts:
  raw — acquire_write + execute + commit
  orm — table().insert().execute(), what a consumer actually writes

Run locally (no remote, the floor):
    uv run python spike_write_latency.py

Run against a real Turso Cloud replica (the number that decides it):
    TURSO_DATABASE_URL=libsql://... TURSO_AUTH_TOKEN=... \
        uv run python spike_write_latency.py

A local run CANNOT answer the question. Writes to a local replica commit in
well under a millisecond with no network in the path. Only a run against a
real remote, under real concurrency, produces the deciding number.
"""

import asyncio
import os
import statistics
import tempfile
import time
import uuid
from pathlib import Path

from declaro_persistum.pool import ConnectionPool  # noqa: E402
from declaro_persistum.query import table  # noqa: E402

THRESHOLD_MS = 50.0          # the write queue's designed trigger
CONCURRENCY = (1, 2, 3, 5, 10)
WRITES_PER_LEVEL = 120

SCHEMA = {
    "spike": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "n": {"type": "integer", "nullable": True},
        },
        "primary_key": ["id"],
        "indexes": {},
    }
}


# --- pure: statistics -------------------------------------------------------


def percentile(sorted_values: list[float], q: float) -> float:
    """Nearest-rank percentile. q in [0, 1]."""
    if not sorted_values:
        return float("nan")
    rank = max(1, min(len(sorted_values), round(q * len(sorted_values))))
    return sorted_values[rank - 1]


def summarize(samples: list[float], failures: int) -> dict[str, float | int]:
    ordered = sorted(samples)
    over = [s for s in ordered if s > THRESHOLD_MS]
    return {
        "n": len(ordered),
        "failures": failures,
        "p50": percentile(ordered, 0.50),
        "p90": percentile(ordered, 0.90),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "max": ordered[-1] if ordered else float("nan"),
        "mean": statistics.fmean(ordered) if ordered else float("nan"),
        "over_threshold": len(over),
        "pct_over": (100.0 * len(over) / len(ordered)) if ordered else float("nan"),
    }


def format_row(label: str, s: dict) -> str:
    return (
        f"  {label:<14} n={s['n']:<5} fail={s['failures']:<4} "
        f"p50={s['p50']:7.2f} p90={s['p90']:7.2f} p95={s['p95']:7.2f} "
        f"p99={s['p99']:8.2f} max={s['max']:9.2f}  "
        f">50ms: {s['over_threshold']:>4} ({s['pct_over']:5.1f}%)"
    )


# --- measurement ------------------------------------------------------------


async def time_raw_write(pool, n: int) -> float:
    """Consumer-visible cost of one raw write, in milliseconds."""
    t0 = time.perf_counter()
    async with pool.acquire_write() as conn:
        await conn.execute(
            "INSERT INTO spike (id, n) VALUES (?, ?)", (str(uuid.uuid4()), n)
        )
    return (time.perf_counter() - t0) * 1000.0


async def time_orm_write(rows, n: int) -> float:
    """Consumer-visible cost of one ORM write, in milliseconds."""
    t0 = time.perf_counter()
    await rows.insert(id=str(uuid.uuid4()), n=n).execute()
    return (time.perf_counter() - t0) * 1000.0


async def run_level(writer, workers: int, total: int) -> tuple[list[float], int]:
    """Drive `total` writes across `workers` concurrent tasks."""
    samples: list[float] = []
    failures = 0
    per_worker = max(1, total // workers)

    async def worker(index: int) -> None:
        nonlocal failures
        for i in range(per_worker):
            try:
                samples.append(await writer(index * 10_000 + i))
            except Exception:
                failures += 1

    await asyncio.gather(*(worker(w) for w in range(workers)))
    return samples, failures


async def main() -> int:
    remote = os.environ.get("TURSO_DATABASE_URL") or os.environ.get("TURSO_URL")
    token = os.environ.get("TURSO_AUTH_TOKEN")

    tmp = tempfile.mkdtemp(prefix="spike-")
    db = str(Path(tmp) / "spike.db")

    if remote:
        print(f"target: Turso Cloud replica ({remote.split('//')[-1][:40]})")
        pool = await ConnectionPool.turso(db, remote_url=remote, auth_token=token,
                                          max_size=max(CONCURRENCY))
    else:
        print("target: LOCAL Turso, no remote")
        print("WARNING: a local run cannot answer the question. It measures the")
        print("         floor with no network in the path. Set TURSO_DATABASE_URL")
        print("         and TURSO_AUTH_TOKEN to get the deciding number.")
        pool = await ConnectionPool.turso(db, max_size=max(CONCURRENCY))

    async with pool.acquire_write(concurrent=False) as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS spike (id TEXT PRIMARY KEY, n INTEGER)"
        )

    rows = table("spike", SCHEMA, pool)

    print(f"\nthreshold = {THRESHOLD_MS:.0f}ms (the write queue's trigger)")
    print(f"{WRITES_PER_LEVEL} writes per concurrency level, latencies in ms\n")

    verdict_rows: list[tuple[int, str, dict]] = []

    for workers in CONCURRENCY:
        print(f"concurrency {workers}:")
        raw_s, raw_f = await run_level(
            lambda n: time_raw_write(pool, n), workers, WRITES_PER_LEVEL
        )
        raw = summarize(raw_s, raw_f)
        print(format_row("raw", raw))
        verdict_rows.append((workers, "raw", raw))

        orm_s, orm_f = await run_level(
            lambda n: time_orm_write(rows, n), workers, WRITES_PER_LEVEL
        )
        orm = summarize(orm_s, orm_f)
        print(format_row("orm", orm))
        verdict_rows.append((workers, "orm", orm))

    await pool.close()

    print("\n--- what this says about the write queue ---")
    worst = max(verdict_rows, key=lambda r: r[2]["pct_over"])
    total_over = sum(r[2]["over_threshold"] for r in verdict_rows)
    total_n = sum(r[2]["n"] for r in verdict_rows)
    print(f"writes over {THRESHOLD_MS:.0f}ms overall: {total_over}/{total_n} "
          f"({100.0 * total_over / total_n:.1f}%)")
    print(f"worst case: concurrency {worst[0]} {worst[1]} path, "
          f"{worst[2]['pct_over']:.1f}% over threshold, p99 {worst[2]['p99']:.1f}ms")
    if not remote:
        print("\nLOCAL RUN — not the deciding number. Re-run against a real remote.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
