# Free-threaded Turso build

This directory holds a patch to Turso, and the recipe to build a patched `pyturso` wheel with it.

## Who needs this

You need this only if you run declaro-persistum on free-threaded CPython (a `t` build, such as 3.14t) with the GIL off. Everyone else can ignore this directory. A normal `pip install declaro-persistum[turso]` is not affected by anything here, and installs a stock `pyturso` from PyPI.

declaro-persistum publishes a pure-Python wheel and takes `pyturso` from PyPI. It cannot ship a patched engine. This directory carries the patch, its base commit, its build recipe and its evidence, so that a free-threaded deployer can reproduce the wheel and knows exactly what they run and why.

## The defect the patch fixes

With the GIL off, a checkpoint can run at the same time as a sync revert-read. The sync engine reads at watermark 0 after `checkpoint_passive` resets `revert_since_wal_watermark` to 0 on a WAL salt mismatch. A concurrent checkpoint advances `nbackfills` past 0. `find_frame` then asserts `frame_watermark >= nbackfills` and panics.

The result is a crash — a worker abort, SIGABRT. It is not a data-correctness defect.

The patch demotes that assertion to a typed, recoverable error, `LimboError::WatermarkBelowBackfill`, and makes the sync watermark-read wrapper skip the page instead of aborting.

This defect is **not** fixed upstream.

## What this patch is not

Do not confuse this patch with upstream PR #7813, "sync: fix race between concurrent opens of the same synced replica". That is a different defect with a different mechanism, it is fixed upstream, and it shipped in Turso 0.7.0. declaro-persistum requires `pyturso>=0.7.0` to get it. See the 0.1.13 entry in CHANGELOG.md.

The base commit below already contains #7813, so a wheel built from this recipe has both fixes.

## The patch

`turso-wal-readpin.patch` — 158 lines, four hunks:

| File | Change |
|------|--------|
| `core/error.rs` | Adds the `WatermarkBelowBackfill { frame_watermark, nbackfills }` variant |
| `core/storage/wal.rs` | Returns that error instead of panicking, plus the regression test |
| `core/connection.rs` | Treats the error as "frame not found" rather than propagating it |
| `sync/engine/src/database_tape.rs` | Skips the page in the watermark-read wrapper |

SHA-256: `80b9c3c187e738a54ce6597022e0e57a0ab66bc1c3309ad4880a3281b2b190ea`

## Base commit

    bc62e48718d5cfe8388deb57f9de5fa9d572c3ae   (2026-08-05)

The patch applies cleanly to **this commit only**. `git apply --check` returns 0 against it. Its context lines are specific to this commit.

It has **not** been tested against any released tag. Do not assume it applies to one. Pin the commit.

## Build recipe

Target ABI is `cp314t`, CPython 3.14 free-threaded. The runtime must set `PYTHON_GIL=0`.

```dockerfile
FROM debian:bookworm-slim AS turso
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl build-essential pkg-config libssl-dev ca-certificates git
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH=/root/.local/bin:/root/.cargo/bin:$PATH
RUN uv python install 3.14t
WORKDIR /t
RUN git clone --filter=blob:none https://github.com/tursodatabase/turso.git . \
 && git checkout bc62e48718d5cfe8388deb57f9de5fa9d572c3ae
# flip the module GIL-safety flag (required for free-threaded import)
RUN sed -i 's/gil_used = true/gil_used = false/' bindings/python/src/lib.rs \
 && grep -q 'gil_used = false' bindings/python/src/lib.rs
COPY deploy/turso-wal-readpin.patch /tmp/turso-wal-readpin.patch
RUN git apply --verbose /tmp/turso-wal-readpin.patch \
 && grep -q 'WatermarkBelowBackfill' core/error.rs
RUN uv venv --python 3.14t /venv && VIRTUAL_ENV=/venv uv pip install maturin
RUN cd bindings/python && VIRTUAL_ENV=/venv /venv/bin/maturin build --release \
      -i /venv/bin/python -o /wheels
```

### The gil_used flip is mandatory and separate

Upstream ships the Python module with `gil_used = true`. Importing it into a free-threaded interpreter therefore re-enables the GIL, silently. The `sed` step above flips it to `false`.

This is not part of the patch. Without it you get a working import and no error message, and the GIL back on — so any measurement you take is a GIL measurement, and the defect the patch fixes cannot occur because the concurrency that triggers it cannot occur.

## Evidence

**Unit test.** `test_watermark_read_below_backfill_returns_error_not_panic`, in `core/storage/wal.rs` in the `pub mod test` block, gated on `#[cfg(feature = "conn_raw_api")]`.

    cargo test -p turso_core --features conn_raw_api test_watermark_read_below_backfill

RED on stock Turso: `find_frame` panics on the `frame_watermark >= nbackfills` assertion. GREEN with the patch: `find_frame` returns `Err(LimboError::WatermarkBelowBackfill { .. })`.

**Server run.** A free-threaded churn run reached the exact `frame_watermark = 0` case 2208 times. Every one was skipped. Zero panics, zero propagated errors.

## Provenance and status

Written and verified by the multicardz team, who reported and diagnosed the defect. **Not upstream, not released, no open pull request.** A disclosure to the Turso maintainers is being prepared.

If it is accepted upstream and released, delete this directory and raise the `pyturso` floor instead.
