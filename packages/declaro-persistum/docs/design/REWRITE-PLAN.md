# Rewriting declaro-persistum from its declaration

The rewrite is done: every function the declaration names now has a module, and 626 tests pass with 33 xfail and none failing. Measured 2026-09-25.

This was the plan. Now it is the record of what happened.

## The numbers

| | Planned 2026-09-21 | Measured 2026-09-25 |
|---|---|---|
| Functions declared | 66 | 75 |
| Modules in `src/declaro_persistum/` | 0 after stage 0 | 76 |
| Doors | 5 | 4 |
| Orchestrators | 5 | 4 |
| Readers | not counted | 5 |
| Output boundaries | 12 | 18 |
| Pure functions | 37 | 44 |
| Tests passing | 0 after stage 0 | 626 |
| `if` statements in `src/` | 0 | 0 |
| Feature files | 110 | 114 |
| Step files | 0 after stage 0 | 77 |
| Features nothing runs | 110 | 42 |

## The declaration

`docs/design/persistum.hd` passes all four checks, run 2026-09-25: it parses, `validate(module)` returns nothing, the explorer's checker returns nothing, and it draws 47 data edges.

The old source predated all of it, 67 modules and 30,013 lines that nobody had ever reconciled with the declaration. Stage 0 deleted it along with 178 unit tests and 23 step files, so 2,955 passing tests went to zero and came back one function at a time.

## How each function got built

Each function was one commit. The loop:

1. Read its feature file and the lines it cites. Read nothing else.
2. Write its step file, binding only that feature.
3. Take the feature off `KNOWN_UNBOUND`.
4. Run it, and every scenario turns red saying what is missing rather than that a name is undefined.
5. Write the function at the signature the declaration gives.
6. Run until green, and nothing else may go red.
7. Check the declaration four ways.
8. Commit, named for the function.

The order got fixed on 2026-09-24: the `.hd` first, then the feature, then the fixtures in `tests/conftest.py`, then the code. Writing code first and patching the declaration after produced two modules that had to be thrown out and written again.

Rules that held throughout:

- No `if` without a dated approval in a comment. `src/` has none.
- No default arguments. Absence is `NOTHING`, resolved at a boundary.
- A failure comes back in the return type, and it says what to do differently.
- A pure function reads no clock, no random source, no environment, no connection.
- A handle gets held and passed to a boundary, and nobody reads it.
- No defensive check the signature already makes.

## Twelve functions got renamed or dropped

The old plan listed 66 functions. Twelve of those names are gone: `answer_from_ticket`, `migration_report`, `query_begin_concurrent`, `query_collect`, `query_deliver`, `room_deposit`, `room_make`, `room_post_receipt`, `room_release_ticket`, `room_run_write`, `turso_pull`, `turso_push`.

The five `room_*` functions are the crew now, because the declaration has no waiting room in it: `type Crew` holds the writes that arrived and the drainers that run them. The five cards are `crew_make`, `crew_deposit`, `crew_release_ticket`, `crew_post_receipt` and `crew_run_write`.

`turso_push` and `turso_pull` are `channel_push` and `channel_pull` now, because no card in the declaration names an engine.

`answer_from_ticket` is `answer_from_receipt`, and the two said opposite things: the old one said a write answers with its ticket, while `HOW-IT-SHOULD-WORK.md` says a write returns a count. Its feature file got deleted on 2026-09-25.

`query_deliver` and `query_collect` folded into `query_answer` and `query_ask`. `migration_report` is the `Report` a `Connecting` carries. `query_begin_concurrent` had a zero-byte feature file and went with it.

## Stage 7 checklist

- **`KNOWN_UNBOUND` is empty.** No. 42 features are still on it. 23 of them are words a caller writes in a question, like `add` and `count`, which the declaration carries as vocabulary rather than as cards. Another 15 are rules about this source that a gate in `tests/unit/` already checks, and 4 wait on real work.
- **The whole suite is green.** Yes, with 626 passing, 33 xfail and none failing.
- **The explorer's checker and the validator return nothing.** Yes.
- **The types gate still passes.** Yes, because every type in the declaration exists in the code with the same fields.
- **The conditional ratchet reads zero.** Yes, and I counted rather than assumed it: `src/` holds no `if` statement at all.

## The command line is gone

Adam deleted it on 2026-09-25. The declaration named no command-line function, no `route` and no `entry`, and nothing in the rewrite called that folder.

`src/declaro_persistum/cli/` turned out to hold no Python at all, only stale bytecode that stage 0 left behind, and seven other folders were in the same state. Meanwhile `pyproject.toml` shipped a `declaro` command pointing at `declaro_persistum.cli.main`, which raised ModuleNotFoundError on import, so anyone who installed this package and typed `declaro` got a stack trace. All of it went, along with the six feature files that described it.

## Two things still broken

No target spells a local Turso, because `fn engine_of` reads a bare path as SQLite and `libsql://` names a primary's address. So `docs/architecture.md` describes a deployment nobody can ask for, and `session_open.feature` carries a scenario that names this gap and cannot pass.

Two sides name a constraint differently: `schema_inspect` calls a NOT NULL rule `not_null_id` while `schema_read_declared` calls the same rule `nn_id`. Nothing compares those names today, so nothing is broken yet. The same defect in column types was live and broke every deploy after the first, until `Inspecting` gained the engine's own word for each thing a column can hold.

## Out of scope

The transfer tool, ximinez and observe have declarations of their own and nobody touched them. The statement store stays out until somebody times a compile against a round trip.
