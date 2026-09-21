# Rewriting declaro-persistum from its declaration

Written 2026-09-21. The declaration is `docs/design/persistum.hd`: 66 functions, 5 doors, 5 orchestrators, and a pure core between them. Every function has a feature file of its own name under `tests/bdd/features/`. The source in `src/` predates all of it: 67 modules, 30,013 lines, written before the declaration existed and never reconciled with it.

This plan deletes that source and builds the declared one, one function at a time, from the inside out. Inside out means a function is written only after everything it calls exists, so a pure function is tested with values and nothing else, and a database is only involved once the work reaches a boundary that is declared to touch one.

## What the call graph says about order

The graph is flat by design. 56 functions call nothing at all, 5 orchestrators call those, and 5 doors call one orchestrator each. Pure functions never call each other, which is what makes them testable one at a time, and it also means depth cannot be the build order. The order below is by what a function needs to exist before it can be written: its types, then the functions its own chain reads.

## Stage 0: clear the ground

1. Branch from `design/persistum-declaration`.
2. Delete `src/declaro_persistum/` entirely, all 67 modules.
3. Delete the 178 unit test files that import the package, and the 23 step files that import it. They test the code being deleted; their scenarios survive in the feature files, which are the specification.
4. Keep, untouched: `tests/bdd/features/` (110 files), `docs/design/`, `pyproject.toml`, the six environment fixtures, the new declared-shape fixtures, and the 19 gates that import nothing from the package.
5. Add every feature file to `KNOWN_UNBOUND` with one reason: the function is declared and not built. Each comes off as its stage binds it.
6. Prove the ground is clear: `grep -r "declaro_persistum" tests/` returns only the gates that name it as a string, and `uv run pytest` collects with no errors.

After stage 0 the suite runs the gates and nothing else. That is the honest state: nothing is built, so nothing passes.

**Cost, stated plainly.** 2,955 passing tests go to zero, and the count comes back one function at a time.

## The loop, which is the same for every function

Each function is one commit. The loop never changes:

1. Read its feature file and the lines of `HOW-IT-SHOULD-WORK.md` or `docs/architecture.md` it cites. Read nothing else.
2. Write its step file, `tests/bdd/steps/test_<name>_steps.py`, binding only that feature.
3. Take the feature off `KNOWN_UNBOUND`.
4. Run it. Every scenario is red, and the red says what is missing rather than that a name is undefined.
5. Write the function, at the signature the declaration gives, in a module named for its family.
6. Run until green. Nothing else may go red.
7. Re-check the declaration: the explorer's map, the validator, the orphan count. A function whose signature drifted from the card is a fault in one of the two, and the card is the one that is right unless a source says otherwise.
8. Commit, named for the function.

Rules that hold at every step, from `CLAUDE.md` and the declaration:

- No `if` without a dated approval in a comment. A branch is usually a table or a tighter type.
- No default argument values. Absence is `NOTHING`, resolved at a boundary.
- A failure comes back in the return type, never a log line, and it says what to do differently.
- A pure function reads no clock, no random source, no environment, no connection.
- A handle is held and passed to a boundary, never read.
- No defensive check the signature already makes.

## Stage 1: the types

Every record, vocabulary and set in the declaration becomes a `TypedDict` or a `Literal`, in one module. 30 types, 8 sets, 10 vocabularies.

Done when a new gate passes: every type named in `persistum.hd` exists in the code with the same fields and the same members, and nothing else is exported. That gate is the poka-yoke for the whole rewrite, because it fails the moment the two drift.

## Stage 2: the pure core, in dependency order within each family

Each of these is the loop above. They are grouped by the chain they serve, and within a group there is no order that matters, because none of them calls another.

**Opening, 6.** `engine_of`, `refuse_unusable_settings`, `permitted_connections`, `crew_size`, `replicates`, `migration_needed`.

**Asking, 9.** `declaration_from_pseudo_sql`, `declaration_from_django`, `declaration_from_prisma`, `declaration_from_sqlalchemy`, `refuse_undeclared_columns`, `join_between`, `compile_statement`, `travels_together`, `answer_from_rows`.

**Writing and collecting, 5.** `answer_from_ticket`, `receipt_for`, `refusal_reaches_caller`, `next_in_order`, `writer_waiting`.

**Retrying, 3.** `worth_retrying`, `budget_spent`, `next_wait`.

**Schema, 11.** `schema_from_declared`, `refuse_owned_names`, `difference`, `name_what_is_ambiguous`, `order_operations`, `name_what_destroys`, `refuse_undeclared_loss`, `capability_of`, `mechanism_for`, `spell_operation`, `spell_view`.

**Shutdown and the room, 3.** `progress_line`, `migration_report`, `room_make`.

37 functions, 37 commits, no database involved in any of them. This is the whole middle of the package and it is written before anything opens a connection.

## Stage 3: the readers

Five, and each one needs a real database rather than a stand-in, because a stand-in that gets a driver contract wrong has manufactured a defect here twice: `clock_read`, `lifecycle_read_registered`, `schema_read_declared`, `schema_inspect`, `turso_pull`.

`schema_inspect` is the largest: it reads a live schema back on three engines, and the scenarios folded into its feature from the old pragma readers say what that has to survive, including a quoted name and a CHECK the engine keeps only in its DDL.

## Stage 4: the output boundaries

Twelve, in this order, because each proves the connection the next one needs: `session_open_connection`, `session_open_writer`, `session_close_connection`, `query_run_statement`, `query_begin_concurrent`, `room_run_write`, `query_run_together`, `room_deposit`, `room_release_ticket`, `room_post_receipt`, `turso_push`, `log_write`, `lifecycle_install_handler`, `lifecycle_register`.

These are the first functions that touch anything outside the process. Each runs against SQLite locally, PostgreSQL at `postgresql://postgres:postgres@localhost/declaro_probe`, and Turso against the credentials already in `.env`.

## Stage 5: the orchestrators

Five, in ascending size: `query_deliver` (3 calls), `lifecycle_shut_down` (7), `lifecycle_catch_up` (11), `query_answer` (9), `session_connect` (25).

`session_connect` last, because it reaches nearly everything: the engine decision, the connections, the crew, the whole migration, and the handle it finally hands back.

## Stage 6: the doors

Five, and each should be thin: `session_open`, `query_ask`, `query_collect`, `lifecycle_stop`, `lifecycle_turn`. A door that needs more than to receive, call one orchestrator and return is a door doing work that belongs behind it.

## Stage 7: the gates come back on

The rewrite is done when all of these hold at once:

- `KNOWN_UNBOUND` is empty. Every feature is bound exactly once.
- The whole suite is green.
- The explorer's checker and the validator both return nothing on all five declarations.
- The types gate from stage 1 still passes, so the code and the declaration still say the same thing.
- `honest-check` reports nothing on `src/`.
- The conditional ratchet reads zero, because the rewrite started from zero.

## What is deliberately not in this plan

The command line, the transfer tool, ximinez and observe have declarations of their own and are not touched. The statement store stays out until somebody times a compile against a round trip. The three feature files that describe a design we removed, the awaited flush, the remembered rename and the plan a person reads, are named in the declaration as scenarios to change, and changing them is not part of this.
