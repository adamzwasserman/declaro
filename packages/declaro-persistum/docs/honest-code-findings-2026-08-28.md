# The 74 Honest Code findings, and what each would take

Measured 2026-08-28 by `slop-audit-l1 1.0.0+gce22a322` across 310 files. Every row is one site the analyzer could not clear. **None is a test failure** — the suite is 3,011 green, lint clean, formatting clean, mypy at 118 unchanged.

## The nine repairs

Seventy-four sites need nine distinct fixes, so the work clusters hard. Two clusters are 23 and 15 sites of identical work.

| Key | Sites | Repair |
|:---|---:|---|
| **R1** | 23 | Collapse them into one function over a table, each row carrying the words that function pasted in |
| **R2** | 15 | Take the boundary marker off, or move the read it was meant to be the edge for into it |
| **R3** | 14 | Take the data as a parameter and let the caller at the edge do the I/O |
| **R4** | 13 | Make the function under test pure, so the step is call it and check the result |
| **R5** | 3 | Take the module global as a parameter, so the dependency is in the signature |
| **R6** | 3 | Replace the event listener with an attribute that declares the intent |
| **R7** | 1 | Return the failure to the caller rather than writing it to a log |
| **R8** | 1 | Let it raise, and map the type to a response at the boundary |
| **R9** | 1 | Drop the async and run the coroutine inside the step |

## Where they sit

| Area | Sites | What it is | Whose call |
|:---|---:|---|---|
| `src` | 36 | The package itself | Mine to do |
| `examples` | 24 | Three copied todo apps, six files, each defect three times | **Yours** — they are the first code a user reads |
| `tests` | 14 | Long step setup; 11 of 14 in one file | Mine to do |

## src — 36 findings

| # | File | Line | Symbol | Shape | What is wrong | Fix |
|---:|:---|---:|:---|:---|:---|:--:|
| 1 | `declaro_persistum/abstractions/enums.py` | 126 | `add_enum_value_sql, remove_enum_value…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: add_enum_value_sql,… | R1 |
| 2 | `declaro_persistum/abstractions/materialized_views.py` | 249 | `is_matview_sql, get_matview_metadata_…` | N functions, one shape, differing by words | 3 functions leave one shape behind once every name and quoted string is erased: is_matview_sql,… | R1 |
| 3 | `declaro_persistum/abstractions/pragma_compat.py` | 62 | `pragma_table_info, pragma_index_list,…` | N functions, one shape, differing by words | 4 functions leave one shape behind once every name and quoted string is erased: pragma_table_info, pragma_index_list,… | R1 |
| 4 | `declaro_persistum/applier/postgresql.py` | 32 | `get_dialect, get_transaction_mode` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_dialect, get_transaction_mode | R1 |
| 5 | `declaro_persistum/applier/templates.py` | 131 | `_sqlite_rows, _postgresql_rows` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _sqlite_rows, _postgresql_rows | R1 |
| 6 | `declaro_persistum/boundary.py` | 35 | `boundary, helper, link, recognizer` | N functions, one shape, differing by words | 4 functions leave one shape behind once every name and quoted string is erased: boundary, helper, link, recognizer | R1 |
| 7 | `declaro_persistum/database.py` | 386 | `no_passes, already_delivered, nothing…` | N functions, one shape, differing by words | 3 functions leave one shape behind once every name and quoted string is erased: no_passes, already_delivered,… | R1 |
| 8 | `declaro_persistum/database.py` | 632 | `refresh, flush` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: refresh, flush | R1 |
| 9 | `declaro_persistum/exceptions.py` | 241 | `__init__, __init__` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: __init__, __init__ | R1 |
| 10 | `declaro_persistum/inspector/postgresql.py` | 329 | `_column_from_row, _index_from_row` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _column_from_row, _index_from_row | R1 |
| 11 | `declaro_persistum/inspector/turso.py` | 239 | `_referring_to, _with_check` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _referring_to, _with_check | R1 |
| 12 | `declaro_persistum/migration_sql.py` | 233 | `stored_hash_sql, _the_database_holds_…` | N functions, one shape, differing by words | 3 functions leave one shape behind once every name and quoted string is erased: stored_hash_sql,… | R1 |
| 13 | `declaro_persistum/migration_sql.py` | 254 | `store_hash_sql, user_tables_sql` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: store_hash_sql, user_tables_sql | R1 |
| 14 | `declaro_persistum/query/builder.py` | 559 | `_the_star, _as_name` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _the_star, _as_name | R1 |
| 15 | `declaro_persistum/query/builder.py` | 625 | `with_limit, with_offset` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: with_limit, with_offset | R1 |
| 16 | `declaro_persistum/turso_database.py` | 417 | `_close_connection, release` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _close_connection, release | R1 |
| 17 | `declaro_persistum/writers.py` | 61 | `commit, rollback` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: commit, rollback | R1 |
| 18 | `declaro_persistum/applier/sqlite.py` | 139 | `_execute_operation` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 19 | `declaro_persistum/applier/turso.py` | 240 | `_rolled_back` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 20 | `declaro_persistum/crew.py` | 262 | `_closed` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 21 | `declaro_persistum/inspector/postgresql.py` | 108 | `introspect` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 22 | `declaro_persistum/migration_sql.py` | 536 | `temp_table_name` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 23 | `declaro_persistum/shutdown.py` | 79 | `_trap_signals` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 24 | `declaro_persistum/shutdown.py` | 90 | `_handler` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 25 | `declaro_persistum/transfer_steps.py` | 362 | `_stream_the_table` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 26 | `declaro_persistum/transfer_steps.py` | 701 | `_turn_fk_checks_off` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 27 | `declaro_persistum/transfer_steps.py` | 752 | `_turn_fk_checks_back_on` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 28 | `declaro_persistum/turso_database.py` | 202 | `_the_folder_for` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 29 | `declaro_persistum/turso_database.py` | 222 | `_closed` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 30 | `declaro_persistum/turso_database.py` | 562 | `replicate_once` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 31 | `declaro_persistum/write_queue.py` | 208 | `deposit` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 32 | `declaro_persistum/write_queue.py` | 331 | `drain` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 33 | `declaro_persistum/cli/parts.py` | 329 | `_confirm_the_overwrite` | I/O reached from its own file | performs I/O (stdin.isatty) and is reached by another function here | R3 |
| 34 | `declaro_persistum/fk_ordering.py` | 240 | `_execute_one` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 35 | `declaro_persistum/instrumentation.py` | 229 | `emit_record` | A failure written to a log, not returned | writes a log line the signature does not admit | R7 |
| 36 | `declaro_persistum/shutdown.py` | 169 | `ValueError OSError TypeError` | Caught and nothing done with it | silences ValueError OSError TypeError with no handler at all, so the block returns as though it succeeded | R8 |

## examples — 24 findings

| # | File | Line | Symbol | Shape | What is wrong | Fix |
|---:|:---|---:|:---|:---|:---|:--:|
| 37 | `todo_app_django_style/app.py` | 117 | `use_sqlite, use_turso_embedded` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: use_sqlite, use_turso_embedded | R1 |
| 38 | `todo_app_django_style/db.py` | 58 | `get_sqlite_config, get_turso_embedded…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_sqlite_config,… | R1 |
| 39 | `todo_app_native/app.py` | 116 | `use_sqlite, use_turso_embedded` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: use_sqlite, use_turso_embedded | R1 |
| 40 | `todo_app_native/db.py` | 58 | `get_sqlite_config, get_turso_embedded…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_sqlite_config,… | R1 |
| 41 | `todo_app_prisma_style/app.py` | 117 | `use_sqlite, use_turso_embedded` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: use_sqlite, use_turso_embedded | R1 |
| 42 | `todo_app_prisma_style/db.py` | 58 | `get_sqlite_config, get_turso_embedded…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_sqlite_config,… | R1 |
| 43 | `todo_app_django_style/db.py` | 147 | `_connect_sqlite` | I/O reached from its own file | performs I/O (aiosqlite.connect) and is reached by another function here | R3 |
| 44 | `todo_app_django_style/db.py` | 155 | `_connect_postgresql` | I/O reached from its own file | performs I/O (asyncpg.connect) and is reached by another function here | R3 |
| 45 | `todo_app_django_style/db.py` | 249 | `_run` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 46 | `todo_app_django_style/db.py` | 258 | `commit` | I/O reached from its own file | performs I/O (commit) and is reached by another function here | R3 |
| 47 | `todo_app_native/db.py` | 147 | `_connect_sqlite` | I/O reached from its own file | performs I/O (aiosqlite.connect) and is reached by another function here | R3 |
| 48 | `todo_app_native/db.py` | 155 | `_connect_postgresql` | I/O reached from its own file | performs I/O (asyncpg.connect) and is reached by another function here | R3 |
| 49 | `todo_app_native/db.py` | 249 | `_run` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 50 | `todo_app_native/db.py` | 258 | `commit` | I/O reached from its own file | performs I/O (commit) and is reached by another function here | R3 |
| 51 | `todo_app_prisma_style/db.py` | 147 | `_connect_sqlite` | I/O reached from its own file | performs I/O (aiosqlite.connect) and is reached by another function here | R3 |
| 52 | `todo_app_prisma_style/db.py` | 155 | `_connect_postgresql` | I/O reached from its own file | performs I/O (asyncpg.connect) and is reached by another function here | R3 |
| 53 | `todo_app_prisma_style/db.py` | 249 | `_run` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 54 | `todo_app_prisma_style/db.py` | 258 | `commit` | I/O reached from its own file | performs I/O (commit) and is reached by another function here | R3 |
| 55 | `todo_app_django_style/db.py` | 123 | `get_database(_current_db)` | Hidden state a caller cannot see | reads `_current_db`, which another function here writes, so its behaviour depends on something no caller can see | R5 |
| 56 | `todo_app_native/db.py` | 123 | `get_database(_current_db)` | Hidden state a caller cannot see | reads `_current_db`, which another function here writes, so its behaviour depends on something no caller can see | R5 |
| 57 | `todo_app_prisma_style/db.py` | 123 | `get_database(_current_db)` | Hidden state a caller cannot see | reads `_current_db`, which another function here writes, so its behaviour depends on something no caller can see | R5 |
| 58 | `todo_app_django_style/templates/db_select.html` | 133 | `innerHTML` | Imperative JavaScript where an attribute would say it | innerHTML describes how, somewhere the reader has to go and find | R6 |
| 59 | `todo_app_native/templates/db_select.html` | 133 | `innerHTML` | Imperative JavaScript where an attribute would say it | innerHTML describes how, somewhere the reader has to go and find | R6 |
| 60 | `todo_app_prisma_style/templates/db_select.html` | 133 | `innerHTML` | Imperative JavaScript where an attribute would say it | innerHTML describes how, somewhere the reader has to go and find | R6 |

## tests — 14 findings

| # | File | Line | Symbol | Shape | What is wrong | Fix |
|---:|:---|---:|:---|:---|:---|:--:|
| 61 | `bdd/steps/common_steps.py` | 96 | `then_count_is` | More setup than the thing it tests | 33 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 62 | `bdd/steps/database_steps.py` | 207 | `when_query_completed` | More setup than the thing it tests | 34 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 63 | `bdd/steps/test_replication_steps.py` | 290 | `_then_converges` | More setup than the thing it tests | 37 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 64 | `bdd/steps/test_table_reconstruction_steps.py` | 202 | `given_table_with_columns` | More setup than the thing it tests | 56 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 65 | `bdd/steps/test_table_reconstruction_steps.py` | 262 | `given_table_contains_data` | More setup than the thing it tests | 65 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 66 | `bdd/steps/test_table_reconstruction_steps.py` | 659 | `when_alter_column_property` | More setup than the thing it tests | 31 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 67 | `bdd/steps/test_table_reconstruction_steps.py` | 911 | `when_drop_column` | More setup than the thing it tests | 106 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 68 | `bdd/steps/test_table_reconstruction_steps.py` | 1225 | `then_fk_preserved` | More setup than the thing it tests | 39 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 69 | `bdd/steps/test_table_reconstruction_steps.py` | 1309 | `then_indexes_functional` | More setup than the thing it tests | 42 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 70 | `bdd/steps/test_table_reconstruction_steps.py` | 1355 | `then_unique_preserved` | More setup than the thing it tests | 41 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 71 | `bdd/steps/test_table_reconstruction_steps.py` | 1400 | `then_only_explicit_indexes` | More setup than the thing it tests | 41 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 72 | `bdd/steps/test_table_reconstruction_steps.py` | 1552 | `then_fk_checked` | More setup than the thing it tests | 45 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 73 | `bdd/steps/test_table_reconstruction_steps.py` | 1725 | `then_invalid_fk_succeeds` | More setup than the thing it tests | 39 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 74 | `unit/test_the_check_scanner_survives_what_sqlite_accepts.py` | 121 | `test_the_schema_converges_whatever_sq…` | A step the runner never actually runs | this step never runs: the runner calls it and throws away what it returns, so an asynchronous body is handed back… | R9 |

## Three notes on rows above

**One row is a false positive and should be 73, not 74.** The R9 row, `test_the_check_scanner_survives_what_sqlite_accepts.py:121`, is reported as a gherkin step that never runs. It is not a step. Its `@given` comes from Hypothesis, not pytest-bdd, and Hypothesis awaits the function. Proved by planting a raise as the first line of the body, which failed the test. Reported to the audit team; the detection I gave them was too loose and my own gate is narrow only because it looks in `tests/bdd/steps/` alone.

**Row for `_the_folder_for` in `turso_database.py` is mine, and the marker is true.** It calls `mkdir` before a database file is opened, which is real work on a disk. The analyzer does not count making a directory as I/O. Recorded in the boundary ratchet with that reason rather than removed.

**The 24 in `examples` are 8 defects, not 24.** `db.py` and `app.py` are copied verbatim across three apps. Rewriting one and deleting two clears all 24.


## What is not in this table

Six open priority-zero items in beads are more serious than anything here, and two of them are a live regression: 0.3.0 opens a sync connection per write where 0.1.28 pooled them. A synced replica takes one sync connection. That costs something in production; nothing in this table does.
