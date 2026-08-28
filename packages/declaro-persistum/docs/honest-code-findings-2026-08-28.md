# The 75 Honest Code findings, and what each would take

Measured 2026-08-28 by `slop-audit-l1 1.0.0+g97fdc315` across 310 files. The count moved from 74 to 75 within the day for two reasons, neither of them this package getting worse: the analyzer gained two clauses, and one of its findings on us turned out to be a false positive I reported and they fixed. Every row is one site the analyzer could not clear. **None is a test failure** — the suite is 3,014 green, lint clean, formatting clean, mypy at 118 unchanged.

## The 8 repairs

75 sites need 8 distinct fixes, so the work clusters hard. The two largest clusters are 22 and 16 sites of identical work.

| Key | Sites | Repair |
|:---|---:|---|
| **R1** | 22 | Collapse them into one function over a table, each row carrying the words that function pasted in |
| **R4** | 16 | Make the function under test pure, so the step is call it and check the result |
| **R2** | 15 | Take the boundary marker off, or move the read it was meant to be the edge for into it |
| **R3** | 14 | Take the data as a parameter and let the caller at the edge do the I/O |
| **R5** | 3 | Take the module global as a parameter, so the dependency is in the signature |
| **R6** | 3 | Replace the event listener with an attribute that declares the intent |
| **R7** | 1 | Return the failure to the caller rather than writing it to a log |
| **R8** | 1 | Let it raise, and map the type to a response at the boundary |

## Where they sit

| Area | Sites | What it is | Whose call |
|:---|---:|---|---|
| `src` | 35 | The package itself | Mine to do |
| `examples` | 24 | Three copied todo apps, six files, each defect three times | **Yours** — they are the first code a user reads |
| `tests` | 16 | Long step setup; 13 of 16 in one file | Mine to do |

## src — 35 findings

| # | File | Line | Symbol | Shape | What is wrong | Fix |
|---:|:---|---:|:---|:---|:---|:--:|
| 1 | `declaro_persistum/abstractions/enums.py` | 122 | `add_enum_value_sql, remove_enum_value…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: add_enum_value_sql,… | R1 |
| 2 | `declaro_persistum/abstractions/materialized_views.py` | 245 | `is_matview_sql, get_matview_metadata_…` | N functions, one shape, differing by words | 3 functions leave one shape behind once every name and quoted string is erased: is_matview_sql,… | R1 |
| 3 | `declaro_persistum/abstractions/pragma_compat.py` | 62 | `pragma_table_info, pragma_index_list,…` | N functions, one shape, differing by words | 4 functions leave one shape behind once every name and quoted string is erased: pragma_table_info, pragma_index_list,… | R1 |
| 4 | `declaro_persistum/applier/postgresql.py` | 32 | `get_dialect, get_transaction_mode` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_dialect, get_transaction_mode | R1 |
| 5 | `declaro_persistum/applier/templates.py` | 125 | `_sqlite_rows, _postgresql_rows` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _sqlite_rows, _postgresql_rows | R1 |
| 6 | `declaro_persistum/boundary.py` | 35 | `boundary, helper, link, recognizer` | N functions, one shape, differing by words | 4 functions leave one shape behind once every name and quoted string is erased: boundary, helper, link, recognizer | R1 |
| 7 | `declaro_persistum/database.py` | 385 | `no_passes, already_delivered, nothing…` | N functions, one shape, differing by words | 3 functions leave one shape behind once every name and quoted string is erased: no_passes, already_delivered,… | R1 |
| 8 | `declaro_persistum/database.py` | 641 | `refresh, flush` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: refresh, flush | R1 |
| 9 | `declaro_persistum/exceptions.py` | 239 | `__init__, __init__` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: __init__, __init__ | R1 |
| 10 | `declaro_persistum/inspector/turso.py` | 234 | `_referring_to, _with_check` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _referring_to, _with_check | R1 |
| 11 | `declaro_persistum/migration_sql.py` | 229 | `stored_hash_sql, _the_database_holds_…` | N functions, one shape, differing by words | 3 functions leave one shape behind once every name and quoted string is erased: stored_hash_sql,… | R1 |
| 12 | `declaro_persistum/migration_sql.py` | 250 | `store_hash_sql, user_tables_sql` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: store_hash_sql, user_tables_sql | R1 |
| 13 | `declaro_persistum/query/builder.py` | 526 | `_the_star, _as_name` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _the_star, _as_name | R1 |
| 14 | `declaro_persistum/query/builder.py` | 592 | `with_limit, with_offset` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: with_limit, with_offset | R1 |
| 15 | `declaro_persistum/turso_database.py` | 406 | `_close_connection, release` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: _close_connection, release | R1 |
| 16 | `declaro_persistum/writers.py` | 61 | `commit, rollback` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: commit, rollback | R1 |
| 17 | `declaro_persistum/applier/sqlite.py` | 132 | `_execute_operation` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 18 | `declaro_persistum/applier/turso.py` | 236 | `_rolled_back` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 19 | `declaro_persistum/crew.py` | 261 | `_closed` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 20 | `declaro_persistum/inspector/postgresql.py` | 108 | `introspect` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 21 | `declaro_persistum/migration_sql.py` | 523 | `temp_table_name` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 22 | `declaro_persistum/shutdown.py` | 79 | `_trap_signals` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 23 | `declaro_persistum/shutdown.py` | 90 | `_handler` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 24 | `declaro_persistum/transfer_steps.py` | 362 | `_stream_the_table` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 25 | `declaro_persistum/transfer_steps.py` | 692 | `_turn_fk_checks_off` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 26 | `declaro_persistum/transfer_steps.py` | 743 | `_turn_fk_checks_back_on` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 27 | `declaro_persistum/turso_database.py` | 195 | `_the_folder_for` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 28 | `declaro_persistum/turso_database.py` | 215 | `_closed` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 29 | `declaro_persistum/turso_database.py` | 548 | `replicate_once` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 30 | `declaro_persistum/write_queue.py` | 208 | `deposit` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 31 | `declaro_persistum/write_queue.py` | 327 | `drain` | Boundary marker the analyzer sees no edge behind | declares itself a boundary and makes no call this reader counts as I/O, so the declaration states an edge that is not… | R2 |
| 32 | `declaro_persistum/cli/parts.py` | 326 | `_confirm_the_overwrite` | I/O reached from its own file | performs I/O (stdin.isatty) and is reached by another function here | R3 |
| 33 | `declaro_persistum/fk_ordering.py` | 238 | `_execute_one` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 34 | `declaro_persistum/instrumentation.py` | 221 | `emit_record` | A failure written to a log, not returned | writes a log line the signature does not admit | R7 |
| 35 | `declaro_persistum/shutdown.py` | 163 | `ValueError OSError TypeError` | Caught and nothing done with it | silences ValueError OSError TypeError with no handler at all, so the block returns as though it succeeded | R8 |

## examples — 24 findings

| # | File | Line | Symbol | Shape | What is wrong | Fix |
|---:|:---|---:|:---|:---|:---|:--:|
| 36 | `todo_app_django_style/app.py` | 121 | `use_sqlite, use_turso_embedded` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: use_sqlite, use_turso_embedded | R1 |
| 37 | `todo_app_django_style/db.py` | 58 | `get_sqlite_config, get_turso_embedded…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_sqlite_config,… | R1 |
| 38 | `todo_app_native/app.py` | 120 | `use_sqlite, use_turso_embedded` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: use_sqlite, use_turso_embedded | R1 |
| 39 | `todo_app_native/db.py` | 58 | `get_sqlite_config, get_turso_embedded…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_sqlite_config,… | R1 |
| 40 | `todo_app_prisma_style/app.py` | 121 | `use_sqlite, use_turso_embedded` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: use_sqlite, use_turso_embedded | R1 |
| 41 | `todo_app_prisma_style/db.py` | 58 | `get_sqlite_config, get_turso_embedded…` | N functions, one shape, differing by words | 2 functions leave one shape behind once every name and quoted string is erased: get_sqlite_config,… | R1 |
| 42 | `todo_app_django_style/db.py` | 145 | `_connect_sqlite` | I/O reached from its own file | performs I/O (aiosqlite.connect) and is reached by another function here | R3 |
| 43 | `todo_app_django_style/db.py` | 153 | `_connect_postgresql` | I/O reached from its own file | performs I/O (asyncpg.connect) and is reached by another function here | R3 |
| 44 | `todo_app_django_style/db.py` | 247 | `_run` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 45 | `todo_app_django_style/db.py` | 256 | `commit` | I/O reached from its own file | performs I/O (commit) and is reached by another function here | R3 |
| 46 | `todo_app_native/db.py` | 145 | `_connect_sqlite` | I/O reached from its own file | performs I/O (aiosqlite.connect) and is reached by another function here | R3 |
| 47 | `todo_app_native/db.py` | 153 | `_connect_postgresql` | I/O reached from its own file | performs I/O (asyncpg.connect) and is reached by another function here | R3 |
| 48 | `todo_app_native/db.py` | 247 | `_run` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 49 | `todo_app_native/db.py` | 256 | `commit` | I/O reached from its own file | performs I/O (commit) and is reached by another function here | R3 |
| 50 | `todo_app_prisma_style/db.py` | 145 | `_connect_sqlite` | I/O reached from its own file | performs I/O (aiosqlite.connect) and is reached by another function here | R3 |
| 51 | `todo_app_prisma_style/db.py` | 153 | `_connect_postgresql` | I/O reached from its own file | performs I/O (asyncpg.connect) and is reached by another function here | R3 |
| 52 | `todo_app_prisma_style/db.py` | 247 | `_run` | I/O reached from its own file | performs I/O (commit, execute) and is reached by another function here | R3 |
| 53 | `todo_app_prisma_style/db.py` | 256 | `commit` | I/O reached from its own file | performs I/O (commit) and is reached by another function here | R3 |
| 54 | `todo_app_django_style/db.py` | 123 | `get_database(_current_db)` | Hidden state a caller cannot see | reads `_current_db`, which another function here writes, so its behaviour depends on something no caller can see | R5 |
| 55 | `todo_app_native/db.py` | 123 | `get_database(_current_db)` | Hidden state a caller cannot see | reads `_current_db`, which another function here writes, so its behaviour depends on something no caller can see | R5 |
| 56 | `todo_app_prisma_style/db.py` | 123 | `get_database(_current_db)` | Hidden state a caller cannot see | reads `_current_db`, which another function here writes, so its behaviour depends on something no caller can see | R5 |
| 57 | `todo_app_django_style/templates/db_select.html` | 133 | `innerHTML` | Imperative JavaScript where an attribute would say it | innerHTML describes how, somewhere the reader has to go and find | R6 |
| 58 | `todo_app_native/templates/db_select.html` | 133 | `innerHTML` | Imperative JavaScript where an attribute would say it | innerHTML describes how, somewhere the reader has to go and find | R6 |
| 59 | `todo_app_prisma_style/templates/db_select.html` | 133 | `innerHTML` | Imperative JavaScript where an attribute would say it | innerHTML describes how, somewhere the reader has to go and find | R6 |

## tests — 16 findings

| # | File | Line | Symbol | Shape | What is wrong | Fix |
|---:|:---|---:|:---|:---|:---|:--:|
| 60 | `bdd/steps/common_steps.py` | 100 | `then_count_is` | More setup than the thing it tests | 31 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 61 | `bdd/steps/test_replication_steps.py` | 220 | `_then_converges` | More setup than the thing it tests | 36 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 62 | `bdd/steps/test_table_reconstruction_steps.py` | 202 | `given_table_with_columns` | More setup than the thing it tests | 56 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 63 | `bdd/steps/test_table_reconstruction_steps.py` | 262 | `given_table_contains_data` | More setup than the thing it tests | 55 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 64 | `bdd/steps/test_table_reconstruction_steps.py` | 652 | `when_alter_column_property` | More setup than the thing it tests | 33 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 65 | `bdd/steps/test_table_reconstruction_steps.py` | 826 | `when_add_foreign_key` | More setup than the thing it tests | 37 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 66 | `bdd/steps/test_table_reconstruction_steps.py` | 869 | `when_add_foreign_key_cascade` | More setup than the thing it tests | 38 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 67 | `bdd/steps/test_table_reconstruction_steps.py` | 911 | `when_drop_foreign_key` | More setup than the thing it tests | 36 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 68 | `bdd/steps/test_table_reconstruction_steps.py` | 951 | `when_drop_column` | More setup than the thing it tests | 107 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 69 | `bdd/steps/test_table_reconstruction_steps.py` | 1280 | `then_fk_preserved` | More setup than the thing it tests | 38 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 70 | `bdd/steps/test_table_reconstruction_steps.py` | 1363 | `then_indexes_functional` | More setup than the thing it tests | 42 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 71 | `bdd/steps/test_table_reconstruction_steps.py` | 1409 | `then_unique_preserved` | More setup than the thing it tests | 41 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 72 | `bdd/steps/test_table_reconstruction_steps.py` | 1454 | `then_only_explicit_indexes` | More setup than the thing it tests | 41 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 73 | `bdd/steps/test_table_reconstruction_steps.py` | 1602 | `then_fk_checked` | More setup than the thing it tests | 41 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 74 | `bdd/steps/test_table_reconstruction_steps.py` | 1778 | `then_invalid_fk_succeeds` | More setup than the thing it tests | 35 lines of setup, which is a readout on the code under test rather than on the test | R4 |
| 75 | `unit/test_the_check_scanner_survives_what_sqlite_accepts.py` | 119 | `test_the_schema_converges_whatever_sq…` | More setup than the thing it tests | 32 lines of setup, which is a readout on the code under test rather than on the test | R4 |

## Two notes on rows above

**Row for `_the_folder_for` in `turso_database.py` is mine, and the marker is true.** It calls `mkdir` before a database file is opened, which is real work on a disk. The analyzer does not count making a directory as I/O. Recorded in the boundary ratchet with that reason rather than removed.

**The 24 in `examples` are 8 defects, not 24.** `db.py` and `app.py` are copied verbatim across three apps. Rewriting one and deleting two clears all 24.


## What is not in this table

Six open priority-zero items in beads are more serious than anything here, and two of them are a live regression: 0.3.0 opens a sync connection per write where 0.1.28 pooled them. A synced replica takes one sync connection. That costs something in production; nothing in this table does.
