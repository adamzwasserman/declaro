# Umbra project audit

Repository: `.`
Full pytest suite: completed with failures. 2774 passed, 3 failed, exit status 1. Coverage was produced, so the Silence index reflects the tests that ran. 184 test files discovered.
Aggregate Silence index: 59.8% (higher = more blind spots) | 1696 closeable silences / 2836 closeable facets.
By kind: unasserted_exception_path: 1, unasserted_return_contract: 61, unexercised_branch: 251, unexercised_input_region: 1383.

## Modules
- src/declaro_persistum/__init__.py: unaudited. Umbra found no top-level functions or methods to audit in this module.
- src/declaro_persistum/abstractions/__init__.py: unaudited. Umbra found no top-level functions or methods to audit in this module.
- src/declaro_persistum/abstractions/arrays.py: Silence index 80.0% | 12 closeable silences / 15 closeable facets
- src/declaro_persistum/abstractions/enums.py: Silence index 68.1% | 64 closeable silences / 94 closeable facets
- src/declaro_persistum/abstractions/hierarchy.py: Silence index 22.2% | 2 closeable silences / 9 closeable facets
- src/declaro_persistum/abstractions/maps.py: Silence index 80.0% | 12 closeable silences / 15 closeable facets
- src/declaro_persistum/abstractions/materialized_views.py: Silence index 53.3% | 8 closeable silences / 15 closeable facets
- src/declaro_persistum/abstractions/pragma_compat.py: Silence index 84.0% | 21 closeable silences / 25 closeable facets
- src/declaro_persistum/abstractions/ranges.py: Silence index 0.0% | 0 closeable silences / 1 closeable facets
- src/declaro_persistum/abstractions/reconstruction.py: Silence index 60.0% | 60 closeable silences / 100 closeable facets
- src/declaro_persistum/abstractions/table_reconstruction.py: Silence index 57.4% | 58 closeable silences / 101 closeable facets
- src/declaro_persistum/applier/__init__.py: Silence index 50.0% | 1 closeable silences / 2 closeable facets
- src/declaro_persistum/applier/postgresql.py: Silence index 82.4% | 84 closeable silences / 102 closeable facets
- src/declaro_persistum/applier/shared.py: Silence index 65.9% | 112 closeable silences / 170 closeable facets
- src/declaro_persistum/applier/sqlite.py: Silence index 33.3% | 6 closeable silences / 18 closeable facets
- src/declaro_persistum/applier/turso.py: Silence index 28.6% | 8 closeable silences / 28 closeable facets
- src/declaro_persistum/bulk_loader.py: Silence index 97.3% | 143 closeable silences / 147 closeable facets
- src/declaro_persistum/cli/__init__.py: unaudited. Umbra found no top-level functions or methods to audit in this module.
- src/declaro_persistum/cli/commands.py: Silence index 75.7% | 134 closeable silences / 177 closeable facets
- src/declaro_persistum/cli/main.py: Silence index 9.5% | 2 closeable silences / 21 closeable facets
- src/declaro_persistum/crew.py: Silence index 50.0% | 8 closeable silences / 16 closeable facets
- src/declaro_persistum/cutover.py: Silence index 82.4% | 14 closeable silences / 17 closeable facets
- src/declaro_persistum/database.py: Silence index 25.0% | 8 closeable silences / 32 closeable facets
- src/declaro_persistum/differ/__init__.py: unaudited. Umbra found no top-level functions or methods to audit in this module.
- src/declaro_persistum/differ/ambiguity.py: Silence index 77.8% | 35 closeable silences / 45 closeable facets
- src/declaro_persistum/differ/core.py: Silence index 32.4% | 47 closeable silences / 145 closeable facets
- src/declaro_persistum/differ/extended.py: Silence index 0.0% | 0 closeable silences / 6 closeable facets
- src/declaro_persistum/differ/toposort.py: Silence index 72.0% | 59 closeable silences / 82 closeable facets
- src/declaro_persistum/exceptions.py: Silence index 83.6% | 92 closeable silences / 110 closeable facets
- src/declaro_persistum/fk_ordering.py: Silence index 37.8% | 17 closeable silences / 45 closeable facets
- src/declaro_persistum/inspector/__init__.py: Silence index 77.8% | 7 closeable silences / 9 closeable facets
- src/declaro_persistum/inspector/postgresql.py: Silence index 71.8% | 74 closeable silences / 103 closeable facets
- src/declaro_persistum/inspector/shared.py: Silence index 36.4% | 59 closeable silences / 162 closeable facets
- src/declaro_persistum/inspector/sqlite.py: Silence index 81.2% | 26 closeable silences / 32 closeable facets
- src/declaro_persistum/inspector/turso.py: Silence index 67.2% | 39 closeable silences / 58 closeable facets
- src/declaro_persistum/instrumentation.py: Silence index 62.2% | 28 closeable silences / 45 closeable facets
- src/declaro_persistum/loader.py: Silence index 48.5% | 48 closeable silences / 99 closeable facets
- src/declaro_persistum/migrations.py: Silence index 58.4% | 59 closeable silences / 101 closeable facets
- src/declaro_persistum/mirror.py: Silence index 52.6% | 10 closeable silences / 19 closeable facets
- src/declaro_persistum/postgres_database.py: Silence index 100.0% | 8 closeable silences / 8 closeable facets
- src/declaro_persistum/pydantic_loader.py: Silence index 21.8% | 24 closeable silences / 110 closeable facets
- src/declaro_persistum/query/__init__.py: unaudited. Umbra found no top-level functions or methods to audit in this module.
- src/declaro_persistum/query/builder.py: Silence index 52.2% | 72 closeable silences / 138 closeable facets
- src/declaro_persistum/query/conditions.py: Silence index 83.7% | 36 closeable silences / 43 closeable facets
- src/declaro_persistum/query/executor.py: Silence index 63.5% | 47 closeable silences / 74 closeable facets
- src/declaro_persistum/query/expressions.py: Silence index 76.5% | 13 closeable silences / 17 closeable facets
- src/declaro_persistum/query/table.py: Silence index 46.7% | 7 closeable silences / 15 closeable facets
- src/declaro_persistum/retry.py: Silence index 25.0% | 2 closeable silences / 8 closeable facets
- src/declaro_persistum/shutdown.py: Silence index 0.0% | 0 closeable silences / 4 closeable facets
- src/declaro_persistum/sqlite_database.py: Silence index 100.0% | 1 closeable silences / 1 closeable facets
- src/declaro_persistum/transfer.py: Silence index 44.4% | 40 closeable silences / 90 closeable facets
- src/declaro_persistum/turso_database.py: Silence index 70.3% | 26 closeable silences / 37 closeable facets
- src/declaro_persistum/types.py: Silence index 81.8% | 9 closeable silences / 11 closeable facets
- src/declaro_persistum/validator.py: Silence index 42.3% | 30 closeable silences / 71 closeable facets
- src/declaro_persistum/write_queue.py: Silence index 21.1% | 4 closeable silences / 19 closeable facets
- src/declaro_persistum/writers.py: Silence index 83.3% | 20 closeable silences / 24 closeable facets
