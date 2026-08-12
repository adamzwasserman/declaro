"""PRAGMA compatibility: try native, fall back to emulation, count both.

`abstractions/pragma_compat.py` sat at 41% with 64 branches — part of the
Slop Audit L1.19 gap (declaro-xu0).

The module exists because Turso is a Rust rewrite, not a libSQL fork, so
"SQLite-compatible" does not mean every PRAGMA is present. Each wrapper
tries the native PRAGMA and falls back to parsing `sqlite_master` when the
engine refuses it.

Two behaviours are worth pinning hard:

- The fallback fires ONLY on a not-supported error. Any other failure must
  propagate, or a genuinely broken database looks like an old engine.
- The counters are how the project learns the emulation is obsolete. A
  native success on a Turso connection is recorded and warned about, so
  the emulation can be deleted once the engine grows the feature.

The connection is a plain fake. There is no mock framework here: the fake
either returns rows or raises the error under test.
"""

import pytest

from declaro_persistum.abstractions.pragma_compat import (
    _is_turso_connection,
    _split_columns,
    _unquote,
    get_affected_tables,
    get_emulation_count,
    get_native_success_count,
    pragma_index_list,
    pragma_table_info,
    reset_counters,
)


@pytest.fixture(autouse=True)
def _clean_counters():
    reset_counters()
    yield
    reset_counters()








_TursoConn.__module__ = "turso.lib_sync_aio"












