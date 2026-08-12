"""Regression tests for the foreign_key_list sqlite_master emulation.

The emulation is the fallback used when the engine has no native PRAGMA
foreign_key_list (pyturso 0.5.1 raises "Not a valid pragma name"; 0.7.2
supports it natively).

Its inline-FK pattern had an unanchored column-name group followed by an
unbounded '.*?' spanning optional constraints, so matching began at the top
of the DDL and captured "CREATE" as the referencing column. The inspector
looked up that name, found no such column, and dropped the foreign key from
the introspected schema — a wrong answer produced silently, with no error.

It only ever worked on DDL that put each column on its own line, because '.'
does not cross a newline without re.DOTALL.

Row format matches SQLite:
(id, seq, table, from, to, on_update, on_delete, match)
"""

import pytest

from declaro_persistum.abstractions.pragma_compat import _emulate_foreign_key_list






async def _emulated_fks(sql: str) -> list[tuple]:
    return await _emulate_foreign_key_list(_FakeConnection(sql), "o")


def _from_table_to(rows: list[tuple]) -> list[tuple[str, str, str]]:
    """Project (from, table, to) out of the SQLite row format."""
    return [(row[3], row[2], row[4]) for row in rows]


