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


class _FakeCursor:
    """Returns one sqlite_master row holding the DDL under test."""

    def __init__(self, sql: str) -> None:
        self._sql = sql

    async def fetchone(self) -> tuple[str]:
        return (self._sql,)


class _FakeConnection:
    """Minimal stand-in for the sqlite_master lookup the emulation performs."""

    def __init__(self, sql: str) -> None:
        self._sql = sql

    async def execute(self, _query: str, _params: tuple = ()) -> _FakeCursor:
        return _FakeCursor(self._sql)


async def _emulated_fks(sql: str) -> list[tuple]:
    return await _emulate_foreign_key_list(_FakeConnection(sql), "o")


def _from_table_to(rows: list[tuple]) -> list[tuple[str, str, str]]:
    """Project (from, table, to) out of the SQLite row format."""
    return [(row[3], row[2], row[4]) for row in rows]


class TestInlineForeignKeyEmulation:
    """The referencing column must be the column, never a DDL keyword."""

    @pytest.mark.parametrize(
        "sql,expected",
        [
            (
                "CREATE TABLE o (id TEXT PRIMARY KEY, user_id TEXT NOT NULL "
                "REFERENCES u(id) ON DELETE CASCADE)",
                [("user_id", "u", "id")],
            ),
            ("CREATE TABLE o (user_id TEXT REFERENCES u(id))", [("user_id", "u", "id")]),
            (
                "CREATE TABLE o (a TEXT REFERENCES x(id), b TEXT REFERENCES y(id))",
                [("a", "x", "id"), ("b", "y", "id")],
            ),
            (
                'CREATE TABLE o ("user id" TEXT REFERENCES "my tbl"("pk"))',
                [("user id", "my tbl", "pk")],
            ),
            ("CREATE TABLE o (c VARCHAR(50) NOT NULL REFERENCES u(id))", [("c", "u", "id")]),
            ("CREATE TABLE o (c DOUBLE PRECISION REFERENCES u(id))", [("c", "u", "id")]),
            ("CREATE TABLE o (c TEXT DEFAULT 'x' REFERENCES u(id))", [("c", "u", "id")]),
            (
                "CREATE TABLE o (\n  id TEXT PRIMARY KEY,\n"
                "  user_id TEXT NOT NULL REFERENCES u(id) ON DELETE CASCADE\n)",
                [("user_id", "u", "id")],
            ),
            ("CREATE TABLE o (id TEXT PRIMARY KEY, name TEXT)", []),
        ],
        ids=[
            "fk-after-other-columns",
            "fk-is-first-column",
            "two-inline-fks",
            "quoted-identifiers",
            "sized-type",
            "multiword-type",
            "default-before-references",
            "multiline-ddl",
            "no-foreign-keys",
        ],
    )
    async def test_referencing_column_is_captured(self, sql, expected):
        assert _from_table_to(await _emulated_fks(sql)) == expected

    async def test_ddl_keyword_never_captured_as_column(self):
        """The specific regression: 'CREATE' must never appear as from_col."""
        sql = (
            "CREATE TABLE test_orders (id TEXT PRIMARY KEY, user_id TEXT NOT NULL "
            "REFERENCES test_users(id) ON DELETE CASCADE)"
        )
        from_cols = [row[3] for row in await _emulated_fks(sql)]

        assert "CREATE" not in from_cols
        assert from_cols == ["user_id"]

    async def test_on_delete_action_preserved(self):
        """Referential actions must survive the parse."""
        rows = await _emulated_fks(
            "CREATE TABLE o (id TEXT PRIMARY KEY, user_id TEXT REFERENCES u(id) ON DELETE CASCADE)"
        )

        assert len(rows) == 1
        assert rows[0][6] == "CASCADE"  # on_delete

    async def test_row_shape_matches_sqlite(self):
        """Eight columns, in SQLite's documented order."""
        rows = await _emulated_fks("CREATE TABLE o (user_id TEXT REFERENCES u(id))")

        assert len(rows[0]) == 8
        assert rows[0][2] == "u"  # table
        assert rows[0][3] == "user_id"  # from
        assert rows[0][4] == "id"  # to
