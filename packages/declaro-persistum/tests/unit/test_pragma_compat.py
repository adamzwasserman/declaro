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


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    """Returns rows, or raises whatever it was given, per SQL fragment."""

    def __init__(self, rows=None, raises=None, match=None):
        self._rows = rows or []
        self._raises = raises
        self._match = match
        self.queries: list[str] = []

    async def execute(self, sql, _params=None):
        self.queries.append(sql)
        if self._raises is not None and (self._match is None or self._match in sql):
            raise self._raises
        return _Cursor(self._rows)


class _TursoConn(_Conn):
    pass


_TursoConn.__module__ = "turso.lib_sync_aio"


class TestUnquote:
    def test_a_bare_identifier_is_unchanged(self):
        assert _unquote("users") == "users"

    @pytest.mark.parametrize("quoted", ['"users"', "'users'", "`users`"])
    def test_each_quote_style_is_stripped(self, quoted):
        assert _unquote(quoted) == "users"

    def test_surrounding_whitespace_is_stripped_first(self):
        assert _unquote('  "users"  ') == "users"

    def test_an_unbalanced_quote_is_left_alone(self):
        assert _unquote('"users') == '"users'

    def test_only_the_outer_pair_is_removed(self):
        assert _unquote('""users""') == '"users"'

    def test_an_empty_string_survives(self):
        assert _unquote("") == ""


class TestSplitColumns:
    def test_a_single_column_comes_back_whole(self):
        assert _split_columns("id") == ["id"]

    def test_columns_split_on_commas(self):
        assert _split_columns("a,b,c") == ["a", "b", "c"]

    def test_a_comma_inside_parentheses_does_not_split(self):
        """`substr(name, 1, 3)` is one indexed expression, not three."""
        assert _split_columns("a,substr(name, 1, 3),b") == [
            "a",
            "substr(name, 1, 3)",
            "b",
        ]

    def test_nested_parentheses_are_tracked_by_depth(self):
        assert _split_columns("f(g(a, b), c),d") == ["f(g(a, b), c)", "d"]

    def test_an_empty_string_yields_nothing(self):
        assert _split_columns("") == []

    def test_a_trailing_comma_does_not_add_an_empty_part(self):
        assert _split_columns("a,") == ["a"]

    def test_whitespace_is_preserved_for_the_caller_to_handle(self):
        assert _split_columns("a, b") == ["a", " b"]


class TestIsTursoConnection:
    def test_a_turso_module_is_detected(self):
        assert _is_turso_connection(_TursoConn()) is True

    def test_a_plain_object_defaults_to_not_turso(self):
        assert _is_turso_connection(object()) is False

    def test_an_aiosqlite_connection_is_not_turso(self):
        class _Aio:
            pass

        _Aio.__module__ = "aiosqlite.core"
        assert _is_turso_connection(_Aio()) is False


class TestCounters:
    def test_counts_start_at_zero(self):
        assert get_emulation_count("index_list") == 0
        assert get_native_success_count("index_list") == 0

    def test_an_unknown_pragma_name_counts_zero_rather_than_raising(self):
        assert get_emulation_count("no_such_pragma") == 0
        assert get_native_success_count("no_such_pragma") == 0

    def test_affected_tables_starts_empty(self):
        assert get_affected_tables() == set()

    def test_the_affected_table_set_is_a_copy(self):
        """A caller mutating the result must not corrupt the module state."""
        got = get_affected_tables()
        got.add("intruder")
        assert get_affected_tables() == set()


class TestPragmaTableInfo:
    @pytest.mark.asyncio
    async def test_it_passes_straight_through_to_the_native_pragma(self):
        conn = _Conn(rows=[(0, "id", "INTEGER", 1, None, 1)])
        assert await pragma_table_info(conn, "users") == [
            (0, "id", "INTEGER", 1, None, 1)
        ]
        assert conn.queries == ["PRAGMA table_info('users')"]

    @pytest.mark.asyncio
    async def test_rows_are_normalised_to_tuples(self):
        conn = _Conn(rows=[[0, "id"]])
        assert await pragma_table_info(conn, "users") == [(0, "id")]


class TestPragmaIndexListNativePath:
    @pytest.mark.asyncio
    async def test_native_rows_are_returned_and_nothing_is_emulated(self):
        conn = _Conn(rows=[(0, "ix_a", 0, "c", 0)])
        assert await pragma_index_list(conn, "users") == [(0, "ix_a", 0, "c", 0)]
        assert get_emulation_count("index_list") == 0

    @pytest.mark.asyncio
    async def test_native_success_on_sqlite_is_not_counted(self):
        """Only Turso successes are interesting — SQLite always worked."""
        await pragma_index_list(_Conn(rows=[]), "users")
        assert get_native_success_count("index_list") == 0

    @pytest.mark.asyncio
    async def test_native_success_on_turso_is_counted(self):
        """This is the signal that the emulation can be deleted."""
        await pragma_index_list(_TursoConn(rows=[]), "users")
        assert get_native_success_count("index_list") == 1


class TestPragmaIndexListFallback:
    @pytest.mark.parametrize(
        "message",
        [
            "not supported",
            "no such pragma: index_list",
            "unknown pragma",
            "Not a valid pragma name",
        ],
    )
    @pytest.mark.asyncio
    async def test_each_not_supported_wording_triggers_emulation(self, message):
        """pyturso has reported this in several wordings across versions."""
        conn = _Conn(raises=RuntimeError(message), match="PRAGMA index_list")
        await pragma_index_list(conn, "users")
        assert get_emulation_count("index_list") == 1

    @pytest.mark.asyncio
    async def test_the_emulating_table_is_recorded(self):
        conn = _Conn(raises=RuntimeError("not supported"), match="PRAGMA index_list")
        await pragma_index_list(conn, "users")
        assert get_affected_tables() == {"users"}

    @pytest.mark.asyncio
    async def test_the_match_is_case_insensitive(self):
        conn = _Conn(raises=RuntimeError("NOT SUPPORTED"), match="PRAGMA index_list")
        await pragma_index_list(conn, "users")
        assert get_emulation_count("index_list") == 1

    @pytest.mark.asyncio
    async def test_a_real_error_propagates_instead_of_being_emulated(self):
        """A broken database must not be mistaken for an old engine."""
        conn = _Conn(
            raises=RuntimeError("disk I/O error"), match="PRAGMA index_list"
        )
        with pytest.raises(RuntimeError, match="disk I/O error"):
            await pragma_index_list(conn, "users")
        assert get_emulation_count("index_list") == 0

    @pytest.mark.asyncio
    async def test_a_missing_table_error_propagates(self):
        conn = _Conn(
            raises=RuntimeError("no such table: users"), match="PRAGMA index_list"
        )
        with pytest.raises(RuntimeError):
            await pragma_index_list(conn, "users")

    @pytest.mark.asyncio
    async def test_emulation_reads_sqlite_master(self):
        conn = _Conn(raises=RuntimeError("not supported"), match="PRAGMA index_list")
        await pragma_index_list(conn, "users")
        assert any("sqlite_master" in q for q in conn.queries)
