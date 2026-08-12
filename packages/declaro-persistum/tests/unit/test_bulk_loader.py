"""Bulk loaders: the SQL each backend issues, and where they differ.

`bulk_loader.py` sat at 26% with 18 branches — part of the Slop Audit
L1.19 gap (declaro-xu0).

The two implementations are not interchangeable and the tests say exactly
where they diverge, because that divergence is the reason the module
exists:

- stable ordering is `ctid` on PostgreSQL and `rowid` on SQLite/Turso
- writes use asyncpg's COPY protocol vs `executemany`
- FK checks go off via `session_replication_role` vs `PRAGMA foreign_keys`
- a deleted-row count is parsed out of asyncpg's "DELETE N" string, but
  counted before the delete on the generic path

`_normalize_pg_value` is the seam that makes a PostgreSQL to Turso
transfer work at all: a UUID object has no SQLite type, so it must become
a string on the way out.
"""

import uuid

import pytest

from declaro_persistum.bulk_loader import (
    GenericBulkLoader,
    PostgreSQLBulkLoader,
    _normalize_pg_value,
    create_bulk_loader,
)


class _Row(dict):
    """asyncpg Record stand-in: ordered mapping exposing .values()."""


class _PgConn:
    def __init__(self, rows=None, fetchval=0, execute_result="DELETE 3"):
        self._rows = rows or []
        self._fetchval = fetchval
        self._execute_result = execute_result
        self.queries: list[str] = []
        self.copied: list[tuple] = []

    async def fetch(self, sql):
        self.queries.append(sql)
        return self._rows

    async def fetchval(self, sql):
        self.queries.append(sql)
        return self._fetchval

    async def execute(self, sql):
        self.queries.append(sql)
        return self._execute_result

    async def copy_records_to_table(self, table, *, columns, records):
        self.copied.append((table, tuple(columns), list(records)))


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _GenericConn:
    def __init__(self, rows=None):
        self._rows = rows or []
        self.queries: list[tuple] = []
        self.many: list[tuple] = []

    async def execute(self, sql, params=()):
        self.queries.append((sql, params))
        return _Cursor(self._rows)

    async def executemany(self, sql, rows):
        self.many.append((sql, list(rows)))


class TestNormalizePgValue:
    def test_a_uuid_becomes_its_string_form(self):
        u = uuid.uuid4()
        assert _normalize_pg_value(u) == str(u)

    @pytest.mark.parametrize("value", [1, "text", None, 3.5, True, b"bytes"])
    def test_everything_else_passes_through_untouched(self, value):
        assert _normalize_pg_value(value) is value

    def test_a_uuid_shaped_string_is_left_alone(self):
        s = str(uuid.uuid4())
        assert _normalize_pg_value(s) is s


class TestCreateBulkLoader:
    def test_postgresql_gets_the_copy_based_loader(self):
        assert isinstance(create_bulk_loader("postgresql"), PostgreSQLBulkLoader)

    @pytest.mark.parametrize("dialect", ["sqlite", "turso"])
    def test_sqlite_and_turso_share_the_generic_loader(self, dialect):
        assert isinstance(create_bulk_loader(dialect), GenericBulkLoader)

    def test_an_unsupported_dialect_raises(self):
        with pytest.raises(ValueError):
            create_bulk_loader("mysql")


class TestPostgreSQLBulkLoader:
    @pytest.mark.asyncio
    async def test_reads_are_ordered_by_ctid(self):
        conn = _PgConn()
        await PostgreSQLBulkLoader().read_rows(conn, "users", ["id"])
        assert conn.queries == ['SELECT "id" FROM "users" ORDER BY ctid']

    @pytest.mark.asyncio
    async def test_a_limit_is_appended(self):
        conn = _PgConn()
        await PostgreSQLBulkLoader().read_rows(conn, "u", ["id"], limit=10)
        assert conn.queries[0].endswith("LIMIT 10")

    @pytest.mark.asyncio
    async def test_a_zero_limit_is_still_emitted(self):
        conn = _PgConn()
        await PostgreSQLBulkLoader().read_rows(conn, "u", ["id"], limit=0)
        assert "LIMIT 0" in conn.queries[0]

    @pytest.mark.asyncio
    async def test_a_zero_offset_is_omitted(self):
        """`if offset:` — page zero needs no OFFSET clause."""
        conn = _PgConn()
        await PostgreSQLBulkLoader().read_rows(conn, "u", ["id"], offset=0)
        assert "OFFSET" not in conn.queries[0]

    @pytest.mark.asyncio
    async def test_a_nonzero_offset_is_appended(self):
        conn = _PgConn()
        await PostgreSQLBulkLoader().read_rows(conn, "u", ["id"], offset=5)
        assert conn.queries[0].endswith("OFFSET 5")

    @pytest.mark.asyncio
    async def test_uuid_values_are_normalised_on_the_way_out(self):
        u = uuid.uuid4()
        conn = _PgConn(rows=[_Row(id=u, name="ada")])
        rows = await PostgreSQLBulkLoader().read_rows(conn, "u", ["id", "name"])
        assert rows == [(str(u), "ada")]

    @pytest.mark.asyncio
    async def test_writes_use_the_copy_protocol(self):
        conn = _PgConn()
        n = await PostgreSQLBulkLoader().load_rows(conn, "u", ["id"], [(1,), (2,)])
        assert n == 2
        assert conn.copied == [("u", ("id",), [(1,), (2,)])]

    @pytest.mark.asyncio
    async def test_writing_nothing_does_not_call_copy(self):
        conn = _PgConn()
        assert await PostgreSQLBulkLoader().load_rows(conn, "u", ["id"], []) == 0
        assert conn.copied == []

    @pytest.mark.asyncio
    async def test_the_row_count_is_returned_as_an_int(self):
        assert await PostgreSQLBulkLoader().count_rows(_PgConn(fetchval="7"), "u") == 7

    @pytest.mark.asyncio
    async def test_the_delete_count_is_parsed_out_of_the_status_string(self):
        conn = _PgConn(execute_result="DELETE 42")
        assert await PostgreSQLBulkLoader().delete_rows(conn, "u") == 42

    @pytest.mark.asyncio
    async def test_an_empty_delete_status_counts_zero(self):
        conn = _PgConn(execute_result="")
        assert await PostgreSQLBulkLoader().delete_rows(conn, "u") == 0

    @pytest.mark.asyncio
    async def test_fk_checks_toggle_via_session_replication_role(self):
        conn = _PgConn()
        loader = PostgreSQLBulkLoader()
        await loader.disable_fk_checks(conn)
        await loader.enable_fk_checks(conn)
        assert conn.queries == [
            "SET session_replication_role = 'replica'",
            "SET session_replication_role = 'origin'",
        ]


class TestGenericBulkLoader:
    @pytest.mark.asyncio
    async def test_reads_are_ordered_by_rowid(self):
        conn = _GenericConn()
        await GenericBulkLoader().read_rows(conn, "users", ["id"])
        assert conn.queries[0][0] == 'SELECT "id" FROM "users" ORDER BY rowid'

    @pytest.mark.asyncio
    async def test_limit_and_offset_are_appended_in_order(self):
        conn = _GenericConn()
        await GenericBulkLoader().read_rows(conn, "u", ["id"], limit=5, offset=10)
        assert conn.queries[0][0].endswith("LIMIT 5 OFFSET 10")

    @pytest.mark.asyncio
    async def test_a_zero_offset_is_omitted(self):
        conn = _GenericConn()
        await GenericBulkLoader().read_rows(conn, "u", ["id"], offset=0)
        assert "OFFSET" not in conn.queries[0][0]

    @pytest.mark.asyncio
    async def test_rows_come_back_as_tuples(self):
        conn = _GenericConn(rows=[[1, "ada"]])
        assert await GenericBulkLoader().read_rows(conn, "u", ["id", "n"]) == [
            (1, "ada")
        ]

    @pytest.mark.asyncio
    async def test_writes_are_parameterised_and_batched(self):
        conn = _GenericConn()
        n = await GenericBulkLoader().load_rows(conn, "u", ["id", "n"], [(1, "a")])
        assert n == 1
        assert conn.many == [
            ('INSERT INTO "u" ("id", "n") VALUES (?, ?)', [(1, "a")])
        ]

    @pytest.mark.asyncio
    async def test_writing_nothing_issues_no_statement(self):
        conn = _GenericConn()
        assert await GenericBulkLoader().load_rows(conn, "u", ["id"], []) == 0
        assert conn.many == []

    @pytest.mark.asyncio
    async def test_counting_reads_the_first_column(self):
        assert await GenericBulkLoader().count_rows(_GenericConn(rows=[(9,)]), "u") == 9

    @pytest.mark.asyncio
    async def test_counting_an_empty_result_gives_zero(self):
        assert await GenericBulkLoader().count_rows(_GenericConn(rows=[]), "u") == 0

    @pytest.mark.asyncio
    async def test_delete_counts_before_deleting(self):
        """There is no status string here, so the count must be taken first."""
        conn = _GenericConn(rows=[(4,)])
        assert await GenericBulkLoader().delete_rows(conn, "u") == 4
        assert any("DELETE FROM" in q for q, _ in conn.queries)

    @pytest.mark.asyncio
    async def test_fk_checks_toggle_via_pragma(self):
        conn = _GenericConn()
        loader = GenericBulkLoader()
        await loader.disable_fk_checks(conn)
        await loader.enable_fk_checks(conn)
        assert [q for q, _ in conn.queries] == [
            "PRAGMA foreign_keys = OFF",
            "PRAGMA foreign_keys = ON",
        ]
