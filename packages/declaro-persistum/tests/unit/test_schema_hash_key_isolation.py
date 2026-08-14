"""Two services sharing one database must not fight over the skip-if-clean stamp.

The stamp lives in _declaro_meta in the database being migrated, under a key
built from the schema file's *name* alone. Two services that both migrate the
same database therefore write the same row.

They disagree whenever their computed hash differs, and the hash covers both
the schema file's contents and the library version. So:

  - two services with their own models.py, different content, same filename
  - or two services on different declaro-persistum versions

each read the other's stamp, see a mismatch, re-introspect, and re-stamp.
Neither is wrong and neither can win. The result is a permanent
never-clean state with operations re-proposed on every boot, which is also
the symptom of a genuine schema drift — so it is easy to misdiagnose.

Reported by a consumer whose stage service pointed its central pool at the
production central database.
"""

import pytest

from declaro_persistum.migrations import _get_stored_hash, _store_hash


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class _FakeMetaConn:
    """A minimal _declaro_meta table shared by every service in a test."""

    def __init__(self) -> None:
        self.rows: dict[str, str] = {}

    async def execute(self, sql: str, params=()):
        s = " ".join(sql.split()).lower()
        if s.startswith("insert into"):
            key, value, _updated = params
            self.rows[key] = value
            return _FakeCursor([])
        if s.startswith("select value from"):
            key = params[0]
            return _FakeCursor([(self.rows[key],)] if key in self.rows else [])
        return _FakeCursor([])

    async def commit(self) -> None:
        pass


class TestTwoServicesOneDatabase:
    """Each service's stamp must survive the other's."""

    @pytest.mark.asyncio
    async def test_different_schema_content_does_not_collide(self):
        """Two services, each with its own models.py, must not evict each other.

        Same filename, different content, so different hashes.
        """
        conn = _FakeMetaConn()

        await _store_hash(conn, "models.py", "hash-from-service-a")
        await _store_hash(conn, "models.py", "hash-from-service-b")

        a = await _get_stored_hash(conn, "models.py", "hash-from-service-a")
        b = await _get_stored_hash(conn, "models.py", "hash-from-service-b")

        assert a == "hash-from-service-a", (
            "service A's stamp was evicted by service B — A will re-introspect "
            "on every boot, and so will B, forever"
        )
        assert b == "hash-from-service-b"

    @pytest.mark.asyncio
    async def test_version_skew_does_not_collide(self):
        """Same models file, two library versions, must not evict each other.

        The version is mixed into the hash deliberately, so an upgraded service
        and a not-yet-upgraded one compute different hashes for identical
        models. Both must be able to record that they are clean.
        """
        conn = _FakeMetaConn()

        await _store_hash(conn, "models.py", "hash-under-0.1.8")
        await _store_hash(conn, "models.py", "hash-under-0.1.14")

        old = await _get_stored_hash(conn, "models.py", "hash-under-0.1.8")
        new = await _get_stored_hash(conn, "models.py", "hash-under-0.1.14")

        assert old == "hash-under-0.1.8"
        assert new == "hash-under-0.1.14"

    @pytest.mark.asyncio
    async def test_unknown_hash_reads_as_absent(self):
        """A hash never stored must read as absent, so the schema is dirty."""
        conn = _FakeMetaConn()
        await _store_hash(conn, "models.py", "known")

        assert await _get_stored_hash(conn, "models.py", "never-stored") is None

    @pytest.mark.asyncio
    async def test_same_service_restamping_is_idempotent(self):
        """Storing the same hash twice must not create a second row."""
        conn = _FakeMetaConn()

        await _store_hash(conn, "models.py", "same")
        await _store_hash(conn, "models.py", "same")

        assert len(conn.rows) == 1

    @pytest.mark.asyncio
    async def test_different_schema_files_stay_separate(self):
        """Distinct schema filenames must remain distinct, as before."""
        conn = _FakeMetaConn()

        await _store_hash(conn, "users.py", "h1")
        await _store_hash(conn, "orders.py", "h2")

        assert await _get_stored_hash(conn, "users.py", "h1") == "h1"
        assert await _get_stored_hash(conn, "orders.py", "h2") == "h2"
