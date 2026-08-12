"""Provisioning databases on the Turso platform.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). This is an HTTP client for the Turso platform
API, not a connection pool: it creates and deletes databases, mints
auth tokens, and lists what exists.

Turso cloud is one database per tenant, so a multi-tenant application
provisions rather than migrates. This holds a pool per tenant and hands
it out by database name.

It reaches the network on every call, which the pools deliberately do
not. Keeping it in its own module keeps that distinction visible.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from declaro_persistum.exceptions import PoolConnectionError
from declaro_persistum.pool import ConnectionPool, TursoPool

logger = logging.getLogger(__name__)


class TursoCloudManager:
    """
    Multi-tenant database manager for Turso cloud.

    Turso cloud is designed for one database per tenant. This manager
    handles database provisioning, token management, and connection pooling
    for multi-tenant applications.

    Usage:
        manager = TursoCloudManager(
            org="mycompany",
            api_token=os.environ["TURSO_API_TOKEN"],
        )

        # Create database for new tenant
        db_info = await manager.create_database("tenant-123")

        # Get connection pool for tenant
        pool = await manager.get_pool("tenant-123")
        async with pool.acquire() as conn:
            ...

        # Delete tenant database
        await manager.delete_database("tenant-123")
    """

    def __init__(
        self,
        org: str,
        api_token: str,
        *,
        group: str = "default",
        region: str | None = None,
        pool_max_size: int = 10,
        pool_acquire_timeout: float = 30.0,
        use_tursodb: bool = False,
    ) -> None:
        """
        Initialize the Turso cloud manager.

        Args:
            org: Turso organization name
            api_token: Platform API token (from `turso auth api-tokens mint`)
            group: Database group (default: "default")
            region: Optional region hint for new databases
            pool_max_size: Max connections per tenant pool (default: 10)
            pool_acquire_timeout: Connection acquire timeout (default: 30s)
            use_tursodb: Beta — create databases on the new tursodb (Rust)
                engine by default. Overridable per-call. Keep False until the
                compatibility gate passes. (default: False)
        """
        self._org = org
        self._api_token = api_token
        self._group = group
        self._region = region
        self._pool_max_size = pool_max_size
        self._pool_acquire_timeout = pool_acquire_timeout
        self._use_tursodb = use_tursodb
        self._base_url = "https://api.turso.tech/v1"

        # Cache for tenant tokens and pools
        self._tokens: dict[str, str] = {}
        self._pools: dict[str, TursoPool] = {}

    def _build_remote_url(self, db_name: str) -> str:
        """Build the Turso Cloud remote URL for a database."""
        return f"https://{db_name}-{self._org}.turso.io"

    async def _api_request(
        self,
        method: str,
        endpoint: str,
        data: dict | None = None,
    ) -> dict:
        """Make a request to the Turso Platform API."""
        import urllib.error
        import urllib.request

        url = f"{self._base_url}/organizations/{self._org}{endpoint}"

        body = json.dumps(data).encode() if data is not None else None

        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("Authorization", f"Bearer {self._api_token}")
        if body:
            req.add_header("Content-Type", "application/json")

        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(None, lambda: urllib.request.urlopen(req))
            return json.loads(response.read())
        except urllib.error.HTTPError as e:
            error_body = e.read().decode()
            raise PoolConnectionError(f"Turso API error ({e.code}): {error_body}") from e

    async def create_database(
        self,
        db_name: str,
        *,
        size_limit: str | None = None,
        use_tursodb: bool | None = None,
        seed: dict[str, Any] | None = None,
    ) -> dict:
        """
        Create a new database for a tenant.

        Args:
            db_name: Database name (lowercase, numbers, dashes; max 64 chars)
            size_limit: Optional size limit (e.g., "256mb", "1gb")
            use_tursodb: Beta — create on the new tursodb (Rust) engine.
                None (default) inherits the manager's use_tursodb setting;
                an explicit True/False overrides it for this call.
            seed: Optional seed spec, passed through to the Turso API
                unmodified so the new database is created as a copy of an
                existing one rather than empty. Provisioning a tenant from a
                pre-built template becomes a single copy instead of
                create-empty then migrate then insert.

                Passed through as an opaque dict rather than being modelled
                as named parameters, so seed variants the API grows are
                usable without a release here. Two forms are documented today:

                    {"type": "database", "name": "<source-db>"}
                    {"type": "database_upload"}

                With type "database", an optional "timestamp" (ISO 8601)
                selects a recovery point rather than the current state —
                within the last 24 hours, or 30 days on the scaler plan:

                    {"type": "database", "name": "tpl", "timestamp": "..."}

                Not validated here; the API is the authority on which
                combinations are legal and reports violations itself.

        Returns:
            Dict with database info (DbId, Hostname, Name)
        """
        payload: dict[str, Any] = {
            "name": db_name,
            "group": self._group,
        }
        if size_limit:
            payload["size_limit"] = size_limit
        if seed is not None:
            payload["seed"] = seed

        resolved_use_tursodb = (
            self._use_tursodb if use_tursodb is None else use_tursodb
        )
        if resolved_use_tursodb:
            payload["use_tursodb"] = True

        result = await self._api_request("POST", "/databases", payload)
        return result.get("database", result)

    async def delete_database(self, db_name: str) -> None:
        """
        Delete a tenant's database.

        Args:
            db_name: Database name to delete
        """
        # Remove from cache
        self._tokens.pop(db_name, None)
        pool = self._pools.pop(db_name, None)
        if pool:
            await pool.close()

        await self._api_request("DELETE", f"/databases/{db_name}")

    async def create_token(self, db_name: str) -> str:
        """
        Create an auth token for a database.

        Args:
            db_name: Database name

        Returns:
            JWT auth token
        """
        result = await self._api_request(
            "POST",
            f"/databases/{db_name}/auth/tokens",
            {},
        )
        token = result.get("jwt", "")
        self._tokens[db_name] = token
        return token

    async def get_token(self, db_name: str) -> str:
        """
        Get auth token for a database (creates if not cached).

        Args:
            db_name: Database name

        Returns:
            JWT auth token
        """
        if db_name not in self._tokens:
            await self.create_token(db_name)
        return self._tokens[db_name]

    async def get_pool(self, db_name: str) -> TursoPool:
        """
        Get a connection pool for a tenant's database.

        Creates the pool on first access and caches it.

        Args:
            db_name: Database name (tenant identifier)

        Returns:
            TursoPool for the tenant's database
        """
        if db_name not in self._pools:
            token = await self.get_token(db_name)
            remote_url = self._build_remote_url(db_name)
            # TODO: local_path derivation for multi-tenant (db_dir/db_name.db)
            local_path = f"./db/{db_name}.db"
            self._pools[db_name] = await ConnectionPool.turso(
                local_path,
                remote_url=remote_url,
                max_size=self._pool_max_size,
                acquire_timeout=self._pool_acquire_timeout,
            )
        return self._pools[db_name]

    async def list_databases(self) -> list[dict]:
        """
        List all databases in the organization.

        Returns:
            List of database info dicts
        """
        result = await self._api_request("GET", "/databases")
        return result.get("databases", [])

    async def database_exists(self, db_name: str) -> bool:
        """
        Check if a database exists.

        Args:
            db_name: Database name to check

        Returns:
            True if database exists
        """
        try:
            await self._api_request("GET", f"/databases/{db_name}")
            return True
        except PoolConnectionError:
            return False

    async def get_or_create_database(self, db_name: str) -> dict:
        """
        Get existing database or create if it doesn't exist.

        Args:
            db_name: Database name

        Returns:
            Database info dict
        """
        if await self.database_exists(db_name):
            result = await self._api_request("GET", f"/databases/{db_name}")
            return result.get("database", result)
        return await self.create_database(db_name)

    async def close(self) -> None:
        """Close all cached connection pools."""
        for pool in self._pools.values():
            await pool.close()
        self._pools.clear()
        self._tokens.clear()
