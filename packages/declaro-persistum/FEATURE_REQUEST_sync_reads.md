# Feature Request: Read connections should use sync driver without pull

## Problem

When `TursoPool` has cloud sync enabled (`remote_url` set), the write holder connects via `turso.aio.sync.connect(path, remote_url, auth_token)`. Read connections from `acquire()` currently use `turso.aio.connect(path)` (plain local driver, no remote_url — changed in 106a4ae).

These two pyturso driver types don't share WAL state on the same file. Tables created by the sync write holder during migration are invisible to plain local reads. Result: `no such table` errors for tables that exist.

## Observed behavior

```
# Write holder (sync driver) creates tables during migration — succeeds
# Read connection (plain driver) queries those tables — fails
turso.lib.DatabaseError: Parse error: no such table: pricing_invites
```

## Requested change

In `TursoPool.acquire()`, create the read connection holder with the same driver type as the write holder — `turso.aio.sync.connect(path, remote_url, auth_token)` — but **skip the pull**. This ensures both sides use the same driver and see the same local state.

```python
# Current (106a4ae):
holder = _TursoConnectionHolder(self._database_path)  # plain driver

# Requested:
holder = _TursoConnectionHolder(self._database_path, self._remote_url, self._auth_token)
# But do NOT call holder.pull() — just connect and read local state
```

This may require a flag on `_TursoConnectionHolder` or `connect_async()` to skip the automatic pull that `turso.aio.sync.connect()` may do on connection open. If `turso.aio.sync.connect()` doesn't pull automatically (only on explicit `pull()` call), then simply passing `remote_url` without calling `pull()` should be sufficient.

## Impact

Blocking: public app's invite page returns 503 on every request because `pricing_invites` table is invisible to reads.
