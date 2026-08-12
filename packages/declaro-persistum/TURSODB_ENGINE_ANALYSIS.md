# Analysis: opt into the new `tursodb` cloud engine (`use_tursodb`)

Status: **proposal / not yet implemented.** Source: Turso Beta invite (Pedro Muniz).

## What changed on Turso's side

Turso opened a Beta for creating cloud databases on the **new `tursodb` engine**
(the Rust-native engine; successor to the libsql-backed Turso Cloud). It is
**opt-in at database-creation time**:

- **CLI:** `turso db create --group <group-name> --tursodb`
- **Platform API:** `POST https://api.turso.tech/v1/organizations/{org}/databases`
  with `"use_tursodb": true` in the JSON body:
  ```json
  {"name": "new-database", "group": "<group_name>", "use_tursodb": true}
  ```
- (TS-only note from the invite: they recommend `@tursodatabase/serverless` for
  serverless TS clients. Not relevant to this Python package.)

This package talks to the **Platform API**, so the relevant lever is the
`use_tursodb` payload flag — not the CLI.

## Where it lives in this package

`src/declaro_persistum/pool.py` — `TursoCloudManager.create_database` (line ~1258).
Current implementation builds the create payload as:

```python
async def create_database(self, db_name: str, *, size_limit: str | None = None) -> dict:
    payload: dict[str, Any] = {
        "name": db_name,
        "group": self._group,
    }
    if size_limit:
        payload["size_limit"] = size_limit
    result = await self._api_request("POST", "/databases", payload)
    return result.get("database", result)
```

There is **no** `use_tursodb` support today — so a caller cannot create a
`tursodb`-engine database through this manager.

## Proposed change (small)

Add an opt-in parameter and pass it through to the payload. Two design options:

**Option A — per-call flag (simplest, explicit):**
```python
async def create_database(
    self,
    db_name: str,
    *,
    size_limit: str | None = None,
    use_tursodb: bool = False,   # NEW: Beta — create on the new tursodb engine
) -> dict:
    payload: dict[str, Any] = {"name": db_name, "group": self._group}
    if size_limit:
        payload["size_limit"] = size_limit
    if use_tursodb:
        payload["use_tursodb"] = True
    ...
```

**Option B — manager-level default (set once at construction):**
Add `use_tursodb: bool = False` to `TursoCloudManager.__init__`, store as
`self._use_tursodb`, and in `create_database` do
`if self._use_tursodb: payload["use_tursodb"] = True` (a per-call override can
still win). This lets a deployment flip the whole org onto `tursodb` without
touching every call site.

Recommendation: **both** — a manager default (Option B) for deployment-wide
rollout, overridable by an explicit per-call `use_tursodb=` (Option A) for
migration/testing. Keep the default `False` until the compatibility gate below
passes.

## Compatibility gate — verify BEFORE defaulting it on

The downstream architecture is **pyturso embedded replicas that sync from Turso
Cloud**. `tursodb` is a different (new) engine. pyturso is the new-Turso Python
binding, so they are *plausibly* the intended pairing — but this must be
verified, not assumed, before any production database is created with
`use_tursodb: true`:

1. Create one Beta DB with `use_tursodb=True`.
2. Mint a token, point a `ConnectionPool.turso(local_path, remote_url, auth_token)`
   embedded replica at it.
3. Confirm the full embedded contract still holds:
   - initial bootstrap sync (cloud → local replica) pulls schema + rows,
   - local writes succeed sub-ms,
   - the push loop delivers writes to cloud,
   - a fresh replica re-synced from cloud sees those writes,
   - migrations (`apply_migrations_async`) run cleanly on a `tursodb` DB.

If any of those differ on `tursodb`, document the delta here before rollout.

## Downstream usage (minimal context)

One consumer creates tenant DBs via `await mgr.create_database(db_name)`. Once
this package supports the flag, that call becomes
`await mgr.create_database(db_name, use_tursodb=True)` (or the manager is
constructed with the default on). No other downstream change is needed — the
payload is built entirely inside this package.

## Suggested sequence

1. Implement the param + payload here (Options A+B), default `False`.
2. Run the compatibility gate against one Beta DB.
3. If green, flip the deployment default (or pass `use_tursodb=True` at the call
   site) and re-provision / migrate as desired.
