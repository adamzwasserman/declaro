# Bug: migrate-remote uses turso.aio.connect() which doesn't support remote URLs

## Problem

`cmd_migrate_remote` at line 344 calls:
```python
conn = await turso.aio.connect(remote_url, auth_token=auth_token)
```

But `turso.aio.connect()` only accepts a local file path — it has no `auth_token` parameter. The call fails with `Error: open: NotFound`.

## pyturso API

- `turso.aio.connect(database)` — local file only, no auth_token param
- `turso.aio.sync.connect(path, remote_url, auth_token=...)` — requires local path + remote URL

There is no direct-to-cloud-only connection in pyturso.

## Fix

Use `turso.aio.sync.connect()` with a temp local file:

```python
import tempfile, os

with tempfile.TemporaryDirectory() as tmpdir:
    local_path = os.path.join(tmpdir, "migrate_remote.db")
    conn = await turso.aio.sync.connect(
        local_path,
        remote_url=remote_url,
        auth_token=auth_token,
    )
    # pull from cloud to get current state
    await conn.sync()
    # ... introspect, diff, apply DDL ...
    # push changes back to cloud
    await conn.sync()
```

## Reproduction

```
uv run declaro migrate-remote \
  --remote "libsql://mc-central-adamzwasserman.aws-us-west-2.turso.io" \
  --token "$CENTRAL_DB_TOKEN" \
  --schema apps/shared/schema/central_tables.py
```

Output: `Error: open: NotFound`
