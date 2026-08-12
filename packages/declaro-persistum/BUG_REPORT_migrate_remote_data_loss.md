# Bug: migrate-remote drops and recreates tables instead of altering them (DATA LOSS)

## Severity: CRITICAL — Production data loss

## Summary

`declaro migrate-remote` emits `create_table` operations for tables that already exist on the cloud DB, instead of `add_column` or `alter_column` for the actual schema diff. This drops all rows in the affected tables.

## Reproduction

1. Run `migrate-remote` to create initial schema on cloud (6 tables created — correct)
2. Add one column (`invite_token`) to the `users` model in the schema file
3. Run `migrate-remote` again

### Expected

```
Found 1 schema difference:
  - add_column on users (invite_token)
Applied 1 operation to cloud DB
```

### Actual

```
Found 6 schema differences:
  - create_table on pricing_invites
  - create_table on subscriptions
  - create_table on users
  - create_table on subscription_plans
  - create_table on database_routes
  - create_table on billing_events

Applied 6 operations to cloud DB
```

All 6 tables were dropped and recreated. All rows in all tables were lost.

## Impact

- All user records deleted
- All database_routes deleted (workspace provisioning broken)
- All pricing_invites deleted (invite links broken)
- All subscriptions and billing_events deleted
- Production login loop caused by missing user records

## Root Cause

The `cmd_migrate_remote` function connects via `turso.aio.sync.connect()` with a temporary local file. It pulls from cloud into the temp file, introspects, diffs, and applies. But the introspection found 0 tables in the temp file — meaning the pull from cloud did not bring the existing tables into the local replica.

Possible causes:
1. The pull into a fresh temp file doesn't actually sync the cloud state — the temp DB starts empty regardless of pull
2. The sync connection's pull is a no-op for a brand new local file (no replication frame to start from)
3. The introspection runs before the pull completes

Because the introspect sees 0 tables locally, the differ compares 0 existing tables vs 6 schema tables and emits 6 `create_table` operations. When these are pushed to cloud, the cloud's existing tables are replaced with empty ones.

## Suggested Fix

Before diffing, verify the introspected table count matches expectations. If the cloud DB should have tables but introspection finds 0, abort with an error rather than proceeding with `create_table` operations that will cause data loss.

Additionally, `create_table` on a table that already exists in the cloud should be rejected or converted to an `alter_table` diff. The applier should never silently drop a table that has rows.

## Workaround

None. Data is lost. Users, routes, invites, and subscriptions must be re-created manually or restored from backups.

## Command Used

```bash
uv run declaro migrate-remote \
  --remote "libsql://mc-central-adamzwasserman.aws-us-west-2.turso.io" \
  --token "$CENTRAL_DB_TOKEN" \
  --schema apps/shared/schema/central_tables.py
```

## Timeline

1. 00:27 UTC — First `migrate-remote` run: created 6 tables on cloud (correct, cloud was empty)
2. 00:32 UTC — App verified working: invite query returned 404 (table exists, token not found)
3. ~03:20 UTC — Added `invite_token` column to User schema
4. ~03:20 UTC — Second `migrate-remote` run: recreated all 6 tables (DATA LOSS)
5. ~03:25 UTC — Login loop: users table empty, database_routes empty, all provisioning fails
