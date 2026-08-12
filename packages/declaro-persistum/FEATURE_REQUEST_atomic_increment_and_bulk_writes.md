# Feature Request: Atomic `increment=` and `update_many` on TableProxy

## Problem

multicardz hits a recurring N+1 read+write pattern when maintaining denormalized counter columns (tag card-counts, group-membership counts). The current declaro APIs let us batch the **read** side of these loops with `find_many(where={"id": {"in": [...]}})` + a Python merge, but the **write** side stays per-row because:

1. `update_one` / `update` accept only literal `data={...}` — there's no `increment={"col": delta}` for atomic counter math.
2. There is no `update_many(where=, data=)` for "apply the same data to every matching row in one call".

Together this means: collapsing a 2N-round-trip RMW loop (find_one + update_one per item) into 1+N (one batched read + N per-row updates) is the best we can do. The N round-trips of writes remain.

## Observed pattern (multicardz `apps/shared/services/tag_count_maintenance.py`)

```python
# Today, after Wave 6 batch-read refactor:
tag_rows = await pool.tags.find_many(where={"tag_id": {"in": tag_ids}})  # 1 read
by_id = index_by_key(tag_rows, "tag_id")
for tag_id in tag_ids:
    current = by_id.get(tag_id, {}).get("card_count", 0)
    await pool.tags.update_one(
        where={"tag_id": tag_id},
        data={"card_count": current + 1},
    )   # N writes
```

Two distinct gaps:

- The RMW is non-atomic. A concurrent writer can clobber the counter between our read and our update.
- We can't batch the writes even when the delta is uniform (e.g. "decrement every tag in `removed_tags` by 1").

## Requested changes

### 1. `increment={"col": delta}` on `update_one` and fluent `update`

Atomic counter math at the storage layer. Removes the need to read first.

```python
# Single-row atomic increment:
await pool.tags.update_one(
    where={"tag_id": tag_id},
    data={},
    increment={"card_count": 1},
)

# Atomic decrement:
await pool.tags.update_one(
    where={"tag_id": tag_id},
    data={},
    increment={"card_count": -1},
)
```

Equivalent SQL: `UPDATE tags SET card_count = card_count + 1 WHERE tag_id = ?`.

`increment=` should compose with `data=` so callers can update other columns in the same statement.

### 2. `update_many(where=, data=, increment=)` on TableProxy

Prisma-style bulk update. Applies the same `data` and/or `increment` to every row matching `where`.

```python
# Decrement card_count by 1 for every tag in removed_tags:
await pool.tags.update_many(
    where={"tag_id": {"in": list(removed_tags)}},
    increment={"card_count": -1},
)

# Increment for added tags:
await pool.tags.update_many(
    where={"tag_id": {"in": list(added_tags)}},
    increment={"card_count": 1},
)
```

Equivalent SQL: `UPDATE tags SET card_count = card_count + 1 WHERE tag_id IN (?,?,...)`.

Return value: count of rows updated (mirrors `delete_many` if that returns a count, or matches Prisma's `{count: N}` shape).

## Impact

Without these:

- multicardz's tag-count maintenance is `1 + N` round-trips per card mutation instead of `1`. For a card with 20 tags that's 21 round-trips where it could be 2 (one increment-many for added tags, one decrement-many for removed tags).
- The non-atomic RMW is technically a race; multicardz accepts this today because pyturso writes are sub-ms and the failure window is small, but `increment=` would close it for free.

Affected multicardz call sites (today, after Wave 6 batch-read refactor):

- `apps/shared/services/tag_count_maintenance.py:26` — `increment_tag_counts`
- `apps/shared/services/tag_count_maintenance.py:55` — `decrement_tag_counts`
- `apps/shared/services/tag_count_maintenance.py:92,103` — `update_tag_counts_on_reassignment` (decrement removed + increment added)
- `apps/shared/services/tag_count_maintenance.py:158` — `create_card_with_counts`
- `apps/shared/repositories/card_repository.py:386` — `_apply_tag_count_deltas`
- `apps/user/routes/import_export_api.py:633-640` — post-import tag-count update

All of these collapse to **one or two `update_many(... increment=...)` calls** with this feature.

## Acceptance

A short integration test in declaro-persistum's suite proving:

- `update_one(... increment={"col": delta})` produces `col = col + delta` (or `col - |delta|` for negative) atomically.
- `update_many(where={col: {"in": [...]}}, increment={...})` applies to every matching row in one statement.
- `update_many` returns the affected row count.
- `data=` and `increment=` compose (both applied in the same UPDATE).
- Combined with `where={"col": {"in": [...]}}`, the SQL emitted is a single statement (verified via pool query log if available).
