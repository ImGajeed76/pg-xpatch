---
title: Tuning read performance
description: Size the content cache to your working set, warm it deliberately, lean on indexes, and read cache_stats to know what to change.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/tuning-read-performance
  type: how-to
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 9m
  status: stable
  aliases:
    - warm cache
    - warm_cache_parallel
    - cache size
    - read performance
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/pg_xpatch.c
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/xpatch_warm.c
  prev: ./tuning-compression.md
  next: ./monitoring.md
---

# Tuning read performance

Reads are fast when the content they need is already in the [shared cache](./caching-and-performance.md). So tuning reads is mostly two jobs: make the cache big enough to hold your hot data, and warm it before it matters.

!!! danger "First, confirm the cache exists"
    None of this helps if pg-xpatch is not in `shared_preload_libraries`. Check with `SELECT cache_max_bytes FROM xpatch.cache_stats()`: a `0` means the cache is off. See [Installation](./installation.md#enable-the-shared-cache).

## Size the content cache

The content cache defaults to 256 MB. If your frequently-read versions do not fit, entries get evicted and reads go cold again. Raise it to comfortably hold the working set:

```sql
ALTER SYSTEM SET pg_xpatch.cache_size_mb = 1024;
-- restart required: this is a postmaster-level setting
```

`cache_size_mb`, `cache_max_entries`, and `cache_partitions` all take effect only after a server restart. Size `cache_max_entries` alongside the bytes if you cache many small values; raise `cache_partitions` (default 32) only under very high backend concurrency.

## Stop large rows from being skipped

A single reconstructed value larger than `cache_max_entry_kb` (default 256 KB) is not cached at all, so it is rebuilt on every read. Unlike the sizing knobs above, this one changes at runtime:

```sql
ALTER SYSTEM SET pg_xpatch.cache_max_entry_kb = 1024;
SELECT pg_reload_conf();
```

Set it above your largest reconstructed value. Watch `skip_count` (below) to know if you need to.

## Warm the cache

After a restart, or before a heavy read job, pre-build content instead of waiting for the first queries to pay for it. Prefer the parallel version: it discovers groups and keyframe sections and spreads the work across background workers. It is implemented in [`xpatch_warm.c`](../src/xpatch_warm.c).

```sql
SELECT * FROM xpatch.warm_cache_parallel('documents');                                   -- default workers
SELECT * FROM xpatch.warm_cache_parallel('documents', max_workers => 8);
SELECT * FROM xpatch.warm_cache_parallel('documents', max_workers => 8, max_groups => 1000);
```

The default worker count is the `pg_xpatch.warm_cache_workers` GUC (4), settable per session. If no background-worker slots are free, it falls back to sequential warming rather than failing. There is also a simpler PL/pgSQL `xpatch.warm_cache('documents')` if you want a plain sequential scan.

!!! tip "Good moments to warm"
    Right after a restart, after a bulk load, and ahead of a known reporting window. Use `max_groups` to warm just the hot subset on a huge table.

## Favor indexed reads

Point lookups through the composite `(group_by, _xp_seq)` index that [`configure()`](./configuration.md) builds are close to heap speed. A cold full-table scan is the slow path, because it rebuilds every row. Filter by group and let the index work; avoid `SELECT COUNT(*)` over the whole table as a habit.

## Read the signals, then adjust

`xpatch.cache_stats()` tells you which knob to reach for:

| Symptom                              | Likely cause                      | Fix                                                       |
| ------------------------------------ | --------------------------------- | -------------------------------------------------------- |
| high `eviction_count`                | working set bigger than the cache | raise `cache_size_mb` (and maybe `cache_max_entries`), restart |
| high `skip_count`                    | values exceed the per-entry cap   | raise `cache_max_entry_kb`, reload                       |
| high `miss_count` right after a restart | cache is cold                  | warm it with `xpatch.warm_cache_parallel()`             |

!!! cards { cols=2 }
    - [Monitoring](./monitoring.md){ icon=activity }
      Every counter `cache_stats()` exposes, and how to read it.

    - [Server parameters](./server-parameters.md){ icon=settings }
      All cache GUCs, with defaults, ranges, and restart rules.
