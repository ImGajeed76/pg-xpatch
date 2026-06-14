---
title: Server parameters
description: Every pg_xpatch.* GUC, with default, range, change context, and what it controls.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/server-parameters
  type: reference
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 7m
  status: stable
  aliases:
    - GUC reference
    - configuration parameters
    - pg_xpatch settings
    - cache_size_mb
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/pg_xpatch.c
  prev: ./sql-functions.md
  next: ./types-and-limitations.md
---

# Server parameters

All parameters are prefixed `pg_xpatch.` and take effect only when pg-xpatch is loaded through `shared_preload_libraries`. They are defined in [`pg_xpatch.c`](../src/pg_xpatch.c).

!!! info "Change context"
    - **restart** (`postmaster`): set in `postgresql.conf` or with `ALTER SYSTEM`, then restart the server.
    - **superuser** (`superuser`): a superuser can change it at runtime with `ALTER SYSTEM` + `SELECT pg_reload_conf()`.
    - **session** (`user`): any user can `SET` it per session.

    Where a range shows `≥ N`, the practical ceiling is available memory.

## Content cache

The big one: reconstructed content, shared across all backends.

| Parameter                  | Default | Range    | Context   | Controls                                                  |
| -------------------------- | ------- | -------- | --------- | -------------------------------------------------------- |
| `pg_xpatch.cache_size_mb`  | 256 MB  | ≥ 1      | restart   | total shared memory for the content cache                |
| `pg_xpatch.cache_max_entries` | 65536 | ≥ 1000  | restart   | how many entries the cache can hold                      |
| `pg_xpatch.cache_max_entry_kb` | 256 KB | ≥ 16   | superuser | entries larger than this are skipped, not cached         |
| `pg_xpatch.cache_slot_size_kb` | 4 KB  | 1 to 64 | restart   | content storage granularity inside the cache             |
| `pg_xpatch.cache_partitions` | 32    | 1 to 256 | restart   | lock stripes; higher allows more concurrent backends     |

`cache_max_entry_kb` is the only content-cache setting you can change without a restart. See [Tuning read performance](./tuning-read-performance.md).

## Lookup caches

Smaller shared caches that keep sequence bookkeeping off the hot path.

| Parameter                       | Default | Range | Context | Controls                                  |
| ------------------------------- | ------- | ----- | ------- | ----------------------------------------- |
| `pg_xpatch.group_cache_size_mb` | 16 MB   | ≥ 1   | restart | group to max-`_xp_seq` cache (INSERT)     |
| `pg_xpatch.tid_cache_size_mb`   | 16 MB   | ≥ 1   | restart | TID to seq cache (READ)                   |
| `pg_xpatch.seq_tid_cache_size_mb` | 16 MB | ≥ 1   | restart | seq to TID reverse cache (reconstruction) |

## Inserts and encoding

| Parameter                     | Default | Range   | Context | Controls                                                       |
| ----------------------------- | ------- | ------- | ------- | ------------------------------------------------------------- |
| `pg_xpatch.insert_cache_slots` | 16     | ≥ 1     | restart | how many (table, group) pairs can hold an active insert FIFO  |
| `pg_xpatch.max_delta_columns` | 32      | ≥ 1     | restart | maximum delta-compressed columns per table                   |
| `pg_xpatch.encode_threads`    | 0       | 0 to 64 | session | worker threads for parallel delta encoding (`0` = sequential) |

`encode_threads` parallelizes the candidate-base encodings that `compress_depth` produces, so it only helps when `compress_depth` is above 1 and the content is large enough to be worth the coordination. At the default `compress_depth` of 1 it does nothing.

## Cache warming

| Parameter                       | Default | Range | Context | Controls                                          |
| ------------------------------- | ------- | ----- | ------- | ------------------------------------------------- |
| `pg_xpatch.warm_cache_workers`  | 4       | ≥ 0   | session | default background workers for `warm_cache_parallel()` (`0` = sequential) |

The effective worker count is also capped by PostgreSQL's `max_worker_processes`.

## A starting point

A reasonable `postgresql.conf` block for a read-heavy deployment with room to grow:

```ini
shared_preload_libraries = 'pg_xpatch'

pg_xpatch.cache_size_mb     = 1024   # fit the hot working set
pg_xpatch.cache_max_entry_kb = 1024  # if you store larger values
pg_xpatch.warm_cache_workers = 8     # faster bulk warming
```

!!! cards { cols=2 }
    - [Tuning read performance](./tuning-read-performance.md){ icon=gauge }
      How to size these from `cache_stats()` readings.

    - [Caching & performance](./caching-and-performance.md){ icon=zap }
      What each cache actually does.
