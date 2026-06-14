---
title: SQL functions
description: Complete reference for the xpatch.* schema, signatures, arguments, and returned columns for every public function.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/sql-functions
  type: reference
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 10m
  status: stable
  aliases:
    - sql reference
    - xpatch functions
    - function reference
    - api reference
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/sql/pg_xpatch--0.7.0.sql
  prev: ./backup-and-restore.md
  next: ./server-parameters.md
---

# SQL functions

Every public function lives in the `xpatch` schema and is defined in [`pg_xpatch--0.7.0.sql`](../sql/pg_xpatch--0.7.0.sql). Internal helpers (names starting with `_`) are omitted. All sizes are in bytes unless noted.

## Configuration

### xpatch.configure

```sql
xpatch.configure(
    table_name     REGCLASS,
    group_by       TEXT     DEFAULT NULL,
    order_by       TEXT     DEFAULT NULL,
    delta_columns  TEXT[]   DEFAULT NULL,
    keyframe_every INT      DEFAULT 100,
    compress_depth INT      DEFAULT 1,
    enable_zstd    BOOLEAN  DEFAULT true
) RETURNS void
```

Sets explicit configuration for a table and, when `group_by` is given, builds the composite `(group_by, _xp_seq)` index. `NULL` for `group_by` / `order_by` / `delta_columns` means auto-detect. Requires `INSERT` privilege on the table. `keyframe_every` is 1 to 10000; `compress_depth` is 1 to 65535. See [Configuring a table](./configuration.md).

### xpatch.get_config

```sql
xpatch.get_config(tbl REGCLASS) RETURNS TABLE (...)
```

Returns the stored config, or no rows if the table uses auto-detection.

| Column           | Type      |
| ---------------- | --------- |
| `group_by`       | text      |
| `order_by`       | text      |
| `delta_columns`  | text[]    |
| `keyframe_every` | int       |
| `compress_depth` | int       |
| `enable_zstd`    | boolean   |

## Inspection and statistics

### xpatch.describe

```sql
xpatch.describe(table_name REGCLASS) RETURNS TABLE (property text, value text)
```

A property/value dump of one table: access method, whether config is explicit or auto-detected, each column's role, and storage stats. The best first command on an unfamiliar table.

### xpatch.stats

```sql
xpatch.stats(tbl REGCLASS) RETURNS TABLE (...)
```

Compression statistics, served instantly from the maintained stats table.

| Column                  | Type          | Meaning                                  |
| ----------------------- | ------------- | ---------------------------------------- |
| `total_rows`            | bigint        | rows across all groups                   |
| `total_groups`          | bigint        | distinct groups                          |
| `keyframe_count`        | bigint        | rows stored as full keyframes            |
| `delta_count`           | bigint        | rows stored as deltas                    |
| `raw_size_bytes`        | bigint        | uncompressed size                        |
| `compressed_size_bytes` | bigint        | on-disk delta size                       |
| `compression_ratio`     | numeric(10,2) | `raw_size_bytes / compressed_size_bytes` |
| `cache_hits`            | bigint        | content cache hits                       |
| `cache_misses`          | bigint        | content cache misses                     |
| `avg_compression_depth` | numeric(10,2) | mean delta tag (rows back) per row       |

### xpatch.inspect

```sql
xpatch.inspect(tbl REGCLASS, group_value ANYELEMENT) RETURNS TABLE (...)
```

One row per version per delta column for a single group. `seq` is 1-based and matches `_xp_seq`.

| Column             | Type    | Meaning                                    |
| ------------------ | ------- | ------------------------------------------ |
| `version`          | bigint  | the `order_by` value                       |
| `seq`              | bigint  | sequence number within the group           |
| `is_keyframe`      | bool    | true for a full snapshot                   |
| `tag`              | int     | rows back to the base (0 for a keyframe)   |
| `delta_size_bytes` | int     | compressed delta size for this column      |
| `column_name`      | text    | which delta column the row describes       |

### xpatch.physical

```sql
xpatch.physical(tbl REGCLASS) RETURNS TABLE (...)
xpatch.physical(tbl REGCLASS, from_seq INT) RETURNS TABLE (...)
xpatch.physical(tbl REGCLASS, group_filter ANYELEMENT, from_seq BIGINT DEFAULT NULL) RETURNS TABLE (...)
```

Raw stored delta bytes and metadata. The one-arg form covers all groups; pass a `group_filter` for one group, and `from_seq` to return only rows with a higher sequence.

| Column         | Type    | Meaning                          |
| -------------- | ------- | -------------------------------- |
| `group_value`  | text    | group identifier, cast to text   |
| `version`      | bigint  | the `order_by` value             |
| `seq`          | bigint  | 1-based sequence within group    |
| `is_keyframe`  | boolean | true for a full snapshot         |
| `tag`          | int     | rows back to the base            |
| `delta_column` | text    | which column the delta is for    |
| `delta_bytes`  | bytea   | the raw compressed delta         |
| `delta_size`   | int     | size of `delta_bytes`            |

### xpatch.stats_exist

```sql
xpatch.stats_exist(table_name REGCLASS) RETURNS boolean
```

True if cached statistics exist for the table.

## Cache

### xpatch.cache_stats

```sql
xpatch.cache_stats() RETURNS TABLE (...)
```

Global content-cache counters. `cache_max_bytes` is `0` when the cache is not loaded (pg-xpatch is not in `shared_preload_libraries`).

| Column             | Type   |
| ------------------ | ------ |
| `cache_size_bytes` | bigint |
| `cache_max_bytes`  | bigint |
| `entries_count`    | bigint |
| `hit_count`        | bigint |
| `miss_count`       | bigint |
| `eviction_count`   | bigint |
| `skip_count`       | bigint |

### xpatch.insert_cache_stats

```sql
xpatch.insert_cache_stats() RETURNS TABLE (...)
```

Counters for the per-group FIFO insert cache.

| Column            | Type   | Meaning                                 |
| ----------------- | ------ | --------------------------------------- |
| `slots_in_use`    | bigint | active (table, group) slots             |
| `total_slots`     | bigint | configured slots                        |
| `hits`            | bigint | base reads served from the FIFO         |
| `misses`          | bigint | base reads that required reconstruction |
| `evictions`       | bigint | slots evicted                           |
| `eviction_misses` | bigint | race-detection counter                  |

### xpatch.warm_cache

```sql
xpatch.warm_cache(
    table_name REGCLASS,
    max_rows   INT DEFAULT NULL,
    max_groups INT DEFAULT NULL
) RETURNS TABLE (rows_scanned bigint, groups_warmed bigint, duration_ms float8)
```

Sequential PL/pgSQL cache warming by scanning the table. `max_groups` may process one extra group at the boundary.

### xpatch.warm_cache_parallel

```sql
xpatch.warm_cache_parallel(
    table_name  REGCLASS,
    max_workers INT DEFAULT NULL,
    max_groups  INT DEFAULT NULL
) RETURNS TABLE (rows_warmed bigint, groups_warmed bigint, sections_warmed bigint, workers_used int, duration_ms float8)
```

Parallel cache warming via background workers. Defaults `max_workers` to `pg_xpatch.warm_cache_workers`. Falls back to sequential C warming when no worker slots are free. Preferred for large tables. See [Tuning read performance](./tuning-read-performance.md).

## Maintenance and migration

### xpatch.refresh_stats

```sql
xpatch.refresh_stats(table_name REGCLASS)
    RETURNS TABLE (groups_scanned bigint, rows_scanned bigint, duration_ms float8)
```

Force a full recompute of a table's statistics. Rarely needed; stats are maintained automatically on `INSERT` and `DELETE`.

### xpatch.dump_configs

```sql
xpatch.dump_configs() RETURNS SETOF text
```

Emits a `SELECT xpatch.configure(...)` statement for every configured table. Useful for migrations. See [Backup & restore](./backup-and-restore.md).

### xpatch.fix_restored_configs

```sql
xpatch.fix_restored_configs() RETURNS int
```

Remaps config entries to restored tables by schema and table name (OIDs change on restore) and drops orphaned entries. Returns the number of configs fixed. Run it once after every `pg_restore`.

### xpatch.version

```sql
xpatch.version() RETURNS text
```

The bundled xpatch library version. A quick check that the `.so` is loading.

!!! cards { cols=2 }
    - [Server parameters](./server-parameters.md){ icon=settings }
      The GUC side of configuration.

    - [Monitoring](./monitoring.md){ icon=activity }
      How to use these functions day to day.
