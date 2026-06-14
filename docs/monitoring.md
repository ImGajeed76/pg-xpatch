---
title: Monitoring & introspection
description: The functions that answer is compression working, is the cache healthy, and how is this group actually stored.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/monitoring
  type: how-to
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 8m
  status: stable
  aliases:
    - xpatch.stats
    - xpatch.describe
    - xpatch.inspect
    - cache_stats
    - monitoring
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/sql/pg_xpatch--0.7.0.sql
  prev: ./tuning-read-performance.md
  next: ./backup-and-restore.md
---

# Monitoring & introspection

A handful of functions tell you everything about a running xpatch table: how well it compresses, how the cache is doing, and how any single group is laid out on disk. Full signatures live in the [SQL reference](./sql-functions.md), all defined in [`pg_xpatch--0.7.0.sql`](../sql/pg_xpatch--0.7.0.sql); this page is about which one to reach for.

## Is compression working?

```sql
SELECT total_rows, keyframe_count, delta_count,
       compression_ratio, avg_compression_depth,
       raw_size_bytes, compressed_size_bytes
FROM xpatch.stats('documents');
```

`compression_ratio` is the headline (`raw / compressed`). `avg_compression_depth` shows how far back deltas reach on average, which tells you whether a high [`compress_depth`](./tuning-compression.md) is earning its keep. These read from a stats table that pg-xpatch maintains on every write, so the call stays instant even on large tables.

!!! note "If the numbers ever look stale"
    Stats are kept current automatically on `INSERT` and `DELETE`. After unusual bulk operations you can force a full recompute with `SELECT * FROM xpatch.refresh_stats('documents')`. You should rarely need it.

## One-look table overview

```sql
SELECT * FROM xpatch.describe('documents');
```

`describe()` is the best first command on any xpatch table. It returns a property/value list covering the access method, whether the config is explicit or auto-detected, each column's role (`group_by`, `order_by`, `delta`, `internal`), and the storage stats in one place.

## How is a group stored?

```sql
SELECT version, seq, is_keyframe, tag, delta_size_bytes, column_name
FROM xpatch.inspect('documents', 1);   -- 1 is the group value
```

`inspect()` shows one row per version per delta column: whether it is a keyframe, its `tag` (rows back to its base), and the compressed `delta_size_bytes`. This is how you see, concretely, where the space is going inside one group.

## Is the cache healthy?

```sql
SELECT * FROM xpatch.cache_stats();
```

The content cache counters: `hit_count`, `miss_count`, `eviction_count`, `skip_count`, plus current and maximum size. Rising evictions mean the cache is undersized; rising skips mean entries exceed `cache_max_entry_kb`. [Tuning read performance](./tuning-read-performance.md) maps each symptom to a fix.

There is a matching `xpatch.insert_cache_stats()` for the write path (`slots_in_use`, `total_slots`, `hits`, `misses`, `evictions`, `eviction_misses`), useful if writes feel slower than expected on tables with many concurrently-written groups.

## The raw bytes (advanced)

```sql
SELECT version, seq, is_keyframe, tag, delta_column, delta_size
FROM xpatch.physical('documents');
```

`physical()` exposes the actual stored delta bytes and their metadata, across all groups or a single one. Reach for it when debugging encoding behavior or verifying what landed on disk; for everyday checks, `stats()` and `inspect()` are friendlier.

!!! cards { cols=2 }
    - [SQL functions](./sql-functions.md){ icon=function-square }
      Exact signatures and every returned column.

    - [Tuning read performance](./tuning-read-performance.md){ icon=gauge }
      Turn these readings into cache settings.
