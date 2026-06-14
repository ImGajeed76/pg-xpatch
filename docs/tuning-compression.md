---
title: Tuning compression & storage
description: How keyframe_every, compress_depth, and enable_zstd trade storage against write speed, and why batching inserts is the cheapest win.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/tuning-compression
  type: how-to
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 9m
  status: stable
  aliases:
    - keyframe_every
    - compress_depth
    - enable_zstd
    - batch inserts
    - compression ratio
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/xpatch_storage.c
  prev: ./configuration.md
  next: ./tuning-read-performance.md
---

# Tuning compression & storage

Three [`configure()`](./configuration.md) options decide how hard pg-xpatch compresses and how much that costs on write. The defaults are sensible; reach for these when you have measured a reason to.

!!! warning "Tuning is forward-looking"
    These settings are read when a row is inserted. Changing them does not rewrite data already on disk, it only affects new inserts. To re-compress an existing table under new settings, copy it into a fresh table.

## keyframe_every

How often a full snapshot is written instead of a delta. Default 100, range 1 to 10000. It sets the longest a delta chain can get, which is the longest a single reconstruction has to walk.

| Lower (e.g. 25)                                  | Higher (e.g. 500)                                  |
| ------------------------------------------------ | -------------------------------------------------- |
| more keyframes, so more storage                  | fewer keyframes, so less storage                   |
| shorter chains, faster cold reconstruction       | longer chains, slower cold reconstruction          |

Raise it for long histories of small edits where space is the priority and a warm cache hides the read cost. Lower it if you frequently read uncached old versions and want a tighter bound on reconstruction work.

## compress_depth

How many previous rows the encoder tries as a delta base, keeping whichever delta comes out smallest. Default 1, range 1 to 65535. The selection loop lives in [`xpatch_storage.c`](../src/xpatch_storage.c).

At the default, every row is encoded against the one immediately before it, which is exactly right for a linear edit history. Raise it when a version is often more similar to something other than its immediate predecessor, for example data that alternates between two shapes, so the encoder can reach back to a closer match.

!!! warning "Depth costs write time and memory"
    Encoding tries every candidate base, so write cost grows with `compress_depth`. The per-group insert buffer also holds `compress_depth` recent versions in memory. Check whether it is paying off: `avg_compression_depth` in `xpatch.stats()` is how far back deltas actually reach. If it stays near 1, a higher depth is just burning CPU.

## enable_zstd

Apply Zstandard on top of the delta encoding. Default on. Leave it on unless write/read CPU matters more than disk, or the content is already compressed (images, archives, encrypted blobs), where a second pass buys nothing and still costs time.

## Batch your inserts

The cheapest win has nothing to do with the three knobs: insert in batches.

```sql
-- one statement, many rows
INSERT INTO documents VALUES
  (1, 1, '...'),
  (1, 2, '...'),
  (1, 3, '...');

-- or straight from another table
INSERT INTO documents SELECT ... ;
```

A batch amortizes the per-row planning and transaction overhead, which is where the speedup comes from. Two more things to keep in mind:

- **Keep a group's rows in order.** Inserting in ascending `order_by` order per group keeps deltas small. Out-of-order inserts still work but compress poorly.
- **Different groups write in parallel.** Inserts serialize only within a single group (a per-group lock), so many writers spread across many groups scale out.

## Measure, do not guess

```sql
SELECT compression_ratio, avg_compression_depth,
       raw_size_bytes, compressed_size_bytes
FROM xpatch.stats('documents');
```

`compression_ratio` is `raw_size_bytes / compressed_size_bytes`. Watch it as you change a setting on representative data. For a per-version view of keyframes and delta sizes, use [`xpatch.inspect()`](./monitoring.md).

!!! cards { cols=2 }
    - [Tuning read performance](./tuning-read-performance.md){ icon=gauge }
      The other half: cache sizing and warming.

    - [Monitoring](./monitoring.md){ icon=activity }
      Read the stats that tell you if tuning worked.
