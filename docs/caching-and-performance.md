---
title: Caching & performance
description: Why pg-xpatch keeps a shared-memory cache, what lives in it, cold versus warm reads, and the storage/read/write trade-off.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/caching-and-performance
  type: explanation
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 8m
  status: stable
  aliases:
    - performance model
    - shared cache
    - cold vs warm reads
    - cache layers
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/xpatch_cache.c
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/xpatch_seq_cache.c
  prev: ./storage-model.md
  next: ./benchmarks.md
---

# Caching & performance

Rebuilding a row from a keyframe and a stack of deltas costs CPU. Do it once and cache the result, and that cost disappears for every later read. The cache is what makes pg-xpatch fast, and it is the single biggest performance lever you control.

!!! danger "No preload, no cache"
    Every cache on this page lives in shared memory allocated at server start, so it exists only when pg-xpatch is loaded through `shared_preload_libraries`. Without it, reads rebuild from scratch every single time, which is dramatically slower. [Installation](./installation.md#enable-the-shared-cache) shows how to turn it on.

## The content cache

The main cache, implemented in [`xpatch_cache.c`](../src/xpatch_cache.c), holds reconstructed column content, keyed by table, group, version, and column. Read version 50 of a document once, and the rebuilt text sits in shared memory for the next reader, whichever backend they land on.

A few properties worth knowing:

- **Shared and striped.** The cache is split into independent partitions, each with its own lock, so many backends use it at once instead of queuing behind one lock.
- **LRU.** When it fills, the least recently used entries are evicted to make room.
- **Capped per entry.** A single entry larger than `cache_max_entry_kb` is skipped rather than cached, since it would evict too much else. The first skip logs a warning that names the limit.

Because reconstruction is recursive, the cache compounds: when many versions share an ancestor, that ancestor is decoded once and every descendant reuses it.

## The lookup caches

Three smaller shared caches keep the bookkeeping off the hot path. Each is filled lazily on first use, then answers from memory instead of scanning the table.

| Cache         | Maps                              | Speeds up                                |
| ------------- | --------------------------------- | ---------------------------------------- |
| Group max-seq | a group to its highest `_xp_seq`  | allocating the next version on `INSERT`   |
| Seq to TID    | a version to its physical row     | finding a base row during reconstruction |
| TID to seq    | a physical row to its version     | reads, so a scanned tuple knows its place |

## The insert cache

Encoding a new delta needs the content of one or more previous versions. Reconstructing them on every insert would be slow, so pg-xpatch keeps a small per-group FIFO of recent, already-decoded versions, sized to `compress_depth`. A warm group encodes straight from that buffer; a cold group reconstructs once to fill it, then runs warm.

## Cold versus warm

The first read of uncached data pays the full reconstruction cost. Every read after it is a cache hit. That is why a freshly restarted server, or a table that has aged out of the cache, feels slow on the first large query and quick from then on.

You do not have to wait for traffic to warm it, though. `xpatch.warm_cache()` and the parallel `xpatch.warm_cache_parallel()` pre-build content in bulk.

!!! tip "Watch it happen"
    `SELECT * FROM xpatch.cache_stats()` shows `hit_count`, `miss_count`, `eviction_count`, and `skip_count`. Rising evictions mean the cache is too small for the working set; rising skips mean entries are bigger than `cache_max_entry_kb`. [Monitoring](./monitoring.md) goes deeper.

## The trade-off, honestly

pg-xpatch optimizes for storage first, and that has consequences in the other two corners:

!!! cards { cols=3 }
    - **Storage**{ type=check }
      The win. Similar versions compress hard.

    - **Reads**{ type=tip }
      Fast on a warm cache and on indexed point lookups. Slow on a cold full scan, which rebuilds every row.

    - **Writes**{ type=warning }
      An insert does encoding work, so it is heavier than a plain heap insert. Batching amortizes most of it.

If your workload is write-once, read-often versioned data, this shape fits. If it is constant cold full scans of unrelated data, it does not. [When pg-xpatch pays off](./overview.md#when-it-pays-off) is the short version.

!!! cards { cols=2 }
    - [Tuning read performance](./tuning-read-performance.md){ icon=gauge }
      Size the caches and warm them deliberately.

    - [Server parameters](./server-parameters.md){ icon=settings }
      Every cache-sizing GUC, with defaults and ranges.
