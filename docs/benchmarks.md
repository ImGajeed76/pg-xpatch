---
title: Benchmarks
description: What pg-xpatch's compression and speed look like in practice, drawn from the xpatch delta library and the pgit project built on it.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/benchmarks
  type: explanation
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 6m
  status: stable
  aliases:
    - benchmarks
    - performance numbers
    - compression ratio benchmark
  references:
    - https://github.com/ImGajeed76/xpatch/blob/master/PERFORMANCE.md
    - https://github.com/ImGajeed76/pgit/blob/master/BENCHMARK.md
  prev: ./caching-and-performance.md
  next: ./configuration.md
---

# Benchmarks

Numbers depend on your data and your hardware, so treat these as direction, not promises. The honest way to size pg-xpatch is to load representative data and read [`xpatch.stats()`](./monitoring.md). With that said, two published benchmarks show the shape of what to expect.

!!! warning "Whose numbers these are"
    pg-xpatch is built on the [xpatch](https://github.com/ImGajeed76/xpatch) delta library, and [pgit](https://github.com/ImGajeed76/pgit) is a Git-in-Postgres tool that stores everything through pg-xpatch. The figures below come from those projects' own benchmark suites, on their hardware. pg-xpatch adds PostgreSQL's storage overhead on top of the raw library.

## Real-world storage: pgit

pgit imports real Git repositories into PostgreSQL and stores their history through pg-xpatch, which makes it a good proxy for "how well does this compress actual versioned data." Its benchmark covers 20 open-source repos totaling 7.3 GB of raw history, measured against Git's own aggressive pack compression.

| Repository | pgit  | git aggressive pack |
| ---------- | ----- | ------------------- |
| serde      | 51.6  | 36.5                |
| fzf        | 71.1  | 61.3                |
| git        | 66.8  | 82.0                |

{ .chart type=bar title="Compression ratio vs raw history (higher is better)" }

On most repositories pgit matches or beats Git's packfiles, which are a high bar for delta compression. On the Git repository itself it lands behind, the price of living in a general-purpose database rather than a bespoke format. Across all 20 repos, PostgreSQL overhead averaged about 22% (ranging from 10% to 40%).

## Raw delta performance: the xpatch library

Underneath, the [xpatch](https://github.com/ImGajeed76/xpatch) library computes and applies the deltas. Its synthetic benchmark pits it against other delta algorithms, and on compression it sits near the top:

| Algorithm   | Saved % |
| ----------- | ------- |
| qbsdiff     | 84.4    |
| xpatch      | 74.6    |
| gdelta_zstd | 74.6    |
| gdelta_lz4  | 70.6    |
| gdelta      | 68.4    |
| vcdiff      | 64.2    |
| zstd_dict   | 55.0    |

{ .chart type=bar horizontal legend=false title="Synthetic compression, % saved (higher is better)" }

Decode is the operation pg-xpatch runs on every cache miss, so its speed is exactly what makes warm reads cheap. xpatch decodes at roughly 2.3 GB/s, in good company:

| Algorithm   | Decode MB/s |
| ----------- | ----------- |
| gdelta      | 4400        |
| gdelta_lz4  | 3900        |
| vcdiff      | 2600        |
| xpatch      | 2300        |
| gdelta_zstd | 2200        |
| qbsdiff     | 111         |
| zstd_dict   | 16          |

{ .chart type=bar horizontal legend=false title="Synthetic decode throughput, MB/s (higher is better)" }

!!! warning "These throughput numbers are best-case"
    The synthetic benchmark ran with the whole dataset resident in CPU cache (L3), so the encode and decode figures reflect the algorithm alone, with no memory or disk pressure. In practice, decode speed is governed mostly by how fast the deltas and base versions can be fetched from memory and disk, not by the algorithm. Treat these as an upper bound, not what a cold read will see.

Encoding is more middling (around 306 MB/s, behind the gdelta family), but it runs once per version at write time, where it is rarely the bottleneck. On real Git histories the compression climbs well past the synthetic figures: 97.5% saved on mdn/content and 97.9% on tokio, because consecutive versions barely differ.

## What this means for pg-xpatch

- **Compression tracks similarity.** Versioned text and code, the things pgit stores, hit the high end. Unrelated data does not. See [when pg-xpatch pays off](./overview.md#when-it-pays-off).
- **Decode is cheap, so warm reads are cheap.** The expensive step is the first reconstruction; after that the [cache](./caching-and-performance.md) serves it.
- **There is a database tax.** A purpose-built format can still win on raw size. You trade some of that for living inside PostgreSQL, with SQL, indexes, and MVCC.

## Measure your own

```sql
SELECT total_rows, compression_ratio, raw_size_bytes, compressed_size_bytes
FROM xpatch.stats('your_table');
```

The repo also ships a benchmark script under [`benchmark/`](../benchmark) for an end-to-end storage and query comparison against a plain heap table.

!!! cards { cols=2 }
    - [Tuning compression](./tuning-compression.md){ icon=minimize-2 }
      Push the ratio further on your own data.

    - [Caching & performance](./caching-and-performance.md){ icon=zap }
      Why decode speed makes warm reads fast.
