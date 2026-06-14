---
title: "Quickstart: your first versioned table"
description: Build a tiny document store with pg-xpatch, from an empty table to seeing the deltas on disk, in six short steps.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/quickstart
  type: tutorial
  applies_to:
    - postgres 16
  language: en
  difficulty: beginner
  time_estimate: 10m
  status: stable
  aliases:
    - pg-xpatch tutorial
    - getting started
    - first xpatch table
  prev: ./installation.md
  next: ./storage-model.md
---

# Quickstart: your first versioned table

This walks you through a tiny document store, from an empty table to seeing the compressed deltas on disk. It assumes pg-xpatch is [installed](./installation.md). Every snippet is plain SQL you can paste into `psql`.

## 1. Create the table

```sql
CREATE TABLE documents (
    doc_id   INT,
    version  INT,
    content  TEXT NOT NULL
) USING xpatch;
```

The `USING xpatch` clause is the whole trick. Text, bytea, and json columns get delta-compressed; everything else is stored as usual. One rule to remember: **delta columns must be `NOT NULL`**, which is why `content` is.

!!! note "An extra column appears"
    xpatch adds an internal `_xp_seq` column for its own bookkeeping. It shows up in `SELECT *` because PostgreSQL has no hidden columns, so we list columns explicitly below to keep it out of the way.

## 2. Tell it how to group and order

```sql
SELECT xpatch.configure('documents', group_by => 'doc_id', order_by => 'version');
```

`group_by` makes each `doc_id` its own independent version chain. `order_by` is the order of versions inside a chain. `configure()` also builds a `(doc_id, _xp_seq)` index for fast lookups.

!!! tip "Why configure at all?"
    Auto-detection can guess `order_by` (the last integer column) and the delta columns, but it cannot guess that you meant to group by document. Set `group_by` whenever one table holds many independent histories. [Configuring a table](./configuration.md) covers the rest.

## 3. Insert some versions

Insert versions in ascending `version` order within each document:

```sql
INSERT INTO documents VALUES
  (1, 1, 'The quick brown fox.'),
  (1, 2, 'The quick brown fox jumps.'),
  (1, 3, 'The quick brown fox jumps over the lazy dog.');
```

Each row picks up the next `_xp_seq` for its group automatically: 1, then 2, then 3. Version 1 is stored in full; versions 2 and 3 keep only what changed.

!!! warning "Order matters within a group"
    Insert versions in ascending `order_by` order within a group. Out-of-order inserts are not rejected, but they compress poorly. And there is no `UPDATE`: to change a value, insert the next version.

## 4. Read it back

`SELECT` works normally. The deltas are reconstructed for you on the fly:

```sql
SELECT doc_id, version, content
FROM documents
WHERE doc_id = 1
ORDER BY version;
```

All three versions come back in full text. Listing the columns instead of `SELECT *` keeps the internal `_xp_seq` out of your result.

## 5. Check the compression

```sql
SELECT total_rows, keyframe_count, delta_count,
       raw_size_bytes, compressed_size_bytes, compression_ratio
FROM xpatch.stats('documents');
```

You will see one keyframe (version 1), two deltas, and `compressed_size_bytes` already below `raw_size_bytes`. That gap widens fast as you add more similar versions. `xpatch.stats()` reads from a stats table that pg-xpatch maintains on every write, so it stays instant even on huge tables.

## 6. Peek at what is on disk

This is the part that makes it click. `xpatch.inspect()` shows how each version is actually stored:

```sql
SELECT version, is_keyframe, tag, delta_size_bytes
FROM xpatch.inspect('documents', 1);   -- 1 is the doc_id
```

Version 1 is the keyframe. Versions 2 and 3 are deltas whose `tag = 1` means "rebuild me from the row one back," and their `delta_size_bytes` is a fraction of the keyframe's.

| version | is_keyframe | tag | stored as |
| ------- | ----------- | --- | ----------------------------- |
| 1       | `true`      | 0   | the full text (a keyframe)    |
| 2       | `false`     | 1   | the diff against version 1    |
| 3       | `false`     | 1   | the diff against version 2    |

## That is the whole loop

You created a table `USING xpatch`, configured grouping, inserted versions, read them back in full, and watched them get stored as deltas. Nothing about your SQL had to change.

!!! cards { cols=3 }
    - [How storage works](./storage-model.md){ icon=layers }
      Keyframes, deltas, and reconstruction in depth.

    - [Configuring a table](./configuration.md){ icon=sliders-horizontal }
      Every `configure()` option, and when to reach for it.

    - [Tuning read performance](./tuning-read-performance.md){ icon=gauge }
      Warm the cache before heavy reads.
