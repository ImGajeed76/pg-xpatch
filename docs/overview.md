---
title: What pg-xpatch is
description: A PostgreSQL table access method that stores versioned rows as deltas, so a hundred near-identical versions cost about as much as one.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/overview
  type: explanation
  applies_to:
    - postgres 16
  language: en
  difficulty: beginner
  time_estimate: 5m
  status: stable
  aliases:
    - what is pg-xpatch
    - delta compression postgres
    - versioned table storage
  references:
    - https://www.postgresql.org/docs/16/tableam.html
    - https://github.com/ImGajeed76/xpatch
  next: ./installation.md
---

# What pg-xpatch is

pg-xpatch is a PostgreSQL [table access method](https://www.postgresql.org/docs/16/tableam.html) for versioned data. You store every version of a row with a plain `INSERT`; under the hood it keeps only the differences between consecutive versions. You `SELECT` normally and get the full content back, reconstructed on the fly.

That's the whole pitch. A table of document revisions, config snapshots, or audit records that would cost megabytes as ordinary rows costs a fraction of that, and your SQL doesn't change.

## The idea

Most versioned data barely changes from one version to the next. Edit one line of a document, save again, and almost every byte is identical to the previous save. A normal table stores the whole thing every time. pg-xpatch stores the first version in full (a *keyframe*), and each later version as a *delta* against the one before it.

```sql
CREATE TABLE documents (
    doc_id   INT,
    version  INT,
    content  TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('documents', group_by => 'doc_id', order_by => 'version');

INSERT INTO documents VALUES (1, 1, 'Hello world');
INSERT INTO documents VALUES (1, 2, 'Hello world, now with more words');

SELECT content FROM documents WHERE doc_id = 1 ORDER BY version;
```

Both rows come back in full. On disk, version 2 is only the handful of bytes that changed.

Two columns shape the storage. `group_by` splits the table into independent version chains, one per document here. `order_by` is the version order within a chain. Each group keyframes and deltas on its own, so a write to one document never touches another.

!!! note "An internal column tags along"
    xpatch adds an internal `_xp_seq` column to every table. It shows up in `SELECT *` (PostgreSQL has no truly hidden columns), so list your columns explicitly when you don't want to see it.

## It's append-only

One rule shapes everything else: **you don't `UPDATE` an xpatch row, you `INSERT` the next version.** A delta chain only holds together if history is immutable, so `UPDATE` raises an error and tells you as much. The same goes for rewriting commands like `CLUSTER`. Need to change a value? Add a row with the next version number.

## When it pays off

xpatch wins when versions resemble each other:

- document or note revisions
- source files across commits
- configuration snapshots over time
- audit records where each entry repeats most of the last

It does not win when versions have nothing in common. If every "version" is unrelated content, the delta is as large as the content plus a little bookkeeping, and a normal table would serve you better. Compression is only as good as the similarity between your versions, so measure with your own data rather than trusting a headline number. [Tuning compression](./tuning-compression.md) covers how to do that.

## What still works normally

It's a real table access method, so the rest of PostgreSQL behaves the way you expect:

- transparent reconstruction on `SELECT` (deltas are decoded for you)
- B-tree indexes on any column, including delta-compressed ones
- full MVCC, from `READ COMMITTED` through `SERIALIZABLE`
- parallel sequential scans
- `DELETE`, `VACUUM`, WAL crash recovery, and `pg_dump` / `pg_restore`

!!! info "Reads are fast only with the shared cache"
    The shared-memory cache that keeps reconstruction quick is active only when pg-xpatch is loaded via `shared_preload_libraries`. Without it, full scans get dramatically slower. [Caching & performance](./caching-and-performance.md) explains why, and [Installation](./installation.md) shows how to turn it on.

## Where to go next

!!! cards { cols=2 }
    - [Install pg-xpatch](./installation.md){ icon=download }
      Docker, a prebuilt binary, or a build from source.

    - [Quickstart](./quickstart.md){ icon=rocket }
      Your first versioned table, start to finish.

    - [How storage works](./storage-model.md){ icon=layers }
      Keyframes, deltas, and how a row gets rebuilt.

    - [Tuning compression](./tuning-compression.md){ icon=minimize-2 }
      Trade write speed and storage against each other.
