---
title: Data types & limitations
description: Which column types work where, and the things pg-xpatch does not do, by design and by current implementation.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/types-and-limitations
  type: reference
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 7m
  status: stable
  aliases:
    - supported types
    - limitations
    - no update
    - constraints
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/tests/test_types.py
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/xpatch_config.c
  prev: ./server-parameters.md
  next: ./example-notes-app.md
---

# Data types & limitations

## Column types

### group_by

| Supported            | Notes                                            |
| -------------------- | ------------------------------------------------ |
| `int`, `bigint`      | tested, including values near the type maximum   |
| `text`, `varchar`    | tested                                           |
| `uuid`               | tested                                           |

Other hashable scalar types generally work through a generic path, but the list above is what is officially covered. `group_by` is optional; without it the whole table is one chain.

### order_by

| Supported                          |
| ---------------------------------- |
| `smallint`, `int`, `bigint`        |
| `timestamp`, `timestamptz`         |

Auto-detection picks the last column of one of these types (see [`xpatch_config.c`](../src/xpatch_config.c)). Every xpatch table needs exactly one `order_by`.

### Delta columns

| Supported                  |
| -------------------------- |
| `text`, `varchar`          |
| `bytea`                    |
| `json`, `jsonb`            |

!!! warning "Delta columns must be NOT NULL"
    A nullable column cannot be delta-encoded; `configure()` rejects it. Add the constraint first.

### Everything else

Columns of any other type (`numeric`, `boolean`, arrays, `date`, and so on) are stored normally, uncompressed, alongside the deltas. They are never delta-encoded, and they work as plain columns.

## Not supported, by design

xpatch is append-only versioned storage, and a few operations are blocked to keep that model honest. Each raises a clear error.

| Operation                | Do this instead                                            |
| ------------------------ | ---------------------------------------------------------- |
| `UPDATE`                 | insert a new version with the next `order_by` value        |
| `NULL` group values      | every row needs a non-null `group_by` value                |
| `CLUSTER`, `VACUUM FULL` | ordinary `VACUUM` and `ANALYZE` work; the rewrite path does not |
| `TABLESAMPLE`            | use a normal `WHERE` filter                                 |

!!! warning "Insert in order (not enforced)"
    Versions should be inserted in ascending `order_by` order within a group. Out-of-order inserts are *not* rejected and reconstruction stays correct, but they compress poorly, because each row is delta-encoded against the previously inserted row rather than the previous version.

## Current limitations

These are implementation realities rather than design choices:

- **`_xp_seq` is visible.** PostgreSQL has no hidden columns, so the internal sequence column appears in `SELECT *`. List columns explicitly to omit it.
- **Cold reads are slow.** The first read of uncached data pays full reconstruction; a cold full-table scan rebuilds every row. Keep the [cache](./caching-and-performance.md) warm.
- **Writes carry encoding overhead.** An insert does compression work, so it is heavier than a plain heap insert. [Batch your inserts](./tuning-compression.md#batch-your-inserts).

!!! info "Known issue: non-atomic cascade DELETE"
    A cascading `DELETE` removes a version and everything after it in the chain, using a separate critical section per tuple. A crash partway through could, in a very narrow window, leave a broken delta chain. It does not affect typical versioned-storage workloads, and a single-record fix is planned.

## PostgreSQL version

pg-xpatch is built and tested against **PostgreSQL 16**. Other major versions are not supported.

!!! cards { cols=2 }
    - [Configuring a table](./configuration.md){ icon=sliders-horizontal }
      Put these types to work with `configure()`.

    - [Overview](./overview.md){ icon=info }
      The append-only model these limits come from.
