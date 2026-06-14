---
title: Configuring a table
description: When auto-detection is enough, when to call xpatch.configure(), and what every option does, plus the validation rules that bite.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/configuration
  type: how-to
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 9m
  status: stable
  aliases:
    - xpatch.configure
    - group_by order_by
    - delta columns
    - auto-detection
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/src/xpatch_config.c
  prev: ./benchmarks.md
  next: ./tuning-compression.md
---

# Configuring a table

Most tables need no configuration. pg-xpatch auto-detects what to compress and how to order it the first time you use the table. You call `xpatch.configure()` when you need grouping, or when a guess is wrong.

## Do you even need it?

Without an explicit config, pg-xpatch derives one from the table's columns (the rules live in [`xpatch_config.c`](../src/xpatch_config.c)):

| Setting          | Auto-detected as                                                       |
| ---------------- | ---------------------------------------------------------------------- |
| `group_by`       | none, so the whole table is a single version chain                     |
| `order_by`       | the last `smallint` / `int` / `bigint` / `timestamp` / `timestamptz` column |
| `delta_columns`  | every `text`, `varchar`, `bytea`, `json`, and `jsonb` column           |
| keyframe / depth / zstd | `100` / `1` / on                                                |

If that matches what you want, you are done. You almost always need `configure()` for one reason: **auto-detection never sets `group_by`**, so a table holding many independent histories (documents, users, files) would otherwise collapse into one chain.

## Calling configure()

Pass the table and only the options you want to override. Everything else keeps its default.

```sql
SELECT xpatch.configure('documents',
    group_by       => 'doc_id',           -- (1)!
    order_by       => 'version',          -- (2)!
    delta_columns  => ARRAY['content'],   -- (3)!
    keyframe_every => 100,                -- (4)!
    compress_depth => 1,                  -- (5)!
    enable_zstd    => true                -- (6)!
);
```

1. The column that splits the table into independent chains. `NULL` (the default) means one chain for the whole table.
2. The version order within each chain. Must be an integer or timestamp type. `NULL` auto-detects the last such column.
3. The columns to delta-compress, as a `text[]`. `NULL` auto-detects all text/varchar/bytea/json/jsonb columns. Every column listed here must be `NOT NULL`.
4. Write a full keyframe every N rows. Range 1 to 10000, default 100. See [Tuning compression](./tuning-compression.md).
5. How many previous rows to try as a delta base, keeping the smallest. Range 1 to 65535, default 1. Higher means better compression, slower writes.
6. Apply Zstandard on top of the delta encoding. Default on.

Setting `group_by` also swaps the table's basic `_xp_seq` index for a composite `(group_by, _xp_seq)` index, which is what keeps per-group lookups fast.

## The rules that bite

!!! warning "Delta columns must be NOT NULL"
    A nullable column cannot be delta-encoded. `configure()` rejects it, so add the constraint first:

    ```sql
    ALTER TABLE documents ALTER COLUMN content SET NOT NULL;
    ```

A few more constraints, all enforced at `configure()` time:

- `order_by` must be `smallint`, `int`, `bigint`, `timestamp`, or `timestamptz`. Anything else is an error.
- The table needs at least one delta column and one usable `order_by` column, whether explicit or detected.
- You need `INSERT` privilege on the table to configure it.

!!! danger "Pin the config before you evolve the schema"
    An auto-detected table re-derives its config from the catalog. If you later `ALTER TABLE ... ADD COLUMN` a text column, auto-detection would pick it up as a new delta column, which does not match the data already on disk. Call `xpatch.configure()` once to write an explicit config to the catalog, and the config stops moving under you.

## Check what is in effect

```sql
SELECT * FROM xpatch.get_config('documents');   -- the stored config, or empty if auto-detected
SELECT * FROM xpatch.describe('documents');      -- config (explicit or detected) plus storage stats
```

!!! cards { cols=2 }
    - [Tuning compression](./tuning-compression.md){ icon=minimize-2 }
      What `keyframe_every`, `compress_depth`, and `enable_zstd` actually trade.

    - [Data types & limitations](./types-and-limitations.md){ icon=table-2 }
      Which column types are allowed where.
