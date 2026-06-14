---
title: Backup & restore
description: pg_dump and pg_restore work with xpatch tables, with two things to remember, install the extension first and run fix_restored_configs() after.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/backup-and-restore
  type: how-to
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 7m
  status: stable
  aliases:
    - pg_dump
    - pg_restore
    - fix_restored_configs
    - dump_configs
    - migration
  references:
    - https://github.com/ImGajeed76/pg-xpatch/blob/master/sql/pg_xpatch--0.7.0.sql
  prev: ./monitoring.md
  next: ./sql-functions.md
---

# Backup & restore

xpatch tables go through `pg_dump` and `pg_restore` like any other table. The dump carries the row data, the internal `_xp_seq` values, and each table's configuration. There are exactly two things to get right: the target server needs the extension, and you run one fixup after the restore.

## What ends up in the dump

- **Row data and `_xp_seq`.** The internal `_xp_seq` values are dumped and restored too, so every version keeps its original sequence number and the restored table rebuilds the same version chains.
- **Per-table config.** The `xpatch.table_config` catalog is [registered to be dumped](../sql/pg_xpatch--0.7.0.sql) with the extension, so your `group_by`, `order_by`, and compression settings travel with the data.

No special flags. A normal dump captures everything:

```bash
pg_dump -Fc mydb > mydb.dump
```

## Restoring

!!! steps "Restore procedure"
    1. Install pg-xpatch on the target server first, so the access method exists when the dump recreates the tables:

       ```bash
       # on the target, then in the target database
       psql -d newdb -c "CREATE EXTENSION IF NOT EXISTS pg_xpatch;"
       ```

    2. Restore the dump as usual:

       ```bash
       pg_restore -d newdb mydb.dump
       ```

    3. Remap the config to the restored tables:

       ```sql
       SELECT xpatch.fix_restored_configs();
       ```

!!! danger "Do not skip step 3"
    pg-xpatch looks up a table's config by its OID, and OIDs change on restore. Until `fix_restored_configs()` re-links the config (by schema and table name), restored tables fall back to auto-detection, which can silently drop your `group_by` and mis-handle grouping. The call also clears configs for tables that no longer exist.

## Moving config between databases

If you want the configuration on its own, for a migration script or to reapply it elsewhere, `dump_configs()` emits ready-to-run `configure()` statements:

```sql
SELECT * FROM xpatch.dump_configs();
-- SELECT xpatch.configure('public.documents', group_by => 'doc_id', ...);
-- SELECT xpatch.configure('public.notes', ...);
```

Run that output against the target after loading the tables. This is the portable alternative when a plain catalog round-trip is not what you want.

!!! cards { cols=2 }
    - [SQL functions](./sql-functions.md){ icon=function-square }
      Signatures for `fix_restored_configs()` and `dump_configs()`.

    - [Configuring a table](./configuration.md){ icon=sliders-horizontal }
      What the restored config actually controls.
