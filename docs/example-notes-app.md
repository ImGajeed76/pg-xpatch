---
title: "Example: a versioned notes app"
description: A real Svelte app where every keystroke is a version, walked through the pg-xpatch parts, schema, aggressive compression config, and the append-only write pattern.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/example-notes-app
  type: tutorial
  applies_to:
    - postgres 16
  language: en
  difficulty: intermediate
  time_estimate: 10m
  status: stable
  aliases:
    - notes app example
    - example app
    - version history demo
  references:
    - https://github.com/ImGajeed76/pg-xpatch/tree/master/examples/notes-app
  prev: ./types-and-limitations.md
---

# Example: a versioned notes app

The repo ships a small notes app under [`examples/notes-app`](../examples/notes-app): a markdown editor where every keystroke saves a new version, yet storage stays tiny and you can jump to any past version instantly. The stack is SvelteKit and Drizzle, but the interesting part is three pieces of SQL. This walks through them.

## The schema

Two tables. `notes` holds metadata; `note_versions` holds every revision and is the xpatch table.

```sql
CREATE TABLE notes (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title      TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE note_versions (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    note_id     UUID NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
    content     TEXT NOT NULL,
    version_num INT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
) USING xpatch;
```

`content` is the big field that repeats almost unchanged between versions, and it is `NOT NULL`, as delta columns must be.

## The configuration

This is where the app tunes for its workload, many tiny edits per document. The call lives in [`setup.ts`](../examples/notes-app/src/lib/server/db/setup.ts):

```sql
SELECT xpatch.configure('note_versions'::regclass,
    group_by       => 'note_id',           -- (1)!
    order_by       => 'version_num',        -- (2)!
    delta_columns  => ARRAY['content'],     -- (3)!
    keyframe_every => 200,                  -- (4)!
    compress_depth => 100,                  -- (5)!
    enable_zstd    => true                  -- (6)!
);
```

1. Each note is its own version chain. A new version of note A never touches note B.
2. Versions are ordered by `version_num` within each note.
3. Only `content` is delta-compressed; `version_num` and timestamps are stored plainly.
4. A full keyframe only every 200 versions. Keystroke edits are tiny, so long delta chains barely cost anything, and fewer keyframes means less storage.
5. Try the last 100 versions as a delta base and keep the smallest. Keystroke-level edits resemble many recent versions, so a deep search finds a very close match.

6. Zstandard on top, the default.

!!! tip "These are aggressive settings on purpose"
    The defaults are `keyframe_every = 100`, `compress_depth = 1`. This app pushes both hard to squeeze keystroke-level edits, trading heavier writes (which happen at human typing speed anyway) for maximum compression. [Tuning compression](./tuning-compression.md) explains the trade.

## Saving a version

There is no `UPDATE`. Each save reads the latest `version_num` for the note, adds one, and inserts a new row:

```sql
-- next version number for this note
SELECT COALESCE(MAX(version_num), 0) + 1 FROM note_versions WHERE note_id = $1;

-- then insert it
INSERT INTO note_versions (note_id, content, version_num)
VALUES ($1, $2, $3)
RETURNING *;
```

That append-only shape is the whole point: history is immutable, and the delta chain just grows.

## Reading versions

Listing or loading a version is a plain query. Reconstruction is invisible:

```sql
SELECT version_num, content, created_at
FROM note_versions
WHERE note_id = $1
ORDER BY version_num DESC;
```

Every version comes back as full text, whether it was stored as a keyframe or a 6-byte delta.

## Showing the compression

The app displays live stats from `xpatch.stats()`:

```sql
SELECT total_rows, compressed_size_bytes, compression_ratio
FROM xpatch.stats('note_versions');
```

!!! note "Computing raw size directly"
    The example computes raw bytes itself with `SELECT SUM(length(content)) FROM note_versions`, because the stats table tracks compressed storage and the raw figure can read `0` depending on how rows were written. If you want an exact raw-versus-stored comparison, doing the `SUM` yourself is the reliable path.

In the example's own measurements, around 150 keystroke-level versions of a document drop from roughly 32 KB of raw content to about 1 KB stored. Your numbers depend entirely on how much each save changes.

## Run it yourself

```bash
cd examples/notes-app
bun run db:start   # PostgreSQL with pg-xpatch, via docker-compose
bun run dev        # tables and config are created on first run
```

Open the editor, type, and watch the version list and compression ratio update.

## The takeaway

The pattern generalizes far beyond notes: pick the entity as `group_by`, a monotonic version as `order_by`, and delta-compress the large text or json column. Wikis, config history, chat-message edits, and audit trails all fit the same three pieces of SQL.

!!! cards { cols=2 }
    - [Configuring a table](./configuration.md){ icon=sliders-horizontal }
      The options this example sets, in full.

    - [Tuning compression](./tuning-compression.md){ icon=minimize-2 }
      Why `keyframe_every = 200` and `compress_depth = 100` here.
