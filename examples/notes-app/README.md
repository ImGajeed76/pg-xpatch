# Notes App

A markdown notes app with version history, demonstrating [pg-xpatch](https://github.com/ImGajeed76/pg-xpatch) delta compression.

## Quick Start

```bash
bun run db:start   # Start PostgreSQL with pg-xpatch
bun run dev        # Start the app (tables created automatically)
```

Open http://localhost:5173

## What This Demonstrates

- **Automatic delta compression** - Each keystroke creates a version, but storage stays tiny
- **Time travel** - Browse and restore any previous version instantly
- **Real compression stats** - See the actual bytes saved in real-time

## Typical Results

After writing ~150 versions of a document:

| Metric | Value |
|--------|-------|
| Raw content | ~32 KB |
| Stored size | ~1 KB |
| Compression | **32x** |
| Avg per version | **6 bytes** |

## How It Works

### 1. Create table with xpatch storage

```sql
CREATE TABLE note_versions (
    id UUID PRIMARY KEY,
    note_id UUID REFERENCES notes(id),
    content TEXT NOT NULL,
    version_num INT NOT NULL
) USING xpatch;
```

### 2. Configure compression

```sql
SELECT xpatch.configure(
    'note_versions'::regclass,
    group_by => 'note_id',      -- Group versions by document
    order_by => 'version_num',  -- Order within group
    delta_columns => ARRAY['content'],
    keyframe_every => 200,      -- Full snapshot every 200 versions
    compress_depth => 100       -- Compare against 100 previous versions
);
```

### 3. Use normal SQL

```typescript
// Insert - compression happens automatically
await db.insert(noteVersions).values({
    noteId: id,
    content: 'new content',
    versionNum: 42
});

// Query - decompression is transparent
const versions = await db
    .select()
    .from(noteVersions)
    .where(eq(noteVersions.noteId, id))
    .orderBy(desc(noteVersions.versionNum));
```

## Project Structure

```
src/
├── lib/
│   ├── api.ts                 # Client API
│   └── server/db/
│       ├── index.ts           # Drizzle client
│       ├── schema.ts          # Table definitions
│       └── setup.ts           # xpatch initialization
└── routes/
    ├── +page.svelte           # Notes list
    ├── editor/[id]/           # Editor with time travel
    └── api/notes/             # REST endpoints
```

## Configuration Reference

| Parameter | Description | This App |
|-----------|-------------|----------|
| `group_by` | Column to group related rows | `note_id` |
| `order_by` | Order within group | `version_num` |
| `delta_columns` | Columns to delta-compress | `['content']` |
| `keyframe_every` | Versions between full snapshots | `200` |
| `compress_depth` | Previous versions to compare | `100` |

Higher `keyframe_every` = better compression, slower worst-case reads.
Higher `compress_depth` = better compression, slower writes.

## Scripts

| Command | Description |
|---------|-------------|
| `bun run db:start` | Start PostgreSQL |
| `bun run db:stop` | Stop PostgreSQL |
| `bun run db:reset` | Reset database (delete all data) |
| `bun run dev` | Development server |
| `bun run build` | Production build |
