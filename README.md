# pg-xpatch

PostgreSQL extension for storing versioned data with automatic delta compression. Built on [xpatch](https://github.com/ImGajeed76/xpatch).

## What It Does

Store versioned rows (like document revisions, config history, audit logs) with 20x space savings. Compression is automatic and transparent - you just INSERT/SELECT normally.

```sql
-- Create table
CREATE TABLE documents (
    doc_id   INT,
    version  INT,
    content  TEXT
) USING xpatch;

-- Configure (or let it auto-detect)
SELECT xpatch.configure('documents',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert versions normally
INSERT INTO documents VALUES (1, 1, 'Hello World');
INSERT INTO documents VALUES (1, 2, 'Hello PostgreSQL!');

-- Query normally - reconstruction is automatic
SELECT * FROM documents WHERE doc_id = 1;

-- Check compression
SELECT * FROM xpatch_stats('documents');
```

## Performance

**Storage:** 20x smaller than heap tables (544 KB vs 11 MB for 10k versioned rows)

**Reads:** Comparable to heap with warm cache, slower on cold reads

**Writes:** 3-5x slower than heap (delta encoding overhead)

Good for append-only versioned data where space matters more than write speed.

## Installation

**Requirements:** PostgreSQL 16, Rust 1.92+, Git

```bash
git clone https://github.com/yourusername/pg-xpatch
cd pg-xpatch

# Build (automatically clones xpatch library)
make clean && make && make install

# Enable
psql -c "CREATE EXTENSION pg_xpatch;"
```

The Makefile automatically clones the xpatch library into `tmp/xpatch` if it doesn't exist.

See `.devcontainer/` for a pre-configured Docker environment.

## How It Works

Groups rows by an ID column, orders by version, stores only deltas between versions. Every 100th row is a keyframe (full content). Three-tier cache speeds up reconstruction.

Delta encoding uses xpatch's tag system - each delta can reference any previous version, not just the immediate predecessor. This makes reverts extremely small.

## Limitations

Version 0.1.0 is append-only:
- No UPDATE/DELETE (insert new versions instead)
- No VACUUM yet
- Basic MVCC only

These trade-offs are fine for immutable version history.

**Note:** Indexes work on all columns, including delta columns (reconstruction happens transparently during index scans).

## License

Dual-licensed: **AGPL-3.0-or-later** for open source, commercial license available for proprietary use.

### Why AGPL?

I love open source. I don't love massive corporations taking community work and giving nothing back. AGPL ensures that if you modify and distribute pg-xpatch (including running it as a service), you share those improvements.

For most people, AGPL is perfect. You're building open source? Great, use it freely.

### Commercial License

If you're a large company with AGPL restrictions or need to use this in proprietary infrastructure at scale, let's talk: **xpatch-commercial@alias.oseifert.ch**

**Small businesses and startups:** Probably free. I just want to know who's using it.

**Large companies:** Yeah, I'll ask for something reasonable. You have the resources to support open source work.

**Want to contribute code instead?** Even better. Help improve pg-xpatch and we'll work out the licensing.

I'm not building a licensing business - this is about fairness. Don't be a massive corp that extracts value without contributing back.

See LICENSE-AGPL.txt and LICENSE-COMMERCIAL.txt for details.

## Contributing

Contributions welcome. If you contribute code, you grant rights to use it under both AGPL and commercial terms (so we can handle licensing without tracking down every contributor).

## Links

- **xpatch library:** https://github.com/ImGajeed76/xpatch
- **Tests:** `./test/run_tests.sh run`
- **Benchmarks:** `./benchmark/run_benchmark.sh`
