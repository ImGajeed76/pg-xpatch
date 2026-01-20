# pg-xpatch

A PostgreSQL Table Access Method (TAM) extension for storing versioned data with automatic delta compression. Built on the [xpatch](https://github.com/ImGajeed76/xpatch) delta encoding library.

## What It Does

Store versioned rows (document revisions, config history, audit logs, etc.) with massive space savings. Compression is automatic and transparent - you just INSERT and SELECT normally.

```sql
-- Create a table using the xpatch access method
CREATE TABLE documents (
    doc_id   INT,
    version  INT,
    content  TEXT
) USING xpatch;

-- Configure grouping and ordering (optional - auto-detection works for most cases)
SELECT xpatch.configure('documents',
    group_by => 'doc_id',
    order_by => 'version'
);

-- Insert versions normally
INSERT INTO documents VALUES (1, 1, 'Hello World');
INSERT INTO documents VALUES (1, 2, 'Hello PostgreSQL World!');
INSERT INTO documents VALUES (1, 3, 'Hello PostgreSQL World! Updated.');

-- Query normally - reconstruction is automatic
SELECT * FROM documents WHERE doc_id = 1 ORDER BY version;

-- Check compression stats
SELECT * FROM xpatch_stats('documents');
```

## Features

### What Works

- **Automatic delta compression** - Only stores differences between versions
- **Transparent reconstruction** - SELECT works normally, deltas are decoded on-the-fly
- **Auto-detection** - Automatically detects group_by, order_by, and delta columns
- **Parallel scans** - Full support for parallel query execution
- **Index support** - B-tree indexes work on all columns (including delta-compressed ones)
- **MVCC** - Basic multi-version concurrency control
- **DELETE** - Cascade delete removes a version and all subsequent versions in the chain
- **VACUUM** - Dead tuple cleanup works
- **WAL logging** - Crash recovery supported
- **Shared memory cache** - LRU cache for reconstructed content across all backends

### What Doesn't Work (By Design)

- **UPDATE** - Not supported. Insert a new version instead. This is intentional for append-only versioned data.
- **Out-of-order inserts** - Versions must be inserted in order within each group
- **Hidden columns** - The internal `_xp_seq` column is visible in `SELECT *` (PostgreSQL limitation)

### Utility Functions

```sql
-- Describe a table: shows config, schema, and storage stats
SELECT * FROM xpatch.describe('documents');

-- Warm the cache for faster subsequent queries
SELECT * FROM xpatch.warm_cache('documents');
SELECT * FROM xpatch.warm_cache('documents', max_groups => 100);

-- Get compression statistics
SELECT * FROM xpatch_stats('documents');

-- Inspect internal storage for a specific group (debugging/analysis)
SELECT * FROM xpatch_inspect('documents', 1);  -- group_value = 1

-- Get cache statistics (requires shared_preload_libraries)
SELECT * FROM xpatch_cache_stats();

-- Get xpatch library version
SELECT xpatch_version();

-- Dump all table configs as SQL (for backup/migration)
SELECT * FROM xpatch.dump_configs();

-- Fix config OIDs after pg_restore
SELECT xpatch.fix_restored_configs();
```

## Performance

### Storage Compression

Space savings depend heavily on your data patterns. Here are real benchmarks with 5000 rows (100 documents x 50 versions each):

| Data Pattern | xpatch Size | heap Size | Space Saved |
|--------------|-------------|-----------|-------------|
| Incremental changes (base content + small additions) | 416 KB | 9.2 MB | **95% smaller** |
| Identical content across versions | 488 KB | 648 KB | 25% smaller |
| Completely random data | 816 KB | 728 KB | 12% larger (overhead) |

**Key insight:** xpatch shines when versions share content. For typical document versioning (where each version is similar to the previous), expect 10-20x space savings. For random/unrelated data, xpatch adds overhead and provides no benefit.

### Query Performance

**Important:** These benchmarks are rough indicators, not precise measurements. Your mileage will vary based on hardware, data patterns, cache state, and query complexity.

Benchmark setup: 10,100 rows (101 documents x 100 versions), incremental text data. xpatch: 776 KB, heap: 17 MB.

| Operation | xpatch | heap | Slowdown |
|-----------|--------|------|----------|
| **Full table COUNT** | | | |
| - Cold cache | 44ms | 2.6ms | 17x slower |
| - Warm cache | 20ms | 1.4ms | 14x slower |
| **Point lookup (single doc, 100 rows)** | 0.7ms | 0.05ms | 14x slower |
| **Point lookup (single row)** | 0.13ms | 0.02ms | 6x slower |
| **GROUP BY aggregate** | 27ms | 3ms | 9x slower |
| **Latest version per doc** | 28ms | 5.5ms | 5x slower |
| **Text search (LIKE)** | 3.4ms | 1.5ms | 2x slower |
| **INSERT (100 rows)** | 33ms | 0.3ms | 100x slower |
| **Parallel scan (2 workers)** | 31ms | 3.5ms | 9x slower |

**Key observations:**
- **Reads are 5-17x slower** due to delta reconstruction overhead
- **Writes are ~100x slower** due to delta encoding (this is the main trade-off)
- **Cache helps** but doesn't eliminate the reconstruction cost
- **Indexed lookups** are faster than full scans but still have overhead

**When to use xpatch:**
- Storage cost is a primary concern (95% space savings)
- Data is written once and read occasionally
- Append-only versioned data (audit logs, document history, config snapshots)
- You can tolerate higher write latency

**When NOT to use xpatch:**
- High-frequency reads on the same data
- Write-heavy workloads where latency matters
- Data with no similarity between versions (random data)

### Cache Behavior

The shared memory cache dramatically improves read performance for repeated access:

```sql
-- First query (cold): ~35ms for 5000 rows
SELECT COUNT(*) FROM documents;

-- Second query (warm): ~1ms for 5000 rows  
SELECT COUNT(*) FROM documents;
```

To enable the shared memory cache, add to `postgresql.conf`:
```
shared_preload_libraries = 'pg_xpatch'
```

## Installation

### Docker (Easiest)

```bash
# Run PostgreSQL with pg-xpatch pre-installed
docker run -d --name pg-xpatch \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=secret \
  ghcr.io/imgajeed76/pg-xpatch:latest

# Connect and enable the extension
psql -h localhost -U postgres -c "CREATE EXTENSION pg_xpatch;"
```

### Pre-built Binaries

Download from [GitHub Releases](https://github.com/ImGajeed76/pg-xpatch/releases):

```bash
# Download and extract
tar -xzf pg_xpatch-v0.1.0-pg16-linux-amd64.tar.gz
cd pg_xpatch-v0.1.0-pg16-linux-amd64

# Install
sudo cp pg_xpatch.so $(pg_config --pkglibdir)/
sudo cp pg_xpatch.control pg_xpatch--0.1.0.sql $(pg_config --sharedir)/extension/

# (Optional) Enable shared memory cache - add to postgresql.conf:
# shared_preload_libraries = 'pg_xpatch'

# Restart PostgreSQL, then:
psql -c "CREATE EXTENSION pg_xpatch;"
```

### Building from Source

Requirements:
- PostgreSQL 16+ (with dev headers)
- Rust 1.70+
- cbindgen (`cargo install cbindgen`)

```bash
git clone https://github.com/ImGajeed76/pg-xpatch
cd pg-xpatch
make clean && make && make install
psql -c "CREATE EXTENSION pg_xpatch;"
```

### Docker Development Environment

A pre-configured Docker environment is available in `.devcontainer/`:

```bash
# Build and run
docker build -t pg-xpatch-dev .devcontainer/
docker run -d --name pg-xpatch-dev -v $(pwd):/workspace pg-xpatch-dev

# Build and test inside container
docker exec pg-xpatch-dev bash -c "cd /workspace && make && make install"
docker exec -u postgres pg-xpatch-dev psql -c "CREATE EXTENSION pg_xpatch;"
```

## Configuration

### Auto-Detection

For most tables, xpatch auto-detects the configuration:
- **group_by**: Not set (whole table is one version chain) or explicitly configured
- **order_by**: Last INT/SMALLINT/BIGINT/TIMESTAMP/TIMESTAMPTZ column before `_xp_seq`
- **delta_columns**: All TEXT, VARCHAR, BYTEA, JSON, JSONB columns

### Explicit Configuration

```sql
SELECT xpatch.configure('my_table',
    group_by => 'doc_id',           -- Column that groups versions (optional)
    order_by => 'version',          -- Column that orders versions
    delta_columns => ARRAY['content', 'metadata']::text[],  -- Columns to compress
    keyframe_every => 100,          -- Full snapshot every N versions (default: 100)
    compress_depth => 1,            -- How many previous versions to consider (default: 1)
    enable_zstd => true             -- Enable zstd compression (default: true)
);
```

### Inspecting Configuration

```sql
-- Full table description
SELECT * FROM xpatch.describe('my_table');

-- Just the config
SELECT * FROM xpatch.get_config('my_table');
```

## How It Works

### Storage Model

1. **Grouping**: Rows are grouped by an optional `group_by` column (e.g., document ID)
2. **Ordering**: Within each group, rows are ordered by an `order_by` column (e.g., version number)
3. **Keyframes**: Every Nth row (default: 100) stores full content
4. **Deltas**: Other rows store only the differences from the previous version

### Reconstruction

When you SELECT a delta-compressed row:
1. Find the nearest keyframe
2. Apply deltas sequentially to reconstruct the content
3. Cache the result for future queries

### Internal Column

xpatch automatically adds an `_xp_seq` column to track sequence numbers. This column:
- Is added automatically via event trigger on `CREATE TABLE ... USING xpatch`
- Is used internally for efficient delta chain traversal
- Is visible in `SELECT *` (PostgreSQL doesn't support truly hidden columns)
- Should be excluded in your queries if you don't want to see it: `SELECT doc_id, version, content FROM ...`

### Automatic Indexes

xpatch automatically creates indexes for efficient lookups:
- Basic `_xp_seq` index on table creation
- Composite `(group_by, _xp_seq)` index when `group_by` is configured

## Testing

```bash
# Run all tests (20 test files)
# First create the test database and extension
createdb xpatch_test
psql -d xpatch_test -c "CREATE EXTENSION pg_xpatch;"

# Then run all test files
for f in test/sql/*.sql; do
    psql -d xpatch_test -f "$f"
done

# Or use the test runner
./test/run_tests.sh run
```

The test suite covers:
- Basic INSERT/SELECT operations
- Delta compression and reconstruction
- Keyframe behavior
- Index support
- Parallel scans
- DELETE with cascade
- VACUUM
- Error handling
- Edge cases (empty tables, NULL values, unusual types, large data)

## Limitations and Known Issues

### Intentional Limitations

- **No UPDATE**: Use INSERT with a new version number instead
- **Ordered inserts only**: Versions must be inserted in ascending order within each group
- **Append-only design**: Optimized for immutable version history

### Current Limitations (May Be Addressed Later)

- **`_xp_seq` visible**: PostgreSQL doesn't support hidden columns
- **Cold read performance**: First query on uncached data is slow
- **Write overhead**: Delta encoding adds INSERT latency

### Technical Debt (Known Implementation Issues)

These issues exist in the current implementation and may be addressed in future versions:

- **MVCC for reconstructed tuples**: The `xpatch_tuple_satisfies_snapshot()` function uses proper MVCC checks for buffer-backed tuples, but trusts that virtual tuples (created during delta reconstruction) were built from visible source tuples. This is correct behavior but means visibility is checked at reconstruction time, not query time.

These issues are documented for transparency. For typical workloads (versioned document storage, audit logs), they don't cause problems.

### PostgreSQL Version

Currently tested on PostgreSQL 16. Other versions may work but are not officially supported.

## License

pg-xpatch is dual-licensed: AGPL-3.0-or-later for open source, with a commercial option for proprietary use.

### The Philosophy

I'm a huge fan of open source. I also don't want massive corporations extracting value from community work without giving anything back. AGPL solves this - if you modify pg-xpatch and distribute it (including running it as a service), those improvements stay open.

That said, I'm not trying to build a licensing business here. This is about fairness, not revenue.

### Do You Need a Commercial License?

**Probably not if you're:**

- Building open source software (AGPL is perfect)
- A small team or indie developer
- Experimenting or doing research
- A startup figuring things out

**Maybe if you're:**

- A large company with AGPL restrictions
- Integrating this into proprietary infrastructure at scale
- Need legal certainty for closed-source use

### How Commercial Licensing Works

Email me at xpatch-commercial@alias.oseifert.ch and let's talk.

Small businesses? Probably free - I just want to know who's using it and how.

Larger companies? Yeah, I'll ask for something, but it'll be reasonable. You have the resources to support open source work, so let's make it fair.

Would rather contribute code than pay? Even better. Help make pg-xpatch better and we'll figure out the licensing stuff.

I'm not interested in complex contracts or pricing games. Just don't be a massive corp that takes community work and gives nothing back. That's literally the only thing I'm trying to prevent.

### Contributor License Agreement

If you contribute code, you're granting us rights to use it under both AGPL and commercial terms. This sounds scarier than it is - it just means we can handle licensing requests without tracking down every contributor for permission.

The AGPL version stays open forever. This just gives us flexibility to be reasonable with companies that need commercial licenses.

See `LICENSE-AGPL.txt` for the full text, or `LICENSE-COMMERCIAL.txt` for commercial terms.

## Contributing

Contributions are welcome! Please open an issue or pull request on GitHub.

Before submitting:
1. Run the test suite (`test/sql/*.sql`)
2. Add tests for new functionality
3. Keep commits focused and well-documented

## Links

- **xpatch library**: https://github.com/ImGajeed76/xpatch
- **Issue tracker**: https://github.com/ImGajeed76/pg-xpatch/issues
- **Tests**: `test/sql/*.sql`
