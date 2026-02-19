# pg-xpatch

A PostgreSQL Table Access Method (TAM) extension for storing versioned data with automatic delta compression. Built on the [xpatch](https://github.com/ImGajeed76/xpatch) delta encoding library.

## What It Does

Store versioned rows (document revisions, config history, audit logs, etc.) with massive space savings. Compression is automatic and transparent - you just INSERT and SELECT normally.

```sql
-- Create a table using the xpatch access method
CREATE TABLE documents (
    doc_id   INT,
    version  INT,
    content  TEXT NOT NULL  -- Delta columns must be NOT NULL
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
SELECT * FROM xpatch.stats('documents');
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
- **Shared memory cache** - Lock-striped LRU cache for reconstructed content across all backends
- **Stats cache** - Statistics updated incrementally on INSERT/DELETE for instant `xpatch.stats()` calls

### What Doesn't Work (By Design)

- **UPDATE** - Not supported. Insert a new version instead. This is intentional for append-only versioned data.
- **Out-of-order inserts** - Versions must be inserted in order within each group
- **Hidden columns** - The internal `_xp_seq` column is visible in `SELECT *` (PostgreSQL limitation)
- **Nullable delta columns** - Columns configured for delta compression must have a NOT NULL constraint

### Utility Functions

```sql
-- Describe a table: shows config, schema, and storage stats
SELECT * FROM xpatch.describe('documents');

-- Get compression statistics (instant - reads from stats table)
SELECT * FROM xpatch.stats('documents');

-- Warm the cache for faster subsequent queries
SELECT * FROM xpatch.warm_cache('documents');
SELECT * FROM xpatch.warm_cache('documents', max_groups => 100);

-- Inspect internal storage for a specific group (debugging/analysis)
SELECT * FROM xpatch.inspect('documents', 1);  -- group_value = 1

-- View raw physical storage (delta bytes)
SELECT * FROM xpatch.physical('documents');

-- Get cache statistics (requires shared_preload_libraries)
-- Returns: size_bytes, max_bytes, entries_count, hit_count, miss_count, eviction_count, skip_count
SELECT * FROM xpatch.cache_stats();

-- Get insert cache statistics
SELECT * FROM xpatch.insert_cache_stats();

-- Get xpatch library version
SELECT xpatch.version();

-- Dump all table configs as SQL (for backup/migration)
SELECT * FROM xpatch.dump_configs();

-- Fix config OIDs after pg_restore
SELECT xpatch.fix_restored_configs();

-- Force recalculate stats (rarely needed - stats are auto-maintained)
SELECT * FROM xpatch.refresh_stats('documents');
```

## Performance

### Storage Compression

Space savings depend heavily on your data patterns. Here are real benchmarks with 5000 rows (100 documents x 50 versions each):

| Data Pattern | xpatch Size | heap Size | Space Saved |
|--------------|-------------|-----------|-------------|
| Incremental changes (base content + small edits) | 432 KB | 5.6 MB | **92% smaller** |
| Identical content across versions | 312 KB | 448 KB | 30% smaller |
| Completely random data | 624 KB | 568 KB | 10% larger (overhead) |

**Key insight:** xpatch shines when versions share content. For typical document versioning (where each version is similar to the previous), expect 10-20x space savings. For random/unrelated data, xpatch adds overhead and provides no benefit.

### Query Performance

**Important:** These benchmarks require `shared_preload_libraries = 'pg_xpatch'` to enable the shared memory cache. Without it, performance is orders of magnitude worse.

These are rough indicators, not precise measurements. Your results will vary based on hardware, data patterns, cache state, and query complexity.

Benchmark setup: 10,100 rows (101 documents x 100 versions), ~1KB content per row. xpatch: 800 KB, heap: 11 MB.

| Operation | xpatch | heap | Notes |
|-----------|--------|------|-------|
| **Full table COUNT** | 13ms | 0.4ms | 32x slower (decodes all rows) |
| **Point lookup (single doc, 100 rows)** | 0.07ms | 0.06ms | ~same with index |
| **Point lookup (single row)** | 0.07ms | 0.04ms | 1.8x slower |
| **GROUP BY aggregate** | 0.12ms | 0.06ms | 2x slower |
| **Latest version per doc** | 19ms | 2ms | 10x slower |
| **Text search (LIKE)** | 21ms | 9ms | 2.3x slower |
| **Parallel scan (2 workers)** | 16ms | 0.4ms | 40x slower |

**Key observations:**
- **Index lookups are fast** - Point queries using the composite index are nearly heap speed
- **Full scans are slow** - Operations requiring all rows to be decoded have significant overhead
- **Cache helps significantly** - Warm cache queries are 2-3x faster than cold

### Write Performance

| Operation | xpatch | heap | Slowdown |
|-----------|--------|------|----------|
| **Individual inserts** (100 rows, loop) | 24ms | 1.3ms | 18x |
| **Batch insert** (100 rows, 1 group) | 5ms | 1.0ms | **5x** |
| **Batch insert** (100 rows, 100 groups) | 5ms | 0.9ms | **6x** |
| **Batch insert** (1000 rows) | 65ms | 2.8ms | 23x |
| **Batch insert** (5000 rows) | 217ms | 12ms | 18x |

**Key insight: Batch inserts are 4-5x faster than individual inserts.** Use `INSERT ... SELECT` or multi-row `INSERT` statements when possible.

The absolute numbers matter more than the ratios: **5ms for 100 rows is 50μs per row**, which is fast enough for most versioned data workloads.

Also remember that **writes parallelize across groups**. If you have 100 users editing 100 different documents, all 100 writes happen concurrently. Sequential writes only apply *within* a single group's version chain.

**When to use xpatch:**
- Storage cost is a primary concern (90%+ space savings)
- Data is written once and read occasionally
- Append-only versioned data (audit logs, document history, config snapshots)
- You can use batch inserts

**When NOT to use xpatch:**
- High-frequency full table scans
- Write-heavy workloads with individual row inserts
- Data with no similarity between versions (random data)

### Cache Configuration

The shared memory cache is **essential** for good read performance. Add to `postgresql.conf`:

```
shared_preload_libraries = 'pg_xpatch'

# Content cache (LRU, shared memory with lock striping)
pg_xpatch.cache_size_mb = 512           # Total cache size (default: 256)
pg_xpatch.cache_max_entries = 65536     # Max cache entries (default: 65536)
pg_xpatch.cache_slot_size_kb = 4        # Content slot size (default: 4, range: 1-64)
pg_xpatch.cache_partitions = 32         # Lock stripes for concurrency (default: 32, range: 1-256)
pg_xpatch.cache_max_entry_kb = 256      # Max single entry size (default: 256, runtime-tunable)

# Sequence caches
pg_xpatch.group_cache_size_mb = 64      # Group max-seq cache (default: 16)
pg_xpatch.tid_cache_size_mb = 64        # TID-to-seq cache (default: 16)
pg_xpatch.seq_tid_cache_size_mb = 64    # Seq-to-TID cache (default: 16)

# Insert performance
pg_xpatch.insert_cache_slots = 64       # Concurrent insert slots (default: 16)
pg_xpatch.max_delta_columns = 32        # Max delta columns per table (default: 32)
pg_xpatch.encode_threads = 4            # Parallel encoding threads (default: 0)
```

**Lock striping** (v0.6.0): The content cache is partitioned into `cache_partitions` independent stripes, each with its own lock. This eliminates contention between concurrent backends. With the default of 32 stripes, up to 32 backends can access the cache simultaneously without blocking each other.

Entries larger than `cache_max_entry_kb` are silently skipped by the cache. A `WARNING` is logged on the first skip per backend session (subsequent skips log at `DEBUG1`). Use `xpatch.cache_stats()` to monitor the `skip_count` counter.

`cache_max_entry_kb` uses `PGC_SUSET` context (superusers can change at runtime). `encode_threads` uses `PGC_USERSET` (any user can change per-session). All other GUCs are `PGC_POSTMASTER` (require a restart).

Cache warming example:
```sql
-- Cold query: ~2.3ms
SELECT COUNT(*) FROM documents;

-- Warm the cache
SELECT * FROM xpatch.warm_cache('documents');

-- Warm query: ~0.7ms
SELECT COUNT(*) FROM documents;
```

### Stats Cache

Statistics are stored in the `xpatch.group_stats` table and updated automatically:
- **INSERT**: Stats for the affected group are updated incrementally
- **DELETE**: Only the affected group's stats are recalculated

This provides instant (~0.4ms) performance for `xpatch.stats()` calls. You typically don't need to call `refresh_stats()` - stats are maintained automatically during normal operations.

### Real-World Example: pgit

For real-world benchmarks comparing pg-xpatch against git's packfile format, see [pgit](https://github.com/ImGajeed76/pgit) — a Git-like CLI that stores repositories in PostgreSQL using pg-xpatch compression.

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
# Download and extract (replace VERSION with actual version, e.g., v0.1.1)
tar -xzf pg_xpatch-VERSION-pg16-linux-amd64.tar.gz
cd pg_xpatch-VERSION-pg16-linux-amd64

# Install
sudo cp pg_xpatch.so $(pg_config --pkglibdir)/
sudo cp pg_xpatch.control *.sql $(pg_config --sharedir)/extension/

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
    delta_columns => ARRAY['content', 'metadata']::text[],  -- Columns to compress (must be NOT NULL)
    keyframe_every => 100,          -- Full snapshot every N versions (default: 100)
    compress_depth => 1,            -- How many previous versions to consider (default: 1)
    enable_zstd => true             -- Enable zstd compression (default: true)
);
```

**Note:** Delta columns must have a `NOT NULL` constraint. The extension will raise an error if you try to configure a nullable column for delta compression.

```sql
-- This will fail:
CREATE TABLE bad (id INT, data TEXT) USING xpatch;
SELECT xpatch.configure('bad', delta_columns => ARRAY['data']::text[]);
-- ERROR: Delta column "data" must be NOT NULL

-- This works:
CREATE TABLE good (id INT, data TEXT NOT NULL) USING xpatch;
SELECT xpatch.configure('good', delta_columns => ARRAY['data']::text[]);
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

pg-xpatch has a comprehensive pytest-based test suite with **496 tests** across 18 test files. Each test runs in an isolated PostgreSQL database that is created and dropped automatically.

### Requirements

- Python 3.10+
- [psycopg](https://www.psycopg.org/) 3 (`pip install psycopg[binary]`)
- pytest (`pip install pytest pytest-timeout`)
- The `pg-xpatch-dev` Docker container running (or a PostgreSQL instance with pg_xpatch installed)

### Running Tests

```bash
# All tests (excludes crash and stress tests by default)
python -m pytest tests/ -v --tb=short -m "not crash_test and not stress"

# All tests including crash recovery and stress tests
python -m pytest tests/ -v --tb=short

# Run a specific test file
python -m pytest tests/test_basic.py -v

# Run a specific test class or test
python -m pytest tests/test_transactions.py::TestMvccVisibilitySeqScan -v

# Parallel execution (requires pytest-xdist)
python -m pytest tests/ -n auto -m "not crash_test and not stress"
```

### Test Coverage

| Test File | Tests | What It Covers |
|-----------|-------|----------------|
| `test_smoke.py` | 6 | Extension loaded, version, schema, event triggers |
| `test_basic.py` | 62 | CREATE TABLE, INSERT/SELECT, COPY FROM/TO, ALTER TABLE, DROP cleanup, custom schemas, nested loop rescan |
| `test_compression.py` | 34 | Delta compression ratios, keyframe intervals, compress_depth, enable_zstd, JSONB operators, encode_threads GUC |
| `test_delete.py` | 23 | Cascade delete semantics, multi-group isolation, re-insert after delete, edge cases |
| `test_empty_content.py` | 9 | Empty content delta correctness: non-empty→empty→empty via COPY and INSERT, tag verification, alternating patterns, keyframe boundaries, multi-group isolation |
| `test_errors.py` | 36 | Blocked operations (UPDATE, CLUSTER), configure() validation (E13/E17), NULL group, ON CONFLICT, TABLESAMPLE, BIGINT _xp_seq |
| `test_indexes.py` | 25 | Auto-created indexes, manual indexes, index/bitmap scan plans, ANALYZE, REINDEX CONCURRENTLY, CREATE INDEX CONCURRENTLY |
| `test_multi_delta.py` | 18 | Multiple delta columns, mixed types (TEXT+BYTEA, TEXT+JSONB), 4+ columns, per-column inspection |
| `test_no_group.py` | 16 | Tables without group_by, single-group stats, delete, inspect, physical |
| `test_parallel.py` | 12 | Parallel sequential scan correctness, aggregation, filters, empty tables |
| `test_restore_mode.py` | 22 | Explicit _xp_seq (restore mode), dump_configs, fix_restored_configs, pg_dump/pg_restore round-trip |
| `test_stats_cache.py` | 20 | group_stats incremental updates, refresh_stats, truncate, cross-validation invariants |
| `test_transactions.py` | 35 | Commit/rollback, savepoints, MVCC visibility (seq/index/bitmap scan), concurrent insert/delete, SERIALIZABLE isolation |
| `test_types.py` | 39 | All group types (INT/BIGINT/TEXT/VARCHAR/UUID), order types (INT/BIGINT/SMALLINT/TIMESTAMP), delta types (TEXT/BYTEA/JSON/JSONB), special characters, boundary values |
| `test_utility_functions.py` | 52 | All SQL-callable functions: version, stats, inspect, describe, physical, cache_stats, warm_cache, refresh_stats, dump_configs, invalidate_config |
| `test_cache_max_entry.py` | 25 | Cache max entry GUC, skip_count stats, oversized entry rejection, mixed sizes, large delta chains |
| `test_guc_settings.py` | 27 | All 11 GUC defaults/metadata, PGC context enforcement, lock striping correctness (multi-group, stats aggregation, cross-stripe invalidation) |
| `test_vacuum.py` | 25 | VACUUM, VACUUM FULL (error), ANALYZE, TRUNCATE + rollback, crash recovery after VACUUM |

### Key Test Scenarios

- **MVCC visibility** across all scan types (sequential, index, bitmap)
- **Concurrent operations**: parallel inserts, concurrent deletes with serialization, SERIALIZABLE isolation conflicts
- **Crash recovery**: SIGKILL PostgreSQL, verify data integrity after recovery
- **pg_dump/pg_restore**: full round-trip preserving data, _xp_seq values, and configuration
- **All supported data types**: 5 group types, 5 order types, 5 delta types, special characters, boundary values
- **Configuration validation**: all xpatch.configure() parameters, error paths, auto-detection

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

Thoroughly tested on PostgreSQL 16 with 496 test cases. Other versions may work but are not officially supported.

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
1. Run the test suite: `python -m pytest tests/ -v --tb=short`
2. Add tests for new functionality in the `tests/` directory
3. Keep commits focused and well-documented

## Links

- **xpatch library**: https://github.com/ImGajeed76/xpatch
- **Issue tracker**: https://github.com/ImGajeed76/pg-xpatch/issues
- **Tests**: `tests/` (pytest)
- **Portfolio**: [Check it out on my website](https://oseifert.ch/projects/pg-xpatch-1137)
