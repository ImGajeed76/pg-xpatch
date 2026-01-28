# Changelog

All notable changes to pg-xpatch will be documented in this file.

## [0.3.0] - 2025-01-28

### Added

- **FIFO insert cache**: DSA-backed per-group ring buffer that caches the last `compress_depth` reconstructed row contents, eliminating O(depth) reconstruction on the warm INSERT path. Configurable via `pg_xpatch.insert_cache_slots` GUC (default 16).

- **Lock-free encode thread pool**: Persistent pthread pool for parallelizing `xpatch_encode()` FFI calls with lock-free task dispatch via atomic fetch-add. Configurable via `pg_xpatch.encode_threads` GUC (default 0, opt-in).

### Performance

- **10.7x INSERT speedup** at depth=1000, 2KB payloads:
  - v0.2.1 baseline: 16.3s for 1000 inserts
  - Warm path sequential: 4.6s (3.5x faster)
  - Warm path with encode_threads=4: 1.5s (10.7x faster)

### Changed

- **Removed version validation**: The `order_by` column is no longer enforced to be strictly increasing. Previously, inserting a duplicate or lower version number would error. Now, the user's version column is treated as regular data, and `_xp_seq` handles all internal ordering. This simplifies the insert path and removes the overhead of version checking.

- **Simplified restore mode**: Explicit `_xp_seq` values are now always honored when provided (value > 0). The `pg_xpatch.restore_mode` GUC has been removed. This makes `pg_dump`/`pg_restore` work out of the box without any special configuration.

- **Auto-seq mode**: Using `_xp_seq=0` as sentinel now skips version validation and enables warm insert path.

### Fixed

- **CI/CD release notes**: Docker image tag in release notes now correctly shows version without `v` prefix (e.g., `0.3.0` instead of `v0.3.0`).

- **Cache invalidation**: Added insert cache invalidation to DELETE/TRUNCATE/VACUUM paths.

### Removed

- `pg_xpatch.restore_mode` GUC - no longer needed, restore mode is automatic when `_xp_seq` is explicitly provided
- `xpatch_compare_versions()` internal function - version comparison no longer performed

## [0.1.0] - 2025-01-19

Initial release.

### Features

- Table Access Method (TAM) implementation for PostgreSQL 16
- Delta compression using xpatch library
- Tag-based optimization (deltas can reference any previous version)
- 3-tier caching system:
  - Content cache (LRU, shared memory)
  - TID→seq cache (O(1) hash lookup)
  - Group→maxseq cache (O(1) hash lookup)
- Auto-configuration with manual override via `xpatch.configure()`
- Index support on all columns (including delta columns with transparent reconstruction)
- DELETE support with cascade (removes version and all subsequent versions in chain)
- VACUUM support for dead tuple cleanup
- Restore mode for pg_dump/pg_restore compatibility
- Utility functions:
  - `xpatch_stats()` - compression statistics
  - `xpatch_cache_stats()` - cache performance
  - `xpatch_inspect()` - inspect internal storage details for a group
  - `xpatch_version()` - get library version
  - `xpatch.describe()` - full table introspection
  - `xpatch.warm_cache()` - pre-populate cache
  - `xpatch.dump_configs()` - export configs as SQL
  - `xpatch.fix_restored_configs()` - fix OIDs after pg_restore
- Comprehensive test suite (20 tests)

### Performance

- 20x compression ratio on typical versioned content
- Sub-millisecond reads with warm cache
- 512ms for 10k row inserts (3-5x slower than heap)

### Known Limitations

- Append-only (no UPDATE support - by design)
- Basic MVCC only
- PostgreSQL 16 only

## [0.2.1] - 2025-01-22

### Fixed

- **TOAST support for large tuples**: Fixed "row is too big" error when inserting large content (>8KB). Now properly calls `heap_toast_insert_or_update()` to move large attributes to the TOAST table. Tested with files up to 1MB.
- **Sequence gap on failed insert**: Fixed critical bug where failed INSERTs would consume sequence numbers, creating gaps in delta chains that caused corruption. Now uses `PG_TRY/CATCH` to rollback sequence allocation on failure.
- **Keyframe fallback for missing base rows**: Delta encoding now gracefully handles missing base rows (from previous failed inserts) by falling back to keyframe encoding instead of erroring.
- **O(n²) performance in `fetch_by_seq`**: Optimized from O(n²) sequential scan to O(log n) using index scan + seq-to-TID cache. INSERT speed improved ~18x (90 rows/s → 1600 rows/s).
- **TRUNCATE cache invalidation**: Fixed cache not being invalidated on TRUNCATE. Added invalidation to `relation_set_new_filelocator()` callback.

### Changed

- `xpatch_logical_to_physical()` now returns allocated sequence via output parameter for rollback support
- Added `xpatch_seq_cache_rollback_seq()` function to decrement sequence on failed insert
- `xpatch_reconstruct_column()` now returns NULL instead of ERROR when row is missing

## [0.2.0] - 2025-01-20

### Added

- **TEXT/VARCHAR group column support**: Tables can now use TEXT or VARCHAR columns as the `group_by` column, not just INT/BIGINT. This enables grouping by string identifiers like UUIDs, slugs, or names.
- **`xpatch.physical()` function**: New function to access raw physical delta storage, returning delta bytes and metadata for debugging and advanced use cases.
- **Comprehensive TEXT group tests**: Added 10 new stress tests specifically for TEXT group column functionality.

### Fixed

- **TEXT group column crash**: Fixed critical bug where tables with TEXT group columns would crash on SELECT. The auto-detection was incorrectly including the group_by column as a delta column, causing corruption.
- **Datum comparison for varlena types**: Fixed incorrect comparison of TEXT/VARCHAR values in group matching. Now uses `TypeCacheEntry` and `FunctionCall2Coll` for proper collation-aware comparison instead of simple pointer comparison.
- **`xpatch_inspect()` with TEXT groups**: Fixed group filtering to work correctly with TEXT/VARCHAR group values.

### Changed

- `auto_detect_delta_columns()` now excludes `group_by`, `order_by`, and `_xp_seq` columns from delta compression
- Added `xpatch_datums_equal()` helper function for type-safe datum comparison across the codebase
- Existing installations can upgrade with: `ALTER EXTENSION pg_xpatch UPDATE TO '0.2.0';`

### Technical Details

- Modified files: `xpatch_config.c`, `xpatch_storage.c`, `xpatch_storage.h`, `xpatch_tam.c`, `xpatch_utils.c`
- All 337 tests pass (42 functional + 49 stress + 101 comprehensive + 54 adversarial + 57 edge case + 18 final + 10 concurrency + 6 concurrent)

## [0.1.1] - 2025-01-20

### Fixed

- **pg_dump/pg_restore data corruption**: Fixed critical bug where data was corrupted after pg_restore. The `xpatch.table_config` table data was not included in dumps, causing configuration loss and incorrect delta reconstruction.
- **pg_dump/pg_restore crash**: Fixed segfault when accessing restored tables. The SPI result was invalidated during OID update, causing crash on first table access after restore.

### Changed

- Added `pg_extension_config_dump()` call to ensure `xpatch.table_config` data is included in database dumps
- Existing installations can upgrade with: `ALTER EXTENSION pg_xpatch UPDATE TO '0.1.1';`

### Documentation

- Added comprehensive stress testing documentation (240 tests covering data types, transactions, concurrency, crash recovery, adversarial inputs, edge cases, and backup/restore)

