# Changelog

All notable changes to pg-xpatch will be documented in this file.

## [0.6.2] - 2026-02-22

### Fixed

- **Critical: Race condition in encode pool causing permanent backend hang** - In `xpatch_encode_pool_execute()`, there was a race window between resetting `next_task` (line 359) and `completed` (line 360). A straggler worker from the previous batch could grab a task from the new batch (after `next_task` was reset to 0) and increment `completed`, only to have that increment wiped when `completed` was reset to 0 on the next line. This caused the main thread's spin-wait (`while (completed < num_tasks)`) to hang permanently. The hang was non-deterministic, triggered by high `encode_threads` (32), multiple small delta columns, and rapid COPY dispatches. Fixed by adding an `atomic_int workers_in_flight` counter: workers increment it when entering the task loop and decrement when exiting. The main thread now drains all stragglers (`while (workers_in_flight > 0)`) before resetting counters for the next batch.

- **Backend unkillable during encode pool hang** - The spin-wait loop in `xpatch_encode_pool_execute()` had no `CHECK_FOR_INTERRUPTS()` call, so a hung backend could not be cancelled via `pg_cancel_backend()` or `pg_terminate_backend()`. Added `CHECK_FOR_INTERRUPTS()` to the spin-wait loop.

## [0.6.1] - 2026-02-20

### Fixed

- **Critical: REPEATABLE READ and SERIALIZABLE isolation broken** - The hand-rolled `xpatch_tuple_is_visible()` function ignored the snapshot entirely, using raw `TransactionIdDidCommit()` instead of checking against the snapshot's xmin/xmax boundaries. This meant REPEATABLE READ transactions could see rows committed after the snapshot was taken, and SERIALIZABLE isolation was completely ineffective. Replaced with PostgreSQL's `HeapTupleSatisfiesVisibility()` which properly handles all isolation levels. Added a pre-check for same-transaction deletes to avoid `TriggeredDataChangeViolation` during xpatch's cascading DELETE.

- **Critical: Use-after-free in DELETE with TEXT/VARCHAR/BYTEA group keys** - In the DELETE path, `heap_getattr()` returns a pointer into the buffer page for pass-by-reference types (TEXT, VARCHAR, BYTEA). The buffer was released before the group value was used for hash computation, group comparison during cascading delete, and seq cache updates. This was a dangling pointer for any non-integer group key type. Fixed by calling `datumCopy()` before releasing the buffer.

- **Critical: Hint bit write under shared buffer lock** - The old `xpatch_tuple_is_visible()` wrote `t_infomask |= HEAP_XMAX_INVALID` (a hint bit) while holding only `BUFFER_LOCK_SHARE`, which is a non-atomic write on a shared page. This could corrupt hint bits under concurrent access. Eliminated by replacing the entire function with `HeapTupleSatisfiesVisibility()`, which handles hint bits correctly via `SetHintBits()`.

- **`xpatch_get_max_seq` counted same-transaction deleted tuples** - On cache miss, the max sequence scan checked `TransactionIdDidCommit(xmax)` to skip deleted tuples, but this returns false for the current transaction's own deletes. A delete-then-insert within the same transaction would see the deleted tuple's sequence number as still valid, producing stale max_seq values and potential sequence gaps or duplicates. Fixed by adding `TransactionIdIsCurrentTransactionId(xmax)` check.

- **Stale page metadata after buffer unlock/relock in stats refresh** - `xpatch_stats_cache_refresh_groups()` unlocked the buffer for delta reconstruction, then re-locked and continued iteration with a stale `maxoff` variable. Although the buffer pin prevents VACUUM from reclaiming tuples, `maxoff` is now re-read after re-locking as a defensive measure.

### Technical

- **567 tests** across 28 test files, all passing. 10 new test files added for regression coverage of all investigated bugs.
- Stats batching: replaced per-row SPI with in-memory `HTAB` accumulator flushed at `XACT_EVENT_PRE_COMMIT`. O(groups) SPI calls instead of O(rows).
- MVCC defense-in-depth: added visibility check to Strategy 3 sequential scan fallback in `xpatch_fetch_by_seq`.

## [0.6.0] - 2026-02-19

### Added

- **Lock striping for shared cache**: The content cache is now partitioned into N independent stripes (default 32), each with its own LWLock, LRU list, entry hash table, and slot free list. This eliminates contention between concurrent backends accessing different stripes. Controlled via `pg_xpatch.cache_partitions` GUC (1-256, default 32).

- **5 new GUCs for full configurability**:
  - `pg_xpatch.cache_max_entries` — Maximum cache entries (default 65536, min 1000, max INT_MAX). Replaces hardcoded `XPATCH_SHMEM_MAX_ENTRIES`.
  - `pg_xpatch.cache_slot_size_kb` — Content slot size (default 4 KB, 1-64 KB). Replaces hardcoded `XPATCH_SLOT_SIZE` and the `XPatchContentSlot` struct.
  - `pg_xpatch.cache_partitions` — Number of lock stripes (default 32, 1-256).
  - `pg_xpatch.seq_tid_cache_size_mb` — Seq-to-TID cache size (default 16 MB). Previously existed as an internal variable but was never exposed as a GUC.
  - `pg_xpatch.max_delta_columns` — Maximum delta columns per table (default 32, min 1, max INT_MAX). Replaces hardcoded `XPATCH_MAX_DELTA_COLUMNS`.

### Changed

- **GUC defaults and maximums raised** for large-scale workloads (target: Linux kernel history, 24M+ entries, 80K+ groups):
  - `cache_size_mb`: default 64 → **256**, max 1024 → **INT_MAX**
  - `cache_max_entry_kb`: max 4096 → **INT_MAX**
  - `group_cache_size_mb`: default 8 → **16**, max 256 → **INT_MAX**
  - `tid_cache_size_mb`: default 8 → **16**, max 256 → **INT_MAX**
  - `insert_cache_slots`: max 256 → **INT_MAX**

- **All hardcoded constants removed**: `XPATCH_SHMEM_MAX_ENTRIES`, `XPATCH_SLOT_SIZE`, and `XPATCH_MAX_DELTA_COLUMNS` have been replaced with GUC-backed variables. The `XPatchContentSlot` struct was removed in favor of pointer-arithmetic helpers computed once at startup.

- **Cache stats now aggregate across stripes**: `xpatch.cache_stats()` and `xpatch_cache_stats()` iterate all stripes and sum hits, misses, evictions, skip_count, entries_count, and size_bytes.

### Fixed

- **Insert cache drops empty content, causing stale delta bases**: The FIFO insert cache (`xpatch_insert_cache_push()`) silently discarded rows with empty content (size=0 early return), but `commit_entry()` still advanced the ring head. Subsequent inserts then computed deltas against stale non-empty predecessors instead of the correct empty predecessor. This produced incorrect deltas on disk (wrong tag values), though the decoder compensated and returned correct data on read. The bug only affected the warm INSERT path (individual INSERTs); bulk COPY was unaffected. Fixed by storing empty content entries in the ring buffer and returning them as valid bases in `get_bases()`.

### Technical

- **11 total GUCs** (5 new, 5 modified, 1 unchanged). All visible in `pg_settings`.
- **496 tests** across 18 test files (27 new in `test_guc_settings.py`, 9 new in `test_empty_content.py`).
- Memory layout: `[header + stripe array][entry arrays][slot buffers]`, with entries and slots distributed evenly across stripes.

## [0.5.1] - 2026-02-15

### Fixed

- **Cache silently rejected entries >64KB**: The shared LRU cache had a hardcoded 64KB per-entry size limit that silently discarded any reconstructed content exceeding that threshold. This caused repeated delta chain walks for large files (e.g., a 127KB file required ~4.7s per lookup with zero cache benefit). Default limit raised to 256KB.

### Added

- **New GUC `pg_xpatch.cache_max_entry_kb`**: Configurable maximum cache entry size (default 256 KB, min 16 KB, max 4 MB). Tunable at runtime by superusers (`PGC_SUSET`) without requiring a server restart.

- **Cache skip observability**: `xpatch_cache_stats()` and `xpatch.cache_stats()` now return a `skip_count` column showing how many entries were rejected by the size limit. A `WARNING` is logged on the first skip per backend to aid diagnosis.

## [0.4.0] - 2026-01-31

### Added

- **Stats cache**: Statistics are now stored in `xpatch.group_stats` table and updated incrementally on INSERT/DELETE. The `xpatch.stats()` function now returns instantly (~0.4ms) instead of scanning the entire table.

- **New utility functions**:
  - `xpatch.refresh_stats(table)` - Force recalculate stats by full table scan (rarely needed)
  - `xpatch.physical(table)` - View raw physical storage including delta bytes
  - `xpatch.stats_exist(table)` - Check if stats cache exists for a table

### Changed

- **Delta columns must be NOT NULL**: The `xpatch.configure()` function now validates that delta columns have a NOT NULL constraint. Attempting to configure a nullable column for delta compression will raise an error with a helpful message.

- **Function naming**: All utility functions now use schema-qualified names (`xpatch.stats()`, `xpatch.inspect()`, `xpatch.cache_stats()`, `xpatch.version()`). The old unqualified names (`xpatch_stats()`, etc.) still work for backwards compatibility.

### Fixed

- **Advisory lock hashing**: Fixed a bug where TEXT group values would hash the pointer instead of the content when computing advisory lock IDs. Now uses BLAKE3 consistently.

- **Raw size tracking in stats**: Stats refresh now decodes delta columns to get actual uncompressed sizes, consistent with INSERT tracking.

- **Empty group handling**: When a group has 0 visible rows after DELETE, its stats are now properly deleted from `xpatch.group_stats`.

- **NULL group validation**: Inserting a NULL value into the group_by column now raises a clear error instead of causing undefined behavior. The group column must have a non-NULL value for each row.

## [0.3.1] - 2025-01-29

### Fixed

- **Critical: Insert cache race condition** - Fixed a race condition where insert cache slots could be evicted while another process was still using them, leading to delta encoding corruption and "xpatch decode error" on read. The fix validates ownership (relid + group_hash) after acquiring the lock; if the slot was evicted, operations gracefully fall back to reconstruction.

### Added

- **Insert cache stats function** - Added `xpatch_insert_cache_stats()` / `xpatch.insert_cache_stats()` to monitor insert cache health. Returns `slots_in_use`, `total_slots`, `hits`, `misses`, `evictions`, and `eviction_misses` (race condition detections).

- **Eviction miss warning** - When a race condition is detected, a WARNING is logged once per backend suggesting to increase `pg_xpatch.insert_cache_slots` or reduce concurrent writers.

## [0.3.0] - 2025-01-28 [YANKED - DO NOT USE]

**WARNING: This version contains a critical bug that can cause data corruption under concurrent writes. Upgrade to v0.3.1 immediately.**

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

