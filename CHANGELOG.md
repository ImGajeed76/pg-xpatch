# Changelog

All notable changes to pg-xpatch will be documented in this file.

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
