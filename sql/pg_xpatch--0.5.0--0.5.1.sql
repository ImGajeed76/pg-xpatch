-- pg_xpatch upgrade script: 0.5.0 -> 0.5.1
--
-- Changes in 0.5.1:
--   - Fixed: cache silently rejected entries >64KB, causing repeated delta
--     chain walks for large files. Default limit raised to 256KB.
--   - New GUC: pg_xpatch.cache_max_entry_kb (default 256, runtime-tunable by superuser)
--   - Added skip_count to xpatch_cache_stats() / xpatch.cache_stats()
--   - WARNING logged on first oversized cache skip per backend

-- ============================================================================
-- 1. Drop old functions (wrapper first, then C function)
-- ============================================================================

DROP FUNCTION IF EXISTS xpatch.cache_stats();
DROP FUNCTION IF EXISTS xpatch_cache_stats();

-- ============================================================================
-- 2. Recreate xpatch_cache_stats() C function with skip_count column
-- ============================================================================

CREATE FUNCTION xpatch_cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT,
    skip_count          BIGINT
) AS 'MODULE_PATHNAME', 'xpatch_cache_stats_fn'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_cache_stats() IS 'Get global LRU cache statistics (includes skip_count for entries rejected by size limit)';

-- ============================================================================
-- 3. Recreate xpatch.cache_stats() wrapper with skip_count column
-- ============================================================================

CREATE OR REPLACE FUNCTION xpatch.cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT,
    skip_count          BIGINT
) AS $$
    SELECT * FROM xpatch_cache_stats();
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.cache_stats() IS 'Get global LRU cache statistics (includes skip_count for entries rejected by size limit)';
