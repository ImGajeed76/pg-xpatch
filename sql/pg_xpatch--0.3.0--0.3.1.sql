-- pg_xpatch upgrade script: 0.3.0 -> 0.3.1
--
-- Changes in 0.3.1:
-- - Fixed insert cache race condition that could cause delta encoding corruption
-- - Added xpatch_insert_cache_stats() function to monitor insert cache health

-- Add xpatch_insert_cache_stats() function
CREATE FUNCTION xpatch_insert_cache_stats()
RETURNS TABLE (
    slots_in_use        BIGINT,
    total_slots         BIGINT,
    hits                BIGINT,
    misses              BIGINT,
    evictions           BIGINT,
    eviction_misses     BIGINT
) AS 'MODULE_PATHNAME', 'xpatch_insert_cache_stats_fn'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_insert_cache_stats() IS 'Get insert cache (FIFO) statistics including eviction_misses for race condition detection';

-- Add xpatch.insert_cache_stats() wrapper
CREATE OR REPLACE FUNCTION xpatch.insert_cache_stats()
RETURNS TABLE (
    slots_in_use        BIGINT,
    total_slots         BIGINT,
    hits                BIGINT,
    misses              BIGINT,
    evictions           BIGINT,
    eviction_misses     BIGINT
) AS $$
    SELECT * FROM xpatch_insert_cache_stats();
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.insert_cache_stats() IS 'Get insert cache (FIFO) statistics including eviction_misses for race condition detection';
