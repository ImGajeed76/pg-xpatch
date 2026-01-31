-- pg_xpatch upgrade script: 0.3.1 -> 0.4.0
--
-- Changes in 0.4.0:
-- - Added stats cache for O(1) lookups of table/group statistics
-- - xpatch_stats() reads from cache, falls back to scan if empty
-- - Incremental updates on INSERT and DELETE (no more invalidation)
-- - New metric: avg_compression_depth (replaces avg_chain_length)

-- ============================================================================
-- Stats cache table
-- ============================================================================

-- Per-group statistics (updated incrementally on INSERT/DELETE)
CREATE TABLE xpatch.group_stats (
    relid                 OID NOT NULL,
    group_hash            BYTEA NOT NULL,           -- BLAKE3 hash of group value (16 bytes)
    row_count             BIGINT NOT NULL DEFAULT 0,
    keyframe_count        BIGINT NOT NULL DEFAULT 0,
    max_seq               INT NOT NULL DEFAULT 0,
    raw_size_bytes        BIGINT NOT NULL DEFAULT 0,
    compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    sum_avg_delta_tags    FLOAT8 NOT NULL DEFAULT 0,  -- Sum of per-row average delta tags
    PRIMARY KEY (relid, group_hash)
);

-- Index for fast aggregation by table
CREATE INDEX group_stats_relid_idx ON xpatch.group_stats(relid);

COMMENT ON TABLE xpatch.group_stats IS 
    'Per-group statistics cache. Updated incrementally on INSERT, deleted on DELETE. Missing groups are recomputed on demand during stats() calls.';

-- ============================================================================
-- Stats cache management functions
-- ============================================================================

-- Check if stats exist for a table (for deciding whether to use cache or full scan)
CREATE OR REPLACE FUNCTION xpatch.stats_exist(table_name REGCLASS)
RETURNS BOOLEAN AS $$
    SELECT EXISTS(SELECT 1 FROM xpatch.group_stats WHERE relid = table_name::OID);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.stats_exist(regclass) IS 
    'Check if cached statistics exist for a table. Used internally to decide cache vs full scan.';

-- Force recompute all stats for a table by doing a full scan
-- This is the slow path, only needed after bulk operations or when stats are missing
CREATE OR REPLACE FUNCTION xpatch.refresh_stats(table_name REGCLASS)
RETURNS TABLE (
    groups_scanned  BIGINT,
    rows_scanned    BIGINT,
    duration_ms     FLOAT8
) AS $$
DECLARE
    v_relid         OID;
    v_start_time    TIMESTAMPTZ;
    v_groups        BIGINT := 0;
    v_rows          BIGINT := 0;
BEGIN
    v_relid := table_name::OID;
    v_start_time := clock_timestamp();
    
    -- Delete existing stats for this table
    DELETE FROM xpatch.group_stats WHERE relid = v_relid;
    
    -- Call internal C function to scan and populate stats
    -- This populates xpatch.group_stats directly
    SELECT * INTO v_groups, v_rows FROM xpatch_refresh_stats_internal(v_relid);
    
    groups_scanned := v_groups;
    rows_scanned := v_rows;
    duration_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION xpatch.refresh_stats(regclass) IS 
    'Force recompute all statistics for a table by doing a full scan. Call this after bulk operations or when stats appear stale.';

-- Internal C function for stats refresh (full table scan)
CREATE FUNCTION xpatch_refresh_stats_internal(relid OID)
RETURNS TABLE (groups_count BIGINT, rows_count BIGINT)
AS 'MODULE_PATHNAME', 'xpatch_refresh_stats_internal'
LANGUAGE C STRICT;

-- Internal C function to update group stats on INSERT (called from TAM)
CREATE FUNCTION xpatch_update_group_stats(
    p_relid OID,
    p_group_hash BYTEA,
    p_is_keyframe BOOLEAN,
    p_max_seq INT,
    p_raw_size BIGINT,
    p_compressed_size BIGINT,
    p_avg_delta_tag FLOAT8
) RETURNS VOID
AS 'MODULE_PATHNAME', 'xpatch_update_group_stats'
LANGUAGE C;


