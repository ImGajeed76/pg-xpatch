-- pg_xpatch upgrade script: 0.3.1 -> 0.4.0
--
-- Changes in 0.4.0:
-- - Added stats cache for O(1) lookups of table/group statistics
-- - xpatch_stats() now reads from cache instead of doing full table scan
-- - Added xpatch.refresh_stats() to force recompute stats

-- ============================================================================
-- Stats cache tables
-- ============================================================================

-- Per-group statistics (authoritative source, updated on INSERT)
CREATE TABLE xpatch.group_stats (
    relid               OID NOT NULL,
    group_hash          BYTEA NOT NULL,         -- BLAKE3 hash of group value (32 bytes)
    group_value_text    TEXT,                   -- Human-readable group value (for display)
    row_count           BIGINT NOT NULL DEFAULT 0,
    keyframe_count      BIGINT NOT NULL DEFAULT 0,
    delta_count         BIGINT NOT NULL DEFAULT 0,
    max_seq             INT NOT NULL DEFAULT 0,
    max_version_typid   OID,                    -- Type OID of order_by column
    max_version_data    BYTEA,                  -- Serialized max version value
    raw_size_bytes      BIGINT NOT NULL DEFAULT 0,
    compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    is_valid            BOOLEAN NOT NULL DEFAULT true,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (relid, group_hash)
);

CREATE INDEX group_stats_relid_idx ON xpatch.group_stats(relid) WHERE is_valid;

COMMENT ON TABLE xpatch.group_stats IS 
    'Per-group statistics cache. Updated incrementally on INSERT, invalidated on DELETE. Use xpatch.refresh_stats() to recompute after bulk operations.';

-- Per-table aggregated statistics (cached, derived from group_stats)
CREATE TABLE xpatch.table_stats (
    relid               OID PRIMARY KEY,
    total_rows          BIGINT NOT NULL DEFAULT 0,
    total_groups        BIGINT NOT NULL DEFAULT 0,
    keyframe_count      BIGINT NOT NULL DEFAULT 0,
    delta_count         BIGINT NOT NULL DEFAULT 0,
    raw_size_bytes      BIGINT NOT NULL DEFAULT 0,
    compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    compression_ratio   FLOAT8,
    avg_chain_length    FLOAT8,
    is_valid            BOOLEAN NOT NULL DEFAULT true,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE xpatch.table_stats IS 
    'Per-table aggregated statistics. Derived from xpatch.group_stats for fast xpatch_stats() queries.';

-- ============================================================================
-- Stats cache management functions
-- ============================================================================

-- Internal: Recompute table_stats from group_stats
CREATE OR REPLACE FUNCTION xpatch._recompute_table_stats(p_relid OID)
RETURNS VOID AS $$
BEGIN
    INSERT INTO xpatch.table_stats (
        relid, total_rows, total_groups, keyframe_count, delta_count,
        raw_size_bytes, compressed_size_bytes, compression_ratio, avg_chain_length,
        is_valid, last_updated
    )
    SELECT 
        p_relid,
        COALESCE(SUM(row_count), 0),
        COUNT(*),
        COALESCE(SUM(keyframe_count), 0),
        COALESCE(SUM(delta_count), 0),
        COALESCE(SUM(raw_size_bytes), 0),
        COALESCE(SUM(compressed_size_bytes), 0),
        CASE WHEN SUM(compressed_size_bytes) > 0 
             THEN SUM(raw_size_bytes)::FLOAT8 / SUM(compressed_size_bytes)::FLOAT8
             ELSE NULL END,
        CASE WHEN SUM(keyframe_count) > 0
             THEN SUM(delta_count)::FLOAT8 / SUM(keyframe_count)::FLOAT8
             ELSE NULL END,
        NOT EXISTS(SELECT 1 FROM xpatch.group_stats WHERE relid = p_relid AND NOT is_valid),
        now()
    FROM xpatch.group_stats
    WHERE relid = p_relid AND is_valid
    ON CONFLICT (relid) DO UPDATE SET
        total_rows = EXCLUDED.total_rows,
        total_groups = EXCLUDED.total_groups,
        keyframe_count = EXCLUDED.keyframe_count,
        delta_count = EXCLUDED.delta_count,
        raw_size_bytes = EXCLUDED.raw_size_bytes,
        compressed_size_bytes = EXCLUDED.compressed_size_bytes,
        compression_ratio = EXCLUDED.compression_ratio,
        avg_chain_length = EXCLUDED.avg_chain_length,
        is_valid = EXCLUDED.is_valid,
        last_updated = EXCLUDED.last_updated;
END;
$$ LANGUAGE plpgsql;

-- Check if stats for a table are valid (no invalidated groups)
CREATE OR REPLACE FUNCTION xpatch.stats_valid(table_name REGCLASS)
RETURNS BOOLEAN AS $$
    SELECT COALESCE(
        (SELECT is_valid FROM xpatch.table_stats WHERE relid = table_name::OID),
        false
    );
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.stats_valid(regclass) IS 
    'Check if cached statistics for a table are valid. Returns false if any group was modified since last refresh.';

-- Force recompute all stats for a table by doing a full scan
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
    DELETE FROM xpatch.table_stats WHERE relid = v_relid;
    
    -- Call internal C function to scan and populate stats
    SELECT * INTO v_groups, v_rows FROM xpatch_refresh_stats_internal(v_relid);
    
    -- Recompute table_stats from group_stats
    PERFORM xpatch._recompute_table_stats(v_relid);
    
    groups_scanned := v_groups;
    rows_scanned := v_rows;
    duration_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_start_time)) * 1000;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION xpatch.refresh_stats(regclass) IS 
    'Force recompute all statistics for a table by doing a full scan. Call this after bulk DELETE operations or when stats appear stale.';

-- Internal C function for stats refresh (full table scan)
CREATE FUNCTION xpatch_refresh_stats_internal(relid OID)
RETURNS TABLE (groups_count BIGINT, rows_count BIGINT)
AS 'MODULE_PATHNAME', 'xpatch_refresh_stats_internal'
LANGUAGE C STRICT;

-- Internal C function to update group stats on INSERT
CREATE FUNCTION xpatch_update_group_stats(
    p_relid OID,
    p_group_hash BYTEA,
    p_group_value_text TEXT,
    p_is_keyframe BOOLEAN,
    p_max_seq INT,
    p_max_version_typid OID,
    p_max_version_data BYTEA,
    p_raw_size BIGINT,
    p_compressed_size BIGINT
) RETURNS VOID
AS 'MODULE_PATHNAME', 'xpatch_update_group_stats'
LANGUAGE C;

-- Internal C function to invalidate group stats on DELETE
CREATE FUNCTION xpatch_invalidate_group_stats(
    p_relid OID,
    p_group_hash BYTEA
) RETURNS VOID
AS 'MODULE_PATHNAME', 'xpatch_invalidate_group_stats'
LANGUAGE C;
