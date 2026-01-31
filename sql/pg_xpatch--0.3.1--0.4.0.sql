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

-- ============================================================================
-- Update configure function to validate delta columns are NOT NULL
-- ============================================================================

CREATE OR REPLACE FUNCTION xpatch.configure(
    table_name REGCLASS,
    group_by TEXT DEFAULT NULL,          -- Column for grouping (NULL = single version chain)
    order_by TEXT DEFAULT NULL,          -- Column for ordering (NULL = auto-detect last INT)
    delta_columns TEXT[] DEFAULT NULL,   -- Columns to compress (NULL = auto-detect TEXT/BYTEA/JSON)
    keyframe_every INT DEFAULT 100,      -- Full snapshot every N rows
    compress_depth INT DEFAULT 1,        -- Try N previous versions for best delta
    enable_zstd BOOLEAN DEFAULT true     -- Additional zstd compression
) RETURNS VOID AS $$
DECLARE
    v_relid OID;
    v_amname NAME;
    v_col TEXT;
BEGIN
    v_relid := table_name::OID;
    
    -- Verify caller owns the table (security check)
    IF NOT pg_catalog.has_table_privilege(current_user, v_relid, 'INSERT') THEN
        RAISE EXCEPTION 'permission denied: must have INSERT privilege on table "%"', table_name;
    END IF;
    
    -- Verify table uses xpatch access method
    SELECT a.amname INTO v_amname
    FROM pg_class c
    JOIN pg_am a ON c.relam = a.oid
    WHERE c.oid = v_relid;
    
    IF v_amname IS NULL OR v_amname != 'xpatch' THEN
        RAISE EXCEPTION 'Table "%" is not using the xpatch access method', table_name;
    END IF;
    
    -- Validate group_by column exists
    IF group_by IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute 
            WHERE attrelid = v_relid AND attname = group_by AND NOT attisdropped
        ) THEN
            RAISE EXCEPTION 'Column "%" does not exist in table "%"', group_by, table_name;
        END IF;
    END IF;
    
    -- Validate order_by column exists
    IF order_by IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute 
            WHERE attrelid = v_relid AND attname = order_by AND NOT attisdropped
        ) THEN
            RAISE EXCEPTION 'Column "%" does not exist in table "%"', order_by, table_name;
        END IF;
    END IF;
    
    -- Validate delta_columns exist and are NOT NULL
    IF delta_columns IS NOT NULL THEN
        FOREACH v_col IN ARRAY delta_columns
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM pg_attribute 
                WHERE attrelid = v_relid AND attname = v_col AND NOT attisdropped
            ) THEN
                RAISE EXCEPTION 'Column "%" does not exist in table "%"', v_col, table_name;
            END IF;
            
            -- Check that delta column is NOT NULL (nullable columns cannot be delta-encoded)
            IF EXISTS (
                SELECT 1 FROM pg_attribute 
                WHERE attrelid = v_relid AND attname = v_col AND NOT attnotnull
            ) THEN
                RAISE EXCEPTION 'Delta column "%" must be NOT NULL. Add a NOT NULL constraint before configuring.', v_col;
            END IF;
        END LOOP;
    END IF;
    
    -- Validate keyframe_every is positive
    IF keyframe_every IS NOT NULL AND keyframe_every < 1 THEN
        RAISE EXCEPTION 'keyframe_every must be at least 1, got %', keyframe_every;
    END IF;
    
    -- Validate compress_depth is at least 1 (matches table constraint)
    IF compress_depth IS NOT NULL AND compress_depth < 1 THEN
        RAISE EXCEPTION 'compress_depth must be at least 1, got %', compress_depth;
    END IF;
    
    -- Upsert config (delete + insert for simplicity)
    DELETE FROM xpatch.table_config WHERE relid = v_relid;
    
    INSERT INTO xpatch.table_config (relid, schema_name, table_name, group_by, order_by, 
                                     delta_columns, keyframe_every, compress_depth, enable_zstd)
    SELECT v_relid, n.nspname, c.relname, group_by, order_by, 
           delta_columns, keyframe_every, compress_depth, enable_zstd
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = v_relid;
    
    -- Create composite index on (group_by, _xp_seq) for efficient delta chain lookups
    -- This is critical for performance when reconstructing rows
    IF group_by IS NOT NULL THEN
        DECLARE
            v_schema TEXT;
            v_tbl TEXT;
            v_idx_name TEXT;
        BEGIN
            SELECT n.nspname, c.relname INTO v_schema, v_tbl
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = v_relid;
            
            v_idx_name := v_tbl || '_xp_group_seq_idx';
            
            -- Drop the basic _xp_seq index if it exists (we're replacing with composite)
            EXECUTE format('DROP INDEX IF EXISTS %I.%I', v_schema, v_tbl || '_xp_seq_idx');
            
            -- Create composite index if it doesn't exist
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind = 'i' AND c.relname = v_idx_name AND n.nspname = v_schema
            ) THEN
                EXECUTE format('CREATE INDEX %I ON %s (%I, _xp_seq)', v_idx_name, table_name, group_by);
                RAISE NOTICE 'xpatch: created index % on (%s, _xp_seq)', v_idx_name, group_by;
            END IF;
        END;
    END IF;
    
    -- Invalidate cached config so changes take effect immediately
    PERFORM xpatch._invalidate_config(v_relid);
            
    RAISE NOTICE 'xpatch: configured "%" (group_by=%, order_by=%, keyframe_every=%)',
        table_name, 
        COALESCE(group_by, '(none)'), 
        COALESCE(order_by, '(auto)'), 
        keyframe_every;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION xpatch.configure IS 
    'Configure an xpatch table. Creates a composite (group_by, _xp_seq) index when group_by is set. Delta columns must be NOT NULL.';
