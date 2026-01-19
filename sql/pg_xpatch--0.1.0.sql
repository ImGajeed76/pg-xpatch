-- pg_xpatch extension SQL definitions
-- Version 0.1.0

-- Complain if script is sourced in psql rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_xpatch" to load this file. \quit

-- Create schema to hold our objects
CREATE SCHEMA IF NOT EXISTS xpatch;

-- Configuration catalog table for xpatch tables
-- Used by xpatch.configure() for explicit configuration when auto-detection isn't enough
CREATE TABLE xpatch.table_config (
    relid           OID PRIMARY KEY,        -- pg_class OID of the table
    group_by        TEXT,                   -- Column name for grouping rows (NULL = single group)
    order_by        TEXT,                   -- Column name for ordering versions (NULL = auto-detect)
    delta_columns   TEXT[],                 -- Array of column names to delta-compress (NULL = auto-detect)
    keyframe_every  INT NOT NULL DEFAULT 100,  -- Create keyframe every N rows
    compress_depth  INT NOT NULL DEFAULT 1,    -- Number of previous versions to try
    enable_zstd     BOOLEAN NOT NULL DEFAULT false, -- Enable zstd compression
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT valid_keyframe_every CHECK (keyframe_every >= 1 AND keyframe_every <= 10000),
    CONSTRAINT valid_compress_depth CHECK (compress_depth >= 1 AND compress_depth <= 65535)
);

COMMENT ON TABLE xpatch.table_config IS 
    'Optional configuration for xpatch tables. Most tables work fine with auto-detection.';

-- Create the table access method handler function
CREATE FUNCTION xpatch_tam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'xpatch_tam_handler'
LANGUAGE C STRICT;

-- Register the xpatch table access method
CREATE ACCESS METHOD xpatch TYPE TABLE HANDLER xpatch_tam_handler;
COMMENT ON ACCESS METHOD xpatch IS 'Table access method for delta-compressed versioned data';

-- Function to configure an xpatch table
-- Call this after CREATE TABLE when you need explicit configuration.
-- Most tables work fine with auto-detection and don't need this.
--
-- Example:
--   CREATE TABLE docs (doc_id INT, rev INT, body TEXT) USING xpatch;
--   SELECT xpatch.configure('docs', group_by => 'doc_id');
--
CREATE OR REPLACE FUNCTION xpatch.configure(
    table_name REGCLASS,
    group_by TEXT DEFAULT NULL,          -- Column for grouping (NULL = single version chain)
    order_by TEXT DEFAULT NULL,          -- Column for ordering (NULL = auto-detect last INT)
    delta_columns TEXT[] DEFAULT NULL,   -- Columns to compress (NULL = auto-detect TEXT/BYTEA/JSON)
    keyframe_every INT DEFAULT 100,      -- Full snapshot every N rows
    compress_depth INT DEFAULT 1,        -- Try N previous versions for best delta
    enable_zstd BOOLEAN DEFAULT false    -- Additional zstd compression
) RETURNS VOID AS $$
DECLARE
    v_relid OID;
    v_amname NAME;
    v_col TEXT;
BEGIN
    v_relid := table_name::OID;
    
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
    
    -- Validate delta_columns exist
    IF delta_columns IS NOT NULL THEN
        FOREACH v_col IN ARRAY delta_columns
        LOOP
            IF NOT EXISTS (
                SELECT 1 FROM pg_attribute 
                WHERE attrelid = v_relid AND attname = v_col AND NOT attisdropped
            ) THEN
                RAISE EXCEPTION 'Column "%" does not exist in table "%"', v_col, table_name;
            END IF;
        END LOOP;
    END IF;
    
    -- Upsert config (delete + insert for simplicity)
    DELETE FROM xpatch.table_config WHERE relid = v_relid;
    
    INSERT INTO xpatch.table_config (relid, group_by, order_by, delta_columns, 
                                     keyframe_every, compress_depth, enable_zstd)
    VALUES (v_relid, group_by, order_by, delta_columns, 
            keyframe_every, compress_depth, enable_zstd);
    
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
    'Configure an xpatch table. Optional - most tables work with auto-detection.';

-- Function to get configuration for an xpatch table
CREATE OR REPLACE FUNCTION xpatch.get_config(table_name REGCLASS)
RETURNS TABLE (
    group_by        TEXT,
    order_by        TEXT,
    delta_columns   TEXT[],
    keyframe_every  INT,
    compress_depth  INT,
    enable_zstd     BOOLEAN
) AS $$
    SELECT group_by, order_by, delta_columns, keyframe_every, compress_depth, enable_zstd
    FROM xpatch.table_config
    WHERE relid = table_name::OID;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.get_config IS 
    'Get the xpatch configuration for a table';

-- Event trigger to clean up config when tables are dropped
CREATE OR REPLACE FUNCTION xpatch._cleanup_dropped_tables()
RETURNS event_trigger AS $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'table' THEN
            DELETE FROM xpatch.table_config WHERE relid = obj.objid;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER xpatch_cleanup_on_drop
    ON sql_drop
    EXECUTE FUNCTION xpatch._cleanup_dropped_tables();

-- Utility function: Get compression statistics for a table
CREATE FUNCTION xpatch_stats(rel regclass)
RETURNS TABLE (
    total_rows          BIGINT,
    total_groups        BIGINT,
    keyframe_count      BIGINT,
    delta_count         BIGINT,
    raw_size_bytes      BIGINT,
    compressed_size_bytes BIGINT,
    compression_ratio   FLOAT8,
    cache_hits          BIGINT,
    cache_misses        BIGINT,
    avg_chain_length    FLOAT8
) AS 'MODULE_PATHNAME', 'xpatch_stats'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_stats(regclass) IS 'Get compression statistics for an xpatch table';

-- Utility function: Inspect a specific group within a table
CREATE FUNCTION xpatch_inspect(rel regclass, group_value anyelement)
RETURNS TABLE (
    version             BIGINT,
    seq                 INT,
    is_keyframe         BOOL,
    tag                 INT,
    delta_size_bytes    INT,
    column_name         TEXT
) AS 'MODULE_PATHNAME', 'xpatch_inspect'
LANGUAGE C;

COMMENT ON FUNCTION xpatch_inspect(regclass, anyelement) IS 'Inspect rows within a specific group of an xpatch table';

-- Utility function: Get global cache statistics
CREATE FUNCTION xpatch_cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT
) AS 'MODULE_PATHNAME', 'xpatch_cache_stats_fn'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_cache_stats() IS 'Get global LRU cache statistics';

-- Utility function: Get xpatch library version
CREATE FUNCTION xpatch_version()
RETURNS TEXT AS 'MODULE_PATHNAME', 'pg_xpatch_version'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_version() IS 'Get xpatch library version';

-- Internal function to invalidate cached config (called by xpatch.configure)
CREATE FUNCTION xpatch._invalidate_config(rel OID)
RETURNS VOID AS 'MODULE_PATHNAME', 'xpatch_invalidate_config_fn'
LANGUAGE C STRICT;
