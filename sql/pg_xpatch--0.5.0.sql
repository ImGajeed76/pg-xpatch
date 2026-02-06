-- pg_xpatch extension SQL definitions
-- Version 0.5.0
--
-- Changes from 0.4.0:
--   - _xp_seq column type changed from INT to BIGINT (supports >2.1B rows/group)
--   - group_stats.max_seq changed from INT to BIGINT
--   - xpatch.configure() now validates order_by column type (E17)
--   - xpatch.configure() now validates auto-detection feasibility (E13)
--   - xpatch_inspect/xpatch_physical seq columns changed from INT to BIGINT
--   - xpatch_update_group_stats p_max_seq changed from INT to BIGINT

-- Complain if script is sourced in psql rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_xpatch" to load this file. \quit

-- Create schema to hold our objects
CREATE SCHEMA IF NOT EXISTS xpatch;

-- Configuration catalog table for xpatch tables
-- Used by xpatch.configure() for explicit configuration when auto-detection isn't enough
-- Stores both OID (for runtime lookup) and names (for dump/restore portability)
CREATE TABLE xpatch.table_config (
    relid           OID PRIMARY KEY,        -- pg_class OID of the table (runtime lookup)
    schema_name     TEXT NOT NULL,          -- Schema name (for dump/restore)
    table_name      TEXT NOT NULL,          -- Table name (for dump/restore)
    group_by        TEXT,                   -- Column name for grouping rows (NULL = single group)
    order_by        TEXT,                   -- Column name for ordering versions (NULL = auto-detect)
    delta_columns   TEXT[],                 -- Array of column names to delta-compress (NULL = auto-detect)
    keyframe_every  INT NOT NULL DEFAULT 100,  -- Create keyframe every N rows
    compress_depth  INT NOT NULL DEFAULT 1,    -- Number of previous versions to try
    enable_zstd     BOOLEAN NOT NULL DEFAULT true,  -- Enable zstd compression
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT valid_keyframe_every CHECK (keyframe_every >= 1 AND keyframe_every <= 10000),
    CONSTRAINT valid_compress_depth CHECK (compress_depth >= 1 AND compress_depth <= 65535)
);

-- Index for lookup by name (used during restore)
CREATE INDEX ON xpatch.table_config (schema_name, table_name);

-- CRITICAL: Tell pg_dump to include this table's data in dumps
-- Without this, xpatch table configurations are lost on pg_dump/pg_restore
SELECT pg_extension_config_dump('xpatch.table_config', '');

COMMENT ON TABLE xpatch.table_config IS 
    'Configuration for xpatch tables. Most tables work with auto-detection; explicit config is needed when: (1) you want a group_by column, (2) auto-detected order_by is wrong, or (3) you need custom keyframe/compression settings.';

-- ============================================================================
-- Stats cache table for O(1) lookups
-- ============================================================================

-- Per-group statistics (updated incrementally on INSERT/DELETE)
CREATE TABLE xpatch.group_stats (
    relid                 OID NOT NULL,
    group_hash            BYTEA NOT NULL,           -- BLAKE3 hash of group value (16 bytes)
    row_count             BIGINT NOT NULL DEFAULT 0,
    keyframe_count        BIGINT NOT NULL DEFAULT 0,
    max_seq               BIGINT NOT NULL DEFAULT 0,
    raw_size_bytes        BIGINT NOT NULL DEFAULT 0,
    compressed_size_bytes BIGINT NOT NULL DEFAULT 0,
    sum_avg_delta_tags    FLOAT8 NOT NULL DEFAULT 0,  -- Sum of per-row average delta tags
    PRIMARY KEY (relid, group_hash)
);

-- Index for fast aggregation by table
CREATE INDEX group_stats_relid_idx ON xpatch.group_stats(relid);

COMMENT ON TABLE xpatch.group_stats IS 
    'Per-group statistics cache. Updated incrementally on INSERT, deleted on DELETE. Missing groups are recomputed on demand during stats() calls.';

-- Create the table access method handler function
CREATE FUNCTION xpatch_tam_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'xpatch_tam_handler'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_tam_handler(internal) IS 'Internal: Table access method handler for xpatch';

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
    
    -- Validate order_by column exists and has a suitable type
    IF order_by IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute 
            WHERE attrelid = v_relid AND attname = order_by AND NOT attisdropped
        ) THEN
            RAISE EXCEPTION 'Column "%" does not exist in table "%"', order_by, table_name;
        END IF;
        -- E17: Validate order_by column is an integer or timestamp type
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = v_relid AND attname = order_by AND NOT attisdropped
              AND atttypid IN (
                  'int2'::regtype, 'int4'::regtype, 'int8'::regtype,
                  'timestamp'::regtype, 'timestamptz'::regtype
              )
        ) THEN
            RAISE EXCEPTION USING
                errcode = 'datatype_mismatch',
                message = format('order_by column "%s" must be an integer or timestamp type', order_by),
                hint = 'Use a column of type SMALLINT, INTEGER, BIGINT, TIMESTAMP, or TIMESTAMPTZ.';
        END IF;
    ELSE
        -- E13: If order_by is NULL, verify auto-detection can succeed
        -- (there must be at least one INT or TIMESTAMP column)
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = v_relid AND NOT attisdropped
              AND attname != '_xp_seq'
              AND attnum > 0
              AND atttypid IN (
                  'int2'::regtype, 'int4'::regtype, 'int8'::regtype,
                  'timestamp'::regtype, 'timestamptz'::regtype
              )
        ) THEN
            RAISE EXCEPTION USING
                errcode = 'invalid_parameter_value',
                message = 'xpatch tables require an order_by column',
                hint = 'Add an INTEGER, BIGINT, or TIMESTAMP column for versioning, '
                       'or specify order_by explicitly.';
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
    'Configure an xpatch table. Creates a composite (group_by, _xp_seq) index when group_by is set. Validates order_by type and delta column constraints. Optional for most tables - auto-detection works.';

-- Function to get configuration for an xpatch table
-- NOTE: Parameter named 'tbl' to avoid conflict with table_config.table_name column
CREATE OR REPLACE FUNCTION xpatch.get_config(tbl REGCLASS)
RETURNS TABLE (
    group_by        TEXT,
    order_by        TEXT,
    delta_columns   TEXT[],
    keyframe_every  INT,
    compress_depth  INT,
    enable_zstd     BOOLEAN
) AS $$
    SELECT tc.group_by, tc.order_by, tc.delta_columns, tc.keyframe_every, tc.compress_depth, tc.enable_zstd
    FROM xpatch.table_config tc
    WHERE tc.relid = tbl::OID;
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

COMMENT ON FUNCTION xpatch._cleanup_dropped_tables() IS 'Internal: Event trigger function that removes config entries when xpatch tables are dropped';

-- Event trigger to automatically add _xp_seq column and index to xpatch tables
-- This runs after CREATE TABLE completes, adding the internal sequence column
-- and a basic index for efficient lookups
CREATE OR REPLACE FUNCTION xpatch._add_seq_column()
RETURNS event_trigger AS $$
DECLARE
    obj RECORD;
    v_relid OID;
    v_amname NAME;
    v_schema TEXT;
    v_table TEXT;
    v_has_xp_seq BOOLEAN;
    v_idx_name TEXT;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE TABLE'
    LOOP
        -- Get the table's access method and names
        SELECT c.oid, a.amname, n.nspname, c.relname 
        INTO v_relid, v_amname, v_schema, v_table
        FROM pg_class c
        LEFT JOIN pg_am a ON c.relam = a.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = obj.objid;
        
        -- Only process xpatch tables
        IF v_amname = 'xpatch' THEN
            -- Check if _xp_seq already exists (user might have added it manually)
            SELECT EXISTS (
                SELECT 1 FROM pg_attribute 
                WHERE attrelid = v_relid AND attname = '_xp_seq' AND NOT attisdropped
            ) INTO v_has_xp_seq;
            
            -- Add the column if it doesn't exist
            IF NOT v_has_xp_seq THEN
                EXECUTE format('ALTER TABLE %s ADD COLUMN _xp_seq BIGINT', obj.objid::regclass);
                RAISE DEBUG 'xpatch: added _xp_seq column to table %', obj.objid::regclass;
            END IF;
            
            -- Create an index on _xp_seq for efficient lookups
            -- Use a unique name based on table name
            v_idx_name := v_table || '_xp_seq_idx';
            
            -- Check if index already exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind = 'i' AND c.relname = v_idx_name AND n.nspname = v_schema
            ) THEN
                EXECUTE format('CREATE INDEX %I ON %s (_xp_seq)', v_idx_name, obj.objid::regclass);
                RAISE DEBUG 'xpatch: created index % on table %', v_idx_name, obj.objid::regclass;
            END IF;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER xpatch_add_seq_column
    ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE')
    EXECUTE FUNCTION xpatch._add_seq_column();

COMMENT ON FUNCTION xpatch._add_seq_column() IS 'Internal: Event trigger function that adds _xp_seq column and index to new xpatch tables';

-- Utility function: Get compression statistics for a table
CREATE FUNCTION xpatch_stats(rel regclass)
RETURNS TABLE (
    total_rows            BIGINT,
    total_groups          BIGINT,
    keyframe_count        BIGINT,
    delta_count           BIGINT,
    raw_size_bytes        BIGINT,
    compressed_size_bytes BIGINT,
    compression_ratio     FLOAT8,
    cache_hits            BIGINT,
    cache_misses          BIGINT,
    avg_compression_depth FLOAT8
) AS 'MODULE_PATHNAME', 'xpatch_stats'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_stats(regclass) IS 'Get compression statistics for an xpatch table';

-- Utility function: Inspect a specific group within a table
CREATE FUNCTION xpatch_inspect(rel regclass, group_value anyelement)
RETURNS TABLE (
    version             BIGINT,
    seq                 BIGINT,
    is_keyframe         BOOL,
    tag                 INT,
    delta_size_bytes    INT,
    column_name         TEXT
) AS 'MODULE_PATHNAME', 'xpatch_inspect'
LANGUAGE C;

COMMENT ON FUNCTION xpatch_inspect(regclass, anyelement) IS 'Inspect internal storage details for a group: shows each row''s sequence number, keyframe status, delta tag (which previous row it references), and compressed size per delta column.';

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

-- Utility function: Get insert cache statistics
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

-- Utility function: Get xpatch library version
CREATE FUNCTION xpatch_version()
RETURNS TEXT AS 'MODULE_PATHNAME', 'pg_xpatch_version'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_version() IS 'Get xpatch library version';

-- Function to warm the cache by scanning a table
-- This pre-populates the LRU cache with reconstructed tuples for faster subsequent queries
CREATE OR REPLACE FUNCTION xpatch.warm_cache(
    table_name REGCLASS,
    max_rows INT DEFAULT NULL,      -- Maximum rows to scan (NULL = all)
    max_groups INT DEFAULT NULL     -- Maximum groups to warm (NULL = all)
) RETURNS TABLE (
    rows_scanned    BIGINT,
    groups_warmed   BIGINT,
    duration_ms     FLOAT8
) AS $$
DECLARE
    v_relid         OID;
    v_amname        NAME;
    v_start_time    TIMESTAMPTZ;
    v_config        RECORD;
    v_group_col     TEXT;
    v_row           RECORD;
    v_rows_scanned  BIGINT := 0;
    v_groups_warmed BIGINT := 0;
    v_last_group    TEXT;  -- NULL initially, use v_first_row to track
    v_current_group TEXT;
    v_sql           TEXT;
    v_first_row     BOOLEAN := TRUE;
BEGIN
    v_start_time := clock_timestamp();
    v_relid := table_name::OID;
    
    -- Validate input parameters
    IF max_rows IS NOT NULL AND max_rows < 0 THEN
        RAISE EXCEPTION 'max_rows must be non-negative, got %', max_rows;
    END IF;
    IF max_groups IS NOT NULL AND max_groups < 0 THEN
        RAISE EXCEPTION 'max_groups must be non-negative, got %', max_groups;
    END IF;
    
    -- Verify caller has SELECT privilege on the table
    IF NOT pg_catalog.has_table_privilege(current_user, v_relid, 'SELECT') THEN
        RAISE EXCEPTION 'permission denied: must have SELECT privilege on table "%"', table_name;
    END IF;
    
    -- Verify table uses xpatch access method
    SELECT a.amname INTO v_amname
    FROM pg_class c
    JOIN pg_am a ON c.relam = a.oid
    WHERE c.oid = v_relid;
    
    IF v_amname IS NULL OR v_amname != 'xpatch' THEN
        RAISE EXCEPTION 'Table "%" is not using the xpatch access method', table_name;
    END IF;
    
    -- Get configuration to find group column
    SELECT * INTO v_config FROM xpatch.table_config WHERE relid = v_relid;
    v_group_col := v_config.group_by;
    
    -- Build the scan query
    -- Simply selecting all rows will trigger reconstruction and cache population
    IF v_group_col IS NOT NULL THEN
        -- Scan ordered by group to minimize cache churn
        v_sql := format('SELECT %I::TEXT as grp FROM %s ORDER BY %I', 
                        v_group_col, table_name, v_group_col);
    ELSE
        -- No group column, just scan
        v_sql := format('SELECT NULL::TEXT as grp FROM %s', table_name);
    END IF;
    
    -- Add limit if specified
    IF max_rows IS NOT NULL THEN
        v_sql := v_sql || format(' LIMIT %s', max_rows);
    END IF;
    
    -- Execute the scan
    FOR v_row IN EXECUTE v_sql
    LOOP
        v_rows_scanned := v_rows_scanned + 1;
        v_current_group := COALESCE(v_row.grp, '');  -- Treat NULL as empty string for comparison
        
        -- Track group changes (first row always counts as a new group)
        IF v_first_row OR v_current_group IS DISTINCT FROM v_last_group THEN
            v_groups_warmed := v_groups_warmed + 1;
            v_last_group := v_current_group;
            v_first_row := FALSE;
            
            -- Check group limit
            IF max_groups IS NOT NULL AND v_groups_warmed > max_groups THEN
                EXIT;
            END IF;
        END IF;
    END LOOP;
    
    -- Return results
    rows_scanned := v_rows_scanned;
    groups_warmed := v_groups_warmed;
    duration_ms := extract(epoch FROM (clock_timestamp() - v_start_time)) * 1000;
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION xpatch.warm_cache(regclass, int, int) IS 
    'Warm the cache by scanning a table. Use max_rows to limit total rows scanned, max_groups to limit groups (note: may process one extra group at boundary).';

-- Internal function to invalidate cached config (called by xpatch.configure)
CREATE FUNCTION xpatch._invalidate_config(rel OID)
RETURNS VOID AS 'MODULE_PATHNAME', 'xpatch_invalidate_config_fn'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch._invalidate_config(oid) IS 'Internal: Invalidates cached config for a table so changes from xpatch.configure take effect immediately';

-- Function to fix config OIDs after pg_restore
-- Call this after restoring a database to update relid values
CREATE OR REPLACE FUNCTION xpatch.fix_restored_configs()
RETURNS INT AS $$
DECLARE
    v_fixed INT := 0;
    v_config RECORD;
    v_new_relid OID;
BEGIN
    -- Find configs where the relid doesn't match the actual table
    FOR v_config IN 
        SELECT tc.relid, tc.schema_name, tc.table_name
        FROM xpatch.table_config tc
    LOOP
        -- Try to find the table by name
        SELECT c.oid INTO v_new_relid
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = v_config.schema_name 
          AND c.relname = v_config.table_name;
        
        IF v_new_relid IS NOT NULL AND v_new_relid != v_config.relid THEN
            -- Update the relid to the new OID
            UPDATE xpatch.table_config 
            SET relid = v_new_relid 
            WHERE relid = v_config.relid;
            
            v_fixed := v_fixed + 1;
            RAISE NOTICE 'xpatch: fixed config for %.% (old OID %, new OID %)',
                v_config.schema_name, v_config.table_name, v_config.relid, v_new_relid;
        ELSIF v_new_relid IS NULL THEN
            -- Table doesn't exist anymore, remove the config
            DELETE FROM xpatch.table_config WHERE relid = v_config.relid;
            RAISE NOTICE 'xpatch: removed orphan config for %.% (table not found)',
                v_config.schema_name, v_config.table_name;
        END IF;
    END LOOP;
    
    RETURN v_fixed;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION xpatch.fix_restored_configs() IS 
    'Fix xpatch configs after pg_restore by updating OIDs to match restored tables';

-- Function to generate SQL for reconfiguring all tables
-- Useful for including in a dump or migration script
CREATE OR REPLACE FUNCTION xpatch.dump_configs()
RETURNS SETOF TEXT AS $$
    SELECT format(
        'SELECT xpatch.configure(%L, group_by => %s, order_by => %s, delta_columns => %s, keyframe_every => %s, compress_depth => %s, enable_zstd => %s);',
        format('%I.%I', schema_name, table_name),
        COALESCE(quote_literal(group_by), 'NULL'),
        COALESCE(quote_literal(order_by), 'NULL'),
        COALESCE(quote_literal(delta_columns::text), 'NULL'),
        keyframe_every,
        compress_depth,
        CASE WHEN enable_zstd THEN 'true' ELSE 'false' END
    )
    FROM xpatch.table_config
    ORDER BY schema_name, table_name;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.dump_configs() IS 
    'Generate SQL statements to reconfigure all xpatch tables (for dump/migration)';

-- Function to describe an xpatch table with full introspection
-- Shows schema, configuration (explicit or auto-detected), and storage info
CREATE OR REPLACE FUNCTION xpatch.describe(table_name REGCLASS)
RETURNS TABLE (
    property    TEXT,
    value       TEXT
) AS $$
DECLARE
    v_relid         OID;
    v_schema        TEXT;
    v_table         TEXT;
    v_amname        NAME;
    v_config        RECORD;
    v_has_config    BOOLEAN;
    v_group_col     TEXT;
    v_order_col     TEXT;
    v_seq_attnum    INT;
    v_delta_cols    TEXT[];
    v_col           RECORD;
    v_total_rows    BIGINT;
    v_groups        BIGINT;
    v_stats         RECORD;
BEGIN
    v_relid := table_name::OID;
    
    -- Get basic table info
    SELECT n.nspname, c.relname, a.amname 
    INTO v_schema, v_table, v_amname
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    LEFT JOIN pg_am a ON c.relam = a.oid
    WHERE c.oid = v_relid;
    
    IF v_amname IS NULL OR v_amname != 'xpatch' THEN
        RAISE EXCEPTION 'Table "%" is not using the xpatch access method', table_name;
    END IF;
    
    -- Check for explicit config
    SELECT * INTO v_config FROM xpatch.table_config WHERE relid = v_relid;
    v_has_config := FOUND;
    
    -- Table identification section
    property := 'schema'; value := v_schema; RETURN NEXT;
    property := 'table'; value := v_table; RETURN NEXT;
    property := 'oid'; value := v_relid::TEXT; RETURN NEXT;
    property := 'access_method'; value := v_amname; RETURN NEXT;
    
    -- Configuration section
    property := 'config_source'; 
    value := CASE WHEN v_has_config THEN 'explicit (xpatch.configure)' ELSE 'auto-detected' END; 
    RETURN NEXT;
    
    -- Group column
    IF v_has_config AND v_config.group_by IS NOT NULL THEN
        v_group_col := v_config.group_by;
    ELSE
        -- Auto-detect: first column that's not order_by or _xp_seq
        -- For now show as auto-detected
        v_group_col := NULL;
    END IF;
    property := 'group_by';
    value := COALESCE(v_group_col, '(none - single version chain)');
    RETURN NEXT;
    
    -- Order column (auto-detect if not explicit)
    IF v_has_config AND v_config.order_by IS NOT NULL THEN
        v_order_col := v_config.order_by;
    ELSE
        -- Auto-detect: last INT/BIGINT column before _xp_seq
        SELECT attname INTO v_order_col
        FROM pg_attribute
        WHERE attrelid = v_relid 
          AND attnum > 0 
          AND NOT attisdropped
          AND attname != '_xp_seq'
          AND atttypid IN ('int4'::regtype, 'int8'::regtype, 'int2'::regtype)
        ORDER BY attnum DESC
        LIMIT 1;
    END IF;
    property := 'order_by';
    value := COALESCE(v_order_col, '(auto-detect failed)');
    RETURN NEXT;
    
    -- Check _xp_seq column
    SELECT attnum INTO v_seq_attnum
    FROM pg_attribute
    WHERE attrelid = v_relid AND attname = '_xp_seq' AND NOT attisdropped;
    
    property := '_xp_seq column';
    value := CASE WHEN v_seq_attnum IS NOT NULL THEN 'present (attnum=' || v_seq_attnum || ')' ELSE 'MISSING - run migration' END;
    RETURN NEXT;
    
    -- Keyframe settings
    property := 'keyframe_every';
    value := COALESCE(v_config.keyframe_every::TEXT, '100 (default)');
    RETURN NEXT;
    
    property := 'compress_depth';
    value := COALESCE(v_config.compress_depth::TEXT, '1 (default)');
    RETURN NEXT;
    
    property := 'enable_zstd';
    value := COALESCE(v_config.enable_zstd::TEXT, 'true (default)');
    RETURN NEXT;
    
    -- Delta columns
    IF v_has_config AND v_config.delta_columns IS NOT NULL THEN
        v_delta_cols := v_config.delta_columns;
    ELSE
        -- Auto-detect: TEXT, BYTEA, JSON, JSONB columns
        SELECT array_agg(attname ORDER BY attnum) INTO v_delta_cols
        FROM pg_attribute
        WHERE attrelid = v_relid 
          AND attnum > 0 
          AND NOT attisdropped
          AND attname NOT IN ('_xp_seq')
          AND atttypid IN ('text'::regtype, 'bytea'::regtype, 'json'::regtype, 'jsonb'::regtype);
    END IF;
    property := 'delta_columns';
    value := COALESCE(array_to_string(v_delta_cols, ', '), '(none detected)');
    RETURN NEXT;
    
    -- Schema section (columns)
    FOR v_col IN 
        SELECT attname, format_type(atttypid, atttypmod) as typename, attnum,
               CASE 
                   WHEN attname = '_xp_seq' THEN 'internal'
                   WHEN attname = v_group_col THEN 'group_by'
                   WHEN attname = v_order_col THEN 'order_by'
                   WHEN attname = ANY(COALESCE(v_delta_cols, ARRAY[]::TEXT[])) THEN 'delta'
                   ELSE 'regular'
               END as role
        FROM pg_attribute
        WHERE attrelid = v_relid AND attnum > 0 AND NOT attisdropped
        ORDER BY attnum
    LOOP
        property := 'column[' || v_col.attnum || ']';
        value := v_col.attname || ' ' || v_col.typename || ' (' || v_col.role || ')';
        RETURN NEXT;
    END LOOP;
    
    -- Storage statistics section
    BEGIN
        SELECT * INTO v_stats FROM xpatch_stats(table_name);
        
        property := 'total_rows'; value := v_stats.total_rows::TEXT; RETURN NEXT;
        property := 'total_groups'; value := v_stats.total_groups::TEXT; RETURN NEXT;
        property := 'keyframes'; value := v_stats.keyframe_count::TEXT; RETURN NEXT;
        property := 'deltas'; value := v_stats.delta_count::TEXT; RETURN NEXT;
        property := 'raw_size'; value := pg_size_pretty(v_stats.raw_size_bytes); RETURN NEXT;
        property := 'compressed_size'; value := pg_size_pretty(v_stats.compressed_size_bytes); RETURN NEXT;
        property := 'compression_ratio'; value := round(v_stats.compression_ratio::numeric, 2)::TEXT || 'x'; RETURN NEXT;
        property := 'avg_compression_depth'; value := round(v_stats.avg_compression_depth::numeric, 2)::TEXT; RETURN NEXT;
        property := 'cache_hits'; value := v_stats.cache_hits::TEXT; RETURN NEXT;
        property := 'cache_misses'; value := v_stats.cache_misses::TEXT; RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        property := 'stats_error'; value := SQLERRM; RETURN NEXT;
    END;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION xpatch.describe(regclass) IS 
    'Describe an xpatch table: schema, configuration (explicit or auto-detected), and storage statistics';

-- ============================================================================
-- Consistent naming wrappers (xpatch.* schema)
-- These provide a unified API where all functions live in the xpatch schema
-- ============================================================================

-- xpatch.version() - wrapper for xpatch_version()
CREATE OR REPLACE FUNCTION xpatch.version()
RETURNS TEXT AS $$
    SELECT xpatch_version();
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.version() IS 'Get xpatch library version';

-- xpatch.stats() - wrapper for xpatch_stats() with improved formatting
CREATE OR REPLACE FUNCTION xpatch.stats(tbl REGCLASS)
RETURNS TABLE (
    total_rows            BIGINT,
    total_groups          BIGINT,
    keyframe_count        BIGINT,
    delta_count           BIGINT,
    raw_size_bytes        BIGINT,
    compressed_size_bytes BIGINT,
    compression_ratio     NUMERIC(10,2),  -- Rounded to 2 decimals
    cache_hits            BIGINT,
    cache_misses          BIGINT,
    avg_compression_depth NUMERIC(10,2)   -- Rounded to 2 decimals
) AS $$
    SELECT 
        s.total_rows,
        s.total_groups,
        s.keyframe_count,
        s.delta_count,
        s.raw_size_bytes,
        s.compressed_size_bytes,
        ROUND(s.compression_ratio::numeric, 2),
        s.cache_hits,
        s.cache_misses,
        ROUND(s.avg_compression_depth::numeric, 2)
    FROM xpatch_stats(tbl) s;
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.stats(regclass) IS 'Get compression statistics for an xpatch table';

-- xpatch.inspect() - wrapper for xpatch_inspect() with 1-based seq
CREATE OR REPLACE FUNCTION xpatch.inspect(tbl REGCLASS, group_value ANYELEMENT)
RETURNS TABLE (
    version             BIGINT,
    seq                 BIGINT,     -- Now 1-based to match _xp_seq
    is_keyframe         BOOL,
    tag                 INT,
    delta_size_bytes    INT,
    column_name         TEXT
) AS $$
    SELECT 
        i.version,
        i.seq + 1,  -- Convert 0-based to 1-based
        i.is_keyframe,
        i.tag,
        i.delta_size_bytes,
        i.column_name
    FROM xpatch_inspect(tbl, group_value) i;
$$ LANGUAGE SQL;

COMMENT ON FUNCTION xpatch.inspect(regclass, anyelement) IS 
    'Inspect internal storage details for a group: shows each row''s sequence number (1-based, matches _xp_seq), keyframe status, delta tag, and compressed size per delta column.';

-- xpatch.cache_stats() - wrapper for xpatch_cache_stats()
CREATE OR REPLACE FUNCTION xpatch.cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT
) AS $$
    SELECT * FROM xpatch_cache_stats();
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.cache_stats() IS 'Get global LRU cache statistics';

-- xpatch.insert_cache_stats() - wrapper for xpatch_insert_cache_stats()
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

-- ============================================================================
-- xpatch_physical() - C function for raw physical delta access
-- ============================================================================

CREATE OR REPLACE FUNCTION xpatch_physical(
    tbl REGCLASS,
    group_filter ANYELEMENT,
    from_seq BIGINT DEFAULT NULL
)
RETURNS TABLE (
    group_value     TEXT,       -- Group identifier (cast to text for uniformity)
    version         BIGINT,     -- The order_by column value  
    seq             BIGINT,     -- 1-based sequence number within group
    is_keyframe     BOOLEAN,    -- True if this is a keyframe
    tag             INT,        -- Delta tag (0=keyframe, 1=prev row, 2=2 back, etc)
    delta_column    TEXT,       -- Which column this delta is for
    delta_bytes     BYTEA,      -- Raw compressed delta data
    delta_size      INT         -- Size of delta_bytes
)
AS 'pg_xpatch', 'xpatch_physical'
LANGUAGE C STABLE;

COMMENT ON FUNCTION xpatch_physical(regclass, anyelement, bigint) IS 
    'Access raw physical delta storage. Returns compressed delta bytes and metadata for each row/column.';

-- ============================================================================
-- xpatch.physical() - Wrapper functions in xpatch schema
-- ============================================================================

-- 3-arg version: specific group filter
CREATE OR REPLACE FUNCTION xpatch.physical(
    tbl REGCLASS,
    group_filter ANYELEMENT,
    from_seq BIGINT DEFAULT NULL
)
RETURNS TABLE (
    group_value     TEXT,
    version         BIGINT,
    seq             BIGINT,
    is_keyframe     BOOLEAN,
    tag             INT,
    delta_column    TEXT,
    delta_bytes     BYTEA,
    delta_size      INT
) AS $$
    SELECT * FROM xpatch_physical(tbl, group_filter, from_seq);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.physical(regclass, anyelement, bigint) IS 
    'Access raw physical delta storage for a specific group. Returns compressed delta bytes and metadata.';

-- 2-arg version: all groups, filter by from_seq
-- Uses INT for from_seq to avoid ambiguity with the 3-arg (regclass, anyelement, bigint) overload.
-- INT is implicitly upcast to BIGINT when calling the C function.
CREATE OR REPLACE FUNCTION xpatch.physical(
    tbl REGCLASS,
    from_seq INT
)
RETURNS TABLE (
    group_value     TEXT,
    version         BIGINT,
    seq             BIGINT,
    is_keyframe     BOOLEAN,
    tag             INT,
    delta_column    TEXT,
    delta_bytes     BYTEA,
    delta_size      INT
) AS $$
DECLARE
    v_config        RECORD;
    v_group_col     TEXT;
    v_sql           TEXT;
    v_grp           RECORD;
BEGIN
    -- Get configuration
    SELECT * INTO v_config FROM xpatch.table_config WHERE relid = tbl::OID;
    v_group_col := v_config.group_by;
    
    IF v_group_col IS NULL THEN
        -- No grouping - call C function with NULL group
        RETURN QUERY
        SELECT * FROM xpatch_physical(tbl, NULL::INT, from_seq::BIGINT);
    ELSE
        -- Iterate through all distinct groups
        v_sql := format('SELECT DISTINCT %I as grp FROM %s ORDER BY 1', v_group_col, tbl);
        FOR v_grp IN EXECUTE v_sql
        LOOP
            RETURN QUERY
            SELECT * FROM xpatch_physical(tbl, v_grp.grp, from_seq::BIGINT);
        END LOOP;
    END IF;
    
    RETURN;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION xpatch.physical(regclass, int) IS 
    'Access raw physical delta storage for all groups, filtered by from_seq (rows with seq > from_seq).';

-- 1-arg version: all groups, all versions
CREATE OR REPLACE FUNCTION xpatch.physical(tbl REGCLASS)
RETURNS TABLE (
    group_value     TEXT,
    version         BIGINT,
    seq             BIGINT,
    is_keyframe     BOOLEAN,
    tag             INT,
    delta_column    TEXT,
    delta_bytes     BYTEA,
    delta_size      INT
) AS $$
    SELECT * FROM xpatch.physical(tbl, NULL::BIGINT);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.physical(regclass) IS 
    'Access raw physical delta storage for all groups and all versions.';

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
    p_max_seq BIGINT,
    p_raw_size BIGINT,
    p_compressed_size BIGINT,
    p_avg_delta_tag FLOAT8
) RETURNS VOID
AS 'MODULE_PATHNAME', 'xpatch_update_group_stats'
LANGUAGE C;


