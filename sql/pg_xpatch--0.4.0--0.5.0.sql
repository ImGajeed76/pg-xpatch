-- pg_xpatch upgrade script: 0.4.0 -> 0.5.0
--
-- Changes in 0.5.0:
--   - _xp_seq column type changed from INT to BIGINT (supports >2.1B rows/group)
--   - group_stats.max_seq changed from INT to BIGINT
--   - xpatch.configure() now validates order_by column type (E17)
--   - xpatch.configure() now validates auto-detection feasibility (E13)
--   - xpatch_inspect/xpatch_physical seq columns changed from INT to BIGINT
--   - xpatch_update_group_stats p_max_seq changed from INT to BIGINT
--   - 11 C-level bug fixes (memory safety, WAL, MVCC, concurrency)
--
-- IMPORTANT: This migration does NOT automatically alter existing user tables.
-- After running ALTER EXTENSION pg_xpatch UPDATE, you should also run:
--
--   DO $$
--   DECLARE r RECORD;
--   BEGIN
--     FOR r IN
--       SELECT c.oid::regclass AS tbl
--       FROM pg_class c
--       JOIN pg_am a ON c.relam = a.oid
--       WHERE a.amname = 'xpatch'
--     LOOP
--       EXECUTE format('ALTER TABLE %s ALTER COLUMN _xp_seq TYPE BIGINT', r.tbl);
--       RAISE NOTICE 'xpatch: migrated _xp_seq to BIGINT on %', r.tbl;
--     END LOOP;
--   END $$;
--
-- This will rewrite each table's _xp_seq column from INT to BIGINT.
-- For large tables, consider doing this during a maintenance window.

-- ============================================================================
-- 1. Widen group_stats.max_seq from INT to BIGINT
-- ============================================================================

ALTER TABLE xpatch.group_stats ALTER COLUMN max_seq TYPE BIGINT;

-- ============================================================================
-- 2. Update event trigger to create _xp_seq as BIGINT for new tables
-- ============================================================================

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

-- ============================================================================
-- 3. Update xpatch.configure() with order_by type validation (E13/E17)
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

-- ============================================================================
-- 4. Replace C functions with updated signatures (seq INT -> BIGINT)
-- ============================================================================

-- Drop old xpatch_inspect and recreate with BIGINT seq
DROP FUNCTION IF EXISTS xpatch_inspect(regclass, anyelement);
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

-- Drop old xpatch_physical and recreate with BIGINT seq/from_seq
DROP FUNCTION IF EXISTS xpatch_physical(regclass, anyelement, int);
CREATE OR REPLACE FUNCTION xpatch_physical(
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
)
AS 'pg_xpatch', 'xpatch_physical'
LANGUAGE C STABLE;

COMMENT ON FUNCTION xpatch_physical(regclass, anyelement, bigint) IS 
    'Access raw physical delta storage. Returns compressed delta bytes and metadata for each row/column.';

-- Drop old xpatch_update_group_stats and recreate with BIGINT p_max_seq
DROP FUNCTION IF EXISTS xpatch_update_group_stats(oid, bytea, boolean, int, bigint, bigint, float8);
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

-- ============================================================================
-- 5. Update SQL wrapper functions with BIGINT signatures
-- ============================================================================

-- xpatch.inspect() wrapper
CREATE OR REPLACE FUNCTION xpatch.inspect(tbl REGCLASS, group_value ANYELEMENT)
RETURNS TABLE (
    version             BIGINT,
    seq                 BIGINT,
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

-- xpatch.physical() 3-arg wrapper
DROP FUNCTION IF EXISTS xpatch.physical(regclass, anyelement, int);
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

-- xpatch.physical() 2-arg wrapper
-- Keep INT for from_seq to avoid ambiguity with (regclass, anyelement, bigint) overload.
DROP FUNCTION IF EXISTS xpatch.physical(regclass, int);
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

-- xpatch.physical() 1-arg wrapper
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
