-- pg_xpatch upgrade script: 0.7.0 -> 0.8.0
--
-- Changes in 0.8.0:
--   - Three-level cache system (L1/L2/L3) with chain index and path planner
--   - L1: renamed GUCs (pg_xpatch.cache_* -> pg_xpatch.l1_cache_*)
--   - L2: compressed delta cache in shared memory
--   - L3: persistent disk cache tables (per-table, opt-in)
--   - Chain index: always-on in-memory index for optimal reconstruction paths
--   - Path planner: bottom-up DP algorithm for cheapest reconstruction

-- L2 cache statistics C function
CREATE FUNCTION xpatch_l2_cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT,
    skip_count          BIGINT
) AS 'MODULE_PATHNAME', 'xpatch_l2_cache_stats_fn'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_l2_cache_stats() IS 'Get L2 compressed delta cache statistics';

-- L2 cache statistics schema wrapper
CREATE OR REPLACE FUNCTION xpatch.l2_cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT,
    skip_count          BIGINT
) AS $$
    SELECT * FROM xpatch_l2_cache_stats();
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.l2_cache_stats() IS 'Get L2 compressed delta cache statistics';

-- Path planner C function
CREATE FUNCTION xpatch_plan_path(
    rel         regclass,
    group_value text,
    attnum      int2,
    target_seq  int8,
    enable_zstd bool DEFAULT true
)
RETURNS TABLE (
    step_num        INT4,
    seq             INT8,
    action          TEXT,
    total_cost_ns   INT8
) AS 'MODULE_PATHNAME', 'xpatch_plan_path_fn'
LANGUAGE C STABLE;

COMMENT ON FUNCTION xpatch_plan_path(regclass, text, int2, int8, bool) IS
    'Compute optimal reconstruction path for a target version using bottom-up DP';

-- Path planner schema wrapper
CREATE OR REPLACE FUNCTION xpatch.plan_path(
    rel         regclass,
    group_value text,
    attnum      int2,
    target_seq  int8,
    enable_zstd bool DEFAULT true
)
RETURNS TABLE (
    step_num        INT4,
    seq             INT8,
    action          TEXT,
    total_cost_ns   INT8
) AS $$
    SELECT * FROM xpatch_plan_path(rel, group_value, attnum, target_seq, enable_zstd);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.plan_path(regclass, text, int2, int8, bool) IS
    'Compute optimal reconstruction path for a target version using bottom-up DP';

-- L3 cache: add columns to table_config
ALTER TABLE xpatch.table_config
    ADD COLUMN IF NOT EXISTS l3_cache_enabled BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS l3_cache_max_size_mb INT NOT NULL DEFAULT 1024;

ALTER TABLE xpatch.table_config
    ADD CONSTRAINT IF NOT EXISTS valid_l3_cache_max_size CHECK (l3_cache_max_size_mb >= 1);

-- Update xpatch.configure() to accept L3 parameters
CREATE OR REPLACE FUNCTION xpatch.configure(
    table_name REGCLASS,
    group_by TEXT DEFAULT NULL,
    order_by TEXT DEFAULT NULL,
    delta_columns TEXT[] DEFAULT NULL,
    keyframe_every INT DEFAULT 100,
    compress_depth INT DEFAULT 1,
    enable_zstd BOOLEAN DEFAULT true,
    l3_cache_enabled BOOLEAN DEFAULT false,
    l3_cache_max_size_mb INT DEFAULT 1024
) RETURNS VOID AS $$
DECLARE
    v_relid OID;
    v_amname NAME;
    v_col TEXT;
BEGIN
    v_relid := table_name::OID;

    -- Verify it's an xpatch table
    SELECT a.amname INTO v_amname
    FROM pg_class c JOIN pg_am a ON c.relam = a.oid
    WHERE c.oid = v_relid;

    IF v_amname IS NULL OR v_amname != 'xpatch' THEN
        RAISE EXCEPTION 'Table "%" is not an xpatch table (am=%)', table_name, COALESCE(v_amname, 'heap');
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

    -- Validate order_by column exists and is int/timestamp
    IF order_by IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_attribute
            WHERE attrelid = v_relid AND attname = order_by AND NOT attisdropped
        ) THEN
            RAISE EXCEPTION 'Column "%" does not exist in table "%"', order_by, table_name;
        END IF;
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
            IF EXISTS (
                SELECT 1 FROM pg_attribute
                WHERE attrelid = v_relid AND attname = v_col AND NOT attnotnull
            ) THEN
                RAISE EXCEPTION 'Delta column "%" must be NOT NULL.', v_col;
            END IF;
        END LOOP;
    END IF;

    -- Validate keyframe_every
    IF keyframe_every IS NOT NULL AND keyframe_every < 1 THEN
        RAISE EXCEPTION 'keyframe_every must be at least 1, got %', keyframe_every;
    END IF;

    -- Validate compress_depth
    IF compress_depth IS NOT NULL AND compress_depth < 1 THEN
        RAISE EXCEPTION 'compress_depth must be at least 1, got %', compress_depth;
    END IF;

    -- Validate l3_cache_max_size_mb
    IF l3_cache_max_size_mb IS NOT NULL AND l3_cache_max_size_mb < 1 THEN
        RAISE EXCEPTION 'l3_cache_max_size_mb must be at least 1, got %', l3_cache_max_size_mb;
    END IF;

    -- Upsert config
    DELETE FROM xpatch.table_config WHERE relid = v_relid;

    INSERT INTO xpatch.table_config (relid, schema_name, table_name, group_by, order_by,
                                     delta_columns, keyframe_every, compress_depth, enable_zstd,
                                     l3_cache_enabled, l3_cache_max_size_mb)
    SELECT v_relid, n.nspname, c.relname, group_by, order_by,
           delta_columns, keyframe_every, compress_depth, enable_zstd,
           l3_cache_enabled, l3_cache_max_size_mb
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = v_relid;

    -- Create composite index if group_by is set
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

            -- Drop basic _xp_seq index if it exists
            IF EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = v_schema AND indexname = v_tbl || '_xp_seq_idx'
            ) THEN
                EXECUTE format('DROP INDEX %I.%I', v_schema, v_tbl || '_xp_seq_idx');
            END IF;

            -- Create composite index if not exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = v_schema AND indexname = v_idx_name
            ) THEN
                EXECUTE format('CREATE INDEX %I ON %I.%I (%I, _xp_seq)',
                    v_idx_name, v_schema, v_tbl, group_by);
                RAISE NOTICE 'xpatch: created index % on (%, _xp_seq)', v_idx_name, group_by;
            END IF;
        END;
    END IF;

    -- Auto-detect delta columns and notify
    PERFORM format('xpatch: auto-detected %s delta column(s): %s',
        array_length(
            ARRAY(
                SELECT attname FROM pg_attribute
                WHERE attrelid = v_relid AND NOT attisdropped AND attnum > 0
                  AND attname NOT IN ('_xp_seq', COALESCE(group_by, ''), COALESCE(order_by, ''))
                  AND atttypid IN ('text'::regtype, 'bytea'::regtype, 'jsonb'::regtype, 'json'::regtype, 'varchar'::regtype)
            ), 1),
        array_to_string(
            ARRAY(
                SELECT attname FROM pg_attribute
                WHERE attrelid = v_relid AND NOT attisdropped AND attnum > 0
                  AND attname NOT IN ('_xp_seq', COALESCE(group_by, ''), COALESCE(order_by, ''))
                  AND atttypid IN ('text'::regtype, 'bytea'::regtype, 'jsonb'::regtype, 'json'::regtype, 'varchar'::regtype)
            ), ', ')
    );

    RAISE NOTICE 'xpatch: configured "%" (group_by=%, order_by=%, keyframe_every=%, l3=%)',
        table_name, COALESCE(group_by, '(none)'), COALESCE(order_by, '(auto)'),
        keyframe_every, l3_cache_enabled;
END;
$$ LANGUAGE plpgsql VOLATILE;

COMMENT ON FUNCTION xpatch.configure(regclass, text, text, text[], int, int, boolean, boolean, int) IS
    'Configure an xpatch table with explicit settings including L3 disk cache';

-- L3 cache: drop function (C)
CREATE FUNCTION xpatch_l3_cache_drop(rel regclass)
RETURNS BOOLEAN
AS 'MODULE_PATHNAME', 'xpatch_l3_cache_drop_fn'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION xpatch_l3_cache_drop(regclass) IS
    'Drop the L3 persistent disk cache table for an xpatch table';

-- L3 cache: drop function (schema wrapper)
CREATE OR REPLACE FUNCTION xpatch.drop_l3_cache(
    table_name REGCLASS
) RETURNS BOOLEAN AS $$
    SELECT xpatch_l3_cache_drop(table_name);
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION xpatch.drop_l3_cache(regclass) IS
    'Drop the L3 persistent disk cache table for an xpatch table. Returns true if the table existed.';

-- L3 eviction pass: manually trigger one eviction cycle
CREATE OR REPLACE FUNCTION xpatch_l3_eviction_pass()
RETURNS INTEGER
AS 'MODULE_PATHNAME', 'xpatch_l3_eviction_pass_fn'
LANGUAGE C VOLATILE;

COMMENT ON FUNCTION xpatch_l3_eviction_pass() IS
    'Run one L3 eviction cycle: flush access buffer and evict over-limit entries';

CREATE OR REPLACE FUNCTION xpatch.l3_eviction_pass()
RETURNS INTEGER AS $$
    SELECT xpatch_l3_eviction_pass();
$$ LANGUAGE SQL VOLATILE;

COMMENT ON FUNCTION xpatch.l3_eviction_pass() IS
    'Run one L3 eviction cycle: flush access buffer and evict over-limit entries. Returns number of access records flushed.';
