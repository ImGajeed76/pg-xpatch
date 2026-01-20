-- pg_xpatch upgrade script: 0.1.1 -> 0.2.0
-- 
-- This upgrade adds:
-- 1. TEXT/VARCHAR group column support (fixed datum comparison for varlena types)
-- 2. xpatch.physical() function for raw physical storage access
-- 3. Fixed auto-detection to exclude group_by/order_by columns from delta compression
--
-- Note: The C code changes require a PostgreSQL restart after extension update.
-- The changes are backward compatible - existing tables with INT group columns
-- will continue to work as before.

-- xpatch.physical() - Access raw physical delta storage
-- This is the core C function that reads physical storage directly
CREATE OR REPLACE FUNCTION xpatch_physical(
    tbl         REGCLASS,
    group_filter ANYELEMENT DEFAULT NULL,
    from_seq    INT DEFAULT NULL
)
RETURNS TABLE (
    group_value  TEXT,
    version      BIGINT,
    seq          INT,
    is_keyframe  BOOLEAN,
    tag          INT,
    delta_column TEXT,
    delta_bytes  BYTEA,
    delta_size   INT
) AS 'pg_xpatch', 'xpatch_physical'
LANGUAGE C STABLE;

COMMENT ON FUNCTION xpatch_physical IS 
    'Access raw physical delta storage. Returns delta bytes and metadata for each row/column.';

-- xpatch.physical(regclass, anyelement, int) - Filter by group and seq
CREATE OR REPLACE FUNCTION xpatch.physical(
    tbl          REGCLASS,
    group_filter ANYELEMENT,
    from_seq     INT DEFAULT NULL
)
RETURNS TABLE (
    group_value  TEXT,
    version      BIGINT,
    seq          INT,
    is_keyframe  BOOLEAN,
    tag          INT,
    delta_column TEXT,
    delta_bytes  BYTEA,
    delta_size   INT
) AS $$
    SELECT * FROM xpatch_physical(tbl, group_filter, from_seq);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.physical(regclass, anyelement, int) IS 
    'Access raw physical delta storage for a specific group. Returns delta bytes and metadata.';

-- xpatch.physical(regclass, int) - All groups, filter by seq
CREATE OR REPLACE FUNCTION xpatch.physical(
    tbl      REGCLASS,
    from_seq INT
)
RETURNS TABLE (
    group_value  TEXT,
    version      BIGINT,
    seq          INT,
    is_keyframe  BOOLEAN,
    tag          INT,
    delta_column TEXT,
    delta_bytes  BYTEA,
    delta_size   INT
) AS $$
    SELECT * FROM xpatch_physical(tbl, NULL::INT, from_seq);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.physical(regclass, int) IS 
    'Access raw physical delta storage for all groups with seq > from_seq.';

-- xpatch.physical(regclass) - All groups, all rows
CREATE OR REPLACE FUNCTION xpatch.physical(tbl REGCLASS)
RETURNS TABLE (
    group_value  TEXT,
    version      BIGINT,
    seq          INT,
    is_keyframe  BOOLEAN,
    tag          INT,
    delta_column TEXT,
    delta_bytes  BYTEA,
    delta_size   INT
) AS $$
    SELECT * FROM xpatch_physical(tbl, NULL::INT, NULL);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.physical(regclass) IS 
    'Access raw physical delta storage for all groups and rows.';
