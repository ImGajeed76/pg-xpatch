-- Test 14: xpatch.describe() Function
-- Tests the table introspection functionality

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Clean up from previous runs
DROP TABLE IF EXISTS test_describe_basic;
DROP TABLE IF EXISTS test_describe_configured;
DROP TABLE IF EXISTS test_describe_unusual;

-- ================================================================
-- Test 1: Basic table with auto-detection
-- ================================================================

CREATE TABLE test_describe_basic (
    id INT,
    version INT,
    data TEXT NOT NULL
) USING xpatch;

-- Insert some data
INSERT INTO test_describe_basic SELECT 1, v, 'Data v' || v FROM generate_series(1, 5) AS v;

-- Describe should show auto-detected config
SELECT property, value FROM xpatch.describe('test_describe_basic') 
WHERE property IN ('config_source', 'group_by', 'order_by', '_xp_seq column', 'delta_columns');

-- ================================================================
-- Test 2: Explicitly configured table
-- ================================================================

CREATE TABLE test_describe_configured (
    doc_id INT,
    rev INT,
    title TEXT NOT NULL,
    body TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_describe_configured', 
    group_by => 'doc_id',
    order_by => 'rev',
    delta_columns => ARRAY['body']::text[],
    keyframe_every => 50,
    compress_depth => 3
);

-- Insert some data
INSERT INTO test_describe_configured 
SELECT d, v, 'Title ' || d, repeat('Body ', 50) || v
FROM generate_series(1, 3) AS d, generate_series(1, 10) AS v;

-- Describe should show explicit config
SELECT property, value FROM xpatch.describe('test_describe_configured') 
WHERE property IN ('config_source', 'group_by', 'order_by', 'keyframe_every', 'compress_depth', 'delta_columns');

-- Verify column roles
SELECT property, value FROM xpatch.describe('test_describe_configured')
WHERE property LIKE 'column%';

-- Verify storage stats (should have data)
SELECT property, value FROM xpatch.describe('test_describe_configured')
WHERE property IN ('total_rows', 'total_groups', 'keyframes', 'deltas');

-- ================================================================
-- Test 3: Table with unusual column types
-- ================================================================

CREATE TABLE test_describe_unusual (
    uuid_col UUID,
    ver INT,
    json_col JSON,
    jsonb_col JSONB,
    bytea_col BYTEA,
    array_col INT[],
    numeric_col NUMERIC(10,2)
) USING xpatch;

SELECT xpatch.configure('test_describe_unusual', group_by => 'uuid_col');

-- Check that unusual types are handled correctly
SELECT property, value FROM xpatch.describe('test_describe_unusual')
WHERE property LIKE 'column%';

-- ================================================================
-- Test 4: Empty table
-- ================================================================

DROP TABLE IF EXISTS test_describe_empty;
CREATE TABLE test_describe_empty (id INT, ver INT, data TEXT NOT NULL) USING xpatch;

-- Describe should work on empty table
SELECT property, value FROM xpatch.describe('test_describe_empty')
WHERE property IN ('total_rows', 'total_groups', 'keyframes', 'deltas');

-- ================================================================
-- Test 5: Error case - non-xpatch table
-- ================================================================

DROP TABLE IF EXISTS test_describe_heap;
CREATE TABLE test_describe_heap (id INT, data TEXT);

-- Should raise an error
DO $$
BEGIN
    PERFORM * FROM xpatch.describe('test_describe_heap');
    RAISE EXCEPTION 'Expected error was not raised';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLERRM LIKE '%not using the xpatch access method%' THEN
            RAISE NOTICE 'Correctly raised error for non-xpatch table';
        ELSE
            RAISE;
        END IF;
END;
$$;

-- Clean up
DROP TABLE test_describe_basic;
DROP TABLE test_describe_configured;
DROP TABLE test_describe_unusual;
DROP TABLE test_describe_empty;
DROP TABLE test_describe_heap;
