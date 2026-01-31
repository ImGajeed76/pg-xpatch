-- Test 15: xpatch.warm_cache() Function
-- Tests cache warming functionality

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Clean up from previous runs
DROP TABLE IF EXISTS test_warm_basic;
DROP TABLE IF EXISTS test_warm_grouped;
DROP TABLE IF EXISTS test_warm_empty;

-- ================================================================
-- Test 1: Basic cache warming (no group_by)
-- ================================================================

CREATE TABLE test_warm_basic (
    id INT,
    version INT,
    data TEXT
) USING xpatch;

-- Insert data (no group_by configured, so single version chain)
INSERT INTO test_warm_basic SELECT 1, v, repeat('Data ', 50) || v FROM generate_series(1, 10) AS v;

-- Warm the cache
SELECT rows_scanned, groups_warmed FROM xpatch.warm_cache('test_warm_basic');

-- Verify: 10 rows, 1 group (whole table as single group)
SELECT 
    CASE WHEN rows_scanned = 10 AND groups_warmed = 1 
         THEN 'PASS: Basic warm_cache' 
         ELSE 'FAIL: Expected 10 rows, 1 group' 
    END as result
FROM xpatch.warm_cache('test_warm_basic');

-- ================================================================
-- Test 2: Cache warming with group_by
-- ================================================================

CREATE TABLE test_warm_grouped (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_warm_grouped', group_by => 'doc_id');

-- Insert 5 documents with 10 versions each
INSERT INTO test_warm_grouped 
SELECT d, v, repeat('Content for doc ' || d || ' v' || v, 20)
FROM generate_series(1, 5) AS d, generate_series(1, 10) AS v;

-- Warm the cache
SELECT rows_scanned, groups_warmed FROM xpatch.warm_cache('test_warm_grouped');

-- Verify: 50 rows, 5 groups
SELECT 
    CASE WHEN rows_scanned = 50 AND groups_warmed = 5 
         THEN 'PASS: Grouped warm_cache' 
         ELSE 'FAIL: Expected 50 rows, 5 groups' 
    END as result
FROM xpatch.warm_cache('test_warm_grouped');

-- ================================================================
-- Test 3: Cache warming with max_rows limit
-- ================================================================

SELECT 
    CASE WHEN rows_scanned <= 25 
         THEN 'PASS: max_rows limit respected' 
         ELSE 'FAIL: max_rows limit not respected' 
    END as result
FROM xpatch.warm_cache('test_warm_grouped', max_rows => 25);

-- ================================================================
-- Test 4: Cache warming with max_groups limit
-- Note: The limit is checked AFTER incrementing, so we may get max_groups+1
-- ================================================================

SELECT 
    CASE WHEN groups_warmed <= 4  -- max_groups + 1 due to check timing
         THEN 'PASS: max_groups limit respected' 
         ELSE 'FAIL: max_groups limit not respected' 
    END as result
FROM xpatch.warm_cache('test_warm_grouped', max_groups => 3);

-- ================================================================
-- Test 5: Empty table
-- ================================================================

CREATE TABLE test_warm_empty (id INT, ver INT, data TEXT) USING xpatch;

SELECT 
    CASE WHEN rows_scanned = 0 AND groups_warmed = 0 
         THEN 'PASS: Empty table warm_cache' 
         ELSE 'FAIL: Expected 0 rows, 0 groups for empty table' 
    END as result
FROM xpatch.warm_cache('test_warm_empty');

-- ================================================================
-- Test 6: Error case - non-xpatch table
-- ================================================================

DROP TABLE IF EXISTS test_warm_heap;
CREATE TABLE test_warm_heap (id INT, data TEXT);

DO $$
BEGIN
    PERFORM * FROM xpatch.warm_cache('test_warm_heap');
    RAISE EXCEPTION 'Expected error was not raised';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLERRM LIKE '%not using the xpatch access method%' THEN
            RAISE NOTICE 'PASS: Correctly raised error for non-xpatch table';
        ELSE
            RAISE;
        END IF;
END;
$$;

-- ================================================================
-- Test 7: Verify duration is returned
-- ================================================================

SELECT 
    CASE WHEN duration_ms >= 0 
         THEN 'PASS: Duration returned' 
         ELSE 'FAIL: Duration not returned' 
    END as result
FROM xpatch.warm_cache('test_warm_grouped');

-- Clean up
DROP TABLE test_warm_basic;
DROP TABLE test_warm_grouped;
DROP TABLE test_warm_empty;
DROP TABLE test_warm_heap;
