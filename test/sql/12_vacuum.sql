-- Test 12: VACUUM and ANALYZE
-- Tests VACUUM (dead tuple cleanup) and ANALYZE (statistics collection)

-- Note: VACUUM cannot run inside a transaction block, so we test in multiple parts

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- ================================================================
-- Test 1: Table stats after ANALYZE
-- ================================================================

CREATE TABLE test_analyze (
    doc_id INT,
    version INT,
    content TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_analyze',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert data
INSERT INTO test_analyze VALUES (1, 1, 'v1');
INSERT INTO test_analyze VALUES (1, 2, 'v2');
INSERT INTO test_analyze VALUES (2, 1, 'doc2');

-- Before ANALYZE, stats may be 0 or stale
SELECT 'Initial stats:' as phase;

-- Run analyze
ANALYZE test_analyze;

-- Check pg_class stats after ANALYZE
SELECT 'After ANALYZE:' as phase, reltuples::int as tuples, relpages as pages 
FROM pg_class WHERE relname = 'test_analyze';

-- Verify ANALYZE found the correct tuple count
SELECT COUNT(*) as actual_count FROM test_analyze;

DROP TABLE test_analyze;

-- ================================================================
-- Test 2: Verify data integrity after delete + vacuum cycle
-- (VACUUM will be run separately in test script)
-- ================================================================

CREATE TABLE test_vacuum_integrity (
    doc_id INT,
    version INT,
    content TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_vacuum_integrity',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Create versioned data
INSERT INTO test_vacuum_integrity VALUES (1, 1, 'First v1');
INSERT INTO test_vacuum_integrity VALUES (1, 2, 'First v2');
INSERT INTO test_vacuum_integrity VALUES (1, 3, 'First v3');
INSERT INTO test_vacuum_integrity VALUES (2, 1, 'Second v1');
INSERT INTO test_vacuum_integrity VALUES (2, 2, 'Second v2');

SELECT 'Before delete:' as phase, COUNT(*) as cnt FROM test_vacuum_integrity;

-- Delete doc_id=1, version 2+ (cascade)
DELETE FROM test_vacuum_integrity WHERE doc_id = 1 AND version = 2;

SELECT 'After delete:' as phase, COUNT(*) as cnt FROM test_vacuum_integrity;

-- Remaining data should be readable
SELECT 'Remaining data:' as phase;
SELECT * FROM test_vacuum_integrity ORDER BY doc_id, version;

-- Stats update
ANALYZE test_vacuum_integrity;

SELECT 'Stats after ANALYZE:' as phase, reltuples::int as tuples
FROM pg_class WHERE relname = 'test_vacuum_integrity';

DROP TABLE test_vacuum_integrity;

SELECT 'VACUUM and ANALYZE tests completed!' as result;
