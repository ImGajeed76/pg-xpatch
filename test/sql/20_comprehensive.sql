-- Test 20: Comprehensive Edge Case Test Suite
-- Tests all edge cases and complex scenarios for pg-xpatch

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- ================================================================
-- SECTION 1: NULL HANDLING
-- ================================================================

\echo '=== SECTION 1: NULL HANDLING ==='

CREATE TABLE test_nulls (
    doc_id INT,
    version INT,
    content TEXT,
    metadata TEXT
) USING xpatch;

SELECT xpatch.configure('test_nulls',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content', 'metadata']::text[]
);

-- Test NULL in delta columns
INSERT INTO test_nulls VALUES (1, 1, 'content1', NULL);
INSERT INTO test_nulls VALUES (1, 2, NULL, 'has metadata');
INSERT INTO test_nulls VALUES (1, 3, 'content3', 'metadata3');
INSERT INTO test_nulls VALUES (1, 4, NULL, NULL);

SELECT 'NULL handling:' as test;
SELECT * FROM test_nulls ORDER BY version;

-- Verify NULLs are preserved correctly
SELECT 'NULL counts:' as test;
SELECT 
    COUNT(*) FILTER (WHERE content IS NULL) as null_content,
    COUNT(*) FILTER (WHERE metadata IS NULL) as null_metadata
FROM test_nulls;

DROP TABLE test_nulls;

-- ================================================================
-- SECTION 2: EMPTY STRINGS AND SPECIAL CHARACTERS
-- ================================================================

\echo '=== SECTION 2: EMPTY STRINGS AND SPECIAL CHARACTERS ==='

CREATE TABLE test_special_chars (
    id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_special_chars',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Empty string
INSERT INTO test_special_chars VALUES (1, 1, '');
INSERT INTO test_special_chars VALUES (1, 2, 'not empty');
INSERT INTO test_special_chars VALUES (1, 3, '');

-- Unicode and special characters
INSERT INTO test_special_chars VALUES (2, 1, 'Hello 世界 🌍');
INSERT INTO test_special_chars VALUES (2, 2, 'Привет мир');
INSERT INTO test_special_chars VALUES (2, 3, '日本語テスト');

-- Newlines and tabs
INSERT INTO test_special_chars VALUES (3, 1, E'line1\nline2\nline3');
INSERT INTO test_special_chars VALUES (3, 2, E'tab\there\ttoo');

-- Long content (but within page size limits)
INSERT INTO test_special_chars VALUES (4, 1, repeat('a', 1000));
INSERT INTO test_special_chars VALUES (4, 2, repeat('b', 1000));

SELECT 'Special chars test:' as test;
SELECT id, version, 
    CASE 
        WHEN length(content) > 50 THEN left(content, 50) || '...'
        ELSE content 
    END as content_preview,
    length(content) as len
FROM test_special_chars
ORDER BY id, version;

DROP TABLE test_special_chars;

-- ================================================================
-- SECTION 3: LARGE DELTA CHAINS
-- ================================================================

\echo '=== SECTION 3: LARGE DELTA CHAINS ==='

CREATE TABLE test_long_chain (
    id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_long_chain',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert 50 versions with small changes
DO $$
BEGIN
    FOR i IN 1..50 LOOP
        INSERT INTO test_long_chain VALUES (1, i, 'Version ' || i || ' content with some text');
    END LOOP;
END $$;

SELECT 'Long chain test:' as test;
SELECT COUNT(*) as total_versions FROM test_long_chain;

-- Verify first, middle, and last versions reconstruct correctly
SELECT 'Verify reconstruction:' as test;
SELECT version, content FROM test_long_chain 
WHERE version IN (1, 25, 50)
ORDER BY version;

DROP TABLE test_long_chain;

-- ================================================================
-- SECTION 4: MULTIPLE GROUPS WITH INTERLEAVED ACCESS
-- ================================================================

\echo '=== SECTION 4: MULTIPLE GROUPS ==='

CREATE TABLE test_multi_group (
    group_id INT,
    version INT,
    data TEXT
) USING xpatch;

SELECT xpatch.configure('test_multi_group',
    group_by => 'group_id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[]
);

-- Insert data for multiple groups
INSERT INTO test_multi_group VALUES (1, 1, 'G1V1');
INSERT INTO test_multi_group VALUES (2, 1, 'G2V1');
INSERT INTO test_multi_group VALUES (3, 1, 'G3V1');
INSERT INTO test_multi_group VALUES (1, 2, 'G1V2');
INSERT INTO test_multi_group VALUES (2, 2, 'G2V2');
INSERT INTO test_multi_group VALUES (1, 3, 'G1V3');

SELECT 'Multi-group data:' as test;
SELECT * FROM test_multi_group ORDER BY group_id, version;

-- Count per group
SELECT 'Versions per group:' as test;
SELECT group_id, COUNT(*) as versions, MAX(version) as max_ver
FROM test_multi_group
GROUP BY group_id
ORDER BY group_id;

DROP TABLE test_multi_group;

-- ================================================================
-- SECTION 5: DELETE EDGE CASES
-- ================================================================

\echo '=== SECTION 5: DELETE EDGE CASES ==='

CREATE TABLE test_delete_edge (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_delete_edge',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Test 5.1: Insert data first
INSERT INTO test_delete_edge VALUES (1, 1, 'v1');
INSERT INTO test_delete_edge VALUES (1, 2, 'v2');
INSERT INTO test_delete_edge VALUES (1, 3, 'v3');
INSERT INTO test_delete_edge VALUES (2, 1, 'other');

SELECT 'Initial state:' as test;
SELECT * FROM test_delete_edge ORDER BY doc_id, version;

-- Test 5.2: Delete with complex WHERE clause (cascade from v2)
DELETE FROM test_delete_edge WHERE doc_id = 1 AND version = 2;

SELECT 'After deleting v2+ (cascade):' as test;
SELECT * FROM test_delete_edge ORDER BY doc_id, version;

-- Test 5.3: Delete last remaining row in doc_id=1
DELETE FROM test_delete_edge WHERE doc_id = 1 AND version = 1;
SELECT 'After deleting all doc_id=1:' as test;
SELECT * FROM test_delete_edge ORDER BY doc_id, version;

-- Test 5.4: Insert after delete (must use higher version than before if seq cache not cleared)
INSERT INTO test_delete_edge VALUES (1, 10, 'new v10');
INSERT INTO test_delete_edge VALUES (1, 11, 'new v11');
SELECT 'After re-inserting:' as test;
SELECT * FROM test_delete_edge ORDER BY doc_id, version;

DROP TABLE test_delete_edge;

-- ================================================================
-- SECTION 6: INDEX OPERATIONS
-- ================================================================

\echo '=== SECTION 6: INDEX OPERATIONS ==='

CREATE TABLE test_indexes (
    doc_id INT,
    version INT,
    content TEXT,
    category TEXT
) USING xpatch;

SELECT xpatch.configure('test_indexes',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert data
INSERT INTO test_indexes VALUES (1, 1, 'content1', 'cat_a');
INSERT INTO test_indexes VALUES (1, 2, 'content2', 'cat_a');
INSERT INTO test_indexes VALUES (2, 1, 'content3', 'cat_b');
INSERT INTO test_indexes VALUES (2, 2, 'content4', 'cat_b');

-- Create various indexes
CREATE INDEX idx_doc ON test_indexes(doc_id);
CREATE INDEX idx_version ON test_indexes(version);
CREATE INDEX idx_category ON test_indexes(category);
CREATE INDEX idx_composite ON test_indexes(doc_id, version);

ANALYZE test_indexes;

-- Test index scans
SET enable_seqscan = off;

SELECT 'Index scan on doc_id:' as test;
EXPLAIN (COSTS OFF) SELECT * FROM test_indexes WHERE doc_id = 1;
SELECT * FROM test_indexes WHERE doc_id = 1 ORDER BY version;

SELECT 'Index scan on category:' as test;
SELECT * FROM test_indexes WHERE category = 'cat_b' ORDER BY doc_id, version;

SELECT 'Composite index scan:' as test;
SELECT * FROM test_indexes WHERE doc_id = 2 AND version = 1;

RESET enable_seqscan;

DROP TABLE test_indexes;

-- ================================================================
-- SECTION 7: AGGREGATE FUNCTIONS
-- ================================================================

\echo '=== SECTION 7: AGGREGATE FUNCTIONS ==='

CREATE TABLE test_aggregates (
    group_id INT,
    version INT,
    value INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_aggregates',
    group_by => 'group_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

INSERT INTO test_aggregates VALUES (1, 1, 10, 'a');
INSERT INTO test_aggregates VALUES (1, 2, 20, 'b');
INSERT INTO test_aggregates VALUES (1, 3, 30, 'c');
INSERT INTO test_aggregates VALUES (2, 1, 100, 'x');
INSERT INTO test_aggregates VALUES (2, 2, 200, 'y');

SELECT 'Basic aggregates:' as test;
SELECT 
    COUNT(*) as cnt,
    SUM(value) as total,
    AVG(value)::numeric(10,2) as average,
    MIN(value) as minimum,
    MAX(value) as maximum
FROM test_aggregates;

SELECT 'Group by aggregates:' as test;
SELECT 
    group_id,
    COUNT(*) as versions,
    SUM(value) as total_value,
    string_agg(content, ',' ORDER BY version) as all_content
FROM test_aggregates
GROUP BY group_id
ORDER BY group_id;

DROP TABLE test_aggregates;

-- ================================================================
-- SECTION 8: SUBQUERIES AND JOINS
-- ================================================================

\echo '=== SECTION 8: SUBQUERIES AND JOINS ==='

CREATE TABLE test_docs (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_docs',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

CREATE TABLE test_metadata (
    doc_id INT PRIMARY KEY,
    title TEXT
);

INSERT INTO test_docs VALUES (1, 1, 'v1');
INSERT INTO test_docs VALUES (1, 2, 'v2');
INSERT INTO test_docs VALUES (2, 1, 'other');

INSERT INTO test_metadata VALUES (1, 'Document One');
INSERT INTO test_metadata VALUES (2, 'Document Two');

-- Test JOIN
SELECT 'JOIN test:' as test;
SELECT d.doc_id, d.version, d.content, m.title
FROM test_docs d
JOIN test_metadata m ON d.doc_id = m.doc_id
ORDER BY d.doc_id, d.version;

-- Test subquery
SELECT 'Subquery test:' as test;
SELECT * FROM test_docs
WHERE doc_id IN (SELECT doc_id FROM test_metadata WHERE title LIKE '%One%')
ORDER BY version;

-- Test correlated subquery
SELECT 'Correlated subquery:' as test;
SELECT d1.doc_id, d1.version, d1.content
FROM test_docs d1
WHERE d1.version = (SELECT MAX(d2.version) FROM test_docs d2 WHERE d2.doc_id = d1.doc_id)
ORDER BY d1.doc_id;

DROP TABLE test_docs;
DROP TABLE test_metadata;

-- ================================================================
-- SECTION 9: TRANSACTIONS AND VISIBILITY
-- ================================================================

\echo '=== SECTION 9: TRANSACTION VISIBILITY ==='

CREATE TABLE test_txn (
    id INT,
    version INT,
    data TEXT
) USING xpatch;

SELECT xpatch.configure('test_txn',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[]
);

-- Test rollback
BEGIN;
INSERT INTO test_txn VALUES (1, 1, 'will rollback');
SELECT 'In transaction:' as test, COUNT(*) as cnt FROM test_txn;
ROLLBACK;

SELECT 'After rollback (should be 0):' as test, COUNT(*) as cnt FROM test_txn;

-- Test commit
BEGIN;
INSERT INTO test_txn VALUES (1, 1, 'committed');
COMMIT;

SELECT 'After commit:' as test, COUNT(*) as cnt FROM test_txn;

-- Test savepoints
BEGIN;
INSERT INTO test_txn VALUES (1, 2, 'before savepoint');
SAVEPOINT sp1;
INSERT INTO test_txn VALUES (1, 3, 'after savepoint');
ROLLBACK TO SAVEPOINT sp1;
-- After rollback to savepoint, v3 is gone so we still have v1 and v2
SELECT 'After savepoint rollback:' as test, COUNT(*) as cnt FROM test_txn;
COMMIT;

SELECT 'Final count (should be 2):' as test, COUNT(*) as cnt FROM test_txn;

-- Test rollback doesn't corrupt future inserts (critical bug fix verification)
TRUNCATE test_txn;
BEGIN;
INSERT INTO test_txn VALUES (1, 1, 'will rollback');
ROLLBACK;

-- This INSERT should succeed because the rolled-back tuple is not visible
INSERT INTO test_txn VALUES (1, 1, 'first after rollback');
INSERT INTO test_txn VALUES (1, 2, 'second after rollback');

SELECT 'Rollback integrity test:' as test;
SELECT * FROM test_txn ORDER BY version;

DROP TABLE test_txn;

-- ================================================================
-- SECTION 10: ORM COMPATIBILITY PATTERNS
-- ================================================================

\echo '=== SECTION 10: ORM COMPATIBILITY ==='

CREATE TABLE test_orm (
    id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_orm',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Test INSERT ... RETURNING (common ORM pattern)
SELECT 'INSERT RETURNING:' as test;
INSERT INTO test_orm VALUES (1, 1, 'inserted') RETURNING *;

-- Test SELECT with LIMIT/OFFSET (pagination)
INSERT INTO test_orm VALUES (1, 2, 'v2');
INSERT INTO test_orm VALUES (1, 3, 'v3');
INSERT INTO test_orm VALUES (2, 1, 'other');

SELECT 'LIMIT/OFFSET pagination:' as test;
SELECT * FROM test_orm ORDER BY id, version LIMIT 2 OFFSET 1;

-- Test EXISTS
SELECT 'EXISTS check:' as test;
SELECT EXISTS(SELECT 1 FROM test_orm WHERE id = 1) as doc_exists;

-- Test CASE expressions
SELECT 'CASE expression:' as test;
SELECT id, version,
    CASE 
        WHEN version = 1 THEN 'first'
        WHEN version = (SELECT MAX(version) FROM test_orm t2 WHERE t2.id = test_orm.id) THEN 'latest'
        ELSE 'middle'
    END as version_type
FROM test_orm
ORDER BY id, version;

-- Test DISTINCT
SELECT 'DISTINCT:' as test;
SELECT DISTINCT id FROM test_orm ORDER BY id;

DROP TABLE test_orm;

-- ================================================================
-- SECTION 11: MULTIPLE DELTA COLUMNS
-- ================================================================

\echo '=== SECTION 11: MULTIPLE DELTA COLUMNS ==='

CREATE TABLE test_multi_delta (
    id INT,
    version INT,
    title TEXT,
    body TEXT,
    metadata JSONB
) USING xpatch;

SELECT xpatch.configure('test_multi_delta',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['title', 'body', 'metadata']::text[]
);

INSERT INTO test_multi_delta VALUES (1, 1, 'Title v1', 'Body v1', '{"key": "value1"}');
INSERT INTO test_multi_delta VALUES (1, 2, 'Title v1', 'Body v2 changed', '{"key": "value1"}');
INSERT INTO test_multi_delta VALUES (1, 3, 'Title v3 changed', 'Body v2 changed', '{"key": "value3"}');

SELECT 'Multi-delta columns:' as test;
SELECT * FROM test_multi_delta ORDER BY version;

-- Verify each column reconstructs correctly
SELECT 'Verify individual columns:' as test;
SELECT version, title, body, metadata->>'key' as key_value
FROM test_multi_delta
ORDER BY version;

DROP TABLE test_multi_delta;

-- ================================================================
-- SECTION 12: CONCURRENT ACCESS SIMULATION
-- ================================================================

\echo '=== SECTION 12: CONCURRENT ACCESS ==='

CREATE TABLE test_concurrent (
    id INT,
    version INT,
    data TEXT
) USING xpatch;

SELECT xpatch.configure('test_concurrent',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[]
);

-- Simulate rapid sequential inserts (advisory locks should serialize)
INSERT INTO test_concurrent VALUES (1, 1, 'seq1');
INSERT INTO test_concurrent VALUES (1, 2, 'seq2');
INSERT INTO test_concurrent VALUES (1, 3, 'seq3');
INSERT INTO test_concurrent VALUES (1, 4, 'seq4');
INSERT INTO test_concurrent VALUES (1, 5, 'seq5');

SELECT 'Sequential inserts:' as test;
SELECT * FROM test_concurrent ORDER BY version;

-- Verify data integrity
SELECT 'Data integrity check:' as test;
SELECT 
    COUNT(*) as total,
    COUNT(DISTINCT version) as unique_versions,
    MIN(version) as min_ver,
    MAX(version) as max_ver
FROM test_concurrent WHERE id = 1;

DROP TABLE test_concurrent;

-- ================================================================
-- SECTION 13: ERROR HANDLING
-- ================================================================

\echo '=== SECTION 13: ERROR HANDLING ==='

DROP TABLE IF EXISTS test_errors;
CREATE TABLE test_errors (
    id INT,
    version INT,
    data TEXT
) USING xpatch;

SELECT xpatch.configure('test_errors',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[]
);

INSERT INTO test_errors VALUES (1, 1, 'first');
INSERT INTO test_errors VALUES (1, 2, 'second');

-- Duplicate/lower version values are now allowed (auto-seq handles ordering)
INSERT INTO test_errors VALUES (1, 1, 'duplicate version value');
INSERT INTO test_errors VALUES (1, 0, 'lower version value');

-- Test: UPDATE should fail
\set ON_ERROR_STOP off
UPDATE test_errors SET data = 'modified' WHERE id = 1 AND version = 1; -- Should fail
\set ON_ERROR_STOP on

-- Verify table has all 4 rows
SELECT 'Table has all rows:' as test;
SELECT id, version, data FROM test_errors ORDER BY _xp_seq;

DROP TABLE test_errors;

-- ================================================================
-- SECTION 14: STATISTICS AND EXPLAIN
-- ================================================================

\echo '=== SECTION 14: STATISTICS ==='

CREATE TABLE test_stats (
    id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_stats',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert data
INSERT INTO test_stats VALUES (1, 1, repeat('x', 100));
INSERT INTO test_stats VALUES (1, 2, repeat('y', 100));
INSERT INTO test_stats VALUES (2, 1, repeat('z', 100));

-- Run ANALYZE
ANALYZE test_stats;

-- Check pg_class statistics
SELECT 'pg_class stats:' as test;
SELECT reltuples::int as tuples, relpages as pages
FROM pg_class WHERE relname = 'test_stats';

-- Check pg_stats for column statistics
SELECT 'Column stats exist:' as test;
SELECT COUNT(*) > 0 as has_stats
FROM pg_stats WHERE tablename = 'test_stats';

-- Test EXPLAIN output
SELECT 'EXPLAIN test:' as test;
EXPLAIN (COSTS OFF) SELECT * FROM test_stats WHERE id = 1;

DROP TABLE test_stats;

-- ================================================================
-- SECTION 15: CLEANUP AND FINAL VERIFICATION
-- ================================================================

\echo '=== SECTION 15: FINAL VERIFICATION ==='

-- Verify no test tables left behind
SELECT 'Orphaned test tables:' as test;
SELECT COUNT(*) as orphaned_tables
FROM pg_tables 
WHERE schemaname = 'public' 
AND tablename LIKE 'test_%';

-- Summary
SELECT '========================================' as separator;
SELECT 'COMPREHENSIVE TEST SUITE COMPLETED!' as result;
SELECT '========================================' as separator;
