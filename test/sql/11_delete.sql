-- Test 11: DELETE with Cascade
-- Tests DELETE functionality which cascades to all subsequent versions

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- ================================================================
-- Test 1: Basic DELETE - deletes a single version (the last one)
-- ================================================================

CREATE TABLE test_delete_basic (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_delete_basic',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert 3 versions for doc_id=1
INSERT INTO test_delete_basic VALUES (1, 1, 'Version 1 content');
INSERT INTO test_delete_basic VALUES (1, 2, 'Version 2 content');
INSERT INTO test_delete_basic VALUES (1, 3, 'Version 3 content');

-- Verify initial state
SELECT 'Before delete:' as phase;
SELECT * FROM test_delete_basic ORDER BY doc_id, version;
SELECT COUNT(*) as count_before FROM test_delete_basic;

-- Delete the last version (version 3) - should only delete version 3
DELETE FROM test_delete_basic WHERE doc_id = 1 AND version = 3;

SELECT 'After deleting version 3:' as phase;
SELECT * FROM test_delete_basic ORDER BY doc_id, version;
SELECT COUNT(*) as count_after FROM test_delete_basic;

DROP TABLE test_delete_basic;

-- ================================================================
-- Test 2: CASCADE DELETE - deletes a version and all subsequent versions
-- ================================================================

CREATE TABLE test_delete_cascade (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_delete_cascade',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert 5 versions for doc_id=1
INSERT INTO test_delete_cascade VALUES (1, 1, 'Version 1');
INSERT INTO test_delete_cascade VALUES (1, 2, 'Version 2 with more content');
INSERT INTO test_delete_cascade VALUES (1, 3, 'Version 3 even more content');
INSERT INTO test_delete_cascade VALUES (1, 4, 'Version 4 with additional info');
INSERT INTO test_delete_cascade VALUES (1, 5, 'Version 5 final version');

SELECT 'Before cascade delete:' as phase;
SELECT * FROM test_delete_cascade ORDER BY doc_id, version;
SELECT COUNT(*) as count_before FROM test_delete_cascade;

-- Delete version 3 - should cascade delete versions 3, 4, 5
DELETE FROM test_delete_cascade WHERE doc_id = 1 AND version = 3;

SELECT 'After deleting version 3 (cascade to 4,5):' as phase;
SELECT * FROM test_delete_cascade ORDER BY doc_id, version;
SELECT COUNT(*) as count_after FROM test_delete_cascade;

-- Verify we can still read the remaining versions correctly
SELECT 'Verify remaining data is readable:' as phase;
SELECT doc_id, version, content FROM test_delete_cascade ORDER BY version;

DROP TABLE test_delete_cascade;

-- ================================================================
-- Test 3: DELETE first version - cascades to delete ALL versions in group
-- ================================================================

CREATE TABLE test_delete_all (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_delete_all',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert versions for multiple docs
INSERT INTO test_delete_all VALUES (1, 1, 'Doc1 v1');
INSERT INTO test_delete_all VALUES (1, 2, 'Doc1 v2');
INSERT INTO test_delete_all VALUES (1, 3, 'Doc1 v3');
INSERT INTO test_delete_all VALUES (2, 1, 'Doc2 v1');
INSERT INTO test_delete_all VALUES (2, 2, 'Doc2 v2');

SELECT 'Before delete all:' as phase;
SELECT * FROM test_delete_all ORDER BY doc_id, version;

-- Delete version 1 of doc_id=1 - should delete ALL versions of doc 1
DELETE FROM test_delete_all WHERE doc_id = 1 AND version = 1;

SELECT 'After deleting doc_id=1 version=1 (all doc1 gone):' as phase;
SELECT * FROM test_delete_all ORDER BY doc_id, version;

-- Doc 2 should still be intact
SELECT 'Doc 2 should still exist:' as phase;
SELECT COUNT(*) as doc2_count FROM test_delete_all WHERE doc_id = 2;

DROP TABLE test_delete_all;

-- ================================================================
-- Test 4: INSERT after DELETE - can add new versions
-- ================================================================

CREATE TABLE test_delete_reinsert (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_delete_reinsert',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert and delete
INSERT INTO test_delete_reinsert VALUES (1, 1, 'First');
INSERT INTO test_delete_reinsert VALUES (1, 2, 'Second');
INSERT INTO test_delete_reinsert VALUES (1, 3, 'Third');

DELETE FROM test_delete_reinsert WHERE doc_id = 1 AND version = 2;

SELECT 'After deleting v2+ (only v1 remains):' as phase;
SELECT * FROM test_delete_reinsert ORDER BY doc_id, version;

-- Now insert new versions - must be > 1 (last remaining)
INSERT INTO test_delete_reinsert VALUES (1, 4, 'Fourth - new after delete');
INSERT INTO test_delete_reinsert VALUES (1, 5, 'Fifth - also new');

SELECT 'After inserting new versions:' as phase;
SELECT * FROM test_delete_reinsert ORDER BY doc_id, version;

DROP TABLE test_delete_reinsert;

-- ================================================================
-- Test 5: UPDATE should still be blocked
-- ================================================================

CREATE TABLE test_update_blocked (
    doc_id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_update_blocked',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

INSERT INTO test_update_blocked VALUES (1, 1, 'Original');

-- This should fail with an error
\set ON_ERROR_STOP off
UPDATE test_update_blocked SET content = 'Modified' WHERE doc_id = 1;
\set ON_ERROR_STOP on

SELECT 'UPDATE was blocked (expected):' as phase;
SELECT * FROM test_update_blocked;

DROP TABLE test_update_blocked;

-- ================================================================
-- Test 6: DELETE with no group_by column (single group)
-- ================================================================

CREATE TABLE test_delete_no_group (
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_delete_no_group',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

INSERT INTO test_delete_no_group VALUES (1, 'v1');
INSERT INTO test_delete_no_group VALUES (2, 'v2');
INSERT INTO test_delete_no_group VALUES (3, 'v3');
INSERT INTO test_delete_no_group VALUES (4, 'v4');

SELECT 'Before delete (no group_by):' as phase;
SELECT * FROM test_delete_no_group ORDER BY version;

DELETE FROM test_delete_no_group WHERE version = 2;

SELECT 'After deleting version 2 (cascade 2,3,4):' as phase;
SELECT * FROM test_delete_no_group ORDER BY version;

DROP TABLE test_delete_no_group;

SELECT 'All DELETE tests passed!' as result;
