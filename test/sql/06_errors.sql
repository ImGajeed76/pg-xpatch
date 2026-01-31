-- Test 06: Error Handling
-- Tests that appropriate errors are raised for invalid operations

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Clean up from previous runs
DROP TABLE IF EXISTS test_errors;

-- Create a test table (content must be NOT NULL for delta encoding)
CREATE TABLE test_errors (
    id INT,
    version INT,
    content TEXT NOT NULL NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_errors',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert initial data
INSERT INTO test_errors VALUES (1, 1, 'Initial version');
INSERT INTO test_errors VALUES (1, 2, 'Second version');

-- Test 1: UPDATE should fail
\set ON_ERROR_STOP off
UPDATE test_errors SET content = 'Modified' WHERE version = 1;
\set ON_ERROR_STOP on

-- Test 2: DELETE should fail (basic DELETE without cascade)
\set ON_ERROR_STOP off
DELETE FROM test_errors WHERE version = 1;
\set ON_ERROR_STOP on

-- Test 3: Duplicate/out-of-order version values are now allowed
-- (auto-seq handles physical ordering, user version is just data)
INSERT INTO test_errors VALUES (1, 1, 'Duplicate version value');
INSERT INTO test_errors VALUES (1, 2, 'Same version value');

-- Test 4: NULL version is allowed (version column is just user data)
INSERT INTO test_errors VALUES (1, NULL, 'Null version');

-- Verify all rows exist (5 rows total: 2 original + 3 new)
SELECT id, version, content FROM test_errors ORDER BY _xp_seq;

-- Test 5: Valid insert still works
INSERT INTO test_errors VALUES (1, 10, 'Tenth version');
SELECT count(*) AS total_rows FROM test_errors;

-- Test 6: Configure on non-xpatch table should fail
CREATE TABLE regular_table (id INT, data TEXT);
\set ON_ERROR_STOP off
SELECT xpatch.configure('regular_table', order_by => 'id');
\set ON_ERROR_STOP on
DROP TABLE regular_table;

-- Test 7: Nullable delta column should be rejected
DROP TABLE IF EXISTS test_nullable_delta;
CREATE TABLE test_nullable_delta (id INT, ver INT, data TEXT) USING xpatch;
\set ON_ERROR_STOP off
SELECT xpatch.configure('test_nullable_delta', 
    group_by => 'id', 
    delta_columns => ARRAY['data']::text[]
);
\set ON_ERROR_STOP on
DROP TABLE test_nullable_delta;

-- Test 8: NOT NULL delta column should work
DROP TABLE IF EXISTS test_notnull_delta;
CREATE TABLE test_notnull_delta (id INT, ver INT, data TEXT NOT NULL) USING xpatch;
SELECT xpatch.configure('test_notnull_delta', 
    group_by => 'id', 
    delta_columns => ARRAY['data']::text[]
);
DROP TABLE test_notnull_delta;

-- Clean up
DROP TABLE test_errors;
