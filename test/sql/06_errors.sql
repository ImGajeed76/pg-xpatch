-- Test 06: Error Handling
-- Tests that appropriate errors are raised for invalid operations

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Clean up from previous runs
DROP TABLE IF EXISTS test_errors;

-- Create a test table
CREATE TABLE test_errors (
    id INT,
    version INT,
    content TEXT
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

-- Test 2: DELETE should fail
\set ON_ERROR_STOP off
DELETE FROM test_errors WHERE version = 1;
\set ON_ERROR_STOP on

-- Test 3: Out-of-order insert should fail (version must be increasing)
\set ON_ERROR_STOP off
INSERT INTO test_errors VALUES (1, 1, 'Duplicate version');
\set ON_ERROR_STOP on

-- Test 4: Same version as existing should fail
\set ON_ERROR_STOP off
INSERT INTO test_errors VALUES (1, 2, 'Same as existing');
\set ON_ERROR_STOP on

-- Test 5: NULL version should fail
\set ON_ERROR_STOP off
INSERT INTO test_errors VALUES (1, NULL, 'Null version');
\set ON_ERROR_STOP on

-- Verify original data is unchanged
SELECT * FROM test_errors ORDER BY version;

-- Test 6: Valid insert still works after errors
INSERT INTO test_errors VALUES (1, 3, 'Third version - valid');
SELECT * FROM test_errors ORDER BY version;

-- Test 7: Configure on non-xpatch table should fail
CREATE TABLE regular_table (id INT, data TEXT);
\set ON_ERROR_STOP off
SELECT xpatch.configure('regular_table', order_by => 'id');
\set ON_ERROR_STOP on
DROP TABLE regular_table;

-- Test 8: Configure with non-existent column 
-- Note: Currently this stores the config but will error on first use
-- This could be improved in the future to validate columns upfront

-- Clean up
DROP TABLE test_errors;
