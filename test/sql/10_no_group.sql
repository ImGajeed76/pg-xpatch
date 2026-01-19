-- Test 10: No Group By (Whole Table as Single Group)
-- Tests tables without group_by configuration

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table without group_by (entire table is one group)
CREATE TABLE test_no_group (
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_no_group',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert sequential versions (all in same implicit group)
INSERT INTO test_no_group VALUES (1, 'First version of the document');
INSERT INTO test_no_group VALUES (2, 'Second version of the document');
INSERT INTO test_no_group VALUES (3, 'Third version of the document');
INSERT INTO test_no_group VALUES (4, 'Fourth version of the document');
INSERT INTO test_no_group VALUES (5, 'Fifth version of the document');

-- Verify all data can be retrieved
SELECT version, content FROM test_no_group ORDER BY version;

-- Verify count
SELECT count(*) as total FROM test_no_group;

-- Test filtering
SELECT version, content FROM test_no_group WHERE version >= 3 ORDER BY version;

-- Check stats - should show 1 group
SELECT total_rows, total_groups, keyframe_count, delta_count 
FROM xpatch_stats('test_no_group');

-- Note: xpatch_inspect requires a group value, but with no group_by configured,
-- there's only one implicit group. Skip inspect test for no-group tables.
-- For tables with group_by, use: xpatch_inspect('table', group_value)

-- Test latest version pattern
SELECT version, content FROM test_no_group ORDER BY version DESC LIMIT 1;

-- Test with aggregation
SELECT max(version) as latest_version, count(*) as total_versions FROM test_no_group;

-- Clean up
DROP TABLE test_no_group;
