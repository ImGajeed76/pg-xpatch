-- Test 08: Parallel Scan
-- 
-- This test verifies that parallel scans work correctly.
-- The _xp_seq column is automatically added by the event trigger when
-- tables are created with USING xpatch. This allows parallel workers
-- to independently reconstruct delta-compressed content.

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table (no need to add _xp_seq - it's added automatically)
CREATE TABLE test_parallel (
    id INT,
    version INT,
    data TEXT
) USING xpatch;

SELECT xpatch.configure('test_parallel',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[]
);

-- Insert data: 50 groups x 10 versions = 500 rows
INSERT INTO test_parallel (id, version, data)
SELECT id, version, 'Data for id=' || id || ' version=' || version || ' ' || repeat('x', 100)
FROM generate_series(1, 50) id, generate_series(1, 10) version;

-- Verify row count before enabling parallel
SELECT count(*) as total_rows FROM test_parallel;

-- Configure parallel query settings to force parallel execution
SET max_parallel_workers_per_gather = 2;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;

-- Test 1: Show that parallel scan is planned
EXPLAIN (COSTS OFF) SELECT count(*) FROM test_parallel;

-- Test 2: Execute with parallel scan and verify correct count
SELECT count(*) as count_result FROM test_parallel;

-- Test 3: Verify data retrieval works correctly with parallel scan
SELECT count(*) as rows_retrieved,
       sum(length(data)) as total_data_length
FROM test_parallel WHERE id <= 10;

-- Test 4: Verify aggregation works with parallel scan
SELECT id, count(*) as versions, max(version) as latest
FROM test_parallel 
WHERE id IN (1, 25, 50)
GROUP BY id
ORDER BY id;

-- Test 5: Verify filter on delta column works with parallel scan
SELECT id, version FROM test_parallel 
WHERE data LIKE '%id=25 version=5%'
ORDER BY id, version;

-- Reset parallel settings
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;

-- Clean up
DROP TABLE test_parallel;
