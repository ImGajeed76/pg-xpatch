-- Test 08: Parallel Scan Support
-- Tests parallel query execution on xpatch tables

-- Suppress NOTICE and WARNING messages for cleaner test output
-- (Parallel scans may produce snapshot reference warnings which are cosmetic)
SET client_min_messages = error;

-- Create a table with enough data to trigger parallel scans
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

-- Insert enough data to make parallel scans worthwhile
-- 100 groups × 20 versions = 2000 rows
INSERT INTO test_parallel 
SELECT id, version, 'Data for id=' || id || ' version=' || version || ' ' || repeat('x', 100)
FROM generate_series(1, 100) id, generate_series(1, 20) version;

-- Verify row count
SELECT count(*) as total_rows FROM test_parallel;

-- Configure parallel query settings to force parallel execution
SET max_parallel_workers_per_gather = 2;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;

-- Test 1: Parallel sequential scan with count
-- Should show "Parallel Seq Scan" in plan
EXPLAIN (COSTS OFF) SELECT count(*) FROM test_parallel;

-- Execute and verify correct count
SELECT count(*) as parallel_count FROM test_parallel;

-- Test 2: Parallel scan with data retrieval
-- Verify data is correctly reconstructed across workers
EXPLAIN (COSTS OFF) SELECT id, version, length(data) FROM test_parallel WHERE id <= 10;

SELECT count(*) as rows_retrieved,
       sum(length(data)) as total_data_length
FROM test_parallel WHERE id <= 10;

-- Test 3: Parallel scan with aggregation
EXPLAIN (COSTS OFF) SELECT id, count(*), max(version) FROM test_parallel GROUP BY id;

SELECT id, count(*) as versions, max(version) as latest
FROM test_parallel 
WHERE id IN (1, 50, 100)
GROUP BY id
ORDER BY id;

-- Test 4: Parallel scan with filter on delta column
-- This tests that reconstruction works correctly in parallel workers
EXPLAIN (COSTS OFF) SELECT * FROM test_parallel WHERE data LIKE '%id=50%';

SELECT id, version FROM test_parallel 
WHERE data LIKE '%id=50 version=10%'
ORDER BY id, version;

-- Reset parallel settings
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;

-- Test 5: Verify same results with and without parallel
-- Non-parallel count
SET max_parallel_workers_per_gather = 0;
SELECT count(*) as sequential_count FROM test_parallel;

-- Parallel count (re-enable)
SET max_parallel_workers_per_gather = 2;
SET parallel_tuple_cost = 0;
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;
SELECT count(*) as parallel_count FROM test_parallel;

-- Reset
RESET max_parallel_workers_per_gather;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET min_parallel_table_scan_size;

-- Clean up
DROP TABLE test_parallel;
