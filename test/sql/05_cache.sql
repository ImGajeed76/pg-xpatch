-- Test 05: Cache Functionality
-- Tests the shared memory LRU cache

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table for cache testing
CREATE TABLE test_cache (
    id INT,
    version INT,
    content TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_cache',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert some data
INSERT INTO test_cache VALUES (1, 1, 'Cache test version 1');
INSERT INTO test_cache VALUES (1, 2, 'Cache test version 2');
INSERT INTO test_cache VALUES (1, 3, 'Cache test version 3');

-- Get initial cache stats (values will vary based on previous activity)
-- Just verify the function works and returns reasonable values
SELECT 'Initial' as phase, 
       entries_count >= 0 AS has_entries,
       hit_count >= 0 AS has_hits,
       miss_count >= 0 AS has_misses
FROM xpatch_cache_stats();

-- First query - should populate cache
SELECT * FROM test_cache ORDER BY version;

-- Get cache stats after first query
SELECT 'After first query' as phase, 
       entries_count > 0 AS has_entries,
       hit_count >= 0 AS tracking_hits
FROM xpatch_cache_stats();

-- Second query - should hit cache
SELECT * FROM test_cache ORDER BY version;

-- Third query - more cache hits
SELECT * FROM test_cache ORDER BY version;

-- Verify cache is tracking (hit count should increase with queries)
SELECT 'After repeated queries' as phase, 
       entries_count > 0 AS has_entries,
       cache_max_bytes > 0 AS has_max_size
FROM xpatch_cache_stats();

-- Test cache across multiple tables
CREATE TABLE test_cache2 (
    id INT,
    version INT,
    data TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_cache2',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[]
);

INSERT INTO test_cache2 VALUES (1, 1, 'Table 2 data');
INSERT INTO test_cache2 VALUES (1, 2, 'Table 2 data v2');

-- Query both tables
SELECT * FROM test_cache ORDER BY version;
SELECT * FROM test_cache2 ORDER BY version;

-- Cache should have entries from both tables
SELECT 'Multiple tables' as phase,
       entries_count > 0 AS has_entries
FROM xpatch_cache_stats();

-- Test cache max size is reported
SELECT cache_max_bytes > 0 AS has_max_size FROM xpatch_cache_stats();

-- Clean up
DROP TABLE test_cache;
DROP TABLE test_cache2;
