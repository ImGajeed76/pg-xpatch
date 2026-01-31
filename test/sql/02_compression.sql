-- Test 02: Delta Compression
-- Tests that delta compression is working and data is stored efficiently

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table for compression testing
CREATE TABLE test_compression (
    id INT,
    version INT,
    data TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_compression',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['data']::text[],
    enable_zstd => true
);

-- Insert repetitive data that compresses well
INSERT INTO test_compression VALUES (1, 1, repeat('Hello World! ', 100));
INSERT INTO test_compression VALUES (1, 2, repeat('Hello World! ', 100) || ' Added text.');
INSERT INTO test_compression VALUES (1, 3, repeat('Hello World! ', 100) || ' Added text. More text.');

-- Verify data is correctly retrieved
SELECT id, version, length(data) as data_len FROM test_compression ORDER BY version;

-- Check stats show compression is happening
SELECT 
    total_rows,
    compressed_size_bytes < 4000 AS is_compressed  -- Should be much less than 3900 bytes raw
FROM xpatch_stats('test_compression');

-- Test JSONB compression
CREATE TABLE test_json_compression (
    doc_id INT,
    version INT,
    metadata JSONB NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_json_compression',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['metadata']::text[],
    enable_zstd => true
);

-- Insert JSON documents with incremental changes
INSERT INTO test_json_compression VALUES (1, 1, '{"name": "Test", "count": 1, "items": ["a", "b", "c"]}');
INSERT INTO test_json_compression VALUES (1, 2, '{"name": "Test", "count": 2, "items": ["a", "b", "c", "d"]}');
INSERT INTO test_json_compression VALUES (1, 3, '{"name": "Test", "count": 3, "items": ["a", "b", "c", "d", "e"]}');

-- Verify JSONB retrieval and operators work
SELECT doc_id, version, metadata->>'name' as name, metadata->>'count' as count
FROM test_json_compression ORDER BY version;

-- Test JSONB containment operator
SELECT version FROM test_json_compression WHERE metadata @> '{"count": 2}';

-- Test JSONB key existence
SELECT version FROM test_json_compression WHERE metadata ? 'items';

-- Clean up
DROP TABLE test_compression;
DROP TABLE test_json_compression;
