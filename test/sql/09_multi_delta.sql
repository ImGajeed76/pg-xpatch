-- Test 09: Multiple Delta Columns
-- Tests tables with multiple delta-compressed columns

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table with multiple delta columns
CREATE TABLE test_multi_delta (
    doc_id INT,
    version INT,
    title TEXT,
    content TEXT,
    summary TEXT,
    metadata JSONB
) USING xpatch;

SELECT xpatch.configure('test_multi_delta',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content', 'summary', 'metadata']::text[]
);

-- Insert multiple versions with changes to different columns
INSERT INTO test_multi_delta VALUES (1, 1, 'Document 1', 'Initial content for doc 1', 'Short summary v1', '{"tags": ["draft"]}');
INSERT INTO test_multi_delta VALUES (1, 2, 'Document 1', 'Updated content for doc 1', 'Short summary v1', '{"tags": ["draft"]}');
INSERT INTO test_multi_delta VALUES (1, 3, 'Document 1', 'Updated content for doc 1', 'Updated summary v3', '{"tags": ["draft", "reviewed"]}');
INSERT INTO test_multi_delta VALUES (1, 4, 'Document 1', 'Final content for doc 1', 'Final summary v4', '{"tags": ["published"]}');

-- Insert for second document
INSERT INTO test_multi_delta VALUES (2, 1, 'Document 2', 'Content A', 'Summary A', '{"author": "alice"}');
INSERT INTO test_multi_delta VALUES (2, 2, 'Document 2', 'Content B', 'Summary B', '{"author": "bob"}');

-- Verify all data can be retrieved correctly
SELECT doc_id, version, title, content, summary, metadata::text
FROM test_multi_delta 
ORDER BY doc_id, version;

-- Verify each delta column independently
SELECT doc_id, version, content FROM test_multi_delta WHERE doc_id = 1 ORDER BY version;
SELECT doc_id, version, summary FROM test_multi_delta WHERE doc_id = 1 ORDER BY version;
SELECT doc_id, version, metadata FROM test_multi_delta WHERE doc_id = 1 ORDER BY version;

-- Test with filters on different delta columns
SELECT doc_id, version FROM test_multi_delta WHERE content LIKE '%Final%';
SELECT doc_id, version FROM test_multi_delta WHERE summary LIKE '%v3%';
SELECT doc_id, version FROM test_multi_delta WHERE metadata @> '{"tags": ["published"]}';

-- Check compression stats (should show 3 delta columns)
SELECT total_rows, keyframe_count, delta_count FROM xpatch_stats('test_multi_delta');

-- Inspect storage for first document
SELECT version, seq, is_keyframe, column_name, delta_size_bytes 
FROM xpatch_inspect('test_multi_delta', 1) 
ORDER BY seq, column_name;

-- Clean up
DROP TABLE test_multi_delta;
