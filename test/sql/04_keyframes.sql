-- Test 04: Keyframe Behavior
-- Tests keyframe creation and configuration

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table with small keyframe interval for testing
CREATE TABLE test_keyframes (
    id INT,
    seq INT,
    data TEXT
) USING xpatch;

SELECT xpatch.configure('test_keyframes',
    group_by => 'id',
    order_by => 'seq',
    delta_columns => ARRAY['data']::text[],
    keyframe_every => 5  -- Keyframe every 5 rows
);

-- Insert 12 rows - should have keyframes at 1, 6, 11
INSERT INTO test_keyframes VALUES (1, 1, 'Version 1 - should be keyframe');
INSERT INTO test_keyframes VALUES (1, 2, 'Version 2 - delta');
INSERT INTO test_keyframes VALUES (1, 3, 'Version 3 - delta');
INSERT INTO test_keyframes VALUES (1, 4, 'Version 4 - delta');
INSERT INTO test_keyframes VALUES (1, 5, 'Version 5 - delta');
INSERT INTO test_keyframes VALUES (1, 6, 'Version 6 - should be keyframe');
INSERT INTO test_keyframes VALUES (1, 7, 'Version 7 - delta');
INSERT INTO test_keyframes VALUES (1, 8, 'Version 8 - delta');
INSERT INTO test_keyframes VALUES (1, 9, 'Version 9 - delta');
INSERT INTO test_keyframes VALUES (1, 10, 'Version 10 - delta');
INSERT INTO test_keyframes VALUES (1, 11, 'Version 11 - should be keyframe');
INSERT INTO test_keyframes VALUES (1, 12, 'Version 12 - delta');

-- Verify all data is retrievable
SELECT seq, left(data, 30) as data_preview FROM test_keyframes ORDER BY seq;

-- Check stats
SELECT total_rows FROM xpatch_stats('test_keyframes');

-- Inspect to see storage details (keyframes vs deltas)
SELECT seq, is_keyframe, delta_size_bytes 
FROM xpatch_inspect('test_keyframes', 1) 
ORDER BY seq;

-- Test compress_depth setting
CREATE TABLE test_compress_depth (
    id INT,
    version INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('test_compress_depth',
    group_by => 'id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[],
    compress_depth => 3  -- Try 3 previous versions for best delta
);

-- Insert versions with similar content
INSERT INTO test_compress_depth VALUES (1, 1, 'AAAAAAAAAA');
INSERT INTO test_compress_depth VALUES (1, 2, 'AAAAAAAAAB');  -- 1 char diff from v1
INSERT INTO test_compress_depth VALUES (1, 3, 'AAAAAAAAAC');  -- 1 char diff from v1, 1 from v2
INSERT INTO test_compress_depth VALUES (1, 4, 'AAAAAAAAAB');  -- Same as v2!

-- Verify retrieval
SELECT version, content FROM test_compress_depth ORDER BY version;

-- Clean up
DROP TABLE test_keyframes;
DROP TABLE test_compress_depth;
