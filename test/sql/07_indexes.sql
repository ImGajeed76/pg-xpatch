-- Test 07: Index Support
-- Tests index creation and index scans on xpatch tables

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a test table
CREATE TABLE test_indexes (
    doc_id INT,
    version INT,
    title TEXT NOT NULL,
    content TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_indexes',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert test data
INSERT INTO test_indexes VALUES (1, 1, 'Doc A', 'Content A v1');
INSERT INTO test_indexes VALUES (1, 2, 'Doc A', 'Content A v2');
INSERT INTO test_indexes VALUES (2, 1, 'Doc B', 'Content B v1');
INSERT INTO test_indexes VALUES (2, 2, 'Doc B', 'Content B v2');
INSERT INTO test_indexes VALUES (3, 1, 'Doc C', 'Content C v1');
INSERT INTO test_indexes VALUES (4, 1, 'Doc D', 'Content D v1');
INSERT INTO test_indexes VALUES (4, 2, 'Doc D', 'Content D v2');
INSERT INTO test_indexes VALUES (4, 3, 'Doc D', 'Content D v3');

-- Create indexes on non-delta columns
CREATE INDEX idx_test_doc_id ON test_indexes(doc_id);
CREATE INDEX idx_test_version ON test_indexes(version);
CREATE INDEX idx_test_title ON test_indexes(title);

-- Run ANALYZE to update statistics
ANALYZE test_indexes;

-- Force index scans for testing
SET enable_seqscan = off;

-- Test index scan on doc_id
EXPLAIN (COSTS OFF) SELECT * FROM test_indexes WHERE doc_id = 2;
SELECT * FROM test_indexes WHERE doc_id = 2 ORDER BY version;

-- Test index scan on version
EXPLAIN (COSTS OFF) SELECT * FROM test_indexes WHERE version = 1;
SELECT doc_id, title FROM test_indexes WHERE version = 1 ORDER BY doc_id;

-- Test index scan on title (non-delta, non-config column)
EXPLAIN (COSTS OFF) SELECT * FROM test_indexes WHERE title = 'Doc B';
SELECT * FROM test_indexes WHERE title = 'Doc B' ORDER BY version;

-- Create index on delta column (should work - indexes reconstructed values)
CREATE INDEX idx_test_content ON test_indexes(content);
ANALYZE test_indexes;

-- Test index scan on delta column
EXPLAIN (COSTS OFF) SELECT * FROM test_indexes WHERE content = 'Content B v1';
SELECT doc_id, version, content FROM test_indexes WHERE content = 'Content B v1';

-- Reset to default
SET enable_seqscan = on;

-- Test composite index
CREATE INDEX idx_test_composite ON test_indexes(doc_id, version);
ANALYZE test_indexes;

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM test_indexes WHERE doc_id = 1 AND version = 2;
SELECT * FROM test_indexes WHERE doc_id = 1 AND version = 2;
SET enable_seqscan = on;

-- Clean up
DROP TABLE test_indexes;
