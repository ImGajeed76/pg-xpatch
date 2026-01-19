-- Test 01: Basic INSERT and SELECT
-- Tests basic table creation, insertion, and selection

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a simple table with auto-detection
CREATE TABLE test_basic (
    id INT,
    version INT,
    content TEXT
) USING xpatch;

-- Insert some rows
INSERT INTO test_basic VALUES (1, 1, 'First version');
INSERT INTO test_basic VALUES (1, 2, 'Second version');
INSERT INTO test_basic VALUES (1, 3, 'Third version');

-- Select all rows (excluding _xp_seq for cleaner output)
SELECT id, version, content FROM test_basic ORDER BY version;

-- Select with WHERE clause
SELECT id, version, content FROM test_basic WHERE version = 2;

-- Select specific columns
SELECT id, version FROM test_basic ORDER BY version;

-- Count rows
SELECT COUNT(*) FROM test_basic;

-- Test with explicit configuration using xpatch.configure()
CREATE TABLE test_configured (
    doc_id INT,
    rev INT,
    title TEXT,
    body TEXT
) USING xpatch;

SELECT xpatch.configure('test_configured', 
    group_by => 'doc_id',
    order_by => 'rev',
    delta_columns => ARRAY['body']::text[]
);

-- Insert into configured table
INSERT INTO test_configured VALUES (1, 1, 'Doc 1', 'Body version 1');
INSERT INTO test_configured VALUES (1, 2, 'Doc 1', 'Body version 2 with changes');
INSERT INTO test_configured VALUES (2, 1, 'Doc 2', 'Different document');
INSERT INTO test_configured VALUES (2, 2, 'Doc 2', 'Different document updated');

-- Verify data retrieval (excluding _xp_seq)
SELECT doc_id, rev, title, body FROM test_configured ORDER BY doc_id, rev;

-- Verify group_by works - each doc can have same rev numbers
SELECT doc_id, COUNT(*) as versions FROM test_configured GROUP BY doc_id ORDER BY doc_id;

-- Clean up
DROP TABLE test_basic;
DROP TABLE test_configured;
