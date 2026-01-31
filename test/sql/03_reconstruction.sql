-- Test 03: Data Reconstruction
-- Tests that delta-compressed data is correctly reconstructed on read

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- Create a table with multiple version chains
CREATE TABLE test_reconstruct (
    doc_id INT,
    version INT,
    content TEXT NOT NULL
) USING xpatch;

SELECT xpatch.configure('test_reconstruct',
    group_by => 'doc_id',
    order_by => 'version',
    delta_columns => ARRAY['content']::text[]
);

-- Insert a chain of versions for doc 1
INSERT INTO test_reconstruct VALUES (1, 1, 'Document 1 - Initial content');
INSERT INTO test_reconstruct VALUES (1, 2, 'Document 1 - Modified content');
INSERT INTO test_reconstruct VALUES (1, 3, 'Document 1 - Further modifications');
INSERT INTO test_reconstruct VALUES (1, 4, 'Document 1 - Even more changes');
INSERT INTO test_reconstruct VALUES (1, 5, 'Document 1 - Final version');

-- Insert a separate chain for doc 2
INSERT INTO test_reconstruct VALUES (2, 1, 'Document 2 - Start');
INSERT INTO test_reconstruct VALUES (2, 2, 'Document 2 - Middle');
INSERT INTO test_reconstruct VALUES (2, 3, 'Document 2 - End');

-- Verify each version is reconstructed correctly
SELECT doc_id, version, content FROM test_reconstruct WHERE doc_id = 1 ORDER BY version;

-- Verify doc 2 is independent
SELECT doc_id, version, content FROM test_reconstruct WHERE doc_id = 2 ORDER BY version;

-- Test accessing versions in random order (tests reconstruction)
SELECT content FROM test_reconstruct WHERE doc_id = 1 AND version = 3;
SELECT content FROM test_reconstruct WHERE doc_id = 1 AND version = 1;
SELECT content FROM test_reconstruct WHERE doc_id = 1 AND version = 5;
SELECT content FROM test_reconstruct WHERE doc_id = 1 AND version = 2;

-- Test with ORDER BY DESC (reverse order reconstruction)
SELECT version, content FROM test_reconstruct WHERE doc_id = 1 ORDER BY version DESC;

-- Test DISTINCT ON to get latest version per doc
SELECT DISTINCT ON (doc_id) doc_id, version, content 
FROM test_reconstruct 
ORDER BY doc_id, version DESC;

-- Verify data integrity with string functions
SELECT doc_id, version, 
       left(content, 12) as prefix,
       length(content) as len
FROM test_reconstruct 
ORDER BY doc_id, version;

-- Clean up
DROP TABLE test_reconstruct;
