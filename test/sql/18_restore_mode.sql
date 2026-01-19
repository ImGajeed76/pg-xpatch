-- Test 18: Restore Mode for pg_dump/pg_restore
-- Tests that explicit _xp_seq values are respected during INSERT (restore mode)
-- This enables pg_dump/pg_restore to work correctly with xpatch tables

-- Suppress NOTICE messages for cleaner test output
SET client_min_messages = warning;

-- =============================================================================
-- Test 1: Basic restore mode - explicit _xp_seq values
-- =============================================================================

CREATE TABLE restore_test1 (
    doc_id INT,
    version INT,
    _xp_seq INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('restore_test1', 
    group_by => 'doc_id', 
    order_by => 'version',
    delta_columns => ARRAY['content']);

-- Normal insert (should auto-generate _xp_seq = 1)
INSERT INTO restore_test1 (doc_id, version, content) VALUES (1, 1, 'Version 1');

-- Check _xp_seq was auto-generated
SELECT doc_id, version, _xp_seq FROM restore_test1;

-- Restore mode: INSERT with explicit _xp_seq = 2
INSERT INTO restore_test1 (doc_id, version, _xp_seq, content) VALUES (1, 2, 2, 'Version 2');

-- Verify explicit _xp_seq was used
SELECT doc_id, version, _xp_seq FROM restore_test1 ORDER BY _xp_seq;

-- Auto-insert should continue from seq 3
INSERT INTO restore_test1 (doc_id, version, content) VALUES (1, 3, 'Version 3');

-- Verify seq 3 was assigned
SELECT doc_id, version, _xp_seq FROM restore_test1 ORDER BY _xp_seq;

-- =============================================================================
-- Test 2: Restore mode skips version validation
-- This allows restoring data in COPY order (not necessarily per-group order)
-- =============================================================================

CREATE TABLE restore_test2 (
    doc_id INT,
    version INT,
    _xp_seq INT,
    content TEXT
) USING xpatch;

SELECT xpatch.configure('restore_test2', 
    group_by => 'doc_id', 
    order_by => 'version',
    delta_columns => ARRAY['content']);

-- Simulate COPY FROM: data comes in table order, not per-group order
-- This would fail without restore mode because doc_id=2/version=1 < doc_id=1/version=3
INSERT INTO restore_test2 (doc_id, version, _xp_seq, content) VALUES 
    (1, 1, 1, 'Doc 1 v1'),
    (1, 2, 2, 'Doc 1 v2'),
    (1, 3, 3, 'Doc 1 v3'),
    (2, 1, 1, 'Doc 2 v1'),  -- version=1 < max(version) but OK in restore mode
    (2, 2, 2, 'Doc 2 v2');

-- Verify all rows were inserted
SELECT doc_id, version, _xp_seq, content FROM restore_test2 ORDER BY doc_id, version;

-- =============================================================================
-- Test 3: dump_configs() generates correct SQL
-- =============================================================================

SELECT 'Testing dump_configs():' AS test;

-- Check that enable_zstd outputs as 'true'/'false' not 't'/'f'
SELECT dc FROM xpatch.dump_configs() dc WHERE dc LIKE '%restore_test1%';

-- =============================================================================
-- Test 4: Continue inserting after restore
-- =============================================================================

-- Insert new rows into restored table
INSERT INTO restore_test2 (doc_id, version, content) VALUES 
    (1, 4, 'Doc 1 v4 - post-restore'),
    (2, 3, 'Doc 2 v3 - post-restore');

-- Verify correct sequence numbers were assigned
SELECT doc_id, version, _xp_seq, content FROM restore_test2 ORDER BY doc_id, version;

-- Verify reconstruction still works
SELECT doc_id, version, content FROM restore_test2 WHERE doc_id = 1 AND version = 2;

-- =============================================================================
-- Cleanup
-- =============================================================================

DROP TABLE restore_test1;
DROP TABLE restore_test2;

SELECT 'Restore mode tests completed successfully!' AS result;
