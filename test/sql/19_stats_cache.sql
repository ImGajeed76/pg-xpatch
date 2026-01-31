-- ============================================================
-- Test 19: Stats Cache
-- Tests the incremental stats cache functionality
-- ============================================================

-- ============================================================
-- TEST 1: Basic INSERT Tracking
-- ============================================================
DROP TABLE IF EXISTS test_stats_insert CASCADE;
CREATE TABLE test_stats_insert (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_insert', group_by => 'grp');

-- Single row insert
INSERT INTO test_stats_insert VALUES ('A', 1, 'hello');
SELECT 'TEST 1a: Single insert' as test,
       CASE WHEN total_rows = 1 AND total_groups = 1 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_insert');

-- Multiple rows same group
INSERT INTO test_stats_insert VALUES ('A', 2, 'world'), ('A', 3, 'test!');
SELECT 'TEST 1b: Multi insert same group' as test,
       CASE WHEN total_rows = 3 AND total_groups = 1 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_insert');

-- New group
INSERT INTO test_stats_insert VALUES ('B', 1, 'other');
SELECT 'TEST 1c: New group' as test,
       CASE WHEN total_rows = 4 AND total_groups = 2 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_insert');

DROP TABLE test_stats_insert;

-- ============================================================
-- TEST 2: Raw Size vs Compressed Size Accuracy
-- ============================================================
DROP TABLE IF EXISTS test_stats_sizes CASCADE;
CREATE TABLE test_stats_sizes (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_sizes', group_by => 'grp');

-- Known size data (100 bytes)
INSERT INTO test_stats_sizes VALUES ('A', 1, repeat('x', 100)::bytea);
SELECT 'TEST 2a: 100 byte input' as test,
       CASE WHEN raw_size_bytes = 100 THEN 'PASS' 
            ELSE 'FAIL: raw=' || raw_size_bytes END as result
FROM xpatch.stats('test_stats_sizes');

-- Cumulative raw size
INSERT INTO test_stats_sizes VALUES ('A', 2, repeat('y', 50)::bytea);
SELECT 'TEST 2b: Cumulative raw size' as test,
       CASE WHEN raw_size_bytes = 150 THEN 'PASS' 
            ELSE 'FAIL: raw=' || raw_size_bytes END as result
FROM xpatch.stats('test_stats_sizes');

-- Verify compression works (highly compressible data)
SELECT 'TEST 2c: Compression' as test,
       CASE WHEN compressed_size_bytes < raw_size_bytes THEN 'PASS' 
            ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_sizes');

DROP TABLE test_stats_sizes;

-- ============================================================
-- TEST 3: DELETE Updates Stats
-- ============================================================
DROP TABLE IF EXISTS test_stats_delete CASCADE;
CREATE TABLE test_stats_delete (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_delete', group_by => 'grp');

-- Setup
INSERT INTO test_stats_delete VALUES 
    ('A', 1, 'aaa1'), ('A', 2, 'aaa2'), ('A', 3, 'aaa3'),
    ('B', 1, 'bbb1'), ('B', 2, 'bbb2');

-- Delete single row (last in sequence)
DELETE FROM test_stats_delete WHERE grp = 'A' AND ver = 3;
SELECT 'TEST 3a: Delete single row' as test,
       CASE WHEN total_rows = 4 THEN 'PASS' 
            ELSE 'FAIL: rows=' || total_rows END as result
FROM xpatch.stats('test_stats_delete');

-- Delete cascade (middle row)
DELETE FROM test_stats_delete WHERE grp = 'A' AND ver = 2;
SELECT 'TEST 3b: Delete cascade' as test,
       CASE WHEN total_rows = 3 THEN 'PASS' 
            ELSE 'FAIL: rows=' || total_rows END as result
FROM xpatch.stats('test_stats_delete');

-- Delete all from group (cascade from keyframe)
DELETE FROM test_stats_delete WHERE grp = 'B' AND ver = 1;
SELECT 'TEST 3c: Delete all from group' as test,
       CASE WHEN total_rows = 1 AND total_groups = 1 THEN 'PASS' 
            ELSE 'FAIL: rows=' || total_rows || ' groups=' || total_groups END as result
FROM xpatch.stats('test_stats_delete');

DROP TABLE test_stats_delete;

-- ============================================================
-- TEST 4: Keyframe Tracking
-- ============================================================
DROP TABLE IF EXISTS test_stats_kf CASCADE;
CREATE TABLE test_stats_kf (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_kf', group_by => 'grp', keyframe_every => 3);

-- Insert 5 rows - keyframes at seq 1 and 4
INSERT INTO test_stats_kf VALUES 
    ('A', 1, 'd1'), ('A', 2, 'd2'), ('A', 3, 'd3'), ('A', 4, 'd4'), ('A', 5, 'd5');
SELECT 'TEST 4: Keyframe count' as test,
       CASE WHEN keyframe_count = 2 THEN 'PASS' 
            ELSE 'FAIL: kf=' || keyframe_count END as result
FROM xpatch.stats('test_stats_kf');

DROP TABLE test_stats_kf;

-- ============================================================
-- TEST 5: Stats Regeneration (delete + refresh)
-- ============================================================
DROP TABLE IF EXISTS test_stats_regen CASCADE;
CREATE TABLE test_stats_regen (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_regen', group_by => 'grp');

INSERT INTO test_stats_regen VALUES ('A', 1, 'aaaa'), ('A', 2, 'bbbb'), ('B', 1, 'cccc');

-- Record original stats
CREATE TEMP TABLE orig_stats AS 
SELECT total_rows, total_groups, raw_size_bytes FROM xpatch.stats('test_stats_regen');

-- Delete stats and verify empty
DELETE FROM xpatch.group_stats WHERE relid = 'test_stats_regen'::regclass::oid;
SELECT 'TEST 5a: Stats deleted' as test,
       CASE WHEN total_rows = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_regen');

-- Refresh and compare (discard output - compression ratio varies slightly)
SELECT xpatch.refresh_stats('test_stats_regen') IS NOT NULL AS refreshed;
SELECT 'TEST 5b: Stats regenerated' as test,
       CASE WHEN s.total_rows = o.total_rows 
             AND s.total_groups = o.total_groups 
             AND s.raw_size_bytes = o.raw_size_bytes
            THEN 'PASS' 
            ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_regen') s, orig_stats o;

DROP TABLE orig_stats;
DROP TABLE test_stats_regen;

-- ============================================================
-- TEST 6: Edge Cases
-- ============================================================

-- 6a: Empty table
DROP TABLE IF EXISTS test_stats_empty CASCADE;
CREATE TABLE test_stats_empty (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_empty', group_by => 'grp');
SELECT 'TEST 6a: Empty table' as test,
       CASE WHEN total_rows = 0 AND total_groups = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_empty');
DROP TABLE test_stats_empty;

-- 6b: NULL group value
DROP TABLE IF EXISTS test_stats_null CASCADE;
CREATE TABLE test_stats_null (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_null', group_by => 'grp');
INSERT INTO test_stats_null VALUES (NULL, 1, 'null_grp');
SELECT 'TEST 6b: NULL group' as test,
       CASE WHEN total_rows = 1 AND total_groups = 1 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_null');
DROP TABLE test_stats_null;

-- 6c: Many groups
DROP TABLE IF EXISTS test_stats_many CASCADE;
CREATE TABLE test_stats_many (grp INT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_many', group_by => 'grp');
INSERT INTO test_stats_many SELECT g, 1, ('data' || g)::bytea FROM generate_series(1, 50) g;
SELECT 'TEST 6c: 50 groups' as test,
       CASE WHEN total_rows = 50 AND total_groups = 50 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_many');
DROP TABLE test_stats_many;

-- ============================================================
-- TEST 7: TRUNCATE Clears Stats
-- ============================================================
DROP TABLE IF EXISTS test_stats_trunc CASCADE;
CREATE TABLE test_stats_trunc (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_trunc', group_by => 'grp');

INSERT INTO test_stats_trunc VALUES ('A', 1, 'a1'), ('B', 1, 'b1');

-- Verify stats exist
SELECT 'TEST 7a: Before truncate' as test,
       CASE WHEN total_rows = 2 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_trunc');

TRUNCATE test_stats_trunc;

SELECT 'TEST 7b: After truncate' as test,
       CASE WHEN total_rows = 0 AND total_groups = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_trunc');

-- Verify group_stats table is cleared
SELECT 'TEST 7c: group_stats cleared' as test,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.group_stats WHERE relid = 'test_stats_trunc'::regclass::oid;

DROP TABLE test_stats_trunc;

-- ============================================================
-- TEST 8: Compression Depth Tracking
-- ============================================================
DROP TABLE IF EXISTS test_stats_depth CASCADE;
CREATE TABLE test_stats_depth (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_depth', group_by => 'grp', compress_depth => 5);

-- Insert chain: keyframe (tag=0), then deltas (tag=1 each)
INSERT INTO test_stats_depth VALUES ('A', 1, 'aaaa'), ('A', 2, 'bbbb'), ('A', 3, 'cccc');
SELECT 'TEST 8a: Compression depth' as test,
       CASE WHEN delta_count = 2 AND ROUND(avg_compression_depth::numeric, 1) = 1.0 
            THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_depth');

-- Delete and verify recalculated
DELETE FROM test_stats_depth WHERE grp = 'A' AND ver = 3;
SELECT 'TEST 8b: After delete' as test,
       CASE WHEN delta_count = 1 THEN 'PASS' ELSE 'FAIL' END as result
FROM xpatch.stats('test_stats_depth');

DROP TABLE test_stats_depth;

-- ============================================================
-- TEST 9: Large Data
-- ============================================================
DROP TABLE IF EXISTS test_stats_large CASCADE;
CREATE TABLE test_stats_large (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_large', group_by => 'grp');

-- Insert 1MB of data
INSERT INTO test_stats_large VALUES ('A', 1, repeat('x', 1024*1024)::bytea);
SELECT 'TEST 9: Large data (1MB)' as test,
       CASE WHEN raw_size_bytes = 1024*1024 THEN 'PASS' 
            ELSE 'FAIL: raw=' || raw_size_bytes END as result
FROM xpatch.stats('test_stats_large');

DROP TABLE test_stats_large;

-- ============================================================
-- TEST 10: SnapshotSelf Visibility (delete sees own changes)
-- ============================================================
DROP TABLE IF EXISTS test_stats_vis CASCADE;
CREATE TABLE test_stats_vis (grp TEXT, ver INT, data BYTEA) USING xpatch;
SELECT xpatch.configure('test_stats_vis', group_by => 'grp');

INSERT INTO test_stats_vis VALUES ('A', 1, 'a1'), ('A', 2, 'a2');

-- Delete non-keyframe (no cascade)
DELETE FROM test_stats_vis WHERE grp = 'A' AND ver = 2;
SELECT 'TEST 10: SnapshotSelf visibility' as test,
       CASE WHEN total_rows = 1 THEN 'PASS' 
            ELSE 'FAIL: rows=' || total_rows END as result
FROM xpatch.stats('test_stats_vis');

DROP TABLE test_stats_vis;
