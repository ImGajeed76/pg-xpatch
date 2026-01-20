"""
Tests for xpatch.inspect() function - inspecting storage details.

Ported from tmp/stress_test/test_xpatch_functions.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "inspect"])
def test_inspect_returns_rows_for_group(db):
    """inspect() returns rows for a specific group."""
    db.execute("""
        CREATE TABLE inspect_test (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_test', group_by => 'group_id', order_by => 'ver', keyframe_every => 3);
        INSERT INTO inspect_test SELECT 1, v, 'Data for version ' || v FROM generate_series(1, 5) v;
        INSERT INTO inspect_test SELECT 2, v, 'Other data v' || v FROM generate_series(1, 2) v;
    """)
    
    rows = db.fetchall(
        "SELECT version, seq, is_keyframe, column_name FROM xpatch.inspect('inspect_test', 1) ORDER BY seq"
    )
    
    assert len(rows) == 5, f"Expected 5 rows for group 1, got {len(rows)}"


@pg_test(tags=["unit", "inspect"])
def test_inspect_seq_is_one_based(db):
    """inspect() seq should be 1-based (first row seq=1)."""
    db.execute("""
        CREATE TABLE inspect_seq (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_seq', group_by => 'group_id', order_by => 'ver');
        INSERT INTO inspect_seq VALUES (1, 1, 'first');
        INSERT INTO inspect_seq VALUES (1, 2, 'second');
    """)
    
    rows = db.fetchall("SELECT seq FROM xpatch.inspect('inspect_seq', 1) ORDER BY seq")
    
    assert rows[0]['seq'] == 1, f"Expected first seq=1, got {rows[0]['seq']}"
    assert rows[1]['seq'] == 2, f"Expected second seq=2, got {rows[1]['seq']}"


@pg_test(tags=["unit", "inspect"])
def test_inspect_first_row_is_keyframe(db):
    """First row in a group should always be a keyframe."""
    db.execute("""
        CREATE TABLE inspect_kf (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_kf', group_by => 'group_id', order_by => 'ver', keyframe_every => 10);
        INSERT INTO inspect_kf VALUES (1, 1, 'first');
        INSERT INTO inspect_kf VALUES (1, 2, 'second');
    """)
    
    rows = db.fetchall("SELECT seq, is_keyframe FROM xpatch.inspect('inspect_kf', 1) ORDER BY seq")
    
    assert rows[0]['is_keyframe'] == True, f"First row should be keyframe, got {rows[0]['is_keyframe']}"


@pg_test(tags=["unit", "inspect"])
def test_inspect_keyframe_pattern(db):
    """Keyframe pattern matches keyframe_every setting."""
    db.execute("""
        CREATE TABLE inspect_pattern (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_pattern', group_by => 'group_id', order_by => 'ver', keyframe_every => 3);
        INSERT INTO inspect_pattern SELECT 1, v, 'Data v' || v FROM generate_series(1, 7) v;
    """)
    
    rows = db.fetchall("SELECT seq, is_keyframe FROM xpatch.inspect('inspect_pattern', 1) ORDER BY seq")
    
    # With keyframe_every=3: seq 1 (keyframe), 2 (delta), 3 (delta), 4 (keyframe), 5 (delta), 6 (delta), 7 (keyframe)
    assert rows[0]['is_keyframe'] == True, "seq 1 should be keyframe"
    assert rows[3]['is_keyframe'] == True, "seq 4 should be keyframe"
    assert rows[6]['is_keyframe'] == True, "seq 7 should be keyframe"


@pg_test(tags=["unit", "inspect"])
def test_inspect_shows_column_name(db):
    """inspect() should show the delta column name."""
    db.execute("""
        CREATE TABLE inspect_col (group_id INT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_col', group_by => 'group_id', order_by => 'ver');
        INSERT INTO inspect_col VALUES (1, 1, 'test content');
    """)
    
    row = db.fetchone("SELECT column_name FROM xpatch.inspect('inspect_col', 1)")
    
    assert row['column_name'] == 'content', f"Expected column_name='content', got '{row['column_name']}'"


@pg_test(tags=["unit", "inspect"])
def test_inspect_empty_for_nonexistent_group(db):
    """inspect() should return empty for non-existent group."""
    db.execute("""
        CREATE TABLE inspect_empty (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_empty', group_by => 'group_id', order_by => 'ver');
        INSERT INTO inspect_empty VALUES (1, 1, 'data');
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.inspect('inspect_empty', 999)")
    
    assert len(rows) == 0, f"Expected 0 rows for non-existent group, got {len(rows)}"


@pg_test(tags=["unit", "inspect"])
def test_inspect_shows_delta_size(db):
    """inspect() should show delta_size_bytes for each row."""
    db.execute("""
        CREATE TABLE inspect_size (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_size', group_by => 'group_id', order_by => 'ver');
        INSERT INTO inspect_size VALUES (1, 1, 'some content here');
        INSERT INTO inspect_size VALUES (1, 2, 'some content here with more');
    """)
    
    rows = db.fetchall("SELECT delta_size_bytes FROM xpatch.inspect('inspect_size', 1)")
    
    for row in rows:
        assert row['delta_size_bytes'] > 0, f"Expected delta_size_bytes > 0, got {row['delta_size_bytes']}"


@pg_test(tags=["unit", "inspect"])
def test_inspect_multiple_groups(db):
    """inspect() returns only rows for the specified group."""
    db.execute("""
        CREATE TABLE inspect_multi (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_multi', group_by => 'group_id', order_by => 'ver');
        INSERT INTO inspect_multi VALUES (1, 1, 'g1v1');
        INSERT INTO inspect_multi VALUES (1, 2, 'g1v2');
        INSERT INTO inspect_multi VALUES (2, 1, 'g2v1');
        INSERT INTO inspect_multi VALUES (2, 2, 'g2v2');
        INSERT INTO inspect_multi VALUES (2, 3, 'g2v3');
    """)
    
    rows_g1 = db.fetchall("SELECT * FROM xpatch.inspect('inspect_multi', 1)")
    rows_g2 = db.fetchall("SELECT * FROM xpatch.inspect('inspect_multi', 2)")
    
    assert len(rows_g1) == 2, f"Expected 2 rows for group 1, got {len(rows_g1)}"
    assert len(rows_g2) == 3, f"Expected 3 rows for group 2, got {len(rows_g2)}"
