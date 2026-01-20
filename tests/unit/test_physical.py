"""
Tests for xpatch.physical() function - raw delta storage access.
"""

from xptest import pg_test


@pg_test(tags=["unit", "physical"])
def test_physical_returns_rows(db):
    """physical() returns rows for xpatch table."""
    db.execute("""
        CREATE TABLE phys_basic (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_basic', group_by => 'grp', order_by => 'ver', keyframe_every => 3);
        INSERT INTO phys_basic VALUES (1, 1, 'Version 1');
        INSERT INTO phys_basic VALUES (1, 2, 'Version 2 with more');
        INSERT INTO phys_basic VALUES (1, 3, 'Version 3');
        INSERT INTO phys_basic VALUES (2, 1, 'Group 2');
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.physical('phys_basic')")
    assert len(rows) == 4, f"Expected 4 rows, got {len(rows)}"


@pg_test(tags=["unit", "physical"])
def test_physical_first_row_is_keyframe(db):
    """First row in physical storage is a keyframe."""
    db.execute("""
        CREATE TABLE phys_kf (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_kf', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_kf VALUES (1, 1, 'First row');
        INSERT INTO phys_kf VALUES (1, 2, 'Second row');
    """)
    
    result = db.fetchone("""
        SELECT is_keyframe FROM xpatch.physical('phys_kf') 
        WHERE group_value = '1' ORDER BY seq LIMIT 1
    """)
    assert result['is_keyframe'] == True, f"First row should be keyframe"


@pg_test(tags=["unit", "physical"])
def test_physical_keyframe_pattern(db):
    """Keyframe pattern matches keyframe_every setting."""
    db.execute("""
        CREATE TABLE phys_pattern (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_pattern', group_by => 'grp', order_by => 'ver', keyframe_every => 3);
        INSERT INTO phys_pattern VALUES (1, 1, 'v1');
        INSERT INTO phys_pattern VALUES (1, 2, 'v2');
        INSERT INTO phys_pattern VALUES (1, 3, 'v3');
        INSERT INTO phys_pattern VALUES (1, 4, 'v4 - should be keyframe');
    """)
    
    rows = db.fetchall("""
        SELECT seq, is_keyframe FROM xpatch.physical('phys_pattern')
        WHERE group_value = '1' ORDER BY seq
    """)
    
    # With keyframe_every=3: seq 1 is keyframe, 2,3 are deltas, 4 is keyframe
    assert len(rows) == 4
    assert rows[0]['is_keyframe'] == True  # seq 1
    assert rows[3]['is_keyframe'] == True  # seq 4


@pg_test(tags=["unit", "physical"])
def test_physical_delta_bytes_is_bytea(db):
    """delta_bytes column is BYTEA type."""
    db.execute("""
        CREATE TABLE phys_bytea (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_bytea', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_bytea VALUES (1, 1, 'data');
    """)
    
    result = db.fetchval("""
        SELECT pg_typeof(delta_bytes)::text FROM xpatch.physical('phys_bytea') LIMIT 1
    """)
    assert result == 'bytea', f"Expected bytea, got {result}"


@pg_test(tags=["unit", "physical"])
def test_physical_delta_size_matches_bytes(db):
    """delta_size matches actual length of delta_bytes."""
    db.execute("""
        CREATE TABLE phys_size (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_size', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_size VALUES (1, 1, 'initial content');
        INSERT INTO phys_size VALUES (1, 2, 'updated content');
    """)
    
    result = db.fetchval("""
        SELECT delta_size = length(delta_bytes) AS matches
        FROM xpatch.physical('phys_size')
        WHERE delta_bytes IS NOT NULL LIMIT 1
    """)
    assert result == True, "delta_size should match length(delta_bytes)"


@pg_test(tags=["unit", "physical"])
def test_physical_filter_by_group(db):
    """physical() can filter by specific group."""
    db.execute("""
        CREATE TABLE phys_filter (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_filter', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_filter VALUES (1, 1, 'g1v1');
        INSERT INTO phys_filter VALUES (1, 2, 'g1v2');
        INSERT INTO phys_filter VALUES (2, 1, 'g2v1');
        INSERT INTO phys_filter VALUES (2, 2, 'g2v2');
        INSERT INTO phys_filter VALUES (2, 3, 'g2v3');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM xpatch_physical('phys_filter', 1, NULL)")
    assert count == 2, f"Expected 2 rows for group 1, got {count}"
    
    count = db.fetchval("SELECT COUNT(*) FROM xpatch_physical('phys_filter', 2, NULL)")
    assert count == 3, f"Expected 3 rows for group 2, got {count}"


@pg_test(tags=["unit", "physical"])
def test_physical_filter_by_from_seq(db):
    """physical() can filter by from_seq (returns rows with seq > from_seq)."""
    db.execute("""
        CREATE TABLE phys_seq (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_seq', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_seq VALUES (1, 1, 'v1');
        INSERT INTO phys_seq VALUES (1, 2, 'v2');
        INSERT INTO phys_seq VALUES (1, 3, 'v3');
        INSERT INTO phys_seq VALUES (1, 4, 'v4');
    """)
    
    # from_seq=2 should return rows with seq > 2 (i.e., seq 3 and 4)
    count = db.fetchval("SELECT COUNT(*) FROM xpatch.physical('phys_seq', 2)")
    assert count == 2, f"Expected 2 rows with seq > 2, got {count}"


@pg_test(tags=["unit", "physical"])
def test_physical_tag_values(db):
    """Tag values are valid (0=keyframe, 1+=delta)."""
    db.execute("""
        CREATE TABLE phys_tag (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_tag', group_by => 'grp', order_by => 'ver', keyframe_every => 5);
        INSERT INTO phys_tag SELECT 1, v, 'version ' || v FROM generate_series(1, 10) v;
    """)
    
    tags = db.fetchall("SELECT DISTINCT tag FROM xpatch.physical('phys_tag') ORDER BY tag")
    tag_values = [r['tag'] for r in tags]
    
    assert all(t >= 0 for t in tag_values), "All tags should be >= 0"
    assert 0 in tag_values, "Should have tag 0 (keyframe)"


@pg_test(tags=["unit", "physical"])
def test_physical_shows_delta_column(db):
    """physical() shows which column is delta-compressed."""
    db.execute("""
        CREATE TABLE phys_col (grp INT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('phys_col', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_col VALUES (1, 1, 'data');
    """)
    
    result = db.fetchval("SELECT delta_column FROM xpatch.physical('phys_col') LIMIT 1")
    assert result == 'content', f"Expected delta_column='content', got {result}"


@pg_test(tags=["unit", "physical", "error"])
def test_physical_rejects_heap_table(db):
    """physical() rejects non-xpatch tables."""
    db.execute("CREATE TABLE heap_for_phys (id INT, data TEXT)")
    
    try:
        db.execute("SELECT * FROM xpatch_physical('heap_for_phys', NULL::INT, NULL)")
        assert False, "Expected error for heap table"
    except Exception as e:
        assert "xpatch" in str(e).lower(), f"Expected xpatch error, got: {e}"
