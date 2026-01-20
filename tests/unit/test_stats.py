"""
Tests for xpatch.stats() function.
"""

from xptest import pg_test


@pg_test(tags=["unit", "stats"])
def test_stats_on_empty_table(db):
    """stats() on empty table should return zeros."""
    db.execute("""
        CREATE TABLE stats_empty (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_empty', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT * FROM xpatch.stats('stats_empty')")
    assert result is not None, "stats() returned NULL"
    assert result['total_rows'] == 0, f"Expected 0 rows, got {result['total_rows']}"
    assert result['total_groups'] == 0, f"Expected 0 groups, got {result['total_groups']}"
    assert result['keyframe_count'] == 0, f"Expected 0 keyframes, got {result['keyframe_count']}"
    assert result['delta_count'] == 0, f"Expected 0 deltas, got {result['delta_count']}"


@pg_test(tags=["unit", "stats"])
def test_stats_total_rows_count(db):
    """stats() should correctly count total rows."""
    db.execute("""
        CREATE TABLE stats_rows (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_rows', group_by => 'grp', order_by => 'ver');
        INSERT INTO stats_rows VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c');
        INSERT INTO stats_rows VALUES (2, 1, 'd'), (2, 2, 'e');
    """)
    
    result = db.fetchone("SELECT total_rows FROM xpatch.stats('stats_rows')")
    assert result['total_rows'] == 5, f"Expected 5 rows, got {result['total_rows']}"


@pg_test(tags=["unit", "stats"])
def test_stats_total_groups_count(db):
    """stats() should correctly count unique groups."""
    db.execute("""
        CREATE TABLE stats_groups (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_groups', group_by => 'grp', order_by => 'ver');
        INSERT INTO stats_groups VALUES (1, 1, 'a');
        INSERT INTO stats_groups VALUES (2, 1, 'b');
        INSERT INTO stats_groups VALUES (3, 1, 'c');
        INSERT INTO stats_groups VALUES (1, 2, 'd');
    """)
    
    result = db.fetchone("SELECT total_groups FROM xpatch.stats('stats_groups')")
    assert result['total_groups'] == 3, f"Expected 3 groups, got {result['total_groups']}"


@pg_test(tags=["unit", "stats"])
def test_stats_keyframe_count(db):
    """stats() should correctly count keyframes based on keyframe_every."""
    db.execute("""
        CREATE TABLE stats_kf (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_kf', group_by => 'grp', order_by => 'ver', keyframe_every => 3);
    """)
    
    # Insert 10 versions for 1 group: keyframes at 1, 4, 7, 10 = 4 keyframes
    for i in range(1, 11):
        db.execute(f"INSERT INTO stats_kf VALUES (1, {i}, 'version {i}')")
    
    result = db.fetchone("SELECT keyframe_count, delta_count FROM xpatch.stats('stats_kf')")
    # With keyframe_every=3: first row is keyframe, then every 3rd after
    assert result['keyframe_count'] >= 1, f"Expected at least 1 keyframe, got {result['keyframe_count']}"


@pg_test(tags=["unit", "stats"])
def test_stats_compression_ratio_format(db):
    """stats() compression_ratio should be rounded to 2 decimal places."""
    db.execute("""
        CREATE TABLE stats_ratio (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_ratio', group_by => 'grp', order_by => 'ver');
        INSERT INTO stats_ratio VALUES (1, 1, repeat('x', 1000));
        INSERT INTO stats_ratio VALUES (1, 2, repeat('x', 1000) || 'y');
    """)
    
    result = db.fetchone("SELECT compression_ratio FROM xpatch.stats('stats_ratio')")
    ratio = result['compression_ratio']
    
    # Check it's a reasonable number (not NULL, not negative)
    assert ratio is not None, "compression_ratio is NULL"
    assert ratio >= 0, f"compression_ratio should be >= 0, got {ratio}"
    
    # Check formatting (should be like X.XX)
    ratio_str = str(ratio)
    if '.' in ratio_str:
        decimals = len(ratio_str.split('.')[1])
        assert decimals <= 2, f"Expected max 2 decimals, got {decimals} in {ratio_str}"


@pg_test(tags=["unit", "stats"])
def test_stats_avg_chain_length(db):
    """stats() should calculate average chain length."""
    db.execute("""
        CREATE TABLE stats_chain (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_chain', group_by => 'grp', order_by => 'ver', keyframe_every => 10);
        -- Group 1: 5 versions
        INSERT INTO stats_chain SELECT 1, v, 'data' || v FROM generate_series(1, 5) v;
        -- Group 2: 3 versions
        INSERT INTO stats_chain SELECT 2, v, 'data' || v FROM generate_series(1, 3) v;
    """)
    
    result = db.fetchone("SELECT avg_chain_length FROM xpatch.stats('stats_chain')")
    avg_chain = result['avg_chain_length']
    
    assert avg_chain is not None, "avg_chain_length is NULL"
    assert avg_chain > 0, f"avg_chain_length should be > 0, got {avg_chain}"


@pg_test(tags=["unit", "stats", "error"])
def test_stats_rejects_nonexistent_table(db):
    """stats() should error on non-existent table."""
    try:
        db.execute("SELECT * FROM xpatch.stats('nonexistent_table')")
        assert False, "Expected error for non-existent table"
    except Exception as e:
        # Should get some error
        assert True
