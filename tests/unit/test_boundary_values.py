"""
Boundary value tests - edge cases and limits.

Ported from tmp/stress_test/stress_test_xpatch.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "boundary"])
def test_stats_on_empty_table(db):
    """stats() on empty table should return 0 rows."""
    db.execute("""
        CREATE TABLE empty_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('empty_test', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT total_rows FROM xpatch.stats('empty_test')")
    assert result['total_rows'] == 0, f"Expected 0 rows, got {result['total_rows']}"


@pg_test(tags=["unit", "boundary"])
def test_single_row_is_always_keyframe(db):
    """Single row table should have that row as a keyframe."""
    db.execute("""
        CREATE TABLE single_row (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('single_row', group_by => 'grp', order_by => 'ver');
        INSERT INTO single_row VALUES (1, 1, 'only row');
    """)
    
    result = db.fetchone("SELECT total_rows, keyframe_count FROM xpatch.stats('single_row')")
    assert result['total_rows'] == 1, f"Expected 1 row, got {result['total_rows']}"
    assert result['keyframe_count'] == 1, f"Expected 1 keyframe, got {result['keyframe_count']}"


@pg_test(tags=["unit", "boundary"])
def test_bigint_max_value_as_group(db):
    """Handles BIGINT max value as group ID."""
    db.execute("""
        CREATE TABLE large_id (grp BIGINT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('large_id', group_by => 'grp', order_by => 'ver');
        INSERT INTO large_id VALUES (9223372036854775807, 1, 'max bigint');
    """)
    
    result = db.fetchval("SELECT grp FROM large_id")
    assert result == 9223372036854775807, f"Expected max bigint, got {result}"


@pg_test(tags=["unit", "boundary"])
def test_negative_group_ids(db):
    """Handles negative group IDs including INT min value."""
    db.execute("""
        CREATE TABLE neg_grp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('neg_grp', group_by => 'grp', order_by => 'ver');
        INSERT INTO neg_grp VALUES (-1, 1, 'negative group');
        INSERT INTO neg_grp VALUES (-2147483648, 1, 'min int');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM neg_grp")
    assert count == 2, f"Expected 2 rows, got {count}"


@pg_test(tags=["unit", "boundary"])
def test_empty_string_content(db):
    """Handles empty string content correctly."""
    db.execute("""
        CREATE TABLE empty_content (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('empty_content', group_by => 'grp', order_by => 'ver');
        INSERT INTO empty_content VALUES (1, 1, '');
        INSERT INTO empty_content VALUES (1, 2, '');
    """)
    
    result = db.fetchval("SELECT data FROM empty_content WHERE ver = 2")
    assert result == '', f"Expected empty string, got '{result}'"


@pg_test(tags=["unit", "boundary"])
def test_null_content_handling(db):
    """Handles NULL content correctly."""
    db.execute("""
        CREATE TABLE null_content (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('null_content', group_by => 'grp', order_by => 'ver');
        INSERT INTO null_content VALUES (1, 1, NULL);
        INSERT INTO null_content VALUES (1, 2, 'not null');
        INSERT INTO null_content VALUES (1, 3, NULL);
    """)
    
    null_count = db.fetchval("SELECT COUNT(*) FROM null_content WHERE data IS NULL")
    assert null_count == 2, f"Expected 2 NULL rows, got {null_count}"


@pg_test(tags=["unit", "boundary", "slow"])
def test_large_content_1mb(db):
    """Handles 1MB content."""
    long_content = 'x' * (1024 * 1024)  # 1MB
    db.execute("""
        CREATE TABLE long_content (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('long_content', group_by => 'grp', order_by => 'ver');
    """)
    db.execute("INSERT INTO long_content VALUES (1, 1, %s)", (long_content,))
    
    length = db.fetchval("SELECT LENGTH(data) FROM long_content")
    assert length == 1048576, f"Expected 1MB (1048576 bytes), got {length}"


@pg_test(tags=["unit", "boundary"])
def test_keyframe_every_one_creates_keyframes(db):
    """keyframe_every=1 should create at least 1 keyframe."""
    db.execute("""
        CREATE TABLE all_keyframes (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('all_keyframes', group_by => 'grp', order_by => 'ver', keyframe_every => 1);
        INSERT INTO all_keyframes VALUES (1, 1, 'v1');
        INSERT INTO all_keyframes VALUES (1, 2, 'v2');
        INSERT INTO all_keyframes VALUES (1, 3, 'v3');
    """)
    
    keyframe_count = db.fetchval("SELECT keyframe_count FROM xpatch.stats('all_keyframes')")
    assert keyframe_count >= 1, f"Expected at least 1 keyframe, got {keyframe_count}"


@pg_test(tags=["unit", "boundary"])
def test_max_keyframe_every_stores_all_rows(db):
    """keyframe_every=10000 (max) stores all rows correctly."""
    db.execute("""
        CREATE TABLE rare_keyframes (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('rare_keyframes', group_by => 'grp', order_by => 'ver', keyframe_every => 10000);
        INSERT INTO rare_keyframes VALUES (1, 1, 'v1');
        INSERT INTO rare_keyframes VALUES (1, 2, 'v2');
        INSERT INTO rare_keyframes VALUES (1, 3, 'v3');
    """)
    
    total_rows = db.fetchval("SELECT total_rows FROM xpatch.stats('rare_keyframes')")
    assert total_rows == 3, f"Expected 3 rows, got {total_rows}"


@pg_test(tags=["unit", "boundary"])
def test_zero_value_group_id(db):
    """Handles zero as a group ID."""
    db.execute("""
        CREATE TABLE zero_grp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('zero_grp', group_by => 'grp', order_by => 'ver');
        INSERT INTO zero_grp VALUES (0, 1, 'zero group');
        INSERT INTO zero_grp VALUES (0, 2, 'zero group v2');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM zero_grp WHERE grp = 0")
    assert count == 2, f"Expected 2 rows in group 0, got {count}"


@pg_test(tags=["unit", "boundary"])
def test_very_long_version_chain(db):
    """Handle a long version chain (100 versions)."""
    db.execute("""
        CREATE TABLE long_chain (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('long_chain', group_by => 'grp', order_by => 'ver', keyframe_every => 10);
    """)
    
    # Insert 100 versions
    for i in range(1, 101):
        db.execute("INSERT INTO long_chain VALUES (1, %s, %s)", (i, f'version {i}'))
    
    count = db.fetchval("SELECT COUNT(*) FROM long_chain WHERE grp = 1")
    assert count == 100, f"Expected 100 rows, got {count}"
    
    # Verify random versions are correct
    for ver in [1, 25, 50, 75, 100]:
        data = db.fetchval("SELECT data FROM long_chain WHERE grp = 1 AND ver = %s", (ver,))
        expected = f'version {ver}'
        assert data == expected, f"Version {ver}: expected '{expected}', got '{data}'"
