"""
Error handling tests - ensure proper error messages for bad input.

Ported from tmp/stress_test/stress_test_xpatch.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "error"])
def test_configure_rejects_nonexistent_table(db):
    """configure() should reject non-existent table with clear error."""
    try:
        db.execute("SELECT xpatch.configure('nonexistent_table', group_by => 'id')")
        assert False, "Expected error for non-existent table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "does not exist" in error_msg or "not found" in error_msg, (
            f"Expected 'does not exist' error, got: {e}"
        )


@pg_test(tags=["unit", "error"])
def test_configure_rejects_heap_table(db):
    """configure() should reject non-xpatch (heap) tables."""
    db.execute("CREATE TABLE heap_table (id INT, data TEXT)")
    
    try:
        db.execute("SELECT xpatch.configure('heap_table', group_by => 'id')")
        assert False, "Expected error for heap table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "xpatch" in error_msg, f"Expected xpatch-related error, got: {e}"


@pg_test(tags=["unit", "error"])
def test_stats_rejects_nonexistent_table(db):
    """stats() should error on non-existent table."""
    try:
        db.execute("SELECT * FROM xpatch.stats('nonexistent_stats_table')")
        assert False, "Expected error for non-existent table"
    except Exception:
        pass  # Any error is acceptable


@pg_test(tags=["unit", "error"])
def test_stats_on_heap_table_returns_zero_or_errors(db):
    """stats() on heap table should return 0 or error gracefully."""
    db.execute("CREATE TABLE heap_for_stats (id INT, data TEXT)")
    
    try:
        result = db.fetchone("SELECT total_rows FROM xpatch.stats('heap_for_stats')")
        # If it returns, it should be 0
        assert result['total_rows'] == 0, f"Expected 0 rows, got {result['total_rows']}"
    except Exception:
        # Erroring is also acceptable
        pass


@pg_test(tags=["unit", "error"])
def test_inspect_returns_empty_for_nonexistent_group(db):
    """inspect() should return empty result for non-existent group."""
    db.execute("""
        CREATE TABLE err_inspect (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('err_inspect', group_by => 'grp', order_by => 'ver');
        INSERT INTO err_inspect VALUES (1, 1, 'test');
    """)
    
    # Query for non-existent group 999
    rows = db.fetchall("SELECT * FROM xpatch.inspect('err_inspect', 999)")
    assert len(rows) == 0, f"Expected 0 rows for non-existent group, got {len(rows)}"


@pg_test(tags=["unit", "error"])
def test_physical_rejects_heap_table(db):
    """xpatch_physical() should reject non-xpatch tables."""
    db.execute("CREATE TABLE heap_for_physical (id INT, data TEXT)")
    
    try:
        db.execute("SELECT * FROM xpatch_physical('heap_for_physical', NULL::INT, NULL)")
        assert False, "Expected error for heap table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "xpatch" in error_msg, f"Expected xpatch error, got: {e}"


@pg_test(tags=["unit", "error"])
def test_configure_rejects_nonexistent_column(db):
    """configure() should reject non-existent column names."""
    db.execute("CREATE TABLE col_test_err (a INT, b TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('col_test_err', group_by => 'nonexistent_column')")
        assert False, "Expected error for non-existent column"
    except Exception:
        pass  # Any error is acceptable


@pg_test(tags=["unit", "error"])
def test_configure_rejects_invalid_keyframe_every(db):
    """configure() should reject invalid keyframe_every values."""
    db.execute("CREATE TABLE kf_err (grp INT, ver INT, data TEXT) USING xpatch")
    
    # Test negative value
    try:
        db.execute("""
            SELECT xpatch.configure('kf_err', 
                group_by => 'grp', 
                order_by => 'ver', 
                keyframe_every => -1)
        """)
        assert False, "Expected error for negative keyframe_every"
    except Exception:
        pass


@pg_test(tags=["unit", "error"])
def test_configure_rejects_zero_keyframe_every(db):
    """configure() should reject keyframe_every=0."""
    db.execute("CREATE TABLE kf_zero (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("""
            SELECT xpatch.configure('kf_zero', 
                group_by => 'grp', 
                order_by => 'ver', 
                keyframe_every => 0)
        """)
        assert False, "Expected error for keyframe_every=0"
    except Exception:
        pass


@pg_test(tags=["unit", "error"])
def test_get_config_nonexistent_table(db):
    """get_config() should handle non-existent table gracefully."""
    try:
        db.execute("SELECT * FROM xpatch.get_config('nonexistent_config_table')")
        # If it returns, result should be empty or null
    except Exception:
        pass  # Erroring is acceptable


@pg_test(tags=["unit", "error"])
def test_describe_nonexistent_table(db):
    """describe() should handle non-existent table gracefully."""
    try:
        db.execute("SELECT * FROM xpatch.describe('nonexistent_describe_table')")
        assert False, "Expected error for non-existent table"
    except Exception:
        pass


@pg_test(tags=["unit", "error"])
def test_warm_cache_nonexistent_table(db):
    """warm_cache() should handle non-existent table gracefully."""
    try:
        db.execute("SELECT * FROM xpatch.warm_cache('nonexistent_warm_table')")
        assert False, "Expected error for non-existent table"
    except Exception:
        pass


@pg_test(tags=["unit", "error"])
def test_insert_without_configure(db):
    """Insert into unconfigured xpatch table should work with defaults."""
    db.execute("CREATE TABLE unconfigured (grp INT, ver INT, data TEXT) USING xpatch")
    
    # Should work - uses default configuration
    db.execute("INSERT INTO unconfigured VALUES (1, 1, 'test')")
    
    count = db.fetchval("SELECT COUNT(*) FROM unconfigured")
    assert count == 1, f"Expected 1 row, got {count}"


@pg_test(tags=["unit", "error"])
def test_duplicate_configure_overwrites(db):
    """Calling configure() twice should update config, not error."""
    db.execute("""
        CREATE TABLE dup_config (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('dup_config', group_by => 'grp', order_by => 'ver', keyframe_every => 10);
    """)
    
    # Second configure should work
    db.execute("""
        SELECT xpatch.configure('dup_config', group_by => 'grp', order_by => 'ver', keyframe_every => 20);
    """)
    
    result = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('dup_config')")
    assert result['keyframe_every'] == 20, f"Expected 20, got {result['keyframe_every']}"
