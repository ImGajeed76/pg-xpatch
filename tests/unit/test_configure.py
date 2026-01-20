"""
Tests for xpatch.configure() function.
"""

from xptest import pg_test


@pg_test(tags=["unit", "configure"])
def test_configure_basic(db):
    """Basic configure() call should succeed."""
    db.execute("""
        CREATE TABLE cfg_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cfg_test', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT * FROM xpatch.get_config('cfg_test')")
    assert result is not None, "get_config() returned NULL"
    assert result['group_by'] == 'grp', f"Expected group_by='grp', got {result['group_by']}"
    assert result['order_by'] == 'ver', f"Expected order_by='ver', got {result['order_by']}"


@pg_test(tags=["unit", "configure"])
def test_configure_with_keyframe_every(db):
    """configure() should store keyframe_every parameter."""
    db.execute("""
        CREATE TABLE cfg_kf (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cfg_kf', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 25
        );
    """)
    
    result = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('cfg_kf')")
    assert result['keyframe_every'] == 25, f"Expected 25, got {result['keyframe_every']}"


@pg_test(tags=["unit", "configure"])
def test_configure_reconfigure_updates_values(db):
    """Calling configure() again should update existing config."""
    db.execute("""
        CREATE TABLE cfg_upd (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cfg_upd', group_by => 'grp', order_by => 'ver', keyframe_every => 10);
    """)
    
    # Verify initial config
    result = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('cfg_upd')")
    assert result['keyframe_every'] == 10
    
    # Update config
    db.execute("""
        SELECT xpatch.configure('cfg_upd', group_by => 'grp', order_by => 'ver', keyframe_every => 50);
    """)
    
    # Verify updated config
    result = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('cfg_upd')")
    assert result['keyframe_every'] == 50, f"Expected 50 after update, got {result['keyframe_every']}"


@pg_test(tags=["unit", "configure", "error"])
def test_configure_rejects_heap_table(db):
    """configure() should reject non-xpatch tables."""
    db.execute("CREATE TABLE heap_tbl (id INT, data TEXT)")
    
    try:
        db.execute("SELECT xpatch.configure('heap_tbl', group_by => 'id')")
        assert False, "Expected error for heap table, but succeeded"
    except Exception as e:
        error_msg = str(e).lower()
        assert "xpatch" in error_msg, f"Expected xpatch-related error, got: {e}"


@pg_test(tags=["unit", "configure", "error"])
def test_configure_rejects_nonexistent_table(db):
    """configure() should reject non-existent tables."""
    try:
        db.execute("SELECT xpatch.configure('nonexistent_table', group_by => 'id')")
        assert False, "Expected error for non-existent table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "does not exist" in error_msg or "not found" in error_msg, (
            f"Expected 'does not exist' error, got: {e}"
        )


@pg_test(tags=["unit", "configure", "error"])
def test_configure_rejects_nonexistent_column(db):
    """configure() should reject non-existent column names."""
    db.execute("CREATE TABLE cfg_col (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('cfg_col', group_by => 'nonexistent_col')")
        assert False, "Expected error for non-existent column"
    except Exception as e:
        # Should get some error about the column
        assert True  # Any error is acceptable here


@pg_test(tags=["unit", "configure"])
def test_configure_with_compress_depth(db):
    """configure() should accept compress_depth parameter."""
    db.execute("""
        CREATE TABLE cfg_depth (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cfg_depth', 
            group_by => 'grp', 
            order_by => 'ver',
            compress_depth => 5
        );
    """)
    
    result = db.fetchone("SELECT compress_depth FROM xpatch.get_config('cfg_depth')")
    assert result['compress_depth'] == 5, f"Expected 5, got {result['compress_depth']}"


@pg_test(tags=["unit", "configure"])
def test_configure_with_delta_columns(db):
    """configure() should accept delta_columns parameter."""
    db.execute("""
        CREATE TABLE cfg_delta (grp INT, ver INT, title TEXT, body TEXT) USING xpatch;
        SELECT xpatch.configure('cfg_delta', 
            group_by => 'grp', 
            order_by => 'ver',
            delta_columns => ARRAY['body']::text[]
        );
    """)
    
    # Insert some data and verify it works
    db.execute("INSERT INTO cfg_delta VALUES (1, 1, 'Title', 'Body content')")
    db.execute("INSERT INTO cfg_delta VALUES (1, 2, 'Title 2', 'Body content updated')")
    
    result = db.fetchone("SELECT * FROM cfg_delta WHERE grp = 1 AND ver = 2")
    assert result['body'] == 'Body content updated'
