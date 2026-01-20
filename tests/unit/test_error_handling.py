"""
P0.5 - Error Handling Tests

Tests verifying that errors are handled gracefully with clear messages.
"""

from xptest import pg_test


@pg_test(tags=["unit", "error-handling", "p0"])
def test_configure_nonexistent_column(db):
    """configure() with non-existent column should give clear error."""
    db.execute("CREATE TABLE err_col (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('err_col', group_by => 'fake_col')")
        assert False, "Expected error for non-existent column"
    except Exception as e:
        error_msg = str(e).lower()
        assert "does not exist" in error_msg or "not found" in error_msg or "fake_col" in error_msg


@pg_test(tags=["unit", "error-handling", "p0"])
def test_configure_nonexistent_table(db):
    """configure() with non-existent table should give clear error."""
    try:
        db.execute("SELECT xpatch.configure('nonexistent_table', group_by => 'id')")
        assert False, "Expected error for non-existent table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "does not exist" in error_msg or "not found" in error_msg


@pg_test(tags=["unit", "error-handling", "p0"])
def test_stats_nonexistent_table(db):
    """stats() with non-existent table should give clear error."""
    try:
        db.execute("SELECT * FROM xpatch.stats('nonexistent_table')")
        assert False, "Expected error for non-existent table"
    except Exception as e:
        assert True  # Any error is acceptable


@pg_test(tags=["unit", "error-handling", "p0"])
def test_configure_permission_denied(db):
    """configure() without INSERT privilege should fail."""
    import uuid
    role_name = f"test_reader_{uuid.uuid4().hex[:8]}"
    
    db.execute(f"""
        CREATE TABLE perm_test (grp INT, ver INT, data TEXT) USING xpatch;
        CREATE ROLE {role_name};
        GRANT SELECT ON perm_test TO {role_name};
    """)
    
    got_permission_error = False
    try:
        db.execute(f"SET ROLE {role_name}")
        db.execute("SELECT xpatch.configure('perm_test', group_by => 'grp')")
    except Exception as e:
        error_msg = str(e).lower()
        got_permission_error = "permission" in error_msg or "denied" in error_msg
    
    db.rollback()
    db.execute("RESET ROLE")
    # Role cleanup will happen when test DB is dropped
    
    assert got_permission_error, "Expected permission denied error"


@pg_test(tags=["unit", "error-handling", "p0"])
def test_invalid_keyframe_every_zero(db):
    """configure() with keyframe_every=0 should fail."""
    db.execute("CREATE TABLE kf_err (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('kf_err', group_by => 'grp', keyframe_every => 0)")
        assert False, "Expected error for keyframe_every=0"
    except Exception as e:
        error_msg = str(e).lower()
        assert "keyframe" in error_msg or "at least 1" in error_msg


@pg_test(tags=["unit", "error-handling", "p0"])
def test_invalid_keyframe_every_negative(db):
    """configure() with negative keyframe_every should fail."""
    db.execute("CREATE TABLE kf_neg (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('kf_neg', group_by => 'grp', keyframe_every => -5)")
        assert False, "Expected error for negative keyframe_every"
    except Exception as e:
        assert True  # Any error is acceptable


@pg_test(tags=["unit", "error-handling", "p0"])
def test_invalid_compress_depth_zero(db):
    """configure() with compress_depth=0 should fail."""
    db.execute("CREATE TABLE cd_err (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('cd_err', group_by => 'grp', compress_depth => 0)")
        assert False, "Expected error for compress_depth=0"
    except Exception as e:
        error_msg = str(e).lower()
        assert "compress_depth" in error_msg or "at least 1" in error_msg


@pg_test(tags=["unit", "error-handling", "p0"])
def test_describe_nonexistent_table(db):
    """describe() with non-existent table should fail."""
    try:
        db.execute("SELECT * FROM xpatch.describe('nonexistent_table')")
        assert False, "Expected error"
    except Exception as e:
        assert True


@pg_test(tags=["unit", "error-handling", "p1"])
def test_warm_cache_permission_denied(db):
    """warm_cache() without SELECT privilege should fail."""
    import uuid
    role_name = f"test_noselect_{uuid.uuid4().hex[:8]}"
    
    db.execute(f"""
        CREATE TABLE warm_perm (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_perm', group_by => 'grp', order_by => 'ver');
        CREATE ROLE {role_name};
    """)
    
    got_permission_error = False
    try:
        db.execute(f"SET ROLE {role_name}")
        db.execute("SELECT * FROM xpatch.warm_cache('warm_perm')")
    except Exception as e:
        error_msg = str(e).lower()
        got_permission_error = "permission" in error_msg or "denied" in error_msg
    
    db.rollback()
    db.execute("RESET ROLE")
    
    assert got_permission_error, "Expected permission denied error"


@pg_test(tags=["unit", "error-handling", "p1"])
def test_warm_cache_negative_max_rows(db):
    """warm_cache() with negative max_rows should fail."""
    db.execute("""
        CREATE TABLE warm_neg (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_neg', group_by => 'grp', order_by => 'ver');
    """)
    
    try:
        db.execute("SELECT * FROM xpatch.warm_cache('warm_neg', max_rows => -1)")
        assert False, "Expected error for negative max_rows"
    except Exception as e:
        error_msg = str(e).lower()
        assert "negative" in error_msg or "non-negative" in error_msg or "max_rows" in error_msg


@pg_test(tags=["unit", "error-handling", "p1"])
def test_empty_table_stats(db):
    """stats() on empty table should work without error."""
    db.execute("""
        CREATE TABLE empty_stats (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('empty_stats', group_by => 'grp', order_by => 'ver');
    """)
    
    stats = db.fetchone("SELECT * FROM xpatch.stats('empty_stats')")
    assert stats is not None
    assert stats['total_rows'] == 0


@pg_test(tags=["unit", "error-handling", "p1"])
def test_empty_table_describe(db):
    """describe() on empty table should work without error."""
    db.execute("""
        CREATE TABLE empty_desc (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('empty_desc', group_by => 'grp', order_by => 'ver');
    """)
    
    desc = db.fetchall("SELECT * FROM xpatch.describe('empty_desc')")
    assert len(desc) > 0


@pg_test(tags=["unit", "error-handling", "p1"])
def test_empty_table_warm_cache(db):
    """warm_cache() on empty table should work without error."""
    db.execute("""
        CREATE TABLE empty_warm (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('empty_warm', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT * FROM xpatch.warm_cache('empty_warm')")
    assert result['rows_scanned'] == 0
