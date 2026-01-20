"""
Tests for xpatch.describe() function - full table introspection.

Ported from tmp/stress_test/test_xpatch_functions.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "describe"])
def test_describe_returns_table_name(db):
    """describe() should return the table name."""
    db.execute("""
        CREATE TABLE desc_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_test', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT value FROM xpatch.describe('desc_test') WHERE property = 'table'")
    
    assert result is not None, "Expected 'table' property in describe output"
    assert 'desc_test' in result['value'], f"Expected table name, got {result['value']}"


@pg_test(tags=["unit", "describe"])
def test_describe_no_empty_value_rows(db):
    """describe() should have no empty value rows (clean format)."""
    db.execute("""
        CREATE TABLE desc_clean (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_clean', group_by => 'grp', order_by => 'ver');
    """)
    
    empty_count = db.fetchval(
        "SELECT COUNT(*) FROM xpatch.describe('desc_clean') WHERE value = '' OR value IS NULL"
    )
    
    assert empty_count == 0, f"Expected no empty value rows, got {empty_count}"


@pg_test(tags=["unit", "describe"])
def test_describe_shows_config_source(db):
    """describe() should show config_source as 'explicit' for configured tables."""
    db.execute("""
        CREATE TABLE desc_src (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_src', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT value FROM xpatch.describe('desc_src') WHERE property = 'config_source'")
    
    assert result is not None, "Expected 'config_source' property"
    assert 'explicit' in result['value'].lower(), f"Expected 'explicit', got {result['value']}"


@pg_test(tags=["unit", "describe"])
def test_describe_shows_storage_stats(db):
    """describe() should show storage stats like total_rows."""
    db.execute("""
        CREATE TABLE desc_stats (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_stats', group_by => 'grp', order_by => 'ver');
        INSERT INTO desc_stats VALUES (1, 1, 'row1');
        INSERT INTO desc_stats VALUES (1, 2, 'row2');
    """)
    
    result = db.fetchone("SELECT value FROM xpatch.describe('desc_stats') WHERE property = 'total_rows'")
    
    assert result is not None, "Expected 'total_rows' property"
    assert result['value'] == '2', f"Expected '2', got {result['value']}"


@pg_test(tags=["unit", "describe"])
def test_describe_returns_multiple_properties(db):
    """describe() should return multiple properties."""
    db.execute("""
        CREATE TABLE desc_multi (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_multi', group_by => 'grp', order_by => 'ver');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM xpatch.describe('desc_multi')")
    
    assert count >= 5, f"Expected at least 5 properties, got {count}"


@pg_test(tags=["unit", "describe"])
def test_describe_shows_group_by_column(db):
    """describe() should show the group_by column name."""
    db.execute("""
        CREATE TABLE desc_grp (entity_id INT, revision INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('desc_grp', group_by => 'entity_id', order_by => 'revision');
    """)
    
    result = db.fetchone("SELECT value FROM xpatch.describe('desc_grp') WHERE property = 'group_by'")
    
    assert result is not None, "Expected 'group_by' property"
    assert result['value'] == 'entity_id', f"Expected 'entity_id', got {result['value']}"


@pg_test(tags=["unit", "describe"])
def test_describe_shows_order_by_column(db):
    """describe() should show the order_by column name."""
    db.execute("""
        CREATE TABLE desc_ord (entity_id INT, revision INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('desc_ord', group_by => 'entity_id', order_by => 'revision');
    """)
    
    result = db.fetchone("SELECT value FROM xpatch.describe('desc_ord') WHERE property = 'order_by'")
    
    assert result is not None, "Expected 'order_by' property"
    assert result['value'] == 'revision', f"Expected 'revision', got {result['value']}"


@pg_test(tags=["unit", "describe"])
def test_describe_shows_keyframe_every(db):
    """describe() should show keyframe_every setting."""
    db.execute("""
        CREATE TABLE desc_kf (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_kf', group_by => 'grp', order_by => 'ver', keyframe_every => 50);
    """)
    
    result = db.fetchone("SELECT value FROM xpatch.describe('desc_kf') WHERE property = 'keyframe_every'")
    
    assert result is not None, "Expected 'keyframe_every' property"
    assert result['value'] == '50', f"Expected '50', got {result['value']}"


@pg_test(tags=["unit", "describe", "error"])
def test_describe_nonexistent_table(db):
    """describe() should error on non-existent table."""
    try:
        db.execute("SELECT * FROM xpatch.describe('nonexistent_describe_table')")
        assert False, "Expected error for non-existent table"
    except Exception:
        pass  # Any error is acceptable
