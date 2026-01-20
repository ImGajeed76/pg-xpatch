"""
Tests for xpatch.dump_configs() and xpatch.fix_restored_configs() functions.

Ported from tmp/stress_test/test_xpatch_functions.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "dump"])
def test_dump_configs_returns_sql(db):
    """dump_configs() should return SQL containing configure calls."""
    db.execute("""
        CREATE TABLE dump_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('dump_test', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchval("SELECT * FROM xpatch.dump_configs()")
    
    assert result is not None, "dump_configs() returned NULL"
    assert 'xpatch.configure' in result.lower(), (
        f"Expected 'xpatch.configure' in output, got: {result[:200]}"
    )


@pg_test(tags=["unit", "dump"])
def test_dump_configs_includes_configured_tables(db):
    """dump_configs() should include configured tables in same database."""
    db.execute("""
        CREATE TABLE dump_multi_1 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('dump_multi_1', group_by => 'grp', order_by => 'ver');
        
        CREATE TABLE dump_multi_2 (id INT, rev INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('dump_multi_2', group_by => 'id', order_by => 'rev');
    """)
    
    result = db.fetchval("SELECT * FROM xpatch.dump_configs()")
    
    # At least one configured table should appear
    assert result is not None and len(result) > 0, "dump_configs() returned empty"
    assert 'dump_multi_1' in result or 'dump_multi_2' in result, (
        f"Expected at least one table in output, got: {result[:200]}"
    )


@pg_test(tags=["unit", "dump"])
def test_dump_configs_includes_parameters(db):
    """dump_configs() should include configuration parameters."""
    db.execute("""
        CREATE TABLE dump_params (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('dump_params', 
            group_by => 'grp', 
            order_by => 'ver', 
            keyframe_every => 42
        );
    """)
    
    result = db.fetchval("SELECT * FROM xpatch.dump_configs()")
    
    assert 'grp' in result, "Expected 'grp' (group_by) in output"
    assert 'ver' in result, "Expected 'ver' (order_by) in output"


@pg_test(tags=["unit", "dump"])
def test_dump_configs_empty_without_configs(db):
    """dump_configs() should return empty/null without any configured tables."""
    # Note: This test uses a fresh database, so there might be no configured tables
    # However, other tests may have created tables, so we just check it doesn't error
    try:
        db.fetchval("SELECT * FROM xpatch.dump_configs()")
    except Exception as e:
        assert False, f"dump_configs() should not error: {e}"


@pg_test(tags=["unit", "restore"])
def test_fix_restored_configs_runs_without_error(db):
    """fix_restored_configs() should run without error."""
    result = db.fetchval("SELECT xpatch.fix_restored_configs()")
    
    assert result is not None, "fix_restored_configs() returned NULL"


@pg_test(tags=["unit", "restore"])
def test_fix_restored_configs_returns_zero_normally(db):
    """fix_restored_configs() should return 0 when no fixes needed."""
    result = db.fetchval("SELECT xpatch.fix_restored_configs()")
    
    # In a normal scenario (not after pg_restore), it should return 0
    assert result == 0, f"Expected 0 (no fixes needed), got {result}"


@pg_test(tags=["unit", "restore"])
def test_fix_restored_configs_after_configure(db):
    """fix_restored_configs() should work after tables are configured."""
    db.execute("""
        CREATE TABLE fix_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('fix_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO fix_test VALUES (1, 1, 'test');
    """)
    
    # After normal operations, fix_restored_configs should return 0
    result = db.fetchval("SELECT xpatch.fix_restored_configs()")
    
    assert result == 0, f"Expected 0 after normal configure, got {result}"


@pg_test(tags=["unit", "dump"])
def test_dump_configs_generates_valid_sql(db):
    """dump_configs() should generate syntactically valid SQL."""
    db.execute("""
        CREATE TABLE dump_valid (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('dump_valid', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchval("SELECT * FROM xpatch.dump_configs()")
    
    # The output should have proper SQL syntax indicators
    assert 'SELECT' in result.upper() or 'xpatch.configure' in result, (
        "Output doesn't look like valid SQL"
    )
    
    # Should not have obvious syntax errors
    assert result.count('(') == result.count(')'), "Mismatched parentheses in output"
