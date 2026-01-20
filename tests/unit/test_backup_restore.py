"""
Backup and restore tests - pg_dump/pg_restore scenarios.

Ported from tmp/stress_test/final_tests.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "backup"])
def test_dump_configs_for_backup(db):
    """xpatch.dump_configs() generates SQL for backup."""
    db.execute("""
        CREATE TABLE backup_cfg (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('backup_cfg', group_by => 'id', order_by => 'ver');
        INSERT INTO backup_cfg VALUES (1, 1, 'test');
    """)
    
    result = db.fetchval("SELECT * FROM xpatch.dump_configs()")
    
    assert result is not None, "dump_configs() should return SQL"
    assert 'xpatch.configure' in result.lower(), "Should contain configure calls"


@pg_test(tags=["unit", "backup"])
def test_fix_restored_configs_after_restore(db):
    """fix_restored_configs() should work after normal operations."""
    db.execute("""
        CREATE TABLE restore_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('restore_test', group_by => 'id', order_by => 'ver');
        INSERT INTO restore_test VALUES (1, 1, 'test');
    """)
    
    # Call fix_restored_configs (simulates post-restore)
    result = db.fetchval("SELECT xpatch.fix_restored_configs()")
    
    # Should return 0 (no fixes needed) in normal operation
    assert result == 0, f"Expected 0, got {result}"


@pg_test(tags=["unit", "backup"])
def test_table_survives_dump_config_cycle(db):
    """Table should survive dump_configs cycle."""
    db.execute("""
        CREATE TABLE dump_cycle (id INT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('dump_cycle', group_by => 'id', order_by => 'ver');
        INSERT INTO dump_cycle VALUES (1, 1, 'original');
        INSERT INTO dump_cycle VALUES (1, 2, 'updated');
    """)
    
    # Get dump SQL
    dump_sql = db.fetchval("SELECT * FROM xpatch.dump_configs()")
    
    # Verify table still works
    count = db.fetchval("SELECT COUNT(*) FROM dump_cycle")
    assert count == 2
    
    # Verify data integrity
    content = db.fetchval("SELECT content FROM dump_cycle WHERE ver = 2")
    assert content == 'updated'


@pg_test(tags=["unit", "backup"])
def test_unicode_data_survives_backup(db):
    """Unicode data should survive backup/restore."""
    db.execute("""
        CREATE TABLE backup_unicode (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('backup_unicode', group_by => 'id', order_by => 'ver');
    """)
    
    # Insert unicode
    db.execute("INSERT INTO backup_unicode VALUES (1, 1, %s)", ('你好世界 🎉 مرحبا',))
    db.execute("INSERT INTO backup_unicode VALUES (1, 2, %s)", ('Привет мир 日本語',))
    
    # Verify data
    data = db.fetchval("SELECT data FROM backup_unicode WHERE ver = 1")
    assert '你好' in data, f"Expected unicode, got '{data}'"


@pg_test(tags=["unit", "backup"])
def test_null_data_survives_backup(db):
    """NULL and empty string data should survive backup/restore."""
    db.execute("""
        CREATE TABLE backup_nulls (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('backup_nulls', group_by => 'id', order_by => 'ver');
        INSERT INTO backup_nulls VALUES (1, 1, NULL);
        INSERT INTO backup_nulls VALUES (1, 2, '');
        INSERT INTO backup_nulls VALUES (1, 3, 'not null');
    """)
    
    null_count = db.fetchval("SELECT COUNT(*) FROM backup_nulls WHERE data IS NULL")
    empty_count = db.fetchval("SELECT COUNT(*) FROM backup_nulls WHERE data = ''")
    
    assert null_count == 1, f"Expected 1 NULL, got {null_count}"
    assert empty_count == 1, f"Expected 1 empty string, got {empty_count}"
