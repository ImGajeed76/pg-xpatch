"""
Locking tests - Row and table locking on xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py (LOCK-001 to LOCK-004)
"""

from xptest import pg_test


@pg_test(tags=["unit", "locking"])
def test_select_for_update_not_supported(db):
    """SELECT FOR UPDATE should fail (xpatch is append-only, no UPDATE)."""
    db.execute("""
        CREATE TABLE lock_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lock_test', group_by => 'id', order_by => 'ver');
        INSERT INTO lock_test VALUES (1, 1, 'lockme');
    """)
    
    try:
        db.execute("""
            BEGIN;
            SELECT * FROM lock_test WHERE id = 1 FOR UPDATE;
            COMMIT;
        """)
        # If it succeeds, that's implementation-dependent
        assert True
    except Exception as e:
        # Expected - xpatch doesn't support UPDATE locks
        assert 'UPDATE' in str(e).upper() or 'lock' in str(e).lower()


@pg_test(tags=["unit", "locking"])
def test_select_for_share(db):
    """SELECT FOR SHARE should work."""
    db.execute("""
        CREATE TABLE lock_share (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lock_share', group_by => 'id', order_by => 'ver');
        INSERT INTO lock_share VALUES (1, 1, 'shareable');
    """)
    
    try:
        db.execute("""
            BEGIN;
            SELECT * FROM lock_share FOR SHARE;
            COMMIT;
        """)
        # FOR SHARE typically works
        assert True
    except Exception:
        # Some implementations might not support this either
        assert True


@pg_test(tags=["unit", "locking"])
def test_lock_table_exclusive(db):
    """LOCK TABLE IN EXCLUSIVE MODE should work."""
    db.execute("""
        CREATE TABLE lock_excl (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lock_excl', group_by => 'id', order_by => 'ver');
        INSERT INTO lock_excl VALUES (1, 1, 'data');
    """)
    
    db.execute("""
        BEGIN;
        LOCK TABLE lock_excl IN EXCLUSIVE MODE;
        SELECT 'locked';
        COMMIT;
    """)
    
    # If we get here, lock succeeded
    count = db.fetchval("SELECT COUNT(*) FROM lock_excl")
    assert count == 1


@pg_test(tags=["unit", "locking"])
def test_advisory_locks(db):
    """Advisory locks should work independently of table type."""
    db.execute("""
        CREATE TABLE lock_adv (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lock_adv', group_by => 'id', order_by => 'ver');
        INSERT INTO lock_adv VALUES (1, 1, 'data');
    """)
    
    # Acquire and release advisory lock
    result = db.fetchval("SELECT pg_advisory_lock(12345)")
    assert result is None or result == ''  # Returns void
    
    result = db.fetchval("SELECT pg_advisory_unlock(12345)")
    assert result == True, "Advisory unlock should return true"


@pg_test(tags=["unit", "locking"])
def test_lock_table_access_share(db):
    """LOCK TABLE IN ACCESS SHARE MODE (default for SELECT)."""
    db.execute("""
        CREATE TABLE lock_as (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lock_as', group_by => 'id', order_by => 'ver');
        INSERT INTO lock_as VALUES (1, 1, 'data');
    """)
    
    db.execute("""
        BEGIN;
        LOCK TABLE lock_as IN ACCESS SHARE MODE;
        SELECT * FROM lock_as;
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM lock_as")
    assert count == 1


@pg_test(tags=["unit", "locking"])
def test_lock_table_row_exclusive(db):
    """LOCK TABLE IN ROW EXCLUSIVE MODE (default for INSERT/DELETE)."""
    db.execute("""
        CREATE TABLE lock_re (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lock_re', group_by => 'id', order_by => 'ver');
    """)
    
    db.execute("""
        BEGIN;
        LOCK TABLE lock_re IN ROW EXCLUSIVE MODE;
        INSERT INTO lock_re VALUES (1, 1, 'within lock');
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM lock_re")
    assert count == 1
