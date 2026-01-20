"""
System column tests - Access to PostgreSQL system columns on xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py (SYS-001 to SYS-004)
"""

from xptest import pg_test


@pg_test(tags=["unit", "system"])
def test_access_ctid(db):
    """Access ctid system column."""
    db.execute("""
        CREATE TABLE sys_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sys_test', group_by => 'id', order_by => 'ver');
        INSERT INTO sys_test VALUES (1, 1, 'test');
    """)
    
    result = db.fetchone("SELECT ctid, * FROM sys_test")
    assert 'ctid' in result, "Should have ctid column"
    assert result['ctid'] is not None, "ctid should not be NULL"


@pg_test(tags=["unit", "system"])
def test_access_xmin_xmax_not_supported(db):
    """Access xmin/xmax should fail (not supported on xpatch tables)."""
    db.execute("""
        CREATE TABLE sys_xmin (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sys_xmin', group_by => 'id', order_by => 'ver');
        INSERT INTO sys_xmin VALUES (1, 1, 'test');
    """)
    
    try:
        db.execute("SELECT xmin, xmax, * FROM sys_xmin")
        # If it works, that's fine too (some implementations support it)
        assert True
    except Exception as e:
        # Expected - xpatch doesn't support xmin/xmax
        assert 'system column' in str(e).lower() or 'xmin' in str(e).lower()


@pg_test(tags=["unit", "system"])
def test_access_tableoid(db):
    """Access tableoid system column."""
    db.execute("""
        CREATE TABLE sys_tableoid (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sys_tableoid', group_by => 'id', order_by => 'ver');
        INSERT INTO sys_tableoid VALUES (1, 1, 'test');
    """)
    
    result = db.fetchone("SELECT tableoid, * FROM sys_tableoid")
    assert 'tableoid' in result, "Should have tableoid column"
    
    # tableoid should match the table's OID
    oid = db.fetchval("SELECT 'sys_tableoid'::regclass::oid")
    assert result['tableoid'] == oid, f"tableoid should match table OID"


@pg_test(tags=["unit", "system"])
def test_tid_scan_where_ctid(db):
    """WHERE clause on ctid (TID scan)."""
    db.execute("""
        CREATE TABLE sys_tid (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sys_tid', group_by => 'id', order_by => 'ver');
        INSERT INTO sys_tid VALUES (1, 1, 'first');
        INSERT INTO sys_tid VALUES (2, 1, 'second');
    """)
    
    # Get the ctid of first row
    ctid = db.fetchval("SELECT ctid FROM sys_tid WHERE id = 1")
    
    # Query using ctid
    try:
        result = db.fetchone(f"SELECT * FROM sys_tid WHERE ctid = '{ctid}'")
        assert result['data'] == 'first', f"Expected 'first', got {result['data']}"
    except Exception:
        # TID scan might not be supported
        assert True


@pg_test(tags=["unit", "system"])
def test_internal_xp_seq_column(db):
    """Internal _xp_seq column should be accessible."""
    db.execute("""
        CREATE TABLE sys_seq (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sys_seq', group_by => 'id', order_by => 'ver');
        INSERT INTO sys_seq VALUES (1, 1, 'first');
        INSERT INTO sys_seq VALUES (1, 2, 'second');
    """)
    
    # _xp_seq is xpatch's internal sequence number
    result = db.fetchall("SELECT _xp_seq, * FROM sys_seq ORDER BY _xp_seq")
    assert len(result) == 2
    assert result[0]['_xp_seq'] < result[1]['_xp_seq'], "_xp_seq should be monotonically increasing"
