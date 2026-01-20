"""
Large transaction tests.

Ported from tmp/stress_test/final_tests.py (LGTX-001 to LGTX-003)
"""

from xptest import pg_test


@pg_test(tags=["stress", "transaction", "slow"])
def test_large_transaction_10000_inserts(db):
    """Single transaction with 10000 inserts should commit."""
    db.execute("""
        CREATE TABLE large_tx (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('large_tx', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Single large transaction
    db.execute("""
        BEGIN;
        INSERT INTO large_tx 
        SELECT g, 1, repeat('x', 100) FROM generate_series(1, 10000) g;
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM large_tx")
    assert count == 10000, f"Expected 10000 rows, got {count}"


@pg_test(tags=["stress", "transaction"])
def test_large_transaction_data_integrity(db):
    """Verify data integrity after large transaction."""
    db.execute("""
        CREATE TABLE large_tx_int (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('large_tx_int', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Insert with unique data
    db.execute("""
        INSERT INTO large_tx_int 
        SELECT g, 1, 'group_' || g || '_data' FROM generate_series(1, 5000) g;
    """)
    
    # Verify specific rows
    data = db.fetchval("SELECT data FROM large_tx_int WHERE group_id = 2500")
    assert data == "group_2500_data", f"Expected 'group_2500_data', got '{data}'"
    
    data = db.fetchval("SELECT data FROM large_tx_int WHERE group_id = 4999")
    assert data == "group_4999_data", f"Expected 'group_4999_data', got '{data}'"


@pg_test(tags=["stress", "transaction"])
def test_large_transaction_rollback(db):
    """Large transaction rollback should leave no data."""
    db.execute("""
        CREATE TABLE large_tx_rb (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('large_tx_rb', group_by => 'group_id', order_by => 'ver');
        
        -- Initial data
        INSERT INTO large_tx_rb VALUES (1, 1, 'keeper');
    """)
    
    # Large transaction that rolls back
    # Note: xpatch tables are append-only, but PostgreSQL MVCC handles rollback
    # at the tuple visibility level. This tests that aborted inserts are not visible.
    db.execute("""
        BEGIN;
        INSERT INTO large_tx_rb 
        SELECT g + 1, 1, 'should be rolled back' FROM generate_series(1, 1000) g;
        ROLLBACK;
    """)
    
    # Only keeper should remain
    count = db.fetchval("SELECT COUNT(*) FROM large_tx_rb")
    assert count == 1, f"Expected 1 row after rollback, got {count}"
    
    data = db.fetchval("SELECT data FROM large_tx_rb WHERE group_id = 1")
    assert data == 'keeper', f"Expected 'keeper', got '{data}'"


@pg_test(tags=["stress", "transaction"])
def test_large_transaction_multiple_groups_versions(db):
    """Large transaction with multiple groups and versions."""
    db.execute("""
        CREATE TABLE large_tx_mv (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('large_tx_mv', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Insert multiple versions for multiple groups in one transaction
    db.execute("""
        BEGIN;
        -- First version for 100 groups
        INSERT INTO large_tx_mv 
        SELECT g, 1, 'v1' FROM generate_series(1, 100) g;
        -- Second version
        INSERT INTO large_tx_mv 
        SELECT g, 2, 'v2' FROM generate_series(1, 100) g;
        -- Third version
        INSERT INTO large_tx_mv 
        SELECT g, 3, 'v3' FROM generate_series(1, 100) g;
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM large_tx_mv")
    assert count == 300, f"Expected 300 rows, got {count}"
    
    # Verify delta reconstruction
    data = db.fetchval("SELECT data FROM large_tx_mv WHERE group_id = 50 AND ver = 2")
    assert data == 'v2', f"Expected 'v2', got '{data}'"
