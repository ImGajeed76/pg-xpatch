"""
P1.5 - Transaction Isolation Tests

Tests verifying that xpatch tables behave correctly under various 
PostgreSQL transaction isolation levels.
"""

from xptest import pg_test


# ============================================================================
# Basic Transaction Behavior
# ============================================================================

@pg_test(tags=["unit", "transactions", "p1"])
def test_commit_makes_visible(db, db2):
    """Data is visible to other sessions after COMMIT."""
    # Setup - create table first (outside of explicit transaction)
    db.execute("""
        CREATE TABLE tx_commit (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_commit', group_by => 'grp', order_by => 'ver');
    """)
    
    # Start transaction in first connection
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_commit VALUES (1, 1, 'hello')")
    
    # Should NOT be visible to second connection before commit
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_commit")
    assert count['cnt'] == 0, "Data visible before commit!"
    
    # Commit
    db.execute("COMMIT")
    
    # NOW should be visible
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_commit")
    assert count['cnt'] == 1, "Data not visible after commit!"


@pg_test(tags=["unit", "transactions", "p1"])
def test_rollback_invisible(db):
    """Data is NOT visible after ROLLBACK."""
    # Setup
    db.execute("""
        CREATE TABLE tx_rollback (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_rollback', group_by => 'grp', order_by => 'ver');
    """)
    
    # Start transaction
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_rollback VALUES (1, 1, 'should disappear')")
    
    # Verify data exists in this transaction
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_rollback")
    assert count['cnt'] == 1
    
    # Rollback
    db.execute("ROLLBACK")
    
    # Data should be gone
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_rollback")
    assert count['cnt'] == 0, "Data survived rollback!"


@pg_test(tags=["unit", "transactions", "p1"])
def test_read_own_writes(db):
    """A transaction can read its own uncommitted changes."""
    db.execute("""
        CREATE TABLE tx_own (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_own', group_by => 'grp', order_by => 'ver');
    """)
    
    db.execute("BEGIN")
    
    # Insert and read in same transaction
    db.execute("INSERT INTO tx_own VALUES (1, 1, 'first')")
    row = db.fetchone("SELECT * FROM tx_own WHERE grp = 1 AND ver = 1")
    assert row is not None
    assert row['data'] == 'first'
    
    # Insert more and read all
    db.execute("INSERT INTO tx_own VALUES (1, 2, 'second')")
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_own")
    assert count['cnt'] == 2
    
    db.execute("COMMIT")


# ============================================================================
# Read Committed Isolation
# ============================================================================

@pg_test(tags=["unit", "transactions", "isolation", "p1"])
def test_read_committed_sees_committed(db, db2):
    """READ COMMITTED sees data committed by other transactions."""
    # Setup first
    db.execute("""
        CREATE TABLE tx_rc (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_rc', group_by => 'grp', order_by => 'ver');
        INSERT INTO tx_rc VALUES (1, 1, 'initial');
    """)
    
    # Start READ COMMITTED transaction (default)
    db2.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
    
    # Read initial data
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_rc")
    assert count['cnt'] == 1
    
    # First connection commits new data
    db.execute("INSERT INTO tx_rc VALUES (1, 2, 'new data')")
    
    # Second connection should see the new data (READ COMMITTED)
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_rc")
    assert count['cnt'] == 2, "READ COMMITTED should see newly committed data"
    
    db2.execute("COMMIT")


@pg_test(tags=["unit", "transactions", "isolation", "p1"])
def test_read_committed_not_sees_uncommitted(db, db2):
    """READ COMMITTED does NOT see uncommitted changes from other transactions."""
    # Setup first
    db.execute("""
        CREATE TABLE tx_rc_uncommit (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_rc_uncommit', group_by => 'grp', order_by => 'ver');
    """)
    
    # Start transaction in first connection (don't commit)
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_rc_uncommit VALUES (1, 1, 'uncommitted')")
    
    # Second connection starts READ COMMITTED
    db2.execute("BEGIN ISOLATION LEVEL READ COMMITTED")
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_rc_uncommit")
    assert count['cnt'] == 0, "Should NOT see uncommitted data"
    db2.execute("COMMIT")
    
    # Rollback first transaction
    db.execute("ROLLBACK")


# ============================================================================
# Repeatable Read Isolation
# ============================================================================

@pg_test(tags=["unit", "transactions", "isolation", "p1"])
def test_repeatable_read_snapshot(db, db2):
    """REPEATABLE READ maintains consistent snapshot throughout transaction."""
    # Setup first
    db.execute("""
        CREATE TABLE tx_rr (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_rr', group_by => 'grp', order_by => 'ver');
        INSERT INTO tx_rr VALUES (1, 1, 'initial');
    """)
    
    # Start REPEATABLE READ transaction
    db2.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    
    # Read initial count
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_rr")
    assert count['cnt'] == 1
    
    # First connection commits new data
    db.execute("INSERT INTO tx_rr VALUES (1, 2, 'new data')")
    
    # Second connection should NOT see new data (repeatable read snapshot)
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_rr")
    assert count['cnt'] == 1, "REPEATABLE READ should NOT see newly committed data"
    
    db2.execute("COMMIT")
    
    # After transaction ends, should see new data
    count = db2.fetchone("SELECT COUNT(*) as cnt FROM tx_rr")
    assert count['cnt'] == 2


# ============================================================================
# Savepoints
# ============================================================================

@pg_test(tags=["unit", "transactions", "savepoint", "p1"])
def test_savepoint_rollback(db):
    """ROLLBACK TO SAVEPOINT works correctly."""
    db.execute("""
        CREATE TABLE tx_sp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_sp', group_by => 'grp', order_by => 'ver');
    """)
    
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_sp VALUES (1, 1, 'keep')")
    
    # Create savepoint
    db.execute("SAVEPOINT sp1")
    
    # Insert more data
    db.execute("INSERT INTO tx_sp VALUES (1, 2, 'discard')")
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_sp")
    assert count['cnt'] == 2
    
    # Rollback to savepoint
    db.execute("ROLLBACK TO SAVEPOINT sp1")
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_sp")
    assert count['cnt'] == 1, "Data after savepoint should be gone"
    
    # Verify the correct row remains
    row = db.fetchone("SELECT data FROM tx_sp WHERE ver = 1")
    assert row['data'] == 'keep'
    
    db.execute("COMMIT")


@pg_test(tags=["unit", "transactions", "savepoint", "p1"])
def test_savepoint_nested(db):
    """Nested savepoints work correctly."""
    db.execute("""
        CREATE TABLE tx_sp_nest (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_sp_nest', group_by => 'grp', order_by => 'ver');
    """)
    
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_sp_nest VALUES (1, 1, 'level0')")
    
    db.execute("SAVEPOINT sp1")
    db.execute("INSERT INTO tx_sp_nest VALUES (1, 2, 'level1')")
    
    db.execute("SAVEPOINT sp2")
    db.execute("INSERT INTO tx_sp_nest VALUES (1, 3, 'level2')")
    
    db.execute("SAVEPOINT sp3")
    db.execute("INSERT INTO tx_sp_nest VALUES (1, 4, 'level3')")
    
    # Should have 4 rows
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_sp_nest")
    assert count['cnt'] == 4
    
    # Rollback to sp2, losing level3 and level2
    db.execute("ROLLBACK TO SAVEPOINT sp2")
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_sp_nest")
    assert count['cnt'] == 2, "Should have 2 rows after rollback to sp2"
    
    # Can still commit
    db.execute("COMMIT")
    
    # Verify final state
    rows = db.fetchall("SELECT ver FROM tx_sp_nest ORDER BY ver")
    assert [r['ver'] for r in rows] == [1, 2]


@pg_test(tags=["unit", "transactions", "savepoint", "p1"])
def test_savepoint_release(db):
    """RELEASE SAVEPOINT works correctly."""
    db.execute("""
        CREATE TABLE tx_sp_rel (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_sp_rel', group_by => 'grp', order_by => 'ver');
    """)
    
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_sp_rel VALUES (1, 1, 'first')")
    
    db.execute("SAVEPOINT sp1")
    db.execute("INSERT INTO tx_sp_rel VALUES (1, 2, 'second')")
    
    # Release the savepoint (merge changes into outer transaction)
    db.execute("RELEASE SAVEPOINT sp1")
    
    # Both rows should still be visible
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_sp_rel")
    assert count['cnt'] == 2
    
    db.execute("COMMIT")


# ============================================================================
# Transaction Abort Cleanup
# ============================================================================

@pg_test(tags=["unit", "transactions", "p1"])
def test_transaction_abort_cleanup(db):
    """Resources are properly released after transaction abort."""
    # Setup first
    db.execute("""
        CREATE TABLE tx_abort (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('tx_abort', group_by => 'grp', order_by => 'ver');
    """)
    
    # Start transaction that will be aborted
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_abort VALUES (1, 1, 'will abort')")
    
    # Force an error to abort
    try:
        db.execute("SELECT 1/0")  # Division by zero
    except Exception:
        pass
    
    # Transaction is now aborted - need to rollback
    db.execute("ROLLBACK")
    
    # Table should be empty
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_abort")
    assert count['cnt'] == 0
    
    # Should be able to start a new transaction
    db.execute("BEGIN")
    db.execute("INSERT INTO tx_abort VALUES (1, 1, 'new data')")
    db.execute("COMMIT")
    
    count = db.fetchone("SELECT COUNT(*) as cnt FROM tx_abort")
    assert count['cnt'] == 1
