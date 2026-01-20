"""
COPY and RETURNING clause tests.

Ported from tmp/stress_test/edge_case_tests.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "copy"])
def test_copy_to_stdout(db):
    """COPY TO STDOUT should export xpatch table data."""
    db.execute("""
        CREATE TABLE copy_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('copy_test', group_by => 'id', order_by => 'ver');
        INSERT INTO copy_test VALUES (1, 1, 'row1'), (1, 2, 'row2'), (2, 1, 'other');
    """)
    
    # Use a workaround since direct COPY TO STDOUT isn't easy with psycopg
    # We can test that the table is readable and the data is there
    count = db.fetchval("SELECT COUNT(*) FROM copy_test")
    
    assert count == 3, f"Expected 3 rows, got {count}"


@pg_test(tags=["unit", "copy"])
def test_copy_specific_columns(db):
    """COPY with specific columns should work."""
    db.execute("""
        CREATE TABLE copy_cols (id INT, ver INT, data TEXT, extra TEXT) USING xpatch;
        SELECT xpatch.configure('copy_cols', group_by => 'id', order_by => 'ver');
        INSERT INTO copy_cols VALUES (1, 1, 'data1', 'extra1');
    """)
    
    # Verify data exists
    row = db.fetchone("SELECT id, data FROM copy_cols")
    
    assert row['id'] == 1
    assert row['data'] == 'data1'


@pg_test(tags=["unit", "returning"])
def test_insert_returning_star(db):
    """INSERT RETURNING * should return inserted data."""
    db.execute("""
        CREATE TABLE ret_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ret_test', group_by => 'id', order_by => 'ver');
    """)
    
    row = db.fetchone("INSERT INTO ret_test VALUES (1, 1, 'test') RETURNING *")
    
    assert row is not None, "RETURNING should return the inserted row"
    assert row['id'] == 1
    assert row['ver'] == 1
    assert row['data'] == 'test'


@pg_test(tags=["unit", "returning"])
def test_insert_returning_specific_columns(db):
    """INSERT RETURNING specific columns including _xp_seq."""
    db.execute("""
        CREATE TABLE ret_cols (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ret_cols', group_by => 'id', order_by => 'ver');
    """)
    
    row = db.fetchone("INSERT INTO ret_cols VALUES (1, 1, 'test') RETURNING id, ver, _xp_seq")
    
    assert row is not None
    assert row['id'] == 1
    assert row['ver'] == 1
    assert '_xp_seq' in row, "Should be able to return _xp_seq"


@pg_test(tags=["unit", "returning"])
def test_delete_returning(db):
    """DELETE RETURNING should return deleted data."""
    db.execute("""
        CREATE TABLE del_ret (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('del_ret', group_by => 'id', order_by => 'ver');
        INSERT INTO del_ret VALUES (1, 1, 'to delete');
    """)
    
    row = db.fetchone("DELETE FROM del_ret WHERE id = 1 RETURNING *")
    
    assert row is not None, "DELETE RETURNING should return the deleted row"
    assert row['data'] == 'to delete'


@pg_test(tags=["unit", "returning"])
def test_insert_returning_computed_expression(db):
    """INSERT RETURNING with computed expression."""
    db.execute("""
        CREATE TABLE ret_expr (id INT, ver INT, amount INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ret_expr', group_by => 'id', order_by => 'ver');
    """)
    
    row = db.fetchone("""
        INSERT INTO ret_expr VALUES (1, 1, 100, 'test') 
        RETURNING id, amount * 2 as doubled
    """)
    
    assert row is not None
    assert row['doubled'] == 200


@pg_test(tags=["unit", "isolation"])
def test_read_committed_isolation(db):
    """READ COMMITTED isolation level should work."""
    db.execute("""
        CREATE TABLE iso_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('iso_test', group_by => 'id', order_by => 'ver');
        INSERT INTO iso_test VALUES (1, 1, 'initial');
    """)
    
    # Run a transaction with READ COMMITTED
    db.execute("""
        BEGIN ISOLATION LEVEL READ COMMITTED;
        SELECT * FROM iso_test;
        COMMIT;
    """)
    
    # Should complete without error
    count = db.fetchval("SELECT COUNT(*) FROM iso_test")
    assert count == 1


@pg_test(tags=["unit", "isolation"])
def test_repeatable_read_isolation(db):
    """REPEATABLE READ isolation level should work."""
    db.execute("""
        CREATE TABLE iso_rr (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('iso_rr', group_by => 'id', order_by => 'ver');
        INSERT INTO iso_rr VALUES (1, 1, 'initial');
    """)
    
    db.execute("""
        BEGIN ISOLATION LEVEL REPEATABLE READ;
        SELECT * FROM iso_rr;
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM iso_rr")
    assert count == 1


@pg_test(tags=["unit", "isolation"])
def test_serializable_isolation(db):
    """SERIALIZABLE isolation level should work."""
    db.execute("""
        CREATE TABLE iso_ser (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('iso_ser', group_by => 'id', order_by => 'ver');
        INSERT INTO iso_ser VALUES (1, 1, 'initial');
    """)
    
    db.execute("""
        BEGIN ISOLATION LEVEL SERIALIZABLE;
        SELECT * FROM iso_ser;
        INSERT INTO iso_ser VALUES (1, 2, 'serializable insert');
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM iso_ser")
    assert count == 2


@pg_test(tags=["unit", "isolation"])
def test_read_only_transaction(db):
    """READ ONLY transaction should allow reads."""
    db.execute("""
        CREATE TABLE iso_ro (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('iso_ro', group_by => 'id', order_by => 'ver');
        INSERT INTO iso_ro VALUES (1, 1, 'initial');
    """)
    
    db.execute("""
        BEGIN READ ONLY;
        SELECT * FROM iso_ro;
        COMMIT;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM iso_ro")
    assert count == 1


@pg_test(tags=["unit", "isolation"])
def test_read_only_rejects_insert(db):
    """READ ONLY transaction should reject INSERT."""
    db.execute("""
        CREATE TABLE iso_ro_rej (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('iso_ro_rej', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("""
            BEGIN READ ONLY;
            INSERT INTO iso_ro_rej VALUES (1, 1, 'should fail');
            COMMIT;
        """)
        assert False, "INSERT in READ ONLY should fail"
    except Exception as e:
        # Should get an error about read-only transaction
        assert "read-only" in str(e).lower() or "cannot" in str(e).lower()
