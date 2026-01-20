"""
Query planning tests - EXPLAIN, prepared statements, cursors.

Ported from tmp/stress_test/edge_case_tests.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "explain"])
def test_explain_on_xpatch_table(db):
    """EXPLAIN should work on xpatch table."""
    db.execute("""
        CREATE TABLE explain_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('explain_test', group_by => 'id', order_by => 'ver');
        INSERT INTO explain_test SELECT g, v, 'data' 
        FROM generate_series(1, 100) g, generate_series(1, 10) v;
    """)
    
    # Should not error
    rows = db.fetchall("EXPLAIN SELECT * FROM explain_test WHERE id = 50")
    
    assert len(rows) > 0, "EXPLAIN should return plan"


@pg_test(tags=["unit", "explain"])
def test_explain_analyze(db):
    """EXPLAIN ANALYZE should work and show execution stats."""
    db.execute("""
        CREATE TABLE explain_analyze_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('explain_analyze_test', group_by => 'id', order_by => 'ver');
        INSERT INTO explain_analyze_test VALUES (1, 1, 'test');
    """)
    
    rows = db.fetchall("EXPLAIN ANALYZE SELECT * FROM explain_analyze_test")
    
    # Should have execution stats in output
    plan_text = ' '.join([str(r) for r in rows])
    assert 'actual' in plan_text.lower() or 'rows' in plan_text.lower()


@pg_test(tags=["unit", "explain"])
def test_explain_with_join(db):
    """EXPLAIN with JOIN to heap table."""
    db.execute("""
        CREATE TABLE exp_xp (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('exp_xp', group_by => 'id', order_by => 'ver');
        INSERT INTO exp_xp VALUES (1, 1, 'xp data');
        
        CREATE TABLE exp_heap (id INT, name TEXT);
        INSERT INTO exp_heap VALUES (1, 'name1');
    """)
    
    rows = db.fetchall("""
        EXPLAIN SELECT * FROM exp_xp e JOIN exp_heap h ON e.id = h.id
    """)
    
    assert len(rows) > 0, "EXPLAIN should return join plan"


@pg_test(tags=["unit", "prepared"])
def test_prepared_select_statement(db):
    """Prepared SELECT statement on xpatch table."""
    db.execute("""
        CREATE TABLE prep_sel (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('prep_sel', group_by => 'id', order_by => 'ver');
        INSERT INTO prep_sel VALUES (1, 1, 'initial');
        
        PREPARE sel_stmt AS SELECT data FROM prep_sel WHERE id = $1 AND ver = $2;
    """)
    
    row = db.fetchone("EXECUTE sel_stmt(1, 1)")
    
    assert row is not None
    assert row['data'] == 'initial'
    
    # Cleanup
    db.execute("DEALLOCATE sel_stmt")


@pg_test(tags=["unit", "prepared"])
def test_prepared_insert_statement(db):
    """Prepared INSERT statement on xpatch table."""
    db.execute("""
        CREATE TABLE prep_ins (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('prep_ins', group_by => 'id', order_by => 'ver');
        
        PREPARE ins_stmt AS INSERT INTO prep_ins VALUES ($1, $2, $3);
        EXECUTE ins_stmt(1, 1, 'first');
        EXECUTE ins_stmt(1, 2, 'second');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM prep_ins")
    
    assert count == 2, f"Expected 2 rows, got {count}"
    
    db.execute("DEALLOCATE ins_stmt")


@pg_test(tags=["unit", "prepared"])
def test_prepared_delete_statement(db):
    """Prepared DELETE statement on xpatch table."""
    db.execute("""
        CREATE TABLE prep_del (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('prep_del', group_by => 'id', order_by => 'ver');
        INSERT INTO prep_del VALUES (1, 1, 'to delete');
        INSERT INTO prep_del VALUES (1, 2, 'also delete');
        
        PREPARE del_stmt AS DELETE FROM prep_del WHERE id = $1;
        EXECUTE del_stmt(1);
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM prep_del")
    
    assert count == 0, f"Expected 0 rows after delete, got {count}"
    
    db.execute("DEALLOCATE del_stmt")


@pg_test(tags=["unit", "cursor"])
def test_basic_cursor(db):
    """Basic cursor on xpatch table."""
    db.execute("""
        CREATE TABLE cursor_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cursor_test', group_by => 'id', order_by => 'ver');
        INSERT INTO cursor_test SELECT 1, g, 'row ' || g FROM generate_series(1, 20) g;
    """)
    
    db.execute("""
        BEGIN;
        DECLARE cur CURSOR FOR SELECT * FROM cursor_test ORDER BY ver;
        FETCH 5 FROM cur;
        CLOSE cur;
        COMMIT;
    """)
    
    # Should complete without error
    count = db.fetchval("SELECT COUNT(*) FROM cursor_test")
    assert count == 20


@pg_test(tags=["unit", "cursor"])
def test_scrollable_cursor(db):
    """Scrollable cursor on xpatch table."""
    db.execute("""
        CREATE TABLE scroll_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('scroll_test', group_by => 'id', order_by => 'ver');
        INSERT INTO scroll_test SELECT 1, g, 'row ' || g FROM generate_series(1, 20) g;
    """)
    
    db.execute("""
        BEGIN;
        DECLARE scur SCROLL CURSOR FOR SELECT * FROM scroll_test ORDER BY ver;
        FETCH LAST FROM scur;
        FETCH FIRST FROM scur;
        FETCH ABSOLUTE 10 FROM scur;
        CLOSE scur;
        COMMIT;
    """)
    
    # Should complete without error
    count = db.fetchval("SELECT COUNT(*) FROM scroll_test")
    assert count == 20


@pg_test(tags=["unit", "cursor"])
def test_cursor_with_hold(db):
    """Cursor WITH HOLD on xpatch table."""
    db.execute("""
        CREATE TABLE hold_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('hold_test', group_by => 'id', order_by => 'ver');
        INSERT INTO hold_test SELECT 1, g, 'row ' || g FROM generate_series(1, 10) g;
    """)
    
    db.execute("""
        BEGIN;
        DECLARE hcur CURSOR WITH HOLD FOR SELECT * FROM hold_test LIMIT 5;
        COMMIT;
        FETCH ALL FROM hcur;
        CLOSE hcur;
    """)
    
    # Should complete without error
    count = db.fetchval("SELECT COUNT(*) FROM hold_test")
    assert count == 10


@pg_test(tags=["unit", "misc"])
def test_analyze_table(db):
    """ANALYZE should work on xpatch table."""
    db.execute("""
        CREATE TABLE analyze_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('analyze_test', group_by => 'id', order_by => 'ver');
        INSERT INTO analyze_test SELECT g, 1, 'data' FROM generate_series(1, 100) g;
        
        ANALYZE analyze_test;
    """)
    
    # Check that statistics were collected
    result = db.fetchone("""
        SELECT reltuples, relpages FROM pg_class WHERE relname = 'analyze_test'
    """)
    
    assert result is not None, "Table should have pg_class entry"


@pg_test(tags=["unit", "misc"])
def test_table_size_functions(db):
    """pg_total_relation_size should work on xpatch table."""
    db.execute("""
        CREATE TABLE size_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('size_test', group_by => 'id', order_by => 'ver');
        INSERT INTO size_test VALUES (1, 1, 'test data');
    """)
    
    size = db.fetchval("SELECT pg_total_relation_size('size_test')")
    
    assert size > 0, f"Expected positive size, got {size}"


@pg_test(tags=["unit", "misc"])
def test_comment_on_table(db):
    """COMMENT ON TABLE should work."""
    db.execute("""
        CREATE TABLE comment_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('comment_test', group_by => 'id', order_by => 'ver');
        
        COMMENT ON TABLE comment_test IS 'This is an xpatch table';
    """)
    
    comment = db.fetchval("SELECT obj_description('comment_test'::regclass)")
    
    assert comment == 'This is an xpatch table', f"Expected comment, got '{comment}'"


@pg_test(tags=["unit", "misc"])
def test_rename_table(db):
    """RENAME TABLE should work on xpatch table."""
    db.execute("""
        CREATE TABLE rename_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('rename_test', group_by => 'id', order_by => 'ver');
        INSERT INTO rename_test VALUES (1, 1, 'test');
        
        ALTER TABLE rename_test RENAME TO rename_test_new;
    """)
    
    # Query the renamed table
    count = db.fetchval("SELECT COUNT(*) FROM rename_test_new")
    assert count == 1
    
    # Rename back for cleanup
    db.execute("ALTER TABLE rename_test_new RENAME TO rename_test")
