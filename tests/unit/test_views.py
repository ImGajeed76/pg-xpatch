"""
View tests - Views on xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "views"])
def test_simple_view_on_xpatch_table(db):
    """Simple view on xpatch table should work."""
    db.execute("""
        CREATE TABLE view_base (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('view_base', group_by => 'id', order_by => 'ver');
        INSERT INTO view_base VALUES (1, 1, 'v1'), (1, 2, 'v2'), (2, 1, 'other');
        
        CREATE VIEW simple_view AS SELECT * FROM view_base WHERE id = 1;
    """)
    
    rows = db.fetchall("SELECT * FROM simple_view")
    
    assert len(rows) == 2, f"Expected 2 rows from view, got {len(rows)}"
    assert rows[0]['data'] == 'v1', f"Expected 'v1', got '{rows[0]['data']}'"


@pg_test(tags=["unit", "views"])
def test_aggregating_view(db):
    """View with aggregation on xpatch table."""
    db.execute("""
        CREATE TABLE view_agg (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('view_agg', group_by => 'id', order_by => 'ver');
        INSERT INTO view_agg VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
        
        CREATE VIEW agg_view AS 
        SELECT id, MAX(ver) as latest_ver, COUNT(*) as versions
        FROM view_agg GROUP BY id;
    """)
    
    rows = db.fetchall("SELECT * FROM agg_view ORDER BY id")
    
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    assert rows[0]['latest_ver'] == 2, f"Expected max ver 2 for id 1, got {rows[0]['latest_ver']}"


@pg_test(tags=["unit", "views"])
def test_materialized_view(db):
    """Materialized view on xpatch table."""
    db.execute("""
        CREATE TABLE view_mat (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('view_mat', group_by => 'id', order_by => 'ver');
        INSERT INTO view_mat VALUES (1, 1, 'a'), (1, 2, 'b');
        
        CREATE MATERIALIZED VIEW mat_view AS SELECT * FROM view_mat;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM mat_view")
    assert count == 2, f"Expected 2 rows in matview, got {count}"


@pg_test(tags=["unit", "views"])
def test_refresh_materialized_view(db):
    """Refresh materialized view after data changes."""
    db.execute("""
        CREATE TABLE view_refresh (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('view_refresh', group_by => 'id', order_by => 'ver');
        INSERT INTO view_refresh VALUES (1, 1, 'a');
        
        CREATE MATERIALIZED VIEW refresh_mat AS SELECT * FROM view_refresh;
    """)
    
    # Add more data
    db.execute("INSERT INTO view_refresh VALUES (2, 1, 'new')")
    
    # Refresh
    db.execute("REFRESH MATERIALIZED VIEW refresh_mat")
    
    count = db.fetchval("SELECT COUNT(*) FROM refresh_mat")
    assert count == 2, f"Expected 2 rows after refresh, got {count}"


@pg_test(tags=["unit", "views"])
def test_view_with_join(db):
    """View joining xpatch table with heap table."""
    db.execute("""
        CREATE TABLE view_xp (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('view_xp', group_by => 'id', order_by => 'ver');
        INSERT INTO view_xp VALUES (1, 1, 'xpatch data');
        
        CREATE TABLE view_heap (id INT, name TEXT);
        INSERT INTO view_heap VALUES (1, 'name1');
        
        CREATE VIEW join_view AS 
        SELECT x.id, x.data, h.name 
        FROM view_xp x JOIN view_heap h ON x.id = h.id;
    """)
    
    row = db.fetchone("SELECT * FROM join_view")
    
    assert row['data'] == 'xpatch data', f"Expected 'xpatch data', got '{row['data']}'"
    assert row['name'] == 'name1', f"Expected 'name1', got '{row['name']}'"
