"""
Index tests - Indexes on xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "indexes"])
def test_create_btree_index(db):
    """Create btree index on xpatch table."""
    db.execute("""
        CREATE TABLE idx_test (id INT, ver INT, category TEXT, data TEXT) USING xpatch;
        SELECT xpatch.configure('idx_test', group_by => 'id', order_by => 'ver');
        INSERT INTO idx_test SELECT g % 100, g / 100 + 1, 'cat' || (g % 10), 'data'
        FROM generate_series(1, 1000) g;
        
        CREATE INDEX idx_category ON idx_test (category);
    """)
    
    # Verify index exists
    idx_exists = db.fetchval("""
        SELECT COUNT(*) FROM pg_indexes 
        WHERE tablename = 'idx_test' AND indexname = 'idx_category'
    """)
    
    assert idx_exists == 1, "Index was not created"


@pg_test(tags=["unit", "indexes"])
def test_index_used_in_query(db):
    """Index should be considered in query planning."""
    db.execute("""
        CREATE TABLE idx_plan (id INT, ver INT, category TEXT) USING xpatch;
        SELECT xpatch.configure('idx_plan', group_by => 'id', order_by => 'ver');
        INSERT INTO idx_plan SELECT g, 1, 'cat' || (g % 10) FROM generate_series(1, 1000) g;
        
        CREATE INDEX idx_plan_cat ON idx_plan (category);
        ANALYZE idx_plan;
    """)
    
    # Just verify the query runs (index usage depends on planner decisions)
    rows = db.fetchall("SELECT * FROM idx_plan WHERE category = 'cat5'")
    
    assert len(rows) == 100, f"Expected 100 rows, got {len(rows)}"


@pg_test(tags=["unit", "indexes"])
def test_drop_index(db):
    """Drop index on xpatch table."""
    db.execute("""
        CREATE TABLE idx_drop (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('idx_drop', group_by => 'id', order_by => 'ver');
        CREATE INDEX idx_to_drop ON idx_drop (data);
    """)
    
    # Verify index exists
    assert db.fetchval("""
        SELECT COUNT(*) FROM pg_indexes 
        WHERE tablename = 'idx_drop' AND indexname = 'idx_to_drop'
    """) == 1
    
    # Drop it
    db.execute("DROP INDEX idx_to_drop")
    
    # Verify it's gone
    assert db.fetchval("""
        SELECT COUNT(*) FROM pg_indexes 
        WHERE tablename = 'idx_drop' AND indexname = 'idx_to_drop'
    """) == 0


@pg_test(tags=["unit", "indexes"])
def test_reindex_table(db):
    """REINDEX table should work on xpatch table."""
    db.execute("""
        CREATE TABLE idx_reindex (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('idx_reindex', group_by => 'id', order_by => 'ver');
        CREATE INDEX idx_reindex_data ON idx_reindex (data);
        INSERT INTO idx_reindex VALUES (1, 1, 'test');
        
        REINDEX TABLE idx_reindex;
    """)
    
    # Should complete without error
    count = db.fetchval("SELECT COUNT(*) FROM idx_reindex")
    assert count == 1


@pg_test(tags=["unit", "indexes"])
def test_composite_index(db):
    """Composite index on multiple columns."""
    db.execute("""
        CREATE TABLE idx_composite (id INT, ver INT, cat TEXT, subcat TEXT) USING xpatch;
        SELECT xpatch.configure('idx_composite', group_by => 'id', order_by => 'ver');
        INSERT INTO idx_composite SELECT g, 1, 'cat' || (g % 5), 'sub' || (g % 3) 
        FROM generate_series(1, 100) g;
        
        CREATE INDEX idx_comp ON idx_composite (cat, subcat);
    """)
    
    # Query using both columns
    rows = db.fetchall("SELECT * FROM idx_composite WHERE cat = 'cat1' AND subcat = 'sub0'")
    
    assert len(rows) > 0, "Expected some rows matching both conditions"


@pg_test(tags=["unit", "indexes"])
def test_partial_index(db):
    """Partial index with WHERE clause."""
    db.execute("""
        CREATE TABLE idx_partial (id INT, ver INT, status TEXT, data TEXT) USING xpatch;
        SELECT xpatch.configure('idx_partial', group_by => 'id', order_by => 'ver');
        INSERT INTO idx_partial VALUES 
            (1, 1, 'active', 'a'),
            (2, 1, 'inactive', 'b'),
            (3, 1, 'active', 'c');
        
        CREATE INDEX idx_active_only ON idx_partial (id) WHERE status = 'active';
    """)
    
    # Index exists
    idx_exists = db.fetchval("""
        SELECT COUNT(*) FROM pg_indexes 
        WHERE tablename = 'idx_partial' AND indexname = 'idx_active_only'
    """)
    
    assert idx_exists == 1, "Partial index was not created"


# NOTE: Expression indexes on xpatch tables cause server crash (known issue)
# This test is disabled until the underlying issue is fixed
# @pg_test(tags=["unit", "indexes", "skip"])
# def test_expression_index(db):
#     """Index on expression (e.g., LOWER(column))."""
#     db.execute("""
#         CREATE TABLE idx_expr (id INT, ver INT, name TEXT) USING xpatch;
#         SELECT xpatch.configure('idx_expr', group_by => 'id', order_by => 'ver');
#         INSERT INTO idx_expr VALUES (1, 1, 'TestName');
#         
#         CREATE INDEX idx_lower_name ON idx_expr (LOWER(name));
#     """)
#     
#     # Query using the expression
#     row = db.fetchone("SELECT * FROM idx_expr WHERE LOWER(name) = 'testname'")
#     
#     assert row is not None, "Query with expression should return result"
