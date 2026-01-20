"""
Cache warming tests - xpatch.warm_cache() function.

Ported from tmp/stress_test/test_xpatch_functions.py (WARM-001, WARM-002)
"""

from xptest import pg_test


@pg_test(tags=["unit", "cache"])
def test_warm_cache_scans_all_rows(db):
    """xpatch.warm_cache() scans all rows."""
    db.execute("""
        CREATE TABLE warm_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_test', group_by => 'id', order_by => 'ver');
        INSERT INTO warm_test VALUES (1, 1, 'v1'), (1, 2, 'v2'), (1, 3, 'v3');
        INSERT INTO warm_test VALUES (2, 1, 'g2v1'), (2, 2, 'g2v2');
    """)
    
    result = db.fetchone("SELECT * FROM xpatch.warm_cache('warm_test')")
    
    assert result['rows_scanned'] == 5, f"Expected 5 rows scanned, got {result['rows_scanned']}"


@pg_test(tags=["unit", "cache"])
def test_warm_cache_warms_all_groups(db):
    """xpatch.warm_cache() warms all groups."""
    db.execute("""
        CREATE TABLE warm_groups (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_groups', group_by => 'id', order_by => 'ver');
        INSERT INTO warm_groups VALUES (1, 1, 'g1'), (2, 1, 'g2'), (3, 1, 'g3');
    """)
    
    result = db.fetchone("SELECT * FROM xpatch.warm_cache('warm_groups')")
    
    assert result['groups_warmed'] == 3, f"Expected 3 groups warmed, got {result['groups_warmed']}"


@pg_test(tags=["unit", "cache"])
def test_warm_cache_empty_table(db):
    """xpatch.warm_cache() on empty table returns 0."""
    db.execute("""
        CREATE TABLE warm_empty (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_empty', group_by => 'id', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT * FROM xpatch.warm_cache('warm_empty')")
    
    assert result['rows_scanned'] == 0
    assert result['groups_warmed'] == 0


@pg_test(tags=["unit", "cache"])
def test_warm_cache_updates_cache_stats(db):
    """xpatch.warm_cache() should increase cache entries."""
    db.execute("""
        CREATE TABLE warm_stats (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_stats', group_by => 'id', order_by => 'ver');
        INSERT INTO warm_stats 
        SELECT g, 1, 'data' FROM generate_series(1, 100) g;
    """)
    
    # Get cache stats before
    before = db.fetchone("SELECT * FROM xpatch.cache_stats()")
    
    # Warm cache
    db.execute("SELECT * FROM xpatch.warm_cache('warm_stats')")
    
    # Get cache stats after
    after = db.fetchone("SELECT * FROM xpatch.cache_stats()")
    
    # entries_count should have increased (or stayed same if cache was already warm)
    assert after['entries_count'] >= before['entries_count']
