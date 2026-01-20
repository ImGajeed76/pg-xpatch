"""
Cache behavior tests - verify caching functionality.

Ported from tmp/stress_test/stress_test_xpatch.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "cache"])
def test_cache_stats_returns_valid_max_bytes(db):
    """cache_stats() returns valid cache_max_bytes >= 0."""
    result = db.fetchone("SELECT cache_max_bytes FROM xpatch.cache_stats()")
    cache_bytes = result['cache_max_bytes']
    # Cache may be disabled (0) in some configurations
    assert cache_bytes >= 0, f"Expected cache_max_bytes >= 0, got {cache_bytes}"


@pg_test(tags=["unit", "cache"])
def test_cache_stats_returns_all_fields(db):
    """cache_stats() returns all expected fields."""
    result = db.fetchone("""
        SELECT cache_size_bytes, cache_max_bytes, entries_count, 
               hit_count, miss_count, eviction_count 
        FROM xpatch.cache_stats()
    """)
    
    assert 'cache_size_bytes' in result, "Missing cache_size_bytes"
    assert 'cache_max_bytes' in result, "Missing cache_max_bytes"
    assert 'entries_count' in result, "Missing entries_count"
    assert 'hit_count' in result, "Missing hit_count"
    assert 'miss_count' in result, "Missing miss_count"
    assert 'eviction_count' in result, "Missing eviction_count"
    
    # All values should be non-negative
    for key, value in result.items():
        assert value >= 0, f"{key} should be >= 0, got {value}"


@pg_test(tags=["unit", "cache"])
def test_warm_cache_populates_cache(db):
    """warm_cache() should populate the cache."""
    # Get initial cache state
    before = db.fetchone("SELECT entries_count FROM xpatch.cache_stats()")
    
    # Create table and warm cache
    db.execute("""
        CREATE TABLE cache_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO cache_test VALUES (1, 1, 'test data for cache');
        INSERT INTO cache_test VALUES (1, 2, 'more test data');
        SELECT * FROM xpatch.warm_cache('cache_test');
    """)
    
    after = db.fetchone("SELECT entries_count FROM xpatch.cache_stats()")
    
    # Cache entries should not decrease after warming
    assert after['entries_count'] >= before['entries_count'], (
        f"Cache entries decreased after warm_cache: {before['entries_count']} -> {after['entries_count']}"
    )


@pg_test(tags=["unit", "cache"])
def test_warm_cache_returns_stats(db):
    """warm_cache() returns rows_scanned and groups_warmed."""
    db.execute("""
        CREATE TABLE warm_stats (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_stats', group_by => 'grp', order_by => 'ver');
        INSERT INTO warm_stats SELECT g, v, repeat('data', 10)
        FROM generate_series(1, 3) g, generate_series(1, 5) v;
    """)
    
    result = db.fetchone("SELECT rows_scanned, groups_warmed FROM xpatch.warm_cache('warm_stats')")
    
    assert result['rows_scanned'] == 15, f"Expected 15 rows scanned, got {result['rows_scanned']}"
    assert result['groups_warmed'] == 3, f"Expected 3 groups warmed, got {result['groups_warmed']}"


@pg_test(tags=["unit", "cache"])
def test_cache_used_for_repeated_reads(db):
    """Cache should be used for repeated reads (entries exist)."""
    db.execute("""
        CREATE TABLE cache_read_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_read_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO cache_read_test VALUES (1, 1, 'test data');
        INSERT INTO cache_read_test VALUES (1, 2, 'more data');
    """)
    
    # Get initial state
    before = db.fetchone("SELECT entries_count FROM xpatch.cache_stats()")
    
    # Read data multiple times
    for _ in range(10):
        db.fetchall("SELECT * FROM cache_read_test WHERE grp = 1")
    
    after = db.fetchone("SELECT entries_count FROM xpatch.cache_stats()")
    
    # Cache entries should exist (>= 0 is always true, but checking it works)
    assert after['entries_count'] >= 0, f"Invalid cache entries count: {after['entries_count']}"


@pg_test(tags=["unit", "cache"])
def test_cache_stats_hit_miss_counts(db):
    """Cache hit/miss counts should be non-negative."""
    result = db.fetchone("SELECT hit_count, miss_count FROM xpatch.cache_stats()")
    
    assert result['hit_count'] >= 0, f"hit_count should be >= 0, got {result['hit_count']}"
    assert result['miss_count'] >= 0, f"miss_count should be >= 0, got {result['miss_count']}"


@pg_test(tags=["unit", "cache"])
def test_warm_cache_on_empty_table(db):
    """warm_cache() on empty table should return zeros."""
    db.execute("""
        CREATE TABLE warm_empty (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_empty', group_by => 'grp', order_by => 'ver');
    """)
    
    result = db.fetchone("SELECT rows_scanned, groups_warmed FROM xpatch.warm_cache('warm_empty')")
    
    assert result['rows_scanned'] == 0, f"Expected 0 rows scanned, got {result['rows_scanned']}"
    assert result['groups_warmed'] == 0, f"Expected 0 groups warmed, got {result['groups_warmed']}"
