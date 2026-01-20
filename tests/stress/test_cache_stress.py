"""
Cache stress tests - Stress the internal group cache.

Ported from tmp/stress_test/final_tests.py (CACHE-001 to CACHE-004)
"""

from xptest import pg_test


@pg_test(tags=["stress", "cache", "slow"])
def test_cache_5000_groups_insert(db):
    """Insert 5000 groups to stress the cache."""
    db.execute("""
        CREATE TABLE cache_stress (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_stress', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Insert 5000 groups
    db.execute("""
        INSERT INTO cache_stress 
        SELECT g, 1, 'group ' || g || ' initial content that is reasonably long'
        FROM generate_series(1, 5000) g;
    """)
    
    count = db.fetchval("SELECT COUNT(DISTINCT group_id) FROM cache_stress")
    assert count == 5000, f"Expected 5000 groups, got {count}"


@pg_test(tags=["stress", "cache", "slow"])
def test_cache_churn_random_version_inserts(db):
    """Add versions to random groups causing cache churn."""
    db.execute("""
        CREATE TABLE cache_churn (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_churn', group_by => 'group_id', order_by => 'ver');
        
        -- Insert initial groups
        INSERT INTO cache_churn
        SELECT g, 1, 'initial' FROM generate_series(1, 1000) g;
    """)
    
    # Add versions to random groups
    db.execute("""
        DO $$ 
        DECLARE
            g INT;
        BEGIN
            FOR i IN 1..500 LOOP
                g := (random() * 999 + 1)::INT;
                BEGIN
                    INSERT INTO cache_churn 
                    SELECT g, COALESCE(MAX(ver), 0) + 1, 'version ' || i
                    FROM cache_churn WHERE group_id = g;
                EXCEPTION WHEN OTHERS THEN
                    NULL;  -- Skip version conflicts
                END;
            END LOOP;
        END $$;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM cache_churn")
    assert count > 1000, f"Expected more than 1000 rows after churn, got {count}"


@pg_test(tags=["stress", "cache", "slow"])
def test_cache_random_reads(db):
    """Read from random groups to test cache hits/misses."""
    db.execute("""
        CREATE TABLE cache_reads (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_reads', group_by => 'group_id', order_by => 'ver');
        
        -- Insert 2000 groups with 2 versions each
        INSERT INTO cache_reads
        SELECT g, 1, 'version 1 data' FROM generate_series(1, 2000) g;
        INSERT INTO cache_reads
        SELECT g, 2, 'version 2 data' FROM generate_series(1, 2000) g;
    """)
    
    # Random reads
    db.execute("""
        DO $$
        DECLARE
            g INT;
            d TEXT;
        BEGIN
            FOR i IN 1..1000 LOOP
                g := (random() * 1999 + 1)::INT;
                SELECT data INTO d FROM cache_reads 
                WHERE group_id = g ORDER BY ver DESC LIMIT 1;
            END LOOP;
        END $$;
    """)
    
    # Verify data integrity
    count = db.fetchval("SELECT COUNT(DISTINCT group_id) FROM cache_reads")
    assert count == 2000, f"Expected 2000 groups intact, got {count}"


@pg_test(tags=["stress", "cache"])
def test_cache_integrity_after_stress(db):
    """Verify data integrity after cache stress operations."""
    db.execute("""
        CREATE TABLE cache_integrity (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_integrity', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Insert lots of groups
    db.execute("""
        INSERT INTO cache_integrity 
        SELECT g, 1, 'group ' || g FROM generate_series(1, 3000) g;
    """)
    
    # Add second version to half of them
    db.execute("""
        INSERT INTO cache_integrity 
        SELECT g, 2, 'group ' || g || ' v2' FROM generate_series(1, 1500) g;
    """)
    
    # Verify counts
    total = db.fetchval("SELECT COUNT(*) FROM cache_integrity")
    assert total == 4500, f"Expected 4500 rows, got {total}"
    
    groups = db.fetchval("SELECT COUNT(DISTINCT group_id) FROM cache_integrity")
    assert groups == 3000, f"Expected 3000 groups, got {groups}"
    
    # Verify delta reconstruction works
    data = db.fetchval("SELECT data FROM cache_integrity WHERE group_id = 1 AND ver = 2")
    assert data == "group 1 v2", f"Expected 'group 1 v2', got '{data}'"
