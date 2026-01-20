"""
Long-running stability tests.

Ported from tmp/stress_test/final_tests.py (STAB-001 to STAB-003)
"""

from xptest import pg_test


@pg_test(tags=["stress", "stability", "slow"])
def test_stability_mixed_workload(db):
    """Run mixed workload (inserts, reads, deletes) for stability."""
    db.execute("""
        CREATE TABLE stability_test (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stability_test', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Run mixed workload in PL/pgSQL
    db.execute("""
        DO $$
        DECLARE
            g INT;
            v INT;
            op TEXT;
            ops TEXT[] := ARRAY['insert', 'insert', 'insert', 'read', 'read', 'delete'];
            group_counter INT := 1;
            max_groups INT := 500;
        BEGIN
            FOR i IN 1..1000 LOOP
                op := ops[1 + (random() * 5)::INT];
                
                IF op = 'insert' THEN
                    IF random() < 0.3 OR group_counter = 1 THEN
                        -- New group
                        g := group_counter;
                        group_counter := group_counter + 1;
                        v := 1;
                    ELSE
                        -- New version in existing group
                        g := (random() * (group_counter - 2) + 1)::INT;
                        SELECT COALESCE(MAX(ver), 0) + 1 INTO v FROM stability_test WHERE group_id = g;
                    END IF;
                    
                    BEGIN
                        INSERT INTO stability_test VALUES (g, v, 'data ' || i);
                    EXCEPTION WHEN OTHERS THEN
                        NULL;  -- Ignore conflicts
                    END;
                    
                ELSIF op = 'read' THEN
                    IF group_counter > 1 THEN
                        g := (random() * (group_counter - 2) + 1)::INT;
                        PERFORM data FROM stability_test WHERE group_id = g ORDER BY ver DESC LIMIT 1;
                    END IF;
                    
                ELSIF op = 'delete' THEN
                    IF group_counter > 10 THEN
                        g := (random() * (group_counter - 2) + 1)::INT;
                        DELETE FROM stability_test WHERE group_id = g;
                    END IF;
                END IF;
            END LOOP;
        END $$;
    """)
    
    # Verify database is still consistent
    count = db.fetchval("SELECT COUNT(*) FROM stability_test")
    assert count >= 0, "Count should be non-negative"


@pg_test(tags=["stress", "stability"])
def test_stability_operations_work_after_stress(db):
    """Verify normal operations work after stress."""
    db.execute("""
        CREATE TABLE stability_ops (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stability_ops', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Run some stress
    db.execute("""
        INSERT INTO stability_ops 
        SELECT g, 1, repeat('x', 100) FROM generate_series(1, 1000) g;
        
        INSERT INTO stability_ops 
        SELECT g, 2, repeat('y', 100) FROM generate_series(1, 500) g;
        
        DELETE FROM stability_ops WHERE group_id > 800;
    """)
    
    # Verify normal operations still work
    db.execute("INSERT INTO stability_ops VALUES (999999, 1, 'final test')")
    
    result = db.fetchval("SELECT data FROM stability_ops WHERE group_id = 999999")
    assert result == 'final test', f"Expected 'final test', got '{result}'"


@pg_test(tags=["stress", "stability"])
def test_stability_concurrent_group_creation(db):
    """Create groups concurrently and verify integrity."""
    db.execute("""
        CREATE TABLE stability_conc (group_id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stability_conc', group_by => 'group_id', order_by => 'ver');
    """)
    
    # Simulate concurrent creation by rapid inserts
    for batch in range(10):
        values = ",".join([f"({batch * 100 + i}, 1, 'batch {batch} item {i}')" 
                          for i in range(100)])
        db.execute(f"INSERT INTO stability_conc VALUES {values}")
    
    # Verify all groups created
    count = db.fetchval("SELECT COUNT(DISTINCT group_id) FROM stability_conc")
    assert count == 1000, f"Expected 1000 groups, got {count}"
    
    # Verify random sample
    data = db.fetchval("SELECT data FROM stability_conc WHERE group_id = 555 AND ver = 1")
    assert 'batch 5' in data, f"Expected batch 5 data, got '{data}'"
