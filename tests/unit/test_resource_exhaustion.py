"""
Resource exhaustion tests - Push the extension to its limits.

Ported from tmp/stress_test/adversarial_tests.py (RES-001 to RES-003)
"""

from xptest import pg_test


@pg_test(tags=["unit", "stress", "slow"], timeout=120)
def test_10000_groups(db):
    """Insert 10,000 groups to test group handling limits."""
    db.execute("""
        CREATE TABLE many_groups (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('many_groups', group_by => 'id', order_by => 'ver');
    """)
    
    db.execute("""
        INSERT INTO many_groups 
        SELECT g, 1, 'group ' || g FROM generate_series(1, 10000) g;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM many_groups")
    assert count == 10000, f"Expected 10000 groups, got {count}"
    
    # Verify random access still works
    data = db.fetchval("SELECT data FROM many_groups WHERE id = 5000")
    assert data == "group 5000", f"Expected 'group 5000', got '{data}'"


@pg_test(tags=["unit", "stress", "slow"], timeout=60)
def test_rapid_table_create_drop(db):
    """100 rapid table create/drop cycles."""
    for i in range(100):
        db.execute(f"""
            DROP TABLE IF EXISTS rapid_{i};
            CREATE TABLE rapid_{i} (id INT, ver INT, data TEXT) USING xpatch;
            SELECT xpatch.configure('rapid_{i}', group_by => 'id', order_by => 'ver');
            INSERT INTO rapid_{i} VALUES (1, 1, 'test');
            DROP TABLE rapid_{i};
        """)
    
    # If we get here, all 100 cycles succeeded
    assert True


@pg_test(tags=["unit", "stress", "slow"], timeout=120)
def test_1000_versions_growing_content(db):
    """1000 versions with growing content tests delta handling."""
    db.execute("""
        CREATE TABLE many_deltas (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('many_deltas', group_by => 'id', order_by => 'ver',
                               keyframe_every => 100);
    """)
    
    # Insert 1000 versions with growing content
    db.execute("""
        DO $$ BEGIN
            FOR i IN 1..1000 LOOP
                INSERT INTO many_deltas VALUES (1, i, 
                    'Version ' || i || ' ' || repeat(chr(65 + (i % 26)), i * 10));
            END LOOP;
        END $$;
    """)
    
    # Verify we can read middle version
    data = db.fetchval("SELECT data FROM many_deltas WHERE ver = 500")
    assert data is not None, "Should be able to read version 500"
    assert "Version 500" in data, f"Expected 'Version 500' in data"
    
    # Verify length is correct (500 * 10 = 5000 chars of repeated letter + prefix)
    length = db.fetchval("SELECT length(data) FROM many_deltas WHERE ver = 500")
    assert length > 5000, f"Expected length > 5000, got {length}"


@pg_test(tags=["unit", "stress"])
def test_many_small_deltas(db):
    """Many small deltas (single character changes)."""
    db.execute("""
        CREATE TABLE small_deltas (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('small_deltas', group_by => 'id', order_by => 'ver',
                               keyframe_every => 50);
    """)
    
    # Insert base
    db.execute("INSERT INTO small_deltas VALUES (1, 1, 'AAAAAAAAAA')")
    
    # Insert 100 versions with single character changes
    for i in range(2, 102):
        char = chr(65 + (i % 26))  # A-Z cycling
        db.execute(f"INSERT INTO small_deltas VALUES (1, {i}, '{char * 10}')")
    
    count = db.fetchval("SELECT COUNT(*) FROM small_deltas")
    assert count == 101, f"Expected 101 rows, got {count}"
    
    # Verify reconstruction
    data = db.fetchval("SELECT data FROM small_deltas WHERE ver = 50")
    assert len(data) == 10, f"Expected 10 chars, got {len(data)}"
