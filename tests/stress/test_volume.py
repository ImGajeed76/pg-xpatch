"""
Volume stress tests - high data volume scenarios.
"""

from xptest import pg_test


@pg_test(tags=["stress", "volume"], slow=True, timeout=120)
def test_1000_versions_single_group(db):
    """Insert and read 1000 versions in a single group."""
    db.execute("""
        CREATE TABLE vol_1k (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('vol_1k', group_by => 'grp', order_by => 'ver', keyframe_every => 50);
    """)
    
    # Batch insert 1000 versions
    values = ",".join([f"(1, {i}, 'Version {i} with content')" for i in range(1, 1001)])
    db.execute(f"INSERT INTO vol_1k VALUES {values}")
    
    # Verify count
    count = db.fetchval("SELECT COUNT(*) FROM vol_1k")
    assert count == 1000, f"Expected 1000 rows, got {count}"
    
    # Verify random access works
    import random
    for _ in range(20):
        ver = random.randint(1, 1000)
        result = db.fetchone(f"SELECT ver, data FROM vol_1k WHERE ver = {ver}")
        assert result['ver'] == ver
        assert f"Version {ver}" in result['data']


@pg_test(tags=["stress", "volume"], slow=True, timeout=180)
def test_100_groups_100_versions(db):
    """Insert 100 groups with 100 versions each (10k total rows)."""
    db.execute("""
        CREATE TABLE vol_100x100 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('vol_100x100', group_by => 'grp', order_by => 'ver', keyframe_every => 20);
    """)
    
    # Insert data
    for g in range(1, 101):
        values = ",".join([f"({g}, {v}, 'G{g}V{v}')" for v in range(1, 101)])
        db.execute(f"INSERT INTO vol_100x100 VALUES {values}")
    
    # Verify total count
    count = db.fetchval("SELECT COUNT(*) FROM vol_100x100")
    assert count == 10000, f"Expected 10000 rows, got {count}"
    
    # Verify group count
    groups = db.fetchval("SELECT COUNT(DISTINCT grp) FROM vol_100x100")
    assert groups == 100, f"Expected 100 groups, got {groups}"
    
    # Verify stats function handles large tables
    stats = db.fetchone("SELECT total_rows, total_groups FROM xpatch.stats('vol_100x100')")
    assert stats['total_rows'] == 10000
    assert stats['total_groups'] == 100


@pg_test(tags=["stress", "volume"], slow=True, timeout=120)
def test_many_small_groups(db):
    """Test 1000 groups with 5 versions each."""
    db.execute("""
        CREATE TABLE vol_many_groups (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('vol_many_groups', group_by => 'grp', order_by => 'ver');
    """)
    
    # Insert many groups
    for g in range(1, 1001):
        db.execute(f"""
            INSERT INTO vol_many_groups VALUES 
            ({g}, 1, 'v1'), ({g}, 2, 'v2'), ({g}, 3, 'v3'), ({g}, 4, 'v4'), ({g}, 5, 'v5')
        """)
    
    count = db.fetchval("SELECT COUNT(*) FROM vol_many_groups")
    assert count == 5000, f"Expected 5000 rows, got {count}"
    
    groups = db.fetchval("SELECT COUNT(DISTINCT grp) FROM vol_many_groups")
    assert groups == 1000, f"Expected 1000 groups, got {groups}"


@pg_test(tags=["stress", "volume"], slow=True, timeout=60)
def test_physical_scan_large_table(db):
    """Test xpatch.physical() scan performance on larger table."""
    db.execute("""
        CREATE TABLE vol_phys (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('vol_phys', group_by => 'grp', order_by => 'ver', keyframe_every => 10);
    """)
    
    # Insert 500 rows
    for g in range(1, 51):
        values = ",".join([f"({g}, {v}, 'data')" for v in range(1, 11)])
        db.execute(f"INSERT INTO vol_phys VALUES {values}")
    
    # Scan with physical
    phys_count = db.fetchval("SELECT COUNT(*) FROM xpatch.physical('vol_phys')")
    assert phys_count == 500, f"Expected 500 physical rows, got {phys_count}"


@pg_test(tags=["stress", "volume"])
def test_text_group_column_stress(db):
    """Test TEXT group columns with many groups."""
    db.execute("""
        CREATE TABLE vol_text_grp (category TEXT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('vol_text_grp', group_by => 'category', order_by => 'ver');
    """)
    
    # Insert 50 groups with 10 versions each
    for g in range(50):
        values = ",".join([f"('group_{g}', {v}, 'g{g}v{v}')" for v in range(1, 11)])
        db.execute(f"INSERT INTO vol_text_grp VALUES {values}")
    
    count = db.fetchval("SELECT COUNT(*) FROM vol_text_grp")
    assert count == 500, f"Expected 500 rows, got {count}"
    
    # Random access by text group
    import random
    for _ in range(10):
        g = random.randint(0, 49)
        cnt = db.fetchval(f"SELECT COUNT(*) FROM vol_text_grp WHERE category = 'group_{g}'")
        assert cnt == 10, f"Expected 10 versions for group_{g}, got {cnt}"
