"""
TEXT group column tests - Non-integer group columns.

Ported from tmp/stress_test/stress_test_xpatch.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "text_group"])
def test_text_group_column_basic_operations(db):
    """TEXT group column basic insert and count."""
    db.execute("""
        CREATE TABLE text_grp (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_grp', group_by => 'category', order_by => 'ver');
        INSERT INTO text_grp VALUES ('alpha', 1, 'content1');
        INSERT INTO text_grp VALUES ('alpha', 2, 'content2');
        INSERT INTO text_grp VALUES ('beta', 1, 'beta_content1');
        INSERT INTO text_grp VALUES ('beta', 2, 'beta_content2');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM text_grp")
    assert count == 4, f"Expected 4 rows, got {count}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_data_retrieval(db):
    """TEXT group data retrieval is correct."""
    db.execute("""
        CREATE TABLE text_grp_read (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_grp_read', group_by => 'category', order_by => 'ver');
        INSERT INTO text_grp_read VALUES ('alpha', 1, 'content1');
        INSERT INTO text_grp_read VALUES ('alpha', 2, 'content2');
    """)
    
    rows = db.fetchall("""
        SELECT category, ver, content FROM text_grp_read 
        WHERE category = 'alpha' ORDER BY ver
    """)
    
    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    assert rows[0]['content'] == 'content1', f"Expected 'content1', got '{rows[0]['content']}'"
    assert rows[1]['content'] == 'content2', f"Expected 'content2', got '{rows[1]['content']}'"


@pg_test(tags=["unit", "text_group"])
def test_text_group_physical_filter(db):
    """xpatch.physical() filters by TEXT group correctly."""
    db.execute("""
        CREATE TABLE text_phys (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_phys', group_by => 'category', order_by => 'ver');
        INSERT INTO text_phys VALUES ('alpha', 1, 'a1');
        INSERT INTO text_phys VALUES ('alpha', 2, 'a2');
        INSERT INTO text_phys VALUES ('beta', 1, 'b1');
    """)
    
    # Use polymorphic version with type hint
    rows = db.fetchall("SELECT * FROM xpatch.physical('text_phys', 'alpha'::TEXT, NULL::INT)")
    assert len(rows) == 2, f"Expected 2 rows for 'alpha', got {len(rows)}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_inspect(db):
    """xpatch.inspect() works with TEXT group filter."""
    db.execute("""
        CREATE TABLE text_inspect (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_inspect', group_by => 'category', order_by => 'ver');
        INSERT INTO text_inspect VALUES ('beta', 1, 'b1');
        INSERT INTO text_inspect VALUES ('beta', 2, 'b2');
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.inspect('text_inspect', 'beta'::TEXT)")
    assert len(rows) == 2, f"Expected 2 rows for 'beta', got {len(rows)}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_stats(db):
    """xpatch.stats() works on TEXT group table."""
    db.execute("""
        CREATE TABLE text_stats (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_stats', group_by => 'category', order_by => 'ver');
        INSERT INTO text_stats VALUES ('a', 1, 'a1');
        INSERT INTO text_stats VALUES ('a', 2, 'a2');
        INSERT INTO text_stats VALUES ('b', 1, 'b1');
        INSERT INTO text_stats VALUES ('b', 2, 'b2');
    """)
    
    result = db.fetchone("SELECT total_rows, total_groups FROM xpatch.stats('text_stats')")
    assert result['total_rows'] == 4, f"Expected 4 rows, got {result['total_rows']}"


@pg_test(tags=["unit", "text_group"])
def test_varchar_group_column(db):
    """VARCHAR group column works correctly."""
    db.execute("""
        CREATE TABLE varchar_grp (category VARCHAR(100), ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('varchar_grp', group_by => 'category', order_by => 'ver');
        INSERT INTO varchar_grp VALUES ('group_a', 1, 'data1');
        INSERT INTO varchar_grp VALUES ('group_a', 2, 'data2');
        INSERT INTO varchar_grp VALUES ('group_b', 1, 'other_data');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM varchar_grp")
    assert count == 3, f"Expected 3 rows, got {count}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_special_characters(db):
    """TEXT group with special characters including spaces and symbols."""
    db.execute("""
        CREATE TABLE text_special (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_special', group_by => 'category', order_by => 'ver');
        INSERT INTO text_special VALUES ('hello world', 1, 'spaces in group');
        INSERT INTO text_special VALUES ('hello world', 2, 'more spaces');
        INSERT INTO text_special VALUES ('special!@#$%', 1, 'symbols');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM text_special")
    assert count == 3, f"Expected 3 rows, got {count}"
    
    # Verify we can query with special characters
    space_count = db.fetchval("SELECT COUNT(*) FROM text_special WHERE category = 'hello world'")
    assert space_count == 2, f"Expected 2 rows for 'hello world', got {space_count}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_not_delta_compressed(db):
    """TEXT group column should NOT be delta-compressed (only content column)."""
    db.execute("""
        CREATE TABLE text_delta_check (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_delta_check', group_by => 'category', order_by => 'ver');
        INSERT INTO text_delta_check VALUES ('test', 1, 'content1');
    """)
    
    # delta_column should be 'content', not 'category'
    result = db.fetchone("SELECT delta_column FROM xpatch.physical('text_delta_check') LIMIT 1")
    assert result['delta_column'] == 'content', (
        f"Expected delta_column='content', got '{result['delta_column']}'"
    )


@pg_test(tags=["unit", "text_group", "slow"])
def test_text_groups_stress(db):
    """50 TEXT groups with 10 versions each - stress test."""
    db.execute("""
        CREATE TABLE text_stress (category TEXT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('text_stress', group_by => 'category', order_by => 'ver');
    """)
    
    # Insert 50 groups with 10 versions each
    for g in range(50):
        values = ",".join([f"('group_{g}', {v}, 'g{g}v{v}_content')" for v in range(1, 11)])
        db.execute(f"INSERT INTO text_stress VALUES {values}")
    
    count = db.fetchval("SELECT COUNT(*) FROM text_stress")
    assert count == 500, f"Expected 500 rows, got {count}"


@pg_test(tags=["unit", "text_group"])
def test_random_text_group_reads(db):
    """Random TEXT group reads return correct counts."""
    db.execute("""
        CREATE TABLE text_random (category TEXT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('text_random', group_by => 'category', order_by => 'ver');
    """)
    
    # Insert 10 groups with 5 versions each
    for g in range(10):
        for v in range(1, 6):
            db.execute("INSERT INTO text_random VALUES (%s, %s, %s)", 
                      (f'group_{g}', v, f'g{g}v{v}'))
    
    # Check random groups
    import random
    for _ in range(10):
        g = random.randint(0, 9)
        count = db.fetchval("SELECT COUNT(*) FROM text_random WHERE category = %s", (f'group_{g}',))
        assert count == 5, f"Expected 5 rows for group_{g}, got {count}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_empty_string(db):
    """Empty string as TEXT group value."""
    db.execute("""
        CREATE TABLE text_empty_grp (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_empty_grp', group_by => 'category', order_by => 'ver');
        INSERT INTO text_empty_grp VALUES ('', 1, 'empty group content');
        INSERT INTO text_empty_grp VALUES ('', 2, 'more content');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM text_empty_grp WHERE category = ''")
    assert count == 2, f"Expected 2 rows in empty string group, got {count}"


@pg_test(tags=["unit", "text_group"])
def test_text_group_unicode(db):
    """Unicode characters in TEXT group values."""
    db.execute("""
        CREATE TABLE text_unicode (category TEXT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('text_unicode', group_by => 'category', order_by => 'ver');
    """)
    
    # Insert with various unicode
    db.execute("INSERT INTO text_unicode VALUES (%s, 1, 'japanese')", ('日本語',))
    db.execute("INSERT INTO text_unicode VALUES (%s, 1, 'chinese')", ('中文',))
    db.execute("INSERT INTO text_unicode VALUES (%s, 1, 'russian')", ('Русский',))
    
    count = db.fetchval("SELECT COUNT(*) FROM text_unicode")
    assert count == 3, f"Expected 3 unicode groups, got {count}"
    
    # Verify retrieval
    content = db.fetchval("SELECT content FROM text_unicode WHERE category = %s", ('日本語',))
    assert content == 'japanese', f"Expected 'japanese', got '{content}'"
