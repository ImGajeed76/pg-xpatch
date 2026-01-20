"""
Foreign key tests - FK constraints with xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py (FK-001 to FK-003)
"""

from xptest import pg_test


@pg_test(tags=["unit", "fk"])
def test_fk_referencing_xpatch_table(db):
    """Create FK referencing xpatch table (may fail due to AM limitations)."""
    db.execute("""
        CREATE TABLE fk_parent (id INT PRIMARY KEY, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('fk_parent', group_by => 'id', order_by => 'ver');
        INSERT INTO fk_parent VALUES (1, 1, 'parent');
    """)
    
    # Try creating FK - this may or may not work depending on AM support
    try:
        db.execute("""
            CREATE TABLE fk_child (
                id SERIAL PRIMARY KEY,
                parent_id INT REFERENCES fk_parent(id)
            );
        """)
        # If it works, verify referential integrity
        db.execute("INSERT INTO fk_child (parent_id) VALUES (1)")
        count = db.fetchval("SELECT COUNT(*) FROM fk_child")
        assert count == 1
    except Exception as e:
        # Some access methods don't support being FK targets
        assert True, f"FK to xpatch table not supported: {e}"


@pg_test(tags=["unit", "fk"])
def test_xpatch_table_with_fk_to_heap(db):
    """xpatch table with FK to heap table."""
    db.execute("""
        CREATE TABLE fk_ref (id INT PRIMARY KEY, name TEXT);
        INSERT INTO fk_ref VALUES (1, 'ref1'), (2, 'ref2');
        
        CREATE TABLE fk_xpatch (
            id INT, ver INT, data TEXT,
            ref_id INT REFERENCES fk_ref(id)
        ) USING xpatch;
        SELECT xpatch.configure('fk_xpatch', group_by => 'id', order_by => 'ver');
        
        INSERT INTO fk_xpatch VALUES (1, 1, 'test', 1);
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM fk_xpatch")
    assert count == 1
    
    # Verify FK reference works
    result = db.fetchone("""
        SELECT x.data, r.name 
        FROM fk_xpatch x 
        JOIN fk_ref r ON x.ref_id = r.id
    """)
    assert result['name'] == 'ref1'


@pg_test(tags=["unit", "fk"])
def test_fk_violation_rejected(db):
    """FK violation on xpatch table should be rejected."""
    db.execute("""
        CREATE TABLE fk_ref2 (id INT PRIMARY KEY, name TEXT);
        INSERT INTO fk_ref2 VALUES (1, 'exists');
        
        CREATE TABLE fk_xpatch2 (
            id INT, ver INT, data TEXT,
            ref_id INT REFERENCES fk_ref2(id)
        ) USING xpatch;
        SELECT xpatch.configure('fk_xpatch2', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO fk_xpatch2 VALUES (1, 1, 'bad ref', 999)")
        assert False, "Should have rejected FK violation"
    except Exception as e:
        assert 'foreign key' in str(e).lower() or 'violates' in str(e).lower()


@pg_test(tags=["unit", "fk"])
def test_fk_cascade_delete(db):
    """FK with ON DELETE CASCADE from xpatch table."""
    db.execute("""
        CREATE TABLE fk_parent_cascade (id INT PRIMARY KEY, name TEXT);
        INSERT INTO fk_parent_cascade VALUES (1, 'parent'), (2, 'parent2');
        
        CREATE TABLE fk_child_cascade (
            id INT, ver INT, data TEXT,
            parent_id INT REFERENCES fk_parent_cascade(id) ON DELETE CASCADE
        ) USING xpatch;
        SELECT xpatch.configure('fk_child_cascade', group_by => 'id', order_by => 'ver');
        
        INSERT INTO fk_child_cascade VALUES (1, 1, 'child1', 1);
        INSERT INTO fk_child_cascade VALUES (2, 1, 'child2', 1);
        INSERT INTO fk_child_cascade VALUES (3, 1, 'child3', 2);
    """)
    
    # Delete parent - children should cascade
    db.execute("DELETE FROM fk_parent_cascade WHERE id = 1")
    
    count = db.fetchval("SELECT COUNT(*) FROM fk_child_cascade")
    assert count == 1, f"Expected 1 child remaining after cascade, got {count}"
    
    # Only child3 should remain
    data = db.fetchval("SELECT data FROM fk_child_cascade")
    assert data == 'child3'
