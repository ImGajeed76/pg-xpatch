"""
Listen/Notify tests - PostgreSQL pub/sub with xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py (NOTIFY-001)
"""

from xptest import pg_test


@pg_test(tags=["unit", "notify"])
def test_trigger_with_pg_notify(db):
    """Trigger that sends pg_notify on xpatch table insert."""
    db.execute("""
        CREATE TABLE notify_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('notify_test', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION notify_insert() RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('xpatch_changes', 'inserted ' || NEW.id);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER notify_trig AFTER INSERT ON notify_test
        FOR EACH ROW EXECUTE FUNCTION notify_insert();
    """)
    
    # Insert should trigger notification (we can't easily verify receipt in same connection)
    db.execute("INSERT INTO notify_test VALUES (1, 1, 'test')")
    
    count = db.fetchval("SELECT COUNT(*) FROM notify_test")
    assert count == 1, "Insert should succeed even with notify trigger"


@pg_test(tags=["unit", "notify"])
def test_notify_payload_with_xpatch_data(db):
    """pg_notify with payload containing xpatch row data."""
    db.execute("""
        CREATE TABLE notify_payload (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('notify_payload', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION notify_with_data() RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('xpatch_data', 
                json_build_object(
                    'id', NEW.id,
                    'ver', NEW.ver,
                    'data', NEW.data
                )::text
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER notify_data_trig AFTER INSERT ON notify_payload
        FOR EACH ROW EXECUTE FUNCTION notify_with_data();
        
        INSERT INTO notify_payload VALUES (42, 1, 'important data');
    """)
    
    data = db.fetchval("SELECT data FROM notify_payload WHERE id = 42")
    assert data == 'important data'


@pg_test(tags=["unit", "notify"])
def test_notify_on_delete(db):
    """pg_notify on DELETE from xpatch table."""
    db.execute("""
        CREATE TABLE notify_delete (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('notify_delete', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION notify_on_delete() RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('xpatch_deleted', 'deleted group ' || OLD.id);
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER notify_del_trig AFTER DELETE ON notify_delete
        FOR EACH ROW EXECUTE FUNCTION notify_on_delete();
        
        INSERT INTO notify_delete VALUES (1, 1, 'will be deleted');
        INSERT INTO notify_delete VALUES (1, 2, 'also deleted');
        INSERT INTO notify_delete VALUES (2, 1, 'keeper');
    """)
    
    db.execute("DELETE FROM notify_delete WHERE id = 1")
    
    count = db.fetchval("SELECT COUNT(*) FROM notify_delete")
    assert count == 1, f"Expected 1 row after delete, got {count}"
