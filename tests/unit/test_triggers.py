"""
Trigger tests - Triggers on xpatch tables.

Ported from tmp/stress_test/edge_case_tests.py
"""

from xptest import pg_test


@pg_test(tags=["unit", "triggers"])
def test_after_insert_trigger(db):
    """AFTER INSERT trigger on xpatch table."""
    db.execute("""
        CREATE TABLE trig_log (action TEXT, ts TIMESTAMP DEFAULT now());
        
        CREATE TABLE trig_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('trig_test', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION log_insert() RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO trig_log (action) VALUES ('INSERT id=' || NEW.id || ' ver=' || NEW.ver);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trig_after_insert AFTER INSERT ON trig_test
        FOR EACH ROW EXECUTE FUNCTION log_insert();
        
        INSERT INTO trig_test VALUES (1, 1, 'test');
    """)
    
    log_entry = db.fetchone("SELECT action FROM trig_log ORDER BY ts DESC LIMIT 1")
    
    assert log_entry is not None, "Trigger did not fire"
    assert 'INSERT id=1' in log_entry['action'], f"Expected insert log, got '{log_entry['action']}'"


@pg_test(tags=["unit", "triggers"])
def test_after_delete_trigger(db):
    """AFTER DELETE trigger on xpatch table."""
    db.execute("""
        CREATE TABLE del_log (action TEXT, ts TIMESTAMP DEFAULT now());
        
        CREATE TABLE del_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('del_test', group_by => 'id', order_by => 'ver');
        INSERT INTO del_test VALUES (1, 1, 'to delete');
        
        CREATE OR REPLACE FUNCTION log_delete() RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO del_log (action) VALUES ('DELETE id=' || OLD.id);
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trig_after_delete AFTER DELETE ON del_test
        FOR EACH ROW EXECUTE FUNCTION log_delete();
        
        DELETE FROM del_test WHERE id = 1;
    """)
    
    log_entry = db.fetchone("SELECT action FROM del_log")
    
    assert log_entry is not None, "Delete trigger did not fire"
    assert 'DELETE id=1' in log_entry['action'], f"Expected delete log, got '{log_entry['action']}'"


@pg_test(tags=["unit", "triggers"])
def test_before_insert_trigger_modifies_data(db):
    """BEFORE INSERT trigger that modifies NEW values."""
    db.execute("""
        CREATE TABLE mod_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('mod_test', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION modify_insert() RETURNS TRIGGER AS $$
        BEGIN
            NEW.data := upper(NEW.data);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trig_before_insert BEFORE INSERT ON mod_test
        FOR EACH ROW EXECUTE FUNCTION modify_insert();
        
        INSERT INTO mod_test VALUES (1, 1, 'lowercase');
    """)
    
    result = db.fetchval("SELECT data FROM mod_test WHERE ver = 1")
    
    assert result == 'LOWERCASE', f"Expected 'LOWERCASE', got '{result}'"


@pg_test(tags=["unit", "triggers"])
def test_statement_level_trigger(db):
    """Statement-level trigger on xpatch table."""
    db.execute("""
        CREATE TABLE stmt_log (action TEXT);
        
        CREATE TABLE stmt_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stmt_test', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION log_statement() RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO stmt_log (action) VALUES ('STATEMENT ' || TG_OP);
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trig_statement AFTER INSERT ON stmt_test
        FOR EACH STATEMENT EXECUTE FUNCTION log_statement();
        
        INSERT INTO stmt_test VALUES (1, 1, 'a'), (1, 2, 'b');
    """)
    
    # Should only have one log entry (statement-level, not row-level)
    count = db.fetchval("SELECT COUNT(*) FROM stmt_log WHERE action LIKE 'STATEMENT%'")
    
    assert count == 1, f"Expected 1 statement trigger entry, got {count}"


@pg_test(tags=["unit", "triggers"])
def test_trigger_with_conditional_logic(db):
    """Trigger with conditional logic based on data."""
    db.execute("""
        CREATE TABLE cond_log (message TEXT);
        
        CREATE TABLE cond_test (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cond_test', group_by => 'id', order_by => 'ver');
        
        CREATE OR REPLACE FUNCTION conditional_trigger() RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.ver > 5 THEN
                INSERT INTO cond_log (message) VALUES ('Version > 5 detected');
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        
        CREATE TRIGGER trig_conditional AFTER INSERT ON cond_test
        FOR EACH ROW EXECUTE FUNCTION conditional_trigger();
        
        INSERT INTO cond_test VALUES (1, 3, 'low version');
        INSERT INTO cond_test VALUES (1, 6, 'high version');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM cond_log")
    
    assert count == 1, f"Expected 1 conditional log entry, got {count}"
