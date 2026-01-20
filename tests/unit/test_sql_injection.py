"""
P0.3 - SQL Injection Security Tests

Tests verifying that all xpatch API functions properly escape/validate 
parameters to prevent SQL injection attacks.
"""

from xptest import pg_test


# ============================================================================
# SQL Injection in configure() parameters
# ============================================================================

@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_group_by_param(db):
    """configure(group_by => "'; DROP TABLE--") should not execute injection."""
    db.execute("CREATE TABLE inj_grp (grp INT, ver INT, data TEXT) USING xpatch")
    
    # Attempt injection through group_by parameter
    injection = "'; DROP TABLE inj_grp; --"
    try:
        db.execute(f"SELECT xpatch.configure('inj_grp', group_by => '{injection}')")
    except Exception:
        db.rollback()  # Reset transaction state
    
    # Verify table still exists (injection did not execute)
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'inj_grp')")
    assert result['exists'], "Table was dropped - SQL injection succeeded!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_order_by_param(db):
    """configure(order_by => "'; DELETE FROM--") should not execute injection."""
    db.execute("""
        CREATE TABLE inj_ord (grp INT, ver INT, data TEXT) USING xpatch;
        INSERT INTO inj_ord VALUES (1, 1, 'test data');
    """)
    
    injection = "'; DELETE FROM inj_ord; --"
    try:
        db.execute(f"SELECT xpatch.configure('inj_ord', group_by => 'grp', order_by => '{injection}')")
    except Exception:
        db.rollback()
    
    # Verify data still exists
    count = db.fetchone("SELECT COUNT(*) as cnt FROM inj_ord")
    assert count['cnt'] == 1, "Data was deleted - SQL injection succeeded!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_delta_columns_array(db):
    """delta_columns => ARRAY["'; DROP TABLE--"] should not execute injection."""
    db.execute("CREATE TABLE inj_delta (grp INT, ver INT, data TEXT, content TEXT) USING xpatch")
    
    injection = "'; DROP TABLE inj_delta; --"
    try:
        db.execute(f"SELECT xpatch.configure('inj_delta', group_by => 'grp', delta_columns => ARRAY['{injection}'])")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'inj_delta')")
    assert result['exists'], "Table was dropped via delta_columns injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_configure(db):
    """configure('evil; DROP TABLE--') should not execute injection."""
    db.execute("CREATE TABLE safe_table (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT xpatch.configure('safe_table; DROP TABLE safe_table; --', group_by => 'grp')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'safe_table')")
    assert result['exists'], "Table was dropped via table name injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_warm_cache(db):
    """warm_cache('evil; DROP--') should not execute injection."""
    db.execute("""
        CREATE TABLE warm_safe (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('warm_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    try:
        db.execute("SELECT * FROM xpatch.warm_cache('warm_safe; DROP TABLE warm_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'warm_safe')")
    assert result['exists'], "Table was dropped via warm_cache injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_stats(db):
    """stats('evil; DROP--') should not execute injection."""
    db.execute("CREATE TABLE stats_safe (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT * FROM xpatch.stats('stats_safe; DROP TABLE stats_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'stats_safe')")
    assert result['exists'], "Table was dropped via stats injection!"


# ============================================================================
# Unicode and encoding-based injection attempts
# ============================================================================

@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_unicode_escape(db):
    """Unicode escape sequences (like \\u0027 for ') should be handled safely."""
    db.execute("CREATE TABLE inj_unicode (grp INT, ver INT, data TEXT) USING xpatch")
    
    # Various unicode quote representations
    injections = [
        "test\u0027; DROP TABLE inj_unicode; --",  # Unicode apostrophe
        "test\u2019; DROP TABLE inj_unicode; --",  # Right single quotation mark
        "test\u02bc; DROP TABLE inj_unicode; --",  # Modifier letter apostrophe
    ]
    
    for injection in injections:
        try:
            # Escape single quotes for SQL
            escaped = injection.replace("'", "''")
            db.execute(f"SELECT xpatch.configure('inj_unicode', group_by => '{escaped}')")
        except Exception:
            db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'inj_unicode')")
    assert result['exists'], "Table was dropped via unicode injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_comment_sequences(db):
    """SQL comment sequences (/* */ or --) should be handled safely."""
    db.execute("CREATE TABLE inj_comment (grp INT, ver INT, data TEXT) USING xpatch")
    
    injections = [
        "grp /* DROP TABLE inj_comment */",
        "grp */ DROP TABLE inj_comment /*",
        "grp -- DROP TABLE inj_comment",
        "grp /**/; DROP TABLE inj_comment; --",
    ]
    
    for injection in injections:
        try:
            escaped = injection.replace("'", "''")
            db.execute(f"SELECT xpatch.configure('inj_comment', group_by => '{escaped}')")
        except Exception:
            db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'inj_comment')")
    assert result['exists'], "Table was dropped via comment injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_nested_quotes(db):
    """Complex nested quote patterns should be handled safely."""
    db.execute("CREATE TABLE inj_nested (grp INT, ver INT, data TEXT) USING xpatch")
    
    injections = [
        "test'; DROP TABLE inj_nested; SELECT '",
        "test''; DROP TABLE inj_nested; --",
        "test\\'; DROP TABLE inj_nested; --",
        "test'''; DROP TABLE inj_nested; --",
        "$$; DROP TABLE inj_nested; $$",
        "$tag$; DROP TABLE inj_nested; $tag$",
    ]
    
    for injection in injections:
        try:
            escaped = injection.replace("'", "''")
            db.execute(f"SELECT xpatch.configure('inj_nested', group_by => '{escaped}')")
        except Exception:
            db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'inj_nested')")
    assert result['exists'], "Table was dropped via nested quote injection!"


# ============================================================================
# Additional injection vectors
# ============================================================================

@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_describe(db):
    """describe() with injection in table name should not execute."""
    db.execute("CREATE TABLE desc_safe (grp INT, ver INT, data TEXT) USING xpatch")
    
    try:
        db.execute("SELECT * FROM xpatch.describe('desc_safe; DROP TABLE desc_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'desc_safe')")
    assert result['exists'], "Table was dropped via describe injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_inspect(db):
    """inspect() with injection in table name should not execute."""
    db.execute("""
        CREATE TABLE inspect_safe (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inspect_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    try:
        db.execute("SELECT * FROM xpatch.inspect('inspect_safe; DROP TABLE inspect_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'inspect_safe')")
    assert result['exists'], "Table was dropped via inspect injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_physical(db):
    """physical() with injection in table name should not execute."""
    db.execute("""
        CREATE TABLE physical_safe (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('physical_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    try:
        db.execute("SELECT * FROM xpatch.physical('physical_safe; DROP TABLE physical_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'physical_safe')")
    assert result['exists'], "Table was dropped via physical injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_cache_stats(db):
    """cache_stats() with injection in table name should not execute."""
    db.execute("""
        CREATE TABLE cache_safe (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cache_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    try:
        db.execute("SELECT * FROM xpatch.cache_stats('cache_safe; DROP TABLE cache_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'cache_safe')")
    assert result['exists'], "Table was dropped via cache_stats injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_inject_table_name_in_get_config(db):
    """get_config() with injection in table name should not execute."""
    db.execute("""
        CREATE TABLE config_safe (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('config_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    try:
        db.execute("SELECT * FROM xpatch.get_config('config_safe; DROP TABLE config_safe; --')")
    except Exception:
        db.rollback()
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'config_safe')")
    assert result['exists'], "Table was dropped via get_config injection!"


# ============================================================================
# Data storage safety
# ============================================================================

@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_malicious_data_stored_safely(db):
    """Malicious text data should be stored safely without execution."""
    db.execute("""
        CREATE TABLE data_safe (grp INT, ver INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('data_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    injections = [
        "'; DROP TABLE data_safe; --",
        "'); DROP TABLE data_safe; --",
        "1); DROP TABLE data_safe; --",
        "1 OR 1=1; DROP TABLE data_safe; --",
    ]
    
    for i, injection in enumerate(injections):
        escaped = injection.replace("'", "''")
        db.execute(f"INSERT INTO data_safe (grp, ver, content) VALUES ({i}, 1, '{escaped}')")
    
    # Verify all data was stored and table exists
    count = db.fetchone("SELECT COUNT(*) as cnt FROM data_safe")
    assert count['cnt'] == len(injections), "Not all rows were inserted"
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'data_safe')")
    assert result['exists'], "Table was dropped via data injection!"


@pg_test(tags=["unit", "security", "sql-injection", "p0"])
def test_malicious_json_stored_safely(db):
    """Malicious JSON data should be stored safely without execution."""
    db.execute("""
        CREATE TABLE json_safe (grp INT, ver INT, data JSONB) USING xpatch;
        SELECT xpatch.configure('json_safe', group_by => 'grp', order_by => 'ver');
    """)
    
    # JSON with SQL-like content - need to escape for JSON and SQL
    malicious_json = '{"key": "''; DROP TABLE json_safe; --"}'
    db.execute(f"INSERT INTO json_safe (grp, ver, data) VALUES (1, 1, '{malicious_json}'::jsonb)")
    
    result = db.fetchone("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'json_safe')")
    assert result['exists'], "Table was dropped via JSON injection!"
    
    # Verify data was stored
    stored = db.fetchone("SELECT data FROM json_safe WHERE grp = 1")
    assert stored is not None, "JSON data was not stored"
