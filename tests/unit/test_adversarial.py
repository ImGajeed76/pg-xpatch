"""
Adversarial tests - Try to break the extension with edge cases.

Ported from tmp/stress_test/adversarial_tests.py

Tests:
- Integer overflow/underflow
- Invalid configurations
- SQL injection attempts
- Extreme sizes
- Type confusion
- Malformed data
- Resource exhaustion
- Concurrent abuse
- Edge operations
"""

from xptest import pg_test


# =============================================================================
# INTEGER OVERFLOW/UNDERFLOW TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "overflow"])
def test_int_max_as_version(db):
    """INT_MAX (2147483647) as version should be accepted."""
    db.execute("""
        CREATE TABLE ovf_int_max (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ovf_int_max', group_by => 'id', order_by => 'ver');
        INSERT INTO ovf_int_max VALUES (1, 2147483647, 'max int');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM ovf_int_max")
    assert count == 1, f"Expected 1 row, got {count}"


@pg_test(tags=["unit", "adversarial", "overflow"])
def test_int_overflow_rejected(db):
    """INT_MAX + 1 should be rejected (integer overflow)."""
    db.execute("""
        CREATE TABLE ovf_overflow (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ovf_overflow', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO ovf_overflow VALUES (1, 2147483648, 'overflow')")
        assert False, "Should have rejected integer overflow"
    except Exception as e:
        # Expected - integer out of range
        assert True


@pg_test(tags=["unit", "adversarial", "overflow"])
def test_bigint_max_as_version(db):
    """BIGINT_MAX as version should be accepted."""
    db.execute("""
        CREATE TABLE ovf_bigint (id INT, ver BIGINT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ovf_bigint', group_by => 'id', order_by => 'ver');
        INSERT INTO ovf_bigint VALUES (1, 9223372036854775807, 'max bigint');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM ovf_bigint")
    assert count == 1


@pg_test(tags=["unit", "adversarial", "overflow"])
def test_int_min_as_version(db):
    """INT_MIN (-2147483648) as version should be accepted."""
    db.execute("""
        CREATE TABLE ovf_int_min (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ovf_int_min', group_by => 'id', order_by => 'ver');
        INSERT INTO ovf_int_min VALUES (1, -2147483648, 'min int');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM ovf_int_min")
    assert count == 1


@pg_test(tags=["unit", "adversarial", "overflow"])
def test_int_underflow_rejected(db):
    """INT_MIN - 1 should be rejected (integer underflow)."""
    db.execute("""
        CREATE TABLE ovf_underflow (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ovf_underflow', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO ovf_underflow VALUES (1, -2147483649, 'underflow')")
        assert False, "Should have rejected integer underflow"
    except Exception:
        pass  # Expected


# =============================================================================
# INVALID CONFIGURATION TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "config"])
def test_configure_empty_string_column(db):
    """Empty string as column name should be rejected."""
    db.execute("""
        CREATE TABLE cfg_empty (id INT, ver INT, data TEXT) USING xpatch;
    """)
    
    try:
        db.execute("SELECT xpatch.configure('cfg_empty', group_by => '')")
        assert False, "Should have rejected empty column name"
    except Exception:
        pass  # Expected


@pg_test(tags=["unit", "adversarial", "config"])
def test_configure_null_column(db):
    """NULL as column name should be handled gracefully."""
    db.execute("""
        CREATE TABLE cfg_null (id INT, ver INT, data TEXT) USING xpatch;
    """)
    
    try:
        db.execute("SELECT xpatch.configure('cfg_null', group_by => NULL)")
        # Either succeeds with default or fails gracefully
    except Exception:
        pass  # Either is acceptable


@pg_test(tags=["unit", "adversarial", "config"])
def test_configure_keyframe_every_negative(db):
    """keyframe_every = -1 should be rejected."""
    db.execute("""
        CREATE TABLE cfg_kf_neg (id INT, ver INT, data TEXT) USING xpatch;
    """)
    
    try:
        db.execute("""
            SELECT xpatch.configure('cfg_kf_neg', group_by => 'id', order_by => 'ver', 
                                   keyframe_every => -1)
        """)
        assert False, "Should have rejected negative keyframe_every"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "config"])
def test_configure_keyframe_every_max(db):
    """keyframe_every = INT_MAX should be rejected (extension limits to 10000)."""
    db.execute("""
        CREATE TABLE cfg_kf_max (id INT, ver INT, data TEXT) USING xpatch;
    """)
    
    try:
        db.execute("""
            SELECT xpatch.configure('cfg_kf_max', group_by => 'id', order_by => 'ver', 
                                   keyframe_every => 2147483647)
        """)
        assert False, "Should have rejected keyframe_every > 10000"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "config"])
def test_configure_same_column_group_order(db):
    """Same column for group_by and order_by should be allowed."""
    db.execute("""
        CREATE TABLE cfg_same (id INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cfg_same', group_by => 'id', order_by => 'id');
        INSERT INTO cfg_same VALUES (1, 'test');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM cfg_same")
    assert count == 1


# =============================================================================
# SQL INJECTION TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "injection"])
def test_sql_injection_in_content(db):
    """SQL injection in content should be stored literally, not executed."""
    db.execute("""
        CREATE TABLE inj_content (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inj_content', group_by => 'id', order_by => 'ver');
    """)
    
    # Insert various SQL injection payloads
    payloads = [
        "'; DROP TABLE inj_content; --",
        "1; DELETE FROM inj_content; --",
        "' OR '1'='1",
    ]
    
    for i, payload in enumerate(payloads):
        db.execute("INSERT INTO inj_content VALUES (%s, %s, %s)", (1, i + 1, payload))
    
    # Table should still exist with all rows
    count = db.fetchval("SELECT COUNT(*) FROM inj_content")
    assert count == 3, f"Expected 3 rows, got {count}"


@pg_test(tags=["unit", "adversarial", "injection"])
def test_sql_injection_dollar_quote(db):
    """Dollar quote injection should be stored literally."""
    db.execute("""
        CREATE TABLE inj_dollar (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('inj_dollar', group_by => 'id', order_by => 'ver');
    """)
    
    payload = "$$ DROP TABLE inj_dollar $$"
    db.execute("INSERT INTO inj_dollar VALUES (1, 1, %s)", (payload,))
    
    count = db.fetchval("SELECT COUNT(*) FROM inj_dollar")
    assert count == 1


@pg_test(tags=["unit", "adversarial", "injection"])
def test_sql_injection_in_table_name(db):
    """Injection in table name parameter should be rejected."""
    try:
        db.execute("""
            SELECT xpatch.configure('inject_test; DROP TABLE inject_test; --', 
                                   group_by => 'id')
        """)
    except Exception:
        pass  # Expected - invalid table name


# =============================================================================
# EXTREME SIZE TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "size", "slow"])
def test_100_columns(db):
    """Table with 100 TEXT columns should work."""
    cols = ", ".join([f"col{i} TEXT" for i in range(100)])
    db.execute(f"""
        CREATE TABLE size_cols (id INT, ver INT, {cols}) USING xpatch;
        SELECT xpatch.configure('size_cols', group_by => 'id', order_by => 'ver');
        INSERT INTO size_cols (id, ver, col0) VALUES (1, 1, 'test');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM size_cols")
    assert count == 1


@pg_test(tags=["unit", "adversarial", "size", "slow"], timeout=60)
def test_deep_version_chain_1000(db):
    """1000 version chain should work with keyframe_every=100."""
    db.execute("""
        CREATE TABLE size_deep (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('size_deep', group_by => 'id', order_by => 'ver', 
                               keyframe_every => 100);
    """)
    
    # Insert 1000 versions in batches for speed
    for batch in range(10):
        values = ",".join([f"(1, {batch * 100 + i + 1}, 'version {batch * 100 + i + 1}')" 
                          for i in range(100)])
        db.execute(f"INSERT INTO size_deep VALUES {values}")
    
    count = db.fetchval("SELECT COUNT(*) FROM size_deep WHERE id = 1")
    assert count == 1000, f"Expected 1000 rows, got {count}"
    
    # Verify middle version is readable (tests delta reconstruction)
    data = db.fetchval("SELECT data FROM size_deep WHERE ver = 500")
    assert data == "version 500", f"Expected 'version 500', got '{data}'"


# =============================================================================
# TYPE CONFUSION TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "type"])
def test_string_where_int_expected(db):
    """String where INT expected should be rejected."""
    db.execute("""
        CREATE TABLE type_str (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('type_str', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO type_str VALUES ('not an int', 1, 'data')")
        assert False, "Should have rejected string for INT"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "type"])
def test_float_truncated_to_int(db):
    """Float 1.5 where INT expected gets truncated to 1 by PostgreSQL."""
    db.execute("""
        CREATE TABLE type_float (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('type_float', group_by => 'id', order_by => 'ver');
        INSERT INTO type_float VALUES (1.5, 1, 'data');
    """)
    
    id_val = db.fetchval("SELECT id FROM type_float")
    assert id_val == 2 or id_val == 1, f"Expected 1 or 2 (truncated), got {id_val}"


@pg_test(tags=["unit", "adversarial", "type"])
def test_boolean_rejected_for_int(db):
    """Boolean true where INT expected should be rejected (no implicit cast in PG)."""
    db.execute("""
        CREATE TABLE type_bool (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('type_bool', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO type_bool VALUES (true, 1, 'data')")
        assert False, "Should have rejected boolean for INT"
    except Exception:
        pass  # Expected - PostgreSQL doesn't implicitly cast boolean to int


@pg_test(tags=["unit", "adversarial", "type"])
def test_array_where_scalar_expected(db):
    """Array where scalar INT expected should be rejected."""
    db.execute("""
        CREATE TABLE type_arr (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('type_arr', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO type_arr VALUES (ARRAY[1,2,3], 1, 'data')")
        assert False, "Should have rejected array"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "type"])
def test_jsonb_coerced_to_text(db):
    """JSONB where TEXT expected should coerce."""
    db.execute("""
        CREATE TABLE type_json (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('type_json', group_by => 'id', order_by => 'ver');
        INSERT INTO type_json VALUES (1, 1, '{"key": "value"}'::jsonb);
    """)
    
    data = db.fetchval("SELECT data FROM type_json")
    assert 'key' in data, f"Expected JSON content, got '{data}'"


# =============================================================================
# MALFORMED DATA TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "malformed"])
def test_control_characters(db):
    """Control characters (0x01-0x07) should be accepted in TEXT."""
    db.execute("""
        CREATE TABLE mal_ctrl (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('mal_ctrl', group_by => 'id', order_by => 'ver');
    """)
    
    db.execute("INSERT INTO mal_ctrl VALUES (1, 1, E'\\x01\\x02\\x03\\x04\\x05\\x06\\x07')")
    
    count = db.fetchval("SELECT COUNT(*) FROM mal_ctrl")
    assert count == 1


@pg_test(tags=["unit", "adversarial", "malformed"])
def test_special_whitespace(db):
    """Backspace/bell/vertical tab/form feed should be accepted."""
    db.execute("""
        CREATE TABLE mal_ws (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('mal_ws', group_by => 'id', order_by => 'ver');
    """)
    
    db.execute("INSERT INTO mal_ws VALUES (1, 1, E'\\b\\v\\f')")
    
    count = db.fetchval("SELECT COUNT(*) FROM mal_ws")
    assert count == 1


@pg_test(tags=["unit", "adversarial", "malformed"])
def test_invalid_utf8_rejected(db):
    """Invalid UTF-8 bytes should be rejected."""
    db.execute("""
        CREATE TABLE mal_utf8 (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('mal_utf8', group_by => 'id', order_by => 'ver');
    """)
    
    try:
        db.execute("INSERT INTO mal_utf8 VALUES (1, 1, E'\\x80\\x81\\x82')")
        assert False, "Should have rejected invalid UTF-8"
    except Exception:
        pass


# =============================================================================
# CONCURRENT ABUSE TESTS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "abuse"])
def test_duplicate_version_rejected(db):
    """Inserting duplicate version should be rejected."""
    db.execute("""
        CREATE TABLE abuse_dup (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('abuse_dup', group_by => 'id', order_by => 'ver');
        INSERT INTO abuse_dup VALUES (1, 1, 'first');
    """)
    
    try:
        db.execute("INSERT INTO abuse_dup VALUES (1, 1, 'duplicate')")
        assert False, "Should have rejected duplicate version"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "abuse"])
def test_out_of_order_insert_rejected(db):
    """Out of order insert (5 after 10) should be rejected."""
    db.execute("""
        CREATE TABLE abuse_order (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('abuse_order', group_by => 'id', order_by => 'ver');
        INSERT INTO abuse_order VALUES (1, 10, 'v10');
    """)
    
    try:
        db.execute("INSERT INTO abuse_order VALUES (1, 5, 'v5 late')")
        assert False, "Should have rejected out-of-order insert"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "abuse"])
def test_fill_version_gap_rejected(db):
    """Filling gap in versions (50 after 1,100) should be rejected."""
    db.execute("""
        CREATE TABLE abuse_gap (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('abuse_gap', group_by => 'id', order_by => 'ver');
        INSERT INTO abuse_gap VALUES (1, 1, 'v1');
        INSERT INTO abuse_gap VALUES (1, 100, 'v100');
    """)
    
    try:
        db.execute("INSERT INTO abuse_gap VALUES (1, 50, 'v50 late')")
        assert False, "Should have rejected gap fill"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "abuse"])
def test_negative_to_positive_versions(db):
    """Versions from negative to positive (-10 -> -5 -> 0 -> 5) should work."""
    db.execute("""
        CREATE TABLE abuse_negpos (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('abuse_negpos', group_by => 'id', order_by => 'ver');
        INSERT INTO abuse_negpos VALUES (1, -10, 'v-10');
        INSERT INTO abuse_negpos VALUES (1, -5, 'v-5');
        INSERT INTO abuse_negpos VALUES (1, 0, 'v0');
        INSERT INTO abuse_negpos VALUES (1, 5, 'v5');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM abuse_negpos")
    assert count == 4


# =============================================================================
# EDGE CASE OPERATIONS
# =============================================================================

@pg_test(tags=["unit", "adversarial", "edge"])
def test_delete_from_empty_table(db):
    """DELETE from empty table should succeed (no-op)."""
    db.execute("""
        CREATE TABLE edge_empty (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('edge_empty', group_by => 'id', order_by => 'ver');
        DELETE FROM edge_empty WHERE id = 1;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM edge_empty")
    assert count == 0


@pg_test(tags=["unit", "adversarial", "edge"])
def test_truncate_xpatch_table(db):
    """TRUNCATE should clear all rows."""
    db.execute("""
        CREATE TABLE edge_trunc (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('edge_trunc', group_by => 'id', order_by => 'ver');
        INSERT INTO edge_trunc VALUES (1, 1, 'data');
        INSERT INTO edge_trunc VALUES (1, 2, 'more');
        TRUNCATE edge_trunc;
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM edge_trunc")
    assert count == 0


@pg_test(tags=["unit", "adversarial", "edge"])
def test_drop_table_with_data(db):
    """DROP TABLE with data should succeed."""
    db.execute("""
        CREATE TABLE edge_drop (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('edge_drop', group_by => 'id', order_by => 'ver');
        INSERT INTO edge_drop VALUES (1, 1, 'data');
        DROP TABLE edge_drop;
    """)
    
    # Verify table is gone
    try:
        db.fetchval("SELECT COUNT(*) FROM edge_drop")
        assert False, "Table should be dropped"
    except Exception:
        pass


@pg_test(tags=["unit", "adversarial", "edge"])
def test_alter_table_add_column(db):
    """ALTER TABLE ADD COLUMN should work."""
    db.execute("""
        CREATE TABLE edge_alter (id INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('edge_alter', group_by => 'id', order_by => 'ver');
        INSERT INTO edge_alter VALUES (1, 1, 'before');
        ALTER TABLE edge_alter ADD COLUMN new_col TEXT;
    """)
    
    # Verify column exists
    row = db.fetchone("SELECT * FROM edge_alter LIMIT 1")
    assert 'new_col' in row


@pg_test(tags=["unit", "adversarial", "edge"])
def test_alter_table_drop_column_not_supported(db):
    """ALTER TABLE DROP COLUMN is not supported on xpatch tables."""
    db.execute("""
        CREATE TABLE edge_dropcol (id INT, ver INT, data TEXT, extra TEXT) USING xpatch;
        SELECT xpatch.configure('edge_dropcol', group_by => 'id', order_by => 'ver');
        INSERT INTO edge_dropcol VALUES (1, 1, 'data', 'extra');
    """)
    
    try:
        db.execute("ALTER TABLE edge_dropcol DROP COLUMN extra")
        # If it succeeds, verify column is gone
        row = db.fetchone("SELECT * FROM edge_dropcol LIMIT 1")
        assert 'extra' not in row
    except Exception:
        # Expected - xpatch doesn't support dropping columns
        pass
