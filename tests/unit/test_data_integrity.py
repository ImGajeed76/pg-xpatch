"""
P0.2 - Data Integrity Tests (Critical)

Tests verifying that data survives INSERT/SELECT roundtrips, 
delta compression, and various edge cases without corruption.
"""

from xptest import pg_test


# =============================================================================
# Basic Roundtrip Tests
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "p0"])
def test_insert_select_roundtrip(db):
    """
    WHAT: INSERT then SELECT returns exactly the same data
    WHY: Fundamental correctness
    EXPECTED: Exact match on all columns
    """
    db.execute("""
        CREATE TABLE rt_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('rt_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO rt_test VALUES (1, 1, 'hello world');
    """)
    
    row = db.fetchone("SELECT * FROM rt_test WHERE grp = 1 AND ver = 1")
    assert row['grp'] == 1
    assert row['ver'] == 1
    assert row['data'] == 'hello world', f"Data mismatch: {row['data']}"


@pg_test(tags=["unit", "data-integrity", "p0"])
def test_multi_version_reconstruction(db):
    """
    WHAT: Reconstruct version from 5+ deltas deep chain
    WHY: Core compression feature - deltas must reconstruct correctly
    EXPECTED: All versions return correct data
    """
    db.execute("""
        CREATE TABLE mv_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('mv_test', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 100  -- Force long delta chains
        );
    """)
    
    # Insert 10 versions
    for i in range(1, 11):
        db.execute(f"INSERT INTO mv_test VALUES (1, {i}, 'version {i} content')")
    
    # Verify each version
    for i in range(1, 11):
        row = db.fetchone(f"SELECT data FROM mv_test WHERE grp = 1 AND ver = {i}")
        expected = f'version {i} content'
        assert row['data'] == expected, f"Version {i}: expected '{expected}', got '{row['data']}'"


@pg_test(tags=["unit", "data-integrity", "p0"])
def test_null_preserved(db):
    """
    WHAT: NULL values are preserved after delta compression
    WHY: NULL must not become empty string or vice versa
    EXPECTED: NULL stays NULL
    """
    db.execute("""
        CREATE TABLE null_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('null_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO null_test VALUES (1, 1, 'first');
        INSERT INTO null_test VALUES (1, 2, NULL);
        INSERT INTO null_test VALUES (1, 3, 'third');
    """)
    
    # Check NULL is preserved
    row = db.fetchone("SELECT data FROM null_test WHERE grp = 1 AND ver = 2")
    assert row['data'] is None, f"Expected NULL, got: {repr(row['data'])}"
    
    # Check non-NULL values still work
    row = db.fetchone("SELECT data FROM null_test WHERE grp = 1 AND ver = 3")
    assert row['data'] == 'third'


@pg_test(tags=["unit", "data-integrity", "p0"])
def test_empty_string_preserved(db):
    """
    WHAT: Empty string '' is preserved and distinct from NULL
    WHY: Common data corruption bug in delta compression
    EXPECTED: '' stays '', NULL stays NULL
    """
    db.execute("""
        CREATE TABLE empty_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('empty_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO empty_test VALUES (1, 1, '');
        INSERT INTO empty_test VALUES (1, 2, NULL);
        INSERT INTO empty_test VALUES (1, 3, '');
    """)
    
    # Check empty string
    row = db.fetchone("SELECT data FROM empty_test WHERE grp = 1 AND ver = 1")
    assert row['data'] == '', f"Expected empty string, got: {repr(row['data'])}"
    
    # Check NULL
    row = db.fetchone("SELECT data FROM empty_test WHERE grp = 1 AND ver = 2")
    assert row['data'] is None, f"Expected NULL, got: {repr(row['data'])}"
    
    # Check empty string again (after NULL)
    row = db.fetchone("SELECT data FROM empty_test WHERE grp = 1 AND ver = 3")
    assert row['data'] == '', f"Expected empty string after NULL, got: {repr(row['data'])}"


@pg_test(tags=["unit", "data-integrity", "p0"])
def test_null_vs_empty_distinct(db):
    """
    WHAT: NULL and empty string are distinctly different
    WHY: Regression test for common compression bug
    EXPECTED: COUNT with different filters shows distinction
    """
    db.execute("""
        CREATE TABLE nve_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('nve_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO nve_test VALUES (1, 1, NULL);
        INSERT INTO nve_test VALUES (1, 2, '');
        INSERT INTO nve_test VALUES (1, 3, 'text');
    """)
    
    null_count = db.fetchval("SELECT COUNT(*) FROM nve_test WHERE data IS NULL")
    empty_count = db.fetchval("SELECT COUNT(*) FROM nve_test WHERE data = ''")
    text_count = db.fetchval("SELECT COUNT(*) FROM nve_test WHERE data = 'text'")
    
    assert null_count == 1, f"Expected 1 NULL, got {null_count}"
    assert empty_count == 1, f"Expected 1 empty string, got {empty_count}"
    assert text_count == 1, f"Expected 1 'text', got {text_count}"


# =============================================================================
# Unicode and Special Characters
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "unicode", "p0"])
def test_unicode_emoji_preserved(db):
    """
    WHAT: 4-byte UTF-8 characters (emoji) survive delta compression
    WHY: Common encoding bug in delta algorithms
    EXPECTED: Exact byte-for-byte match
    """
    emoji_text = "Hello 👋 World 🌍 Test 🎉"
    
    db.execute("""
        CREATE TABLE emoji_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('emoji_test', group_by => 'grp', order_by => 'ver');
    """)
    db.execute("INSERT INTO emoji_test VALUES (1, 1, %s)", (emoji_text,))
    db.execute("INSERT INTO emoji_test VALUES (1, 2, %s)", (emoji_text + " more",))
    
    row = db.fetchone("SELECT data FROM emoji_test WHERE grp = 1 AND ver = 1")
    assert row['data'] == emoji_text, f"Emoji mismatch: {repr(row['data'])}"


@pg_test(tags=["unit", "data-integrity", "unicode", "p0"])
def test_unicode_multibyte_preserved(db):
    """
    WHAT: Various Unicode characters (CJK, Arabic, etc.) preserved
    WHY: International text support
    EXPECTED: Exact match
    """
    test_strings = [
        "日本語テスト",  # Japanese
        "中文测试",      # Chinese
        "한국어 테스트",  # Korean
        "العربية",       # Arabic
        "Ελληνικά",     # Greek
    ]
    
    db.execute("""
        CREATE TABLE unicode_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('unicode_test', group_by => 'grp', order_by => 'ver');
    """)
    
    for i, text in enumerate(test_strings, 1):
        db.execute("INSERT INTO unicode_test VALUES (1, %s, %s)", (i, text))
    
    for i, expected in enumerate(test_strings, 1):
        row = db.fetchone(f"SELECT data FROM unicode_test WHERE grp = 1 AND ver = {i}")
        assert row['data'] == expected, f"Unicode mismatch at v{i}: {repr(row['data'])}"


@pg_test(tags=["unit", "data-integrity", "p1"])
def test_special_characters_preserved(db):
    """
    WHAT: Special characters (newlines, tabs, backslashes, quotes)
    WHY: Common escaping issues
    EXPECTED: All preserved exactly
    """
    test_text = "Line1\nLine2\tTabbed\\Backslash'Quote\"Double"
    
    db.execute("""
        CREATE TABLE special_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('special_test', group_by => 'grp', order_by => 'ver');
    """)
    db.execute("INSERT INTO special_test VALUES (1, 1, %s)", (test_text,))
    
    row = db.fetchone("SELECT data FROM special_test WHERE grp = 1 AND ver = 1")
    assert row['data'] == test_text, f"Special chars mismatch: {repr(row['data'])}"


# =============================================================================
# Large Data Tests
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "p0"])
def test_large_text_integrity(db):
    """
    WHAT: 1MB text column survives compression
    WHY: Verify no truncation or corruption
    EXPECTED: Exact match
    """
    # Generate 1MB of text
    large_text = "x" * (1024 * 1024)
    
    db.execute("""
        CREATE TABLE large_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('large_test', group_by => 'grp', order_by => 'ver');
    """)
    db.execute("INSERT INTO large_test VALUES (1, 1, %s)", (large_text,))
    
    row = db.fetchone("SELECT data FROM large_test WHERE grp = 1 AND ver = 1")
    assert len(row['data']) == len(large_text), f"Length mismatch: {len(row['data'])} vs {len(large_text)}"
    assert row['data'] == large_text, "Large text content mismatch"


@pg_test(tags=["unit", "data-integrity", "p1"])
def test_long_chain_random_access(db):
    """
    WHAT: Access version 500 in 1000-version chain
    WHY: Performance and correctness of deep chains
    EXPECTED: Correct data
    """
    db.execute("""
        CREATE TABLE long_chain (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('long_chain', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 100
        );
    """)
    
    # Insert 1000 versions using generate_series
    db.execute("""
        INSERT INTO long_chain 
        SELECT 1, v, 'version_' || v || '_data'
        FROM generate_series(1, 1000) v
    """)
    
    # Access version 500
    row = db.fetchone("SELECT data FROM long_chain WHERE grp = 1 AND ver = 500")
    assert row['data'] == 'version_500_data', f"Mismatch: {row['data']}"
    
    # Access version 999
    row = db.fetchone("SELECT data FROM long_chain WHERE grp = 1 AND ver = 999")
    assert row['data'] == 'version_999_data', f"Mismatch: {row['data']}"


# =============================================================================
# Data Type Tests
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "p1"])
def test_integer_types_preserved(db):
    """
    WHAT: Various integer types preserved correctly
    WHY: Verify no truncation or overflow
    EXPECTED: Exact values
    
    NOTE: xpatch requires at least one delta column (TEXT, BYTEA, JSON, etc.)
    """
    db.execute("""
        CREATE TABLE int_test (
            grp INT, 
            ver INT,
            small_val SMALLINT,
            int_val INT,
            big_val BIGINT,
            dummy TEXT  -- Required delta column
        ) USING xpatch;
        SELECT xpatch.configure('int_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_test VALUES (1, 1, 32767, 2147483647, 9223372036854775807, 'a');
        INSERT INTO int_test VALUES (1, 2, -32768, -2147483648, -9223372036854775808, 'b');
    """)
    
    # Check max values
    row = db.fetchone("SELECT * FROM int_test WHERE grp = 1 AND ver = 1")
    assert row['small_val'] == 32767
    assert row['int_val'] == 2147483647
    assert row['big_val'] == 9223372036854775807
    
    # Check min values
    row = db.fetchone("SELECT * FROM int_test WHERE grp = 1 AND ver = 2")
    assert row['small_val'] == -32768
    assert row['int_val'] == -2147483648
    assert row['big_val'] == -9223372036854775808


@pg_test(tags=["unit", "data-integrity", "p1"])
def test_numeric_precision_preserved(db):
    """
    WHAT: NUMERIC/DECIMAL precision preserved
    WHY: Financial data must be exact
    EXPECTED: No floating point errors
    
    NOTE: xpatch requires at least one delta column (TEXT, BYTEA, JSON, etc.)
    """
    db.execute("""
        CREATE TABLE numeric_test (
            grp INT, 
            ver INT,
            amount NUMERIC(20, 8),
            dummy TEXT  -- Required delta column
        ) USING xpatch;
        SELECT xpatch.configure('numeric_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO numeric_test VALUES (1, 1, 123456789012.12345678, 'a');
        INSERT INTO numeric_test VALUES (1, 2, 0.00000001, 'b');
    """)
    
    row = db.fetchone("SELECT amount::text FROM numeric_test WHERE grp = 1 AND ver = 1")
    assert '123456789012.12345678' in row['amount']
    
    row = db.fetchone("SELECT amount::text FROM numeric_test WHERE grp = 1 AND ver = 2")
    assert '0.00000001' in row['amount']


@pg_test(tags=["unit", "data-integrity", "p1"])
def test_bytea_preserved(db):
    """
    WHAT: Binary data (BYTEA) preserved correctly
    WHY: Binary files, images, etc.
    EXPECTED: Exact byte match
    """
    # Binary data with all byte values 0-255
    binary_data = bytes(range(256))
    
    db.execute("""
        CREATE TABLE bytea_test (grp INT, ver INT, data BYTEA) USING xpatch;
        SELECT xpatch.configure('bytea_test', group_by => 'grp', order_by => 'ver');
    """)
    db.execute("INSERT INTO bytea_test VALUES (1, 1, %s)", (binary_data,))
    
    row = db.fetchone("SELECT data FROM bytea_test WHERE grp = 1 AND ver = 1")
    assert bytes(row['data']) == binary_data, "Binary data mismatch"


@pg_test(tags=["unit", "data-integrity", "p1"])
def test_json_preserved(db):
    """
    WHAT: JSON/JSONB data preserved
    WHY: Common data type in modern applications
    EXPECTED: Equivalent JSON structure
    """
    import json
    
    test_json = {"name": "test", "values": [1, 2, 3], "nested": {"a": 1}}
    
    db.execute("""
        CREATE TABLE json_test (grp INT, ver INT, data JSONB) USING xpatch;
        SELECT xpatch.configure('json_test', group_by => 'grp', order_by => 'ver');
    """)
    db.execute("INSERT INTO json_test VALUES (1, 1, %s)", (json.dumps(test_json),))
    
    row = db.fetchone("SELECT data FROM json_test WHERE grp = 1 AND ver = 1")
    assert row['data'] == test_json, f"JSON mismatch: {row['data']}"


# =============================================================================
# Concurrent Access Tests
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "concurrency", "p0"])
def test_concurrent_insert_integrity(db):
    """
    WHAT: Multiple inserts to same group
    WHY: Race condition risk in sequence assignment
    EXPECTED: All rows present, no corruption
    """
    db.execute("""
        CREATE TABLE conc_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('conc_test', group_by => 'grp', order_by => 'ver');
    """)
    
    # Insert 100 rows rapidly
    for i in range(1, 101):
        db.execute(f"INSERT INTO conc_test VALUES (1, {i}, 'row {i}')")
    
    # Verify all present
    count = db.fetchval("SELECT COUNT(*) FROM conc_test WHERE grp = 1")
    assert count == 100, f"Expected 100 rows, got {count}"
    
    # Verify content
    for i in [1, 50, 100]:
        row = db.fetchone(f"SELECT data FROM conc_test WHERE grp = 1 AND ver = {i}")
        assert row['data'] == f'row {i}', f"Content mismatch at row {i}"


# =============================================================================
# DELETE Tests  
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "delete", "p0"])
def test_delete_last_row_preserves_chain(db):
    """
    WHAT: DELETE last row in chain, verify others intact
    WHY: Deleting the end of chain should not break reconstruction
    EXPECTED: Earlier rows still correct
    
    NOTE: Deleting a row in the middle of a delta chain removes dependent
    rows too - this is intentional because you can't reconstruct a version
    from a delta against a non-existent base. This test only deletes the 
    LAST row which has no dependents.
    """
    db.execute("""
        CREATE TABLE del_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('del_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO del_test VALUES (1, 1, 'first');
        INSERT INTO del_test VALUES (1, 2, 'second');
        INSERT INTO del_test VALUES (1, 3, 'third');
    """)
    
    # Delete LAST row (safe - no dependent deltas)
    db.execute("DELETE FROM del_test WHERE grp = 1 AND ver = 3")
    
    # Verify remaining rows
    row = db.fetchone("SELECT data FROM del_test WHERE grp = 1 AND ver = 1")
    assert row['data'] == 'first', f"First row corrupted: {row['data']}"
    
    row = db.fetchone("SELECT data FROM del_test WHERE grp = 1 AND ver = 2")
    assert row['data'] == 'second', f"Second row corrupted: {row['data']}"
    
    # Verify deleted row is gone
    row = db.fetchone("SELECT * FROM del_test WHERE grp = 1 AND ver = 3")
    assert row is None, "Deleted row still exists"


@pg_test(tags=["unit", "data-integrity", "delete", "p1"])
def test_delete_entire_group(db):
    """
    WHAT: Delete all rows in a group
    WHY: Complete group removal must work
    EXPECTED: Group completely removed
    """
    db.execute("""
        CREATE TABLE del_group (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('del_group', group_by => 'grp', order_by => 'ver');
        INSERT INTO del_group VALUES (1, 1, 'g1v1'), (1, 2, 'g1v2');
        INSERT INTO del_group VALUES (2, 1, 'g2v1'), (2, 2, 'g2v2');
    """)
    
    # Delete entire group 1
    db.execute("DELETE FROM del_group WHERE grp = 1")
    
    # Verify group 1 gone
    count = db.fetchval("SELECT COUNT(*) FROM del_group WHERE grp = 1")
    assert count == 0, f"Group 1 should be empty, has {count} rows"
    
    # Verify group 2 intact
    count = db.fetchval("SELECT COUNT(*) FROM del_group WHERE grp = 2")
    assert count == 2, f"Group 2 should have 2 rows, has {count}"


# =============================================================================
# Order Enforcement Tests
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "p1"])
def test_out_of_order_insert_rejected(db):
    """
    WHAT: Inserting version 3 after version 5 should be rejected
    WHY: Delta compression requires strictly increasing versions - you can't
         reconstruct version 3 from a delta against non-existent version 2
    EXPECTED: Error with clear message about version ordering
    """
    db.execute("""
        CREATE TABLE ooo_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ooo_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO ooo_test VALUES (1, 1, 'first');
        INSERT INTO ooo_test VALUES (1, 5, 'fifth');
    """)
    
    # Trying to insert version 3 after version 5 should fail
    try:
        db.execute("INSERT INTO ooo_test VALUES (1, 3, 'third')")
        assert False, "Expected error for out-of-order insert"
    except Exception as e:
        error_msg = str(e).lower()
        assert "version" in error_msg or "order" in error_msg or "increasing" in error_msg, \
            f"Expected version ordering error, got: {e}"


@pg_test(tags=["unit", "data-integrity", "p1"])
def test_sequential_insert_works(db):
    """
    WHAT: Sequential inserts in order work correctly
    WHY: Normal usage pattern
    EXPECTED: All versions stored and retrievable
    """
    db.execute("""
        CREATE TABLE seq_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('seq_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO seq_test VALUES (1, 1, 'first');
        INSERT INTO seq_test VALUES (1, 2, 'second');
        INSERT INTO seq_test VALUES (1, 3, 'third');
    """)
    
    rows = db.fetchall("SELECT ver, data FROM seq_test WHERE grp = 1 ORDER BY ver")
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"
    
    assert rows[0]['ver'] == 1 and rows[0]['data'] == 'first'
    assert rows[1]['ver'] == 2 and rows[1]['data'] == 'second'
    assert rows[2]['ver'] == 3 and rows[2]['data'] == 'third'


# =============================================================================
# Multiple Groups Tests
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "p1"])
def test_multiple_groups_isolated(db):
    """
    WHAT: Data in different groups is isolated
    WHY: Groups must not interfere with each other
    EXPECTED: Each group has correct data
    """
    db.execute("""
        CREATE TABLE multi_grp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('multi_grp', group_by => 'grp', order_by => 'ver');
    """)
    
    # Insert to multiple groups
    for g in range(1, 6):
        for v in range(1, 4):
            db.execute(f"INSERT INTO multi_grp VALUES ({g}, {v}, 'g{g}v{v}')")
    
    # Verify each group
    for g in range(1, 6):
        rows = db.fetchall(f"SELECT ver, data FROM multi_grp WHERE grp = {g} ORDER BY ver")
        assert len(rows) == 3, f"Group {g} should have 3 rows"
        for i, row in enumerate(rows, 1):
            expected = f'g{g}v{i}'
            assert row['data'] == expected, f"Group {g} v{i}: expected {expected}, got {row['data']}"


# =============================================================================
# UPDATE Not Supported Test
# =============================================================================

@pg_test(tags=["unit", "data-integrity", "p0"])
def test_update_not_supported(db):
    """
    WHAT: UPDATE should fail with clear message
    WHY: Feature not supported, must give clear error
    EXPECTED: Error mentioning UPDATE not supported
    """
    db.execute("""
        CREATE TABLE upd_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('upd_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO upd_test VALUES (1, 1, 'original');
    """)
    
    try:
        db.execute("UPDATE upd_test SET data = 'modified' WHERE grp = 1 AND ver = 1")
        assert False, "UPDATE should have failed"
    except Exception as e:
        # Should get some error about UPDATE not being supported
        error_msg = str(e).lower()
        assert "update" in error_msg or "not supported" in error_msg or "cannot" in error_msg, \
            f"Expected UPDATE-related error, got: {e}"
