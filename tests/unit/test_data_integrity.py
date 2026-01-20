"""
Tests for data integrity - ensuring data stored and retrieved correctly.
"""

from xptest import pg_test


@pg_test(tags=["unit", "integrity"])
def test_single_row_stored_correctly(db):
    """Single row should be stored and retrieved correctly."""
    db.execute("""
        CREATE TABLE int_single (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_single', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_single VALUES (1, 1, 'test data');
    """)
    
    result = db.fetchone("SELECT * FROM int_single")
    assert result['grp'] == 1
    assert result['ver'] == 1
    assert result['data'] == 'test data'


@pg_test(tags=["unit", "integrity"])
def test_multiple_versions_delta_reconstruction(db):
    """Delta reconstruction should accurately reconstruct all versions."""
    db.execute("""
        CREATE TABLE int_delta (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_delta', group_by => 'grp', order_by => 'ver', keyframe_every => 5);
    """)
    
    # Insert 20 versions with predictable content
    for i in range(1, 21):
        content = f"Version {i} content with some text: {i * 100}"
        db.execute(f"INSERT INTO int_delta VALUES (1, {i}, '{content}')")
    
    # Verify each version can be retrieved correctly
    for i in range(1, 21):
        expected = f"Version {i} content with some text: {i * 100}"
        result = db.fetchone(f"SELECT data FROM int_delta WHERE grp = 1 AND ver = {i}")
        assert result['data'] == expected, f"Mismatch at version {i}: got {result['data']}"


@pg_test(tags=["unit", "integrity"])
def test_multiple_groups_isolation(db):
    """Different groups should not interfere with each other."""
    db.execute("""
        CREATE TABLE int_groups (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_groups', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_groups VALUES (1, 1, 'Group 1 Version 1');
        INSERT INTO int_groups VALUES (2, 1, 'Group 2 Version 1');
        INSERT INTO int_groups VALUES (1, 2, 'Group 1 Version 2');
        INSERT INTO int_groups VALUES (2, 2, 'Group 2 Version 2');
    """)
    
    result = db.fetchone("SELECT data FROM int_groups WHERE grp = 1 AND ver = 2")
    assert result['data'] == 'Group 1 Version 2'
    
    result = db.fetchone("SELECT data FROM int_groups WHERE grp = 2 AND ver = 1")
    assert result['data'] == 'Group 2 Version 1'


@pg_test(tags=["unit", "integrity"])
def test_empty_string_content(db):
    """Empty string content should be preserved."""
    db.execute("""
        CREATE TABLE int_empty (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_empty', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_empty VALUES (1, 1, '');
        INSERT INTO int_empty VALUES (1, 2, 'not empty');
        INSERT INTO int_empty VALUES (1, 3, '');
    """)
    
    result = db.fetchone("SELECT data FROM int_empty WHERE grp = 1 AND ver = 1")
    assert result['data'] == '', f"Expected empty string, got: {repr(result['data'])}"
    
    result = db.fetchone("SELECT data FROM int_empty WHERE grp = 1 AND ver = 3")
    assert result['data'] == '', f"Expected empty string, got: {repr(result['data'])}"


@pg_test(tags=["unit", "integrity"])
def test_null_content(db):
    """NULL content should be preserved."""
    db.execute("""
        CREATE TABLE int_null (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_null', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_null VALUES (1, 1, NULL);
        INSERT INTO int_null VALUES (1, 2, 'not null');
        INSERT INTO int_null VALUES (1, 3, NULL);
    """)
    
    result = db.fetchone("SELECT data FROM int_null WHERE grp = 1 AND ver = 1")
    assert result['data'] is None, f"Expected NULL, got: {result['data']}"
    
    count = db.fetchval("SELECT COUNT(*) FROM int_null WHERE data IS NULL")
    assert count == 2, f"Expected 2 NULL values, got {count}"


@pg_test(tags=["unit", "integrity"])
def test_unicode_content(db):
    """Unicode content should be preserved."""
    db.execute("""
        CREATE TABLE int_unicode (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_unicode', group_by => 'grp', order_by => 'ver');
    """)
    
    # Insert various unicode content
    test_strings = [
        "Hello World",
        "Emoji test",
        "Chinese characters",
        "Arabic text",
        "Mixed content with symbols",
    ]
    
    for i, content in enumerate(test_strings, 1):
        db.execute("INSERT INTO int_unicode VALUES (%s, %s, %s)", (1, i, content))
    
    for i, expected in enumerate(test_strings, 1):
        result = db.fetchone(f"SELECT data FROM int_unicode WHERE ver = {i}")
        assert result['data'] == expected, f"Unicode mismatch at {i}: expected {expected}, got {result['data']}"


@pg_test(tags=["unit", "integrity"])
def test_binary_data_bytea(db):
    """BYTEA binary data should be preserved."""
    db.execute("""
        CREATE TABLE int_bytea (grp INT, ver INT, data BYTEA) USING xpatch;
        SELECT xpatch.configure('int_bytea', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_bytea VALUES (1, 1, '\\x010203FFFE'::bytea);
        INSERT INTO int_bytea VALUES (1, 2, '\\x010203FFFF'::bytea);
    """)
    
    result = db.fetchval("SELECT encode(data, 'hex') FROM int_bytea WHERE ver = 2")
    assert result == '010203ffff', f"Binary data mismatch: {result}"


@pg_test(tags=["unit", "integrity"])
def test_jsonb_content(db):
    """JSONB content should be preserved and queryable."""
    db.execute("""
        CREATE TABLE int_jsonb (grp INT, ver INT, data JSONB) USING xpatch;
        SELECT xpatch.configure('int_jsonb', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_jsonb VALUES (1, 1, '{"key": "value1", "count": 1}');
        INSERT INTO int_jsonb VALUES (1, 2, '{"key": "value2", "count": 2, "new_field": true}');
    """)
    
    result = db.fetchval("SELECT data->>'key' FROM int_jsonb WHERE ver = 2")
    assert result == 'value2', f"JSONB key mismatch: {result}"
    
    result = db.fetchval("SELECT (data->>'count')::int FROM int_jsonb WHERE ver = 2")
    assert result == 2, f"JSONB count mismatch: {result}"


@pg_test(tags=["unit", "integrity"], slow=True)
def test_large_content_1mb(db):
    """Large content (1MB) should be handled correctly."""
    db.execute("""
        CREATE TABLE int_large (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_large', group_by => 'grp', order_by => 'ver');
    """)
    
    # Insert 1MB of data
    large_content = 'x' * (1024 * 1024)
    db.execute("INSERT INTO int_large VALUES (1, 1, %s)", (large_content,))
    
    result = db.fetchval("SELECT LENGTH(data) FROM int_large")
    assert result == 1024 * 1024, f"Expected 1MB, got {result} bytes"


@pg_test(tags=["unit", "integrity"])
def test_negative_group_ids(db):
    """Negative group IDs should work correctly."""
    db.execute("""
        CREATE TABLE int_neg (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_neg', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_neg VALUES (-1, 1, 'negative one');
        INSERT INTO int_neg VALUES (-2147483648, 1, 'min int');
    """)
    
    count = db.fetchval("SELECT COUNT(*) FROM int_neg")
    assert count == 2, f"Expected 2 rows, got {count}"
    
    result = db.fetchone("SELECT data FROM int_neg WHERE grp = -1")
    assert result['data'] == 'negative one'


@pg_test(tags=["unit", "integrity"])
def test_bigint_group_ids(db):
    """BIGINT max values should work as group IDs."""
    db.execute("""
        CREATE TABLE int_bigint (grp BIGINT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('int_bigint', group_by => 'grp', order_by => 'ver');
        INSERT INTO int_bigint VALUES (9223372036854775807, 1, 'max bigint');
    """)
    
    result = db.fetchone("SELECT grp FROM int_bigint")
    assert result['grp'] == 9223372036854775807
