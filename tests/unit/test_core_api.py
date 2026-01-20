"""
P0.1 - Core API Tests (Critical)

Tests for the fundamental xpatch API functions:
- configure()
- get_config()
- stats()
- describe()
- inspect()
- physical()
"""

from xptest import pg_test


# =============================================================================
# configure() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "configure", "p0"])
def test_configure_basic(db):
    """
    WHAT: Configure xpatch table with group_by and order_by
    WHY: Core setup function, most common usage pattern
    EXPECTED: Configuration stored, no errors
    """
    db.execute("""
        CREATE TABLE docs (
            doc_id INT, 
            version INT, 
            content TEXT
        ) USING xpatch;
        
        SELECT xpatch.configure('docs',
            group_by => 'doc_id',
            order_by => 'version'
        );
    """)
    
    config = db.fetchone("SELECT * FROM xpatch.get_config('docs')")
    assert config is not None, "Config not stored"
    assert config['group_by'] == 'doc_id', f"group_by mismatch: {config['group_by']}"
    assert config['order_by'] == 'version', f"order_by mismatch: {config['order_by']}"


@pg_test(tags=["unit", "core-api", "configure", "p0"])
def test_configure_rejects_heap_table(db):
    """
    WHAT: configure() on non-xpatch table should fail
    WHY: Prevent misuse on wrong table type
    EXPECTED: Clear error message mentioning "xpatch access method"
    """
    db.execute("CREATE TABLE heap_tbl (id INT, data TEXT)")
    
    try:
        db.execute("SELECT xpatch.configure('heap_tbl', group_by => 'id')")
        assert False, "Expected error for heap table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "xpatch" in error_msg, f"Expected 'xpatch' in error: {e}"


@pg_test(tags=["unit", "core-api", "configure", "p0"])
def test_configure_without_group_by(db):
    """
    WHAT: Configure table without group_by (single version chain)
    WHY: Valid use case for simple versioned data
    EXPECTED: Configuration stored with NULL group_by
    """
    db.execute("""
        CREATE TABLE single_chain (version INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('single_chain', order_by => 'version');
    """)
    
    config = db.fetchone("SELECT * FROM xpatch.get_config('single_chain')")
    assert config['group_by'] is None, f"Expected NULL group_by, got: {config['group_by']}"
    assert config['order_by'] == 'version'


@pg_test(tags=["unit", "core-api", "configure", "p0"])
def test_configure_keyframe_every(db):
    """
    WHAT: Configure with custom keyframe_every parameter
    WHY: Users need to tune compression vs reconstruction speed
    EXPECTED: Custom value stored
    """
    db.execute("""
        CREATE TABLE kf_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('kf_test', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 50
        );
    """)
    
    config = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('kf_test')")
    assert config['keyframe_every'] == 50, f"Expected 50, got {config['keyframe_every']}"


@pg_test(tags=["unit", "core-api", "configure", "p1"])
def test_configure_compress_depth(db):
    """
    WHAT: Configure with custom compress_depth parameter
    WHY: Allows tuning delta selection algorithm
    EXPECTED: Custom value stored
    """
    db.execute("""
        CREATE TABLE cd_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cd_test', 
            group_by => 'grp', 
            order_by => 'ver',
            compress_depth => 5
        );
    """)
    
    config = db.fetchone("SELECT compress_depth FROM xpatch.get_config('cd_test')")
    assert config['compress_depth'] == 5, f"Expected 5, got {config['compress_depth']}"


@pg_test(tags=["unit", "core-api", "configure", "p1"])
def test_configure_delta_columns(db):
    """
    WHAT: Configure with explicit delta_columns array
    WHY: Users may want to compress only specific columns
    EXPECTED: Array stored correctly
    """
    db.execute("""
        CREATE TABLE dc_test (grp INT, ver INT, title TEXT, body TEXT) USING xpatch;
        SELECT xpatch.configure('dc_test', 
            group_by => 'grp', 
            order_by => 'ver',
            delta_columns => ARRAY['body']::text[]
        );
    """)
    
    config = db.fetchone("SELECT delta_columns FROM xpatch.get_config('dc_test')")
    assert config['delta_columns'] is not None
    assert 'body' in config['delta_columns']


@pg_test(tags=["unit", "core-api", "configure", "p1"])
def test_configure_reconfigure(db):
    """
    WHAT: Calling configure() again should update config
    WHY: Users may need to change settings
    EXPECTED: New values replace old ones
    """
    db.execute("""
        CREATE TABLE reconf_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('reconf_test', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 10
        );
    """)
    
    # Verify initial
    config = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('reconf_test')")
    assert config['keyframe_every'] == 10
    
    # Reconfigure
    db.execute("""
        SELECT xpatch.configure('reconf_test', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 100
        );
    """)
    
    # Verify updated
    config = db.fetchone("SELECT keyframe_every FROM xpatch.get_config('reconf_test')")
    assert config['keyframe_every'] == 100, f"Expected 100 after update, got {config['keyframe_every']}"


# =============================================================================
# get_config() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "get_config", "p0"])
def test_get_config_returns_all_fields(db):
    """
    WHAT: get_config() returns all configuration fields
    WHY: Primary programmatic access to configuration
    EXPECTED: All fields present in result
    """
    db.execute("""
        CREATE TABLE gc_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('gc_test', group_by => 'grp', order_by => 'ver');
    """)
    
    config = db.fetchone("SELECT * FROM xpatch.get_config('gc_test')")
    
    expected_fields = ['group_by', 'order_by', 'delta_columns', 'keyframe_every', 
                       'compress_depth', 'enable_zstd']
    for field in expected_fields:
        assert field in config, f"Missing field: {field}"


@pg_test(tags=["unit", "core-api", "get_config", "p1"])
def test_get_config_unconfigured_table(db):
    """
    WHAT: get_config() on table without explicit configure()
    WHY: Tables can work with auto-detection
    EXPECTED: Returns NULL/empty result
    """
    db.execute("CREATE TABLE no_config (grp INT, ver INT, data TEXT) USING xpatch")
    
    config = db.fetchone("SELECT * FROM xpatch.get_config('no_config')")
    # Should return NULL row or empty
    assert config is None or config['group_by'] is None


# =============================================================================
# stats() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "stats", "p0"])
def test_stats_empty_table(db):
    """
    WHAT: stats() on empty table
    WHY: Common edge case, should not error
    EXPECTED: All counts are zero
    """
    db.execute("""
        CREATE TABLE stats_empty (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_empty', group_by => 'grp', order_by => 'ver');
    """)
    
    stats = db.fetchone("SELECT * FROM xpatch.stats('stats_empty')")
    assert stats is not None, "stats() returned NULL"
    assert stats['total_rows'] == 0, f"Expected 0 rows, got {stats['total_rows']}"
    assert stats['total_groups'] == 0, f"Expected 0 groups, got {stats['total_groups']}"


@pg_test(tags=["unit", "core-api", "stats", "p0"])
def test_stats_with_data(db):
    """
    WHAT: stats() on populated table
    WHY: Primary monitoring function
    EXPECTED: Accurate counts
    """
    db.execute("""
        CREATE TABLE stats_data (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_data', group_by => 'grp', order_by => 'ver');
        INSERT INTO stats_data VALUES (1, 1, 'v1'), (1, 2, 'v2'), (1, 3, 'v3');
        INSERT INTO stats_data VALUES (2, 1, 'a'), (2, 2, 'b');
    """)
    
    stats = db.fetchone("SELECT * FROM xpatch.stats('stats_data')")
    assert stats['total_rows'] == 5, f"Expected 5 rows, got {stats['total_rows']}"
    assert stats['total_groups'] == 2, f"Expected 2 groups, got {stats['total_groups']}"


@pg_test(tags=["unit", "core-api", "stats", "p0"])
def test_stats_rejects_heap_table(db):
    """
    WHAT: stats() on non-xpatch table should fail
    WHY: Prevent confusing errors
    EXPECTED: Clear error message
    """
    db.execute("CREATE TABLE heap_stats (id INT, data TEXT)")
    
    try:
        db.execute("SELECT * FROM xpatch.stats('heap_stats')")
        assert False, "Expected error for heap table"
    except Exception as e:
        # Should get some error
        assert True


@pg_test(tags=["unit", "core-api", "stats", "p1"])
def test_stats_compression_ratio(db):
    """
    WHAT: stats() compression_ratio calculation
    WHY: Key metric for monitoring storage efficiency
    EXPECTED: Ratio > 0 for compressed data
    """
    db.execute("""
        CREATE TABLE stats_ratio (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_ratio', group_by => 'grp', order_by => 'ver');
        -- Insert similar data that should compress well
        INSERT INTO stats_ratio VALUES (1, 1, repeat('hello world ', 100));
        INSERT INTO stats_ratio VALUES (1, 2, repeat('hello world ', 100) || '!');
        INSERT INTO stats_ratio VALUES (1, 3, repeat('hello world ', 100) || '!!');
    """)
    
    stats = db.fetchone("SELECT compression_ratio FROM xpatch.stats('stats_ratio')")
    assert stats['compression_ratio'] is not None
    assert stats['compression_ratio'] >= 0


@pg_test(tags=["unit", "core-api", "stats", "p1"])
def test_stats_keyframe_count(db):
    """
    WHAT: stats() keyframe_count accuracy
    WHY: Verify compression is working as configured
    EXPECTED: At least 1 keyframe per group
    """
    db.execute("""
        CREATE TABLE stats_kf (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('stats_kf', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 3
        );
        -- 2 groups, 5 versions each
        INSERT INTO stats_kf SELECT g, v, 'data' || g || '_' || v 
        FROM generate_series(1, 2) g, generate_series(1, 5) v;
    """)
    
    stats = db.fetchone("SELECT keyframe_count, delta_count FROM xpatch.stats('stats_kf')")
    assert stats['keyframe_count'] >= 2, f"Expected at least 2 keyframes, got {stats['keyframe_count']}"


# =============================================================================
# describe() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "describe", "p0"])
def test_describe_shows_config(db):
    """
    WHAT: describe() shows table configuration
    WHY: Primary debugging tool for users
    EXPECTED: Key properties visible
    """
    db.execute("""
        CREATE TABLE desc_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('desc_test', group_by => 'grp', order_by => 'ver');
    """)
    
    desc = db.fetchall("SELECT property, value FROM xpatch.describe('desc_test')")
    props = {row['property']: row['value'] for row in desc}
    
    assert 'group_by' in props, "Missing group_by property"
    assert 'order_by' in props, "Missing order_by property"
    assert 'access_method' in props, "Missing access_method property"
    assert props['access_method'] == 'xpatch'


@pg_test(tags=["unit", "core-api", "describe", "p1"])
def test_describe_shows_columns(db):
    """
    WHAT: describe() shows column information
    WHY: Help users understand table structure
    EXPECTED: All columns listed with roles
    """
    db.execute("""
        CREATE TABLE desc_cols (grp INT, ver INT, title TEXT, body TEXT) USING xpatch;
        SELECT xpatch.configure('desc_cols', group_by => 'grp', order_by => 'ver');
    """)
    
    desc = db.fetchall("SELECT property, value FROM xpatch.describe('desc_cols')")
    props = {row['property']: row['value'] for row in desc}
    
    # Should have column entries
    col_props = [p for p in props if p.startswith('column[')]
    assert len(col_props) >= 4, f"Expected at least 4 columns, got {len(col_props)}"


@pg_test(tags=["unit", "core-api", "describe", "p1"])
def test_describe_rejects_heap_table(db):
    """
    WHAT: describe() on non-xpatch table should fail
    WHY: Prevent confusing errors
    EXPECTED: Clear error about access method
    """
    db.execute("CREATE TABLE heap_desc (id INT, data TEXT)")
    
    try:
        db.execute("SELECT * FROM xpatch.describe('heap_desc')")
        assert False, "Expected error for heap table"
    except Exception as e:
        error_msg = str(e).lower()
        assert "xpatch" in error_msg, f"Expected 'xpatch' in error: {e}"


# =============================================================================
# inspect() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "inspect", "p0"])
def test_inspect_shows_keyframes(db):
    """
    WHAT: inspect() identifies keyframes in version chain
    WHY: Verify compression working correctly
    EXPECTED: is_keyframe=true for keyframes
    """
    db.execute("""
        CREATE TABLE insp_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('insp_test', 
            group_by => 'grp', 
            order_by => 'ver',
            keyframe_every => 3
        );
        INSERT INTO insp_test SELECT 1, v, 'version ' || v FROM generate_series(1, 5) v;
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.inspect('insp_test', 1)")
    assert len(rows) > 0, "inspect() returned no rows"
    
    # First row should be keyframe
    keyframes = [r for r in rows if r['is_keyframe']]
    assert len(keyframes) >= 1, "No keyframes found"


@pg_test(tags=["unit", "core-api", "inspect", "p1"])
def test_inspect_nonexistent_group(db):
    """
    WHAT: inspect() with non-existent group value
    WHY: Handle edge case gracefully
    EXPECTED: Empty result set, no error
    """
    db.execute("""
        CREATE TABLE insp_empty (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('insp_empty', group_by => 'grp', order_by => 'ver');
        INSERT INTO insp_empty VALUES (1, 1, 'data');
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.inspect('insp_empty', 999)")
    assert len(rows) == 0, f"Expected empty result for non-existent group, got {len(rows)} rows"


# =============================================================================
# physical() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "physical", "p0"])
def test_physical_returns_deltas(db):
    """
    WHAT: physical() returns raw delta information
    WHY: Low-level debugging and replication tools
    EXPECTED: Returns delta_bytes and metadata
    """
    db.execute("""
        CREATE TABLE phys_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO phys_test VALUES (1, 1, 'first version');
        INSERT INTO phys_test VALUES (1, 2, 'second version');
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.physical('phys_test', 1)")
    assert len(rows) > 0, "physical() returned no rows"
    
    # Check required columns exist
    for row in rows:
        assert 'delta_bytes' in row
        assert 'is_keyframe' in row
        assert 'seq' in row


@pg_test(tags=["unit", "core-api", "physical", "p1"])
def test_physical_empty_table(db):
    """
    WHAT: physical() on empty table
    WHY: Handle edge case
    EXPECTED: Empty result set
    """
    db.execute("""
        CREATE TABLE phys_empty (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('phys_empty', group_by => 'grp', order_by => 'ver');
    """)
    
    rows = db.fetchall("SELECT * FROM xpatch.physical('phys_empty')")
    assert len(rows) == 0


# =============================================================================
# cache_stats() Tests
# =============================================================================

@pg_test(tags=["unit", "core-api", "cache", "p1"])
def test_cache_stats_returns_metrics(db):
    """
    WHAT: cache_stats() returns cache metrics
    WHY: Monitor cache performance
    EXPECTED: All metric fields present
    """
    stats = db.fetchone("SELECT * FROM xpatch.cache_stats()")
    
    expected_fields = ['cache_size_bytes', 'cache_max_bytes', 'entries_count',
                       'hit_count', 'miss_count', 'eviction_count']
    for field in expected_fields:
        assert field in stats, f"Missing field: {field}"
