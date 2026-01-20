"""
Benchmark tests for read/query performance.
"""

from xptest import pg_test


def _setup_read_test_data(db, table_name: str, num_groups: int = 10, versions_per_group: int = 50):
    """Helper to setup test data for read benchmarks."""
    db.execute(f"""
        CREATE TABLE {table_name} (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('{table_name}', group_by => 'grp', order_by => 'ver', keyframe_every => 10);
    """)
    
    for g in range(1, num_groups + 1):
        values = ",".join([
            f"({g}, {v}, 'Group {g} version {v} content data')" 
            for v in range(1, versions_per_group + 1)
        ])
        db.execute(f"INSERT INTO {table_name} VALUES {values}")


@pg_test(benchmark=True, tags=["benchmark", "read"])
def bench_sequential_read_100(db):
    """Benchmark: Sequential read of 100 rows."""
    _setup_read_test_data(db, "bench_seq_read", num_groups=2, versions_per_group=50)
    
    # Sequential scan
    rows = db.fetchall("SELECT * FROM bench_seq_read ORDER BY grp, ver")
    assert len(rows) == 100


@pg_test(benchmark=True, tags=["benchmark", "read"])
def bench_random_access_50(db):
    """Benchmark: Random access to 50 specific versions."""
    import random
    _setup_read_test_data(db, "bench_random_read", num_groups=5, versions_per_group=20)
    
    # Random point queries
    for _ in range(50):
        grp = random.randint(1, 5)
        ver = random.randint(1, 20)
        result = db.fetchone(
            f"SELECT * FROM bench_random_read WHERE grp = {grp} AND ver = {ver}"
        )
        assert result is not None


@pg_test(benchmark=True, tags=["benchmark", "read"])
def bench_group_scan(db):
    """Benchmark: Scan all versions of a single group."""
    _setup_read_test_data(db, "bench_group_scan", num_groups=10, versions_per_group=50)
    
    # Scan single group (10 times for different groups)
    for g in range(1, 11):
        rows = db.fetchall(f"SELECT * FROM bench_group_scan WHERE grp = {g} ORDER BY ver")
        assert len(rows) == 50


@pg_test(benchmark=True, tags=["benchmark", "read"])
def bench_latest_version_query(db):
    """Benchmark: Query latest version per group (common pattern)."""
    _setup_read_test_data(db, "bench_latest", num_groups=20, versions_per_group=25)
    
    # Get latest version for each group
    rows = db.fetchall("""
        SELECT DISTINCT ON (grp) grp, ver, data 
        FROM bench_latest 
        ORDER BY grp, ver DESC
    """)
    assert len(rows) == 20


@pg_test(benchmark=True, tags=["benchmark", "read", "stats"])
def bench_stats_function(db):
    """Benchmark: xpatch.stats() function call."""
    _setup_read_test_data(db, "bench_stats", num_groups=10, versions_per_group=50)
    
    # Call stats multiple times
    for _ in range(10):
        result = db.fetchone("SELECT * FROM xpatch.stats('bench_stats')")
        assert result['total_rows'] == 500


@pg_test(benchmark=True, tags=["benchmark", "read", "physical"])
def bench_physical_scan(db):
    """Benchmark: xpatch.physical() full table scan."""
    _setup_read_test_data(db, "bench_phys", num_groups=10, versions_per_group=30)
    
    # Scan physical storage
    rows = db.fetchall("SELECT * FROM xpatch.physical('bench_phys')")
    assert len(rows) == 300


@pg_test(benchmark=True, tags=["benchmark", "read", "physical"])
def bench_physical_group_filter(db):
    """Benchmark: xpatch.physical() with group filter."""
    _setup_read_test_data(db, "bench_phys_filter", num_groups=20, versions_per_group=25)
    
    # Filter by specific groups
    for g in range(1, 11):
        rows = db.fetchall(f"SELECT * FROM xpatch.physical('bench_phys_filter', {g}::INT, NULL::INT)")
        assert len(rows) == 25
