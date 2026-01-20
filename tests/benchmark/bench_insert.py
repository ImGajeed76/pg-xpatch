"""
Benchmark tests for insert performance.
"""

from xptest import pg_test


@pg_test(benchmark=True, tags=["benchmark", "insert"])
def bench_insert_100_versions(db):
    """Benchmark: Insert 100 versions to single group."""
    db.execute("""
        CREATE TABLE bench_ins_100 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('bench_ins_100', group_by => 'grp', order_by => 'ver');
    """)
    
    for i in range(1, 101):
        db.execute(f"INSERT INTO bench_ins_100 VALUES (1, {i}, 'version {i} content data')")


@pg_test(benchmark=True, tags=["benchmark", "insert"])
def bench_batch_insert_500(db):
    """Benchmark: Batch insert 500 rows."""
    db.execute("""
        CREATE TABLE bench_batch_500 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('bench_batch_500', group_by => 'grp', order_by => 'ver');
    """)
    
    values = ",".join([f"(1, {i}, 'data {i}')" for i in range(1, 501)])
    db.execute(f"INSERT INTO bench_batch_500 VALUES {values}")


@pg_test(benchmark=True, tags=["benchmark", "insert"])
def bench_insert_50_groups(db):
    """Benchmark: Insert into 50 different groups."""
    db.execute("""
        CREATE TABLE bench_multi_grp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('bench_multi_grp', group_by => 'grp', order_by => 'ver');
    """)
    
    for g in range(1, 51):
        db.execute(f"INSERT INTO bench_multi_grp VALUES ({g}, 1, 'group {g} initial')")
        db.execute(f"INSERT INTO bench_multi_grp VALUES ({g}, 2, 'group {g} updated')")


@pg_test(benchmark=True, tags=["benchmark", "insert", "compression"])
def bench_insert_compressible_data(db):
    """Benchmark: Insert highly compressible (similar) data."""
    db.execute("""
        CREATE TABLE bench_compress (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('bench_compress', group_by => 'grp', order_by => 'ver');
    """)
    
    # Data with small differences (highly compressible deltas)
    base_content = "This is the base content that will be mostly the same across versions. " * 10
    
    for i in range(1, 51):
        content = base_content + f" Version {i} specific ending."
        db.execute("INSERT INTO bench_compress VALUES (%s, %s, %s)", (1, i, content))


@pg_test(benchmark=True, tags=["benchmark", "insert", "nocompression"])
def bench_insert_random_data(db):
    """Benchmark: Insert random (incompressible) data."""
    import random
    import string
    
    db.execute("""
        CREATE TABLE bench_random (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('bench_random', group_by => 'grp', order_by => 'ver');
    """)
    
    for i in range(1, 51):
        # Generate random content each time
        content = ''.join(random.choices(string.ascii_letters + string.digits, k=500))
        db.execute("INSERT INTO bench_random VALUES (%s, %s, %s)", (1, i, content))
