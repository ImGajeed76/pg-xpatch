"""
Concurrency stress tests - concurrent access patterns to find race conditions.

Ported from tmp/stress_test/concurrent_test.py

Note: These tests use threading to simulate concurrent database access.
They require the xpatch_table fixture which provides a pre-configured table.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from xptest import pg_test
from xptest.database import DatabaseConnection


def create_connection(db_name: str) -> DatabaseConnection:
    """Create and connect a new DatabaseConnection."""
    conn = DatabaseConnection(db_name)
    conn.connect()
    return conn


@pg_test(tags=["stress", "concurrency"])
def test_concurrent_inserts_different_groups(db):
    """Multiple threads inserting into different groups should all succeed."""
    db.execute("""
        CREATE TABLE cc_diff_groups (id INT, version INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('cc_diff_groups', group_by => 'id', order_by => 'version');
    """)
    
    db_name = db.fetchval("SELECT current_database()")
    errors = []
    success_count = [0]
    lock = threading.Lock()
    
    def insert_worker(group_id, num_inserts):
        """Worker that inserts into its own group."""
        conn = None
        try:
            # Create a new connection for this thread
            conn = create_connection(db_name)
            for i in range(num_inserts):
                conn.execute(
                    "INSERT INTO cc_diff_groups VALUES (%s, %s, %s)",
                    (group_id, i + 1, f'group {group_id} version {i + 1}')
                )
                with lock:
                    success_count[0] += 1
        except Exception as e:
            with lock:
                errors.append(f"Group {group_id}: {str(e)[:100]}")
        finally:
            if conn:
                conn.close()
    
    # Start 10 workers, each inserting into their own group
    threads = []
    for g in range(10):
        t = threading.Thread(target=insert_worker, args=(g, 20))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join(timeout=60)
    
    # All 200 inserts should succeed
    count = db.fetchval("SELECT COUNT(*) FROM cc_diff_groups")
    
    assert len(errors) == 0, f"Unexpected errors: {errors[:5]}"
    assert count == 200, f"Expected 200 rows, got {count}"


@pg_test(tags=["stress", "concurrency"])
def test_concurrent_read_write(db):
    """Readers and writers accessing the same table concurrently."""
    db.execute("""
        CREATE TABLE cc_read_write (id INT, version INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('cc_read_write', group_by => 'id', order_by => 'version');
    """)
    
    # Pre-populate with some data
    for g in range(5):
        for v in range(1, 11):
            db.execute("INSERT INTO cc_read_write VALUES (%s, %s, %s)", 
                      (g, v, f'initial g{g}v{v}'))
    
    db_name = db.fetchval("SELECT current_database()")
    read_errors = []
    write_errors = []
    read_count = [0]
    write_count = [0]
    lock = threading.Lock()
    stop_flag = [False]
    
    def reader_worker():
        """Worker that reads from random groups."""
        conn = None
        try:
            conn = create_connection(db_name)
            import random
            while not stop_flag[0]:
                group_id = random.randint(0, 4)
                conn.fetchall(f"SELECT * FROM cc_read_write WHERE id = {group_id}")
                with lock:
                    read_count[0] += 1
                time.sleep(0.01)
        except Exception as e:
            with lock:
                read_errors.append(str(e)[:50])
        finally:
            if conn:
                conn.close()
    
    def writer_worker(group_id, start_version):
        """Worker that writes to a specific group."""
        conn = None
        try:
            conn = create_connection(db_name)
            version = start_version
            while not stop_flag[0]:
                try:
                    conn.execute(
                        "INSERT INTO cc_read_write VALUES (%s, %s, %s)",
                        (group_id, version, f'concurrent write v{version}')
                    )
                    with lock:
                        write_count[0] += 1
                    version += 1
                except Exception as e:
                    # Out-of-order errors are expected
                    if "order" not in str(e).lower():
                        with lock:
                            write_errors.append(str(e)[:50])
                time.sleep(0.05)
        except Exception as e:
            with lock:
                write_errors.append(str(e)[:50])
        finally:
            if conn:
                conn.close()
    
    # Start readers and writers
    threads = []
    for _ in range(3):
        t = threading.Thread(target=reader_worker)
        threads.append(t)
        t.start()
    
    for g in range(5):
        t = threading.Thread(target=writer_worker, args=(g, 11))
        threads.append(t)
        t.start()
    
    # Run for 3 seconds
    time.sleep(3)
    stop_flag[0] = True
    
    for t in threads:
        t.join(timeout=5)
    
    # Should have completed many reads and writes without errors
    assert len(read_errors) == 0, f"Read errors: {read_errors[:5]}"
    assert len(write_errors) == 0, f"Write errors: {write_errors[:5]}"
    assert read_count[0] > 0, "No reads completed"
    assert write_count[0] > 0, "No writes completed"


@pg_test(tags=["stress", "concurrency"])
def test_high_contention(db):
    """High contention scenario - many threads on few resources.
    
    This test verifies that the system handles high contention without crashes.
    Some transaction failures are expected due to race conditions.
    """
    db.execute("""
        CREATE TABLE cc_contention (id INT, version INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('cc_contention', group_by => 'id', order_by => 'version');
        INSERT INTO cc_contention VALUES (1, 1, 'group 1 start');
        INSERT INTO cc_contention VALUES (2, 1, 'group 2 start');
    """)
    
    db_name = db.fetchval("SELECT current_database()")
    success_count = [0]
    lock = threading.Lock()
    next_version = {1: 2, 2: 2}
    version_lock = threading.Lock()
    
    def worker(worker_id):
        """Worker that competes for writes to shared groups."""
        conn = None
        try:
            conn = create_connection(db_name)
            for _ in range(10):
                group_id = (worker_id % 2) + 1
                with version_lock:
                    version = next_version[group_id]
                    next_version[group_id] += 1
                
                try:
                    conn.execute(
                        "INSERT INTO cc_contention VALUES (%s, %s, %s)",
                        (group_id, version, f'worker {worker_id}')
                    )
                    with lock:
                        success_count[0] += 1
                except Exception:
                    # Transaction errors are expected due to contention
                    # Reconnect if transaction is aborted
                    try:
                        conn.close()
                        conn = create_connection(db_name)
                    except Exception:
                        pass
        except Exception:
            pass  # Connection errors during high contention are acceptable
        finally:
            if conn:
                conn.close()
    
    # 10 threads all competing
    threads = []
    for w in range(10):
        t = threading.Thread(target=worker, args=(w,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join(timeout=60)
    
    # Under high contention, we expect some successes (not all will succeed)
    # The key is that the system doesn't crash
    assert success_count[0] > 0, "Expected at least some successful inserts"


@pg_test(tags=["stress", "concurrency"])
def test_transaction_commit_is_visible(db):
    """Test that committed transactions are visible.
    
    Note: xpatch tables are append-only and may have different isolation
    semantics than regular heap tables. This test verifies that committed
    data is visible to other connections.
    """
    db.execute("""
        CREATE TABLE cc_commit_test (id INT, version INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('cc_commit_test', group_by => 'id', order_by => 'version');
        INSERT INTO cc_commit_test VALUES (1, 1, 'initial');
    """)
    
    db_name = db.fetchval("SELECT current_database()")
    results = {"commit_done": False}
    lock = threading.Lock()
    
    def writer():
        """Insert and commit in a separate connection."""
        conn = None
        try:
            conn = create_connection(db_name)
            conn.execute("INSERT INTO cc_commit_test VALUES (1, 2, 'committed')")
            with lock:
                results["commit_done"] = True
        except Exception:
            pass
        finally:
            if conn:
                conn.close()
    
    t_writer = threading.Thread(target=writer)
    t_writer.start()
    t_writer.join(timeout=10)
    
    # After commit, we should see both rows
    final_count = db.fetchval("SELECT COUNT(*) FROM cc_commit_test WHERE id = 1")
    
    assert final_count == 2, f"Expected 2 rows after commit, got {final_count}"


@pg_test(tags=["stress", "concurrency", "slow"])
def test_concurrent_inserts_same_group_with_ordering(db):
    """Multiple threads inserting into the same group with proper ordering.
    
    xpatch requires versions to be inserted in order within a group.
    When multiple threads compete for the same group, some will fail
    due to out-of-order constraints. This is expected behavior.
    """
    db.execute("""
        CREATE TABLE cc_same_ordered (id INT, version INT, content TEXT) USING xpatch;
        SELECT xpatch.configure('cc_same_ordered', group_by => 'id', order_by => 'version');
    """)
    
    db_name = db.fetchval("SELECT current_database()")
    success_count = [0]
    error_count = [0]
    lock = threading.Lock()
    version_counter = [1]
    version_lock = threading.Lock()
    
    def insert_worker(num_inserts):
        """Worker that gets a version atomically then inserts."""
        conn = None
        try:
            conn = create_connection(db_name)
            for _ in range(num_inserts):
                # Get version atomically
                with version_lock:
                    version = version_counter[0]
                    version_counter[0] += 1
                
                try:
                    conn.execute(
                        "INSERT INTO cc_same_ordered VALUES (1, %s, %s)",
                        (version, f'version {version}')
                    )
                    with lock:
                        success_count[0] += 1
                except Exception:
                    with lock:
                        error_count[0] += 1
        except Exception:
            with lock:
                error_count[0] += 1
        finally:
            if conn:
                conn.close()
    
    # Start 5 workers, each doing 10 inserts
    threads = []
    for _ in range(5):
        t = threading.Thread(target=insert_worker, args=(10,))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join(timeout=60)
    
    # Due to race conditions between getting version and actual insert,
    # some inserts will fail with out-of-order errors. 
    # We expect at least some successes - the exact number depends on timing.
    total = success_count[0] + error_count[0]
    assert total == 50, f"Expected 50 total attempts, got {total}"
    # Lower threshold since concurrent access to same group has inherent races
    assert success_count[0] >= 20, f"Expected >= 20 successes, got {success_count[0]}"
