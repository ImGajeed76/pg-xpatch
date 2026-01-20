"""
P0.4 - SQL Features Tests

Tests for PostgreSQL SQL feature compatibility with xpatch tables.
"""

from xptest import pg_test


# =============================================================================
# WHERE Clause Tests
# =============================================================================

@pg_test(tags=["unit", "sql", "where", "p0"])
def test_where_equality(db):
    """Basic WHERE with equality."""
    db.execute("""
        CREATE TABLE w_eq (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('w_eq', group_by => 'grp', order_by => 'ver');
        INSERT INTO w_eq VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """)
    
    rows = db.fetchall("SELECT * FROM w_eq WHERE grp = 1")
    assert len(rows) == 2


@pg_test(tags=["unit", "sql", "where", "p0"])
def test_where_and(db):
    """WHERE with AND condition."""
    db.execute("""
        CREATE TABLE w_and (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('w_and', group_by => 'grp', order_by => 'ver');
        INSERT INTO w_and VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """)
    
    row = db.fetchone("SELECT data FROM w_and WHERE grp = 1 AND ver = 2")
    assert row['data'] == 'b'


@pg_test(tags=["unit", "sql", "where", "p0"])
def test_where_or(db):
    """WHERE with OR condition."""
    db.execute("""
        CREATE TABLE w_or (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('w_or', group_by => 'grp', order_by => 'ver');
        INSERT INTO w_or VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
    """)
    
    rows = db.fetchall("SELECT * FROM w_or WHERE grp = 1 OR grp = 3")
    assert len(rows) == 2


@pg_test(tags=["unit", "sql", "where", "p1"])
def test_where_in(db):
    """WHERE with IN clause."""
    db.execute("""
        CREATE TABLE w_in (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('w_in', group_by => 'grp', order_by => 'ver');
        INSERT INTO w_in VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c'), (4, 1, 'd');
    """)
    
    rows = db.fetchall("SELECT * FROM w_in WHERE grp IN (1, 3, 5)")
    assert len(rows) == 2


@pg_test(tags=["unit", "sql", "where", "p1"])
def test_where_like(db):
    """WHERE with LIKE pattern."""
    db.execute("""
        CREATE TABLE w_like (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('w_like', group_by => 'grp', order_by => 'ver');
        INSERT INTO w_like VALUES (1, 1, 'hello'), (1, 2, 'help'), (1, 3, 'world');
    """)
    
    rows = db.fetchall("SELECT * FROM w_like WHERE data LIKE 'hel%'")
    assert len(rows) == 2


# =============================================================================
# JOIN Tests
# =============================================================================

@pg_test(tags=["unit", "sql", "join", "p0"])
def test_inner_join(db):
    """INNER JOIN xpatch with heap table."""
    db.execute("""
        CREATE TABLE j_xp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('j_xp', group_by => 'grp', order_by => 'ver');
        CREATE TABLE j_ref (id INT PRIMARY KEY, name TEXT);
        INSERT INTO j_xp VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
        INSERT INTO j_ref VALUES (1, 'one'), (2, 'two'), (4, 'four');
    """)
    
    rows = db.fetchall("""
        SELECT x.grp, x.data, r.name 
        FROM j_xp x 
        JOIN j_ref r ON x.grp = r.id
    """)
    assert len(rows) == 2  # grp 1,2 match


@pg_test(tags=["unit", "sql", "join", "p0"])
def test_left_join(db):
    """LEFT JOIN xpatch with heap table."""
    db.execute("""
        CREATE TABLE lj_xp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lj_xp', group_by => 'grp', order_by => 'ver');
        CREATE TABLE lj_ref (id INT PRIMARY KEY, name TEXT);
        INSERT INTO lj_xp VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
        INSERT INTO lj_ref VALUES (1, 'one'), (2, 'two');
    """)
    
    rows = db.fetchall("""
        SELECT x.grp, x.data, r.name 
        FROM lj_xp x 
        LEFT JOIN lj_ref r ON x.grp = r.id
        ORDER BY x.grp
    """)
    assert len(rows) == 3
    assert rows[2]['name'] is None  # grp=3 has no match


@pg_test(tags=["unit", "sql", "join", "p0"])
def test_right_join(db):
    """RIGHT JOIN heap with xpatch table."""
    db.execute("""
        CREATE TABLE rj_xp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('rj_xp', group_by => 'grp', order_by => 'ver');
        CREATE TABLE rj_ref (id INT PRIMARY KEY, name TEXT);
        INSERT INTO rj_xp VALUES (1, 1, 'a'), (2, 1, 'b');
        INSERT INTO rj_ref VALUES (1, 'one'), (2, 'two'), (3, 'three');
    """)
    
    rows = db.fetchall("""
        SELECT x.grp, x.data, r.name 
        FROM rj_xp x 
        RIGHT JOIN rj_ref r ON x.grp = r.id
        ORDER BY r.id
    """)
    assert len(rows) == 3
    assert rows[2]['grp'] is None  # id=3 has no match


@pg_test(tags=["unit", "sql", "join", "p1"])
def test_self_join(db):
    """Self-join on xpatch table."""
    db.execute("""
        CREATE TABLE sj_xp (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sj_xp', group_by => 'grp', order_by => 'ver');
        INSERT INTO sj_xp VALUES (1, 1, 'v1'), (1, 2, 'v2'), (1, 3, 'v3');
    """)
    
    rows = db.fetchall("""
        SELECT a.ver as ver_a, b.ver as ver_b, a.data, b.data as data_next
        FROM sj_xp a
        JOIN sj_xp b ON a.grp = b.grp AND a.ver = b.ver - 1
    """)
    assert len(rows) == 2  # v1->v2, v2->v3


# =============================================================================
# Subquery Tests
# =============================================================================

@pg_test(tags=["unit", "sql", "subquery", "p0"])
def test_subquery_exists(db):
    """WHERE EXISTS subquery."""
    db.execute("""
        CREATE TABLE sq_main (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sq_main', group_by => 'grp', order_by => 'ver');
        CREATE TABLE sq_filter (grp_id INT);
        INSERT INTO sq_main VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
        INSERT INTO sq_filter VALUES (1), (3);
    """)
    
    rows = db.fetchall("""
        SELECT * FROM sq_main m
        WHERE EXISTS (SELECT 1 FROM sq_filter f WHERE f.grp_id = m.grp)
    """)
    assert len(rows) == 2


@pg_test(tags=["unit", "sql", "subquery", "p0"])
def test_subquery_in(db):
    """WHERE IN (SELECT ...) subquery."""
    db.execute("""
        CREATE TABLE sqi_main (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('sqi_main', group_by => 'grp', order_by => 'ver');
        CREATE TABLE sqi_filter (grp_id INT);
        INSERT INTO sqi_main VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
        INSERT INTO sqi_filter VALUES (2), (3);
    """)
    
    rows = db.fetchall("""
        SELECT * FROM sqi_main
        WHERE grp IN (SELECT grp_id FROM sqi_filter)
    """)
    assert len(rows) == 2


@pg_test(tags=["unit", "sql", "subquery", "p1"])
def test_scalar_subquery(db):
    """Scalar subquery in SELECT."""
    db.execute("""
        CREATE TABLE ssq (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ssq', group_by => 'grp', order_by => 'ver');
        INSERT INTO ssq VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """)
    
    row = db.fetchone("""
        SELECT grp, (SELECT COUNT(*) FROM ssq WHERE grp = 1) as cnt
        FROM ssq WHERE grp = 2
    """)
    assert row['cnt'] == 2


# =============================================================================
# CTE Tests
# =============================================================================

@pg_test(tags=["unit", "sql", "cte", "p0"])
def test_cte_basic(db):
    """Basic CTE (WITH clause)."""
    db.execute("""
        CREATE TABLE cte_test (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cte_test', group_by => 'grp', order_by => 'ver');
        INSERT INTO cte_test VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """)
    
    rows = db.fetchall("""
        WITH grp1 AS (
            SELECT * FROM cte_test WHERE grp = 1
        )
        SELECT * FROM grp1 ORDER BY ver
    """)
    assert len(rows) == 2


@pg_test(tags=["unit", "sql", "cte", "p1"])
def test_cte_multiple(db):
    """Multiple CTEs."""
    db.execute("""
        CREATE TABLE cte_multi (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('cte_multi', group_by => 'grp', order_by => 'ver');
        INSERT INTO cte_multi VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
    """)
    
    rows = db.fetchall("""
        WITH 
            grp1 AS (SELECT * FROM cte_multi WHERE grp = 1),
            grp2 AS (SELECT * FROM cte_multi WHERE grp = 2)
        SELECT * FROM grp1 UNION ALL SELECT * FROM grp2
    """)
    assert len(rows) == 2


# =============================================================================
# Window Function Tests
# =============================================================================

@pg_test(tags=["unit", "sql", "window", "p0"])
def test_window_row_number(db):
    """ROW_NUMBER() window function."""
    db.execute("""
        CREATE TABLE wf_rn (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('wf_rn', group_by => 'grp', order_by => 'ver');
        INSERT INTO wf_rn VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c');
    """)
    
    rows = db.fetchall("""
        SELECT ver, ROW_NUMBER() OVER (ORDER BY ver) as rn
        FROM wf_rn WHERE grp = 1
    """)
    assert rows[0]['rn'] == 1
    assert rows[2]['rn'] == 3


@pg_test(tags=["unit", "sql", "window", "p0"])
def test_window_lag(db):
    """LAG() window function for version comparison."""
    db.execute("""
        CREATE TABLE wf_lag (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('wf_lag', group_by => 'grp', order_by => 'ver');
        INSERT INTO wf_lag VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c');
    """)
    
    rows = db.fetchall("""
        SELECT ver, data, LAG(data) OVER (ORDER BY ver) as prev_data
        FROM wf_lag WHERE grp = 1
    """)
    assert rows[0]['prev_data'] is None  # No previous
    assert rows[1]['prev_data'] == 'a'
    assert rows[2]['prev_data'] == 'b'


@pg_test(tags=["unit", "sql", "window", "p1"])
def test_window_partition_by(db):
    """Window with PARTITION BY."""
    db.execute("""
        CREATE TABLE wf_part (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('wf_part', group_by => 'grp', order_by => 'ver');
        INSERT INTO wf_part VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c'), (2, 2, 'd');
    """)
    
    rows = db.fetchall("""
        SELECT grp, ver, 
               ROW_NUMBER() OVER (PARTITION BY grp ORDER BY ver) as rn
        FROM wf_part ORDER BY grp, ver
    """)
    assert rows[0]['rn'] == 1  # grp=1, ver=1
    assert rows[1]['rn'] == 2  # grp=1, ver=2
    assert rows[2]['rn'] == 1  # grp=2, ver=1 (resets)


# =============================================================================
# Aggregation Tests
# =============================================================================

@pg_test(tags=["unit", "sql", "agg", "p0"])
def test_count_group_by(db):
    """COUNT with GROUP BY."""
    db.execute("""
        CREATE TABLE agg_cnt (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('agg_cnt', group_by => 'grp', order_by => 'ver');
        INSERT INTO agg_cnt VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
    """)
    
    rows = db.fetchall("SELECT grp, COUNT(*) as cnt FROM agg_cnt GROUP BY grp ORDER BY grp")
    assert rows[0]['cnt'] == 2  # grp=1
    assert rows[1]['cnt'] == 1  # grp=2


@pg_test(tags=["unit", "sql", "agg", "p1"])
def test_max_min(db):
    """MAX and MIN aggregations."""
    db.execute("""
        CREATE TABLE agg_mm (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('agg_mm', group_by => 'grp', order_by => 'ver');
        INSERT INTO agg_mm VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c');
    """)
    
    row = db.fetchone("SELECT MIN(ver) as min_ver, MAX(ver) as max_ver FROM agg_mm")
    assert row['min_ver'] == 1
    assert row['max_ver'] == 3


@pg_test(tags=["unit", "sql", "agg", "p1"])
def test_string_agg(db):
    """STRING_AGG function."""
    db.execute("""
        CREATE TABLE agg_str (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('agg_str', group_by => 'grp', order_by => 'ver');
        INSERT INTO agg_str VALUES (1, 1, 'a'), (1, 2, 'b'), (1, 3, 'c');
    """)
    
    row = db.fetchone("SELECT STRING_AGG(data, ',' ORDER BY ver) as combined FROM agg_str")
    assert row['combined'] == 'a,b,c'


# =============================================================================
# Set Operations
# =============================================================================

@pg_test(tags=["unit", "sql", "set-ops", "p0"])
def test_union_all(db):
    """UNION ALL of xpatch tables."""
    db.execute("""
        CREATE TABLE u1 (grp INT, ver INT, data TEXT) USING xpatch;
        CREATE TABLE u2 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('u1', group_by => 'grp', order_by => 'ver');
        SELECT xpatch.configure('u2', group_by => 'grp', order_by => 'ver');
        INSERT INTO u1 VALUES (1, 1, 'a'), (1, 2, 'b');
        INSERT INTO u2 VALUES (2, 1, 'c'), (2, 2, 'd');
    """)
    
    rows = db.fetchall("SELECT * FROM u1 UNION ALL SELECT * FROM u2")
    assert len(rows) == 4


@pg_test(tags=["unit", "sql", "set-ops", "p1"])
def test_union_distinct(db):
    """UNION removes duplicates."""
    db.execute("""
        CREATE TABLE ud1 (grp INT, ver INT, data TEXT) USING xpatch;
        CREATE TABLE ud2 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ud1', group_by => 'grp', order_by => 'ver');
        SELECT xpatch.configure('ud2', group_by => 'grp', order_by => 'ver');
        INSERT INTO ud1 VALUES (1, 1, 'same');
        INSERT INTO ud2 VALUES (1, 1, 'same');
    """)
    
    rows = db.fetchall("SELECT grp, ver, data FROM ud1 UNION SELECT grp, ver, data FROM ud2")
    assert len(rows) == 1


@pg_test(tags=["unit", "sql", "set-ops", "p1"])
def test_except(db):
    """EXCEPT set operation."""
    db.execute("""
        CREATE TABLE e1 (grp INT, ver INT, data TEXT) USING xpatch;
        CREATE TABLE e2 (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('e1', group_by => 'grp', order_by => 'ver');
        SELECT xpatch.configure('e2', group_by => 'grp', order_by => 'ver');
        INSERT INTO e1 VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 1, 'c');
        INSERT INTO e2 VALUES (2, 1, 'b');
    """)
    
    rows = db.fetchall("SELECT grp, ver, data FROM e1 EXCEPT SELECT grp, ver, data FROM e2")
    assert len(rows) == 2  # grp 1 and 3


# =============================================================================
# ORDER BY and LIMIT
# =============================================================================

@pg_test(tags=["unit", "sql", "order", "p0"])
def test_order_by(db):
    """ORDER BY on xpatch table."""
    db.execute("""
        CREATE TABLE ob (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('ob', group_by => 'grp', order_by => 'ver');
        INSERT INTO ob VALUES (3, 1, 'c'), (1, 1, 'a'), (2, 1, 'b');
    """)
    
    rows = db.fetchall("SELECT grp FROM ob ORDER BY grp")
    assert rows[0]['grp'] == 1
    assert rows[2]['grp'] == 3


@pg_test(tags=["unit", "sql", "limit", "p0"])
def test_limit_offset(db):
    """LIMIT and OFFSET."""
    db.execute("""
        CREATE TABLE lo (grp INT, ver INT, data TEXT) USING xpatch;
        SELECT xpatch.configure('lo', group_by => 'grp', order_by => 'ver');
        INSERT INTO lo SELECT v, 1, 'data' FROM generate_series(1, 10) v;
    """)
    
    rows = db.fetchall("SELECT grp FROM lo ORDER BY grp LIMIT 3 OFFSET 2")
    assert len(rows) == 3
    assert rows[0]['grp'] == 3
