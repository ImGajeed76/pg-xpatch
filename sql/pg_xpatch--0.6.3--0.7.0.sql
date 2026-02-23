-- pg_xpatch upgrade script: 0.6.3 -> 0.7.0
--
-- Changes in 0.7.0:
--   - New function: xpatch.warm_cache_parallel() — C implementation of
--     cache warming that uses PostgreSQL dynamic background workers for
--     dramatically faster cache population.
--   - Discovers all groups and keyframe sections, distributes work across
--     N workers via a lock-free shared work queue in dynamic shared memory.
--   - New GUC: pg_xpatch.warm_cache_workers (default 4, PGC_USERSET)
--   - The existing xpatch.warm_cache() PL/pgSQL function is preserved as-is.

CREATE OR REPLACE FUNCTION xpatch.warm_cache_parallel(
    table_name      REGCLASS,
    max_workers     INT DEFAULT NULL,
    max_groups      INT DEFAULT NULL
) RETURNS TABLE (
    rows_warmed         BIGINT,
    groups_warmed       BIGINT,
    sections_warmed     BIGINT,
    workers_used        INT,
    duration_ms         FLOAT8
) AS 'pg_xpatch', 'xpatch_warm_cache_parallel'
LANGUAGE C CALLED ON NULL INPUT;

COMMENT ON FUNCTION xpatch.warm_cache_parallel(regclass, int, int) IS
    'Parallel cache warming using dynamic background workers. '
    'Discovers groups and keyframe sections from xpatch.group_stats, '
    'distributes reconstruction work across N workers (default: pg_xpatch.warm_cache_workers GUC). '
    'Leader process also participates in the work queue. '
    'Falls back to sequential C warming if no workers are available.';
