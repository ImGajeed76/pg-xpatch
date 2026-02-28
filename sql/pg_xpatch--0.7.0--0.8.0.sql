-- pg_xpatch upgrade script: 0.7.0 -> 0.8.0
--
-- Changes in 0.8.0:
--   - Three-level cache system (L1/L2/L3) with chain index and path planner
--   - L1: renamed GUCs (pg_xpatch.cache_* -> pg_xpatch.l1_cache_*)
--   - L2: compressed delta cache in shared memory
--   - L3: persistent disk cache tables (per-table, opt-in)
--   - Chain index: always-on in-memory index for optimal reconstruction paths
--   - Path planner: bottom-up DP algorithm for cheapest reconstruction

-- L2 cache statistics C function
CREATE FUNCTION xpatch_l2_cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT,
    skip_count          BIGINT
) AS 'MODULE_PATHNAME', 'xpatch_l2_cache_stats_fn'
LANGUAGE C STRICT;

COMMENT ON FUNCTION xpatch_l2_cache_stats() IS 'Get L2 compressed delta cache statistics';

-- L2 cache statistics schema wrapper
CREATE OR REPLACE FUNCTION xpatch.l2_cache_stats()
RETURNS TABLE (
    cache_size_bytes    BIGINT,
    cache_max_bytes     BIGINT,
    entries_count       BIGINT,
    hit_count           BIGINT,
    miss_count          BIGINT,
    eviction_count      BIGINT,
    skip_count          BIGINT
) AS $$
    SELECT * FROM xpatch_l2_cache_stats();
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.l2_cache_stats() IS 'Get L2 compressed delta cache statistics';

-- Path planner C function
CREATE FUNCTION xpatch_plan_path(
    rel         regclass,
    group_value text,
    attnum      int2,
    target_seq  int8,
    enable_zstd bool DEFAULT true
)
RETURNS TABLE (
    step_num        INT4,
    seq             INT8,
    action          TEXT,
    total_cost_ns   INT8
) AS 'MODULE_PATHNAME', 'xpatch_plan_path_fn'
LANGUAGE C STABLE;

COMMENT ON FUNCTION xpatch_plan_path(regclass, text, int2, int8, bool) IS
    'Compute optimal reconstruction path for a target version using bottom-up DP';

-- Path planner schema wrapper
CREATE OR REPLACE FUNCTION xpatch.plan_path(
    rel         regclass,
    group_value text,
    attnum      int2,
    target_seq  int8,
    enable_zstd bool DEFAULT true
)
RETURNS TABLE (
    step_num        INT4,
    seq             INT8,
    action          TEXT,
    total_cost_ns   INT8
) AS $$
    SELECT * FROM xpatch_plan_path(rel, group_value, attnum, target_seq, enable_zstd);
$$ LANGUAGE SQL STABLE;

COMMENT ON FUNCTION xpatch.plan_path(regclass, text, int2, int8, bool) IS
    'Compute optimal reconstruction path for a target version using bottom-up DP';
