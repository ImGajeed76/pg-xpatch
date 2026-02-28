/*
 * pg-xpatch - PostgreSQL Table Access Method for delta-compressed data
 * Copyright (c) 2025 Oliver Seifert
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Commercial License Option:
 * For commercial use in proprietary software, a commercial license is
 * available. Contact xpatch-commercial@alias.oseifert.ch for details.
 */

/*
 * xpatch_path_planner.c — Optimal reconstruction path planner
 *
 * Implements bottom-up dynamic programming to find the cheapest path for
 * reconstructing a target version from the chain index.
 *
 * Algorithm:
 *   1. Walk the chain backward from target to keyframe, collecting the
 *      versions in dependency order. The chain is linear: each non-keyframe
 *      version has exactly one base via its tag (base_offset).
 *
 *   2. Walk forward (keyframe first) computing the optimal cost for each
 *      version via DP:
 *        - Option A: Use decompressed content as anchor (L1 or L3)
 *        - Option B: Decode keyframe (L2 or disk) — only for keyframes
 *        - Option C: Apply delta from base (L2 or disk) + cost[base]
 *      Pick the minimum at each step.
 *
 *   3. Backtrack from the target to build the execution path (anchor first).
 *
 * The DP arrays are stack-allocated for chains up to PATH_PLANNER_MAX_STACK_CHAIN
 * entries (covers keyframe_every=100 with headroom). Longer chains fall back
 * to palloc.
 *
 * Cost model uses nanosecond constants from xpatch_chain_index.h, with a
 * zstd multiplier applied to L2 costs when enable_zstd is true.
 */

#include "xpatch_path_planner.h"

/* DP state per chain entry */
typedef struct DPEntry
{
    int64       cost;           /* Minimum cost in nanoseconds (INT64_MAX = unset) */
    PathAction  action;         /* Action taken to achieve this cost */
    int32       base_idx;       /* Index into chain[] of the base version, or -1 */
} DPEntry;

/*
 * xpatch_plan_path — Main entry point for the path planner.
 *
 * See xpatch_path_planner.h for full documentation.
 */
PathPlan *
xpatch_plan_path(const ChainWalkResult *chain, int64 target_seq,
                 bool enable_zstd)
{
    /* ---- Local variables ---- */

    /* Chain walk: versions in dependency order (keyframe first) */
    int32           chain_seqs_stack[PATH_PLANNER_MAX_STACK_CHAIN];
    int32           *chain_seqs = chain_seqs_stack;
    bool            chain_seqs_palloc = false;
    int32           chain_len = 0;

    /* DP arrays (parallel to chain_seqs) */
    DPEntry         dp_stack[PATH_PLANNER_MAX_STACK_CHAIN];
    DPEntry         *dp = dp_stack;
    bool            dp_palloc = false;

    /* Backward walk */
    int64           cur_seq;
    int32           cur_idx;
    ChainIndexEntry *entry;
    uint32          base_offset;

    /* Forward DP */
    int32           i;
    int64           cost_l2_apply;
    int64           cost_l2_kf;
    int64           cost_disk;

    /* Backtrack / plan construction */
    int32           path_len;
    int32           path_stack[PATH_PLANNER_MAX_STACK_CHAIN];
    int32           *path_indices = path_stack;
    bool            path_palloc = false;
    PathPlan        *plan;
    int32           step_idx;
    int32           bi;

    /* ---- Validate inputs ---- */

    if (chain == NULL || chain->entries == NULL || chain->count <= 0)
        return NULL;

    /* Target must be within the chain's range */
    if (target_seq < chain->base_seq || target_seq > chain->max_seq)
        return NULL;

    /* Check that the target entry exists and is not a sentinel */
    cur_idx = (int32)(target_seq - chain->base_seq);
    if (cur_idx < 0 || cur_idx >= chain->count)
        return NULL;
    entry = &chain->entries[cur_idx];
    if (chain_entry_is_sentinel(entry))
        return NULL;

    /* ---- Compute effective costs (apply zstd multiplier) ---- */

    if (enable_zstd)
    {
        cost_l2_apply = (int64)COST_L2_APPLY_NS * COST_ZSTD_MULTIPLIER;
        cost_l2_kf    = (int64)COST_L2_KEYFRAME_NS * COST_ZSTD_MULTIPLIER;
    }
    else
    {
        cost_l2_apply = (int64)COST_L2_APPLY_NS;
        cost_l2_kf    = (int64)COST_L2_KEYFRAME_NS;
    }
    cost_disk = (int64)COST_DISK_NS;

    /* ---- Phase 1: Walk backward from target to keyframe ---- */

    /*
     * We collect indices into chain->entries[] in reverse order (target first),
     * then reverse them so keyframe is first. This gives us the dependency
     * chain in forward order for the DP.
     */
    cur_seq = target_seq;
    for (;;)
    {
        cur_idx = (int32)(cur_seq - chain->base_seq);

        /* Cycle / infinite-loop guard: can never visit more entries than exist */
        if (chain_len > chain->count)
        {
            elog(DEBUG1, "xpatch path_planner: cycle detected after %d steps",
                 chain_len);
            goto cleanup;
        }

        /* Bounds check */
        if (cur_idx < 0 || cur_idx >= chain->count)
        {
            elog(DEBUG1, "xpatch path_planner: chain broken at seq=" INT64_FORMAT
                 " (index %d out of range [0,%d))",
                 cur_seq, cur_idx, chain->count);
            goto cleanup;
        }

        entry = &chain->entries[cur_idx];
        if (chain_entry_is_sentinel(entry))
        {
            elog(DEBUG1, "xpatch path_planner: chain broken at seq=" INT64_FORMAT
                 " (sentinel entry)", cur_seq);
            goto cleanup;
        }

        /* Ensure we have space */
        if (chain_len >= PATH_PLANNER_MAX_STACK_CHAIN && !chain_seqs_palloc)
        {
            /* Upgrade to palloc'd arrays */
            int32 new_cap = chain->count + 16;

            chain_seqs = (int32 *) palloc(new_cap * sizeof(int32));
            memcpy(chain_seqs, chain_seqs_stack, chain_len * sizeof(int32));
            chain_seqs_palloc = true;
        }

        chain_seqs[chain_len++] = cur_idx;

        /* Stop at keyframe */
        base_offset = chain_entry_get_base_offset(entry);
        if (base_offset == 0)
            break;

        /* Follow the chain backward */
        cur_seq = cur_seq - (int64)base_offset;
    }

    /* Reverse so keyframe is at index 0 */
    for (i = 0; i < chain_len / 2; i++)
    {
        int32 tmp = chain_seqs[i];
        chain_seqs[i] = chain_seqs[chain_len - 1 - i];
        chain_seqs[chain_len - 1 - i] = tmp;
    }

    /* ---- Allocate DP array ---- */

    if (chain_len > PATH_PLANNER_MAX_STACK_CHAIN)
    {
        dp = (DPEntry *) palloc(chain_len * sizeof(DPEntry));
        dp_palloc = true;
    }

    /* ---- Phase 2: Forward DP ---- */

    for (i = 0; i < chain_len; i++)
    {
        int64       best_cost = INT64_MAX;
        PathAction  best_action = PATH_ACTION_ANCHOR_KF_DISK;
        int32       best_base = -1;
        uint8       bits;

        cur_idx = chain_seqs[i];
        entry = &chain->entries[cur_idx];
        bits = entry->cache_bits;
        base_offset = chain_entry_get_base_offset(entry);

        /* Option A: L1 anchor (decompressed content in shmem) */
        if (bits & CHAIN_BIT_L1)
        {
            int64 c = (int64)COST_L1_NS;
            if (c < best_cost)
            {
                best_cost = c;
                best_action = PATH_ACTION_ANCHOR_L1;
                best_base = -1;
            }
        }

        /* Option A2: L3 anchor (decompressed content on disk) */
        if (bits & CHAIN_BIT_L3)
        {
            int64 c = (int64)COST_L3_NS;
            if (c < best_cost)
            {
                best_cost = c;
                best_action = PATH_ACTION_ANCHOR_L3;
                best_base = -1;
            }
        }

        /* Option B: Keyframe decode (only for keyframes: base_offset == 0) */
        if (base_offset == 0)
        {
            if (bits & CHAIN_BIT_L2)
            {
                int64 c = cost_l2_kf;
                if (c < best_cost)
                {
                    best_cost = c;
                    best_action = PATH_ACTION_ANCHOR_KF_L2;
                    best_base = -1;
                }
            }
            if (bits & CHAIN_BIT_DISK)
            {
                int64 c = cost_disk;
                if (c < best_cost)
                {
                    best_cost = c;
                    best_action = PATH_ACTION_ANCHOR_KF_DISK;
                    best_base = -1;
                }
            }
        }

        /* Option C: Delta application from base (non-keyframe only) */
        if (base_offset != 0 && i > 0)
        {
            /*
             * The base is always chain_seqs[i-1] in our dependency chain,
             * because we walked backward following base_offsets. The chain
             * array is the exact dependency path — chain_seqs[0] is the
             * keyframe, chain_seqs[1] is the first delta, etc.
             *
             * We already verified in Phase 1 that the chain is connected.
             */
            int64 base_cost = dp[i - 1].cost;

            if (base_cost < INT64_MAX)
            {
                /* Delta from L2 */
                if (bits & CHAIN_BIT_L2)
                {
                    int64 c = base_cost + cost_l2_apply;
                    if (c < best_cost)
                    {
                        best_cost = c;
                        best_action = PATH_ACTION_DELTA_L2;
                        best_base = i - 1;
                    }
                }

                /* Delta from disk */
                if (bits & CHAIN_BIT_DISK)
                {
                    int64 c = base_cost + cost_disk;
                    if (c < best_cost)
                    {
                        best_cost = c;
                        best_action = PATH_ACTION_DELTA_DISK;
                        best_base = i - 1;
                    }
                }
            }
        }

        dp[i].cost = best_cost;
        dp[i].action = best_action;
        dp[i].base_idx = best_base;
    }

    /* Check that we found a valid path to the target */
    if (dp[chain_len - 1].cost == INT64_MAX)
    {
        elog(DEBUG1, "xpatch path_planner: no valid path found for target seq="
             INT64_FORMAT, target_seq);
        goto cleanup;
    }

    /* ---- Phase 3: Backtrack to build the path ---- */

    /*
     * Walk backward from the target through base_idx pointers, collecting
     * the chain indices. Then reverse to get anchor-first order.
     */
    if (chain_len > PATH_PLANNER_MAX_STACK_CHAIN)
    {
        path_indices = (int32 *) palloc(chain_len * sizeof(int32));
        path_palloc = true;
    }

    path_len = 0;
    bi = chain_len - 1;    /* Start at target */
    while (bi >= 0)
    {
        path_indices[path_len++] = bi;

        if (dp[bi].base_idx < 0)
            break;  /* Reached an anchor — done */

        bi = dp[bi].base_idx;
    }

    /* Reverse: anchor first, target last */
    for (i = 0; i < path_len / 2; i++)
    {
        int32 tmp = path_indices[i];
        path_indices[i] = path_indices[path_len - 1 - i];
        path_indices[path_len - 1 - i] = tmp;
    }

    /* ---- Allocate and fill the PathPlan ---- */

    plan = (PathPlan *) palloc(offsetof(PathPlan, steps) +
                               path_len * sizeof(PathStep));
    plan->num_steps = path_len;
    plan->total_cost_ns = dp[chain_len - 1].cost;

    for (step_idx = 0; step_idx < path_len; step_idx++)
    {
        int32 ci = path_indices[step_idx];
        int32 entry_idx = chain_seqs[ci];

        plan->steps[step_idx].seq = chain->base_seq + entry_idx;
        plan->steps[step_idx].action = dp[ci].action;
    }

    /* ---- Cleanup temporary allocations and return ---- */

    if (path_palloc)
        pfree(path_indices);
    if (dp_palloc)
        pfree(dp);
    if (chain_seqs_palloc)
        pfree(chain_seqs);

    return plan;

cleanup:
    /* Error path: free any palloc'd temporaries and return NULL */
    if (path_palloc)
        pfree(path_indices);
    if (dp_palloc)
        pfree(dp);
    if (chain_seqs_palloc)
        pfree(chain_seqs);

    return NULL;
}
