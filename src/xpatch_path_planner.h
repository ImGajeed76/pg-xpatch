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
 * xpatch_path_planner.h — Optimal reconstruction path planner
 *
 * Given the chain index for a group+column, finds the cheapest path to
 * reconstruct a target version using bottom-up dynamic programming.
 *
 * The planner considers all available cache levels (L1, L2, L3, disk)
 * and picks the minimum-cost combination of anchors and delta applications.
 *
 * Actions:
 *   anchor_l1:      Read decompressed content from L1 shmem (terminal)
 *   anchor_l3:      Read decompressed content from L3 disk table (terminal)
 *   anchor_kf_l2:   Decode keyframe from L2 compressed blob (terminal)
 *   anchor_kf_disk: Decode keyframe from xpatch heap table (terminal)
 *   delta_l2:       Apply compressed delta from L2 to running content
 *   delta_disk:     Apply compressed delta from disk to running content
 *
 * Usage:
 *   1. Check L1 directly (fast path, before planning).
 *   2. Call xpatch_chain_index_get_chain() to get the chain snapshot.
 *   3. Call xpatch_plan_path() with the chain and target seq.
 *   4. If NULL is returned, fall back to old recursive path.
 *   5. Execute the returned PathPlan steps from index 0 to num_steps-1.
 *   6. pfree the PathPlan when done (single palloc'd block).
 *
 * The DP arrays are stack-allocated, bounded by keyframe_every (max ~100).
 * The returned PathPlan is palloc'd (caller must pfree).
 */

#ifndef XPATCH_PATH_PLANNER_H
#define XPATCH_PATH_PLANNER_H

#include "xpatch_chain_index.h"

/* Maximum chain length the planner will handle on the stack.
 * Chains longer than this trigger a palloc'd DP array instead.
 * Sized to cover keyframe_every=100 with compress_depth headroom. */
#define PATH_PLANNER_MAX_STACK_CHAIN    128

/* ---------------------------------------------------------------------------
 * Path step actions
 * ---------------------------------------------------------------------------
 * Terminal actions (anchors): the step produces decompressed content from
 * scratch — no base version needed.
 * Delta actions: the step applies a compressed delta to the running
 * decompressed content from the previous step.
 */
typedef enum PathAction
{
    PATH_ACTION_ANCHOR_L1,          /* Read decompressed content from L1 */
    PATH_ACTION_ANCHOR_L3,          /* Read decompressed content from L3 */
    PATH_ACTION_ANCHOR_KF_L2,      /* Decode keyframe from L2 shmem */
    PATH_ACTION_ANCHOR_KF_DISK,    /* Decode keyframe from xpatch table */
    PATH_ACTION_DELTA_L2,          /* Apply delta from L2 shmem */
    PATH_ACTION_DELTA_DISK         /* Apply delta from xpatch table */
} PathAction;

/* ---------------------------------------------------------------------------
 * One step in the reconstruction path
 * ---------------------------------------------------------------------------
 */
typedef struct PathStep
{
    int64       seq;        /* Version sequence number for this step */
    PathAction  action;     /* What to do at this step */
} PathStep;

/* ---------------------------------------------------------------------------
 * Complete reconstruction plan
 * ---------------------------------------------------------------------------
 * Allocated as a single palloc block: the PathPlan header followed by
 * the steps array. Caller must pfree() when done.
 *
 * Steps are ordered anchor-first: steps[0] is the anchor (terminal),
 * steps[num_steps-1] is the target version.
 */
typedef struct PathPlan
{
    int32       num_steps;      /* Number of steps in the path */
    int64       total_cost_ns;  /* Estimated total cost in nanoseconds */
    PathStep    steps[FLEXIBLE_ARRAY_MEMBER];
} PathPlan;

/* ---------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------------
 */

/*
 * Plan the optimal reconstruction path for a target version.
 *
 * Parameters:
 *   chain       - Chain walk result from xpatch_chain_index_get_chain().
 *                 Must not be NULL. Caller retains ownership.
 *   target_seq  - The version to reconstruct.
 *   enable_zstd - Whether zstd is enabled for this table (from XPatchConfig).
 *                 When true, L2 costs are multiplied by COST_ZSTD_MULTIPLIER.
 *
 * Returns:
 *   A palloc'd PathPlan with the optimal path, or NULL if:
 *   - target_seq is not found in the chain
 *   - the chain is malformed (broken links)
 *   - no valid path exists (should not happen if DISK bit is always set)
 *
 *   Caller must pfree() the returned plan.
 *
 * The function uses stack-allocated DP arrays for chains up to
 * PATH_PLANNER_MAX_STACK_CHAIN entries. Longer chains use palloc.
 */
extern PathPlan *xpatch_plan_path(const ChainWalkResult *chain,
                                  int64 target_seq,
                                  bool enable_zstd);

/*
 * Check if a PathAction is a terminal (anchor) action.
 * Terminal actions produce decompressed content from scratch.
 * Delta actions require a base from the previous step.
 */
static inline bool
path_action_is_anchor(PathAction action)
{
    return action == PATH_ACTION_ANCHOR_L1 ||
           action == PATH_ACTION_ANCHOR_L3 ||
           action == PATH_ACTION_ANCHOR_KF_L2 ||
           action == PATH_ACTION_ANCHOR_KF_DISK;
}

/*
 * Return a human-readable name for a PathAction (for debug logging).
 */
static inline const char *
path_action_name(PathAction action)
{
    switch (action)
    {
        case PATH_ACTION_ANCHOR_L1:      return "anchor_l1";
        case PATH_ACTION_ANCHOR_L3:      return "anchor_l3";
        case PATH_ACTION_ANCHOR_KF_L2:   return "anchor_kf_l2";
        case PATH_ACTION_ANCHOR_KF_DISK: return "anchor_kf_disk";
        case PATH_ACTION_DELTA_L2:       return "delta_l2";
        case PATH_ACTION_DELTA_DISK:     return "delta_disk";
    }
    return "unknown";
}

#endif /* XPATCH_PATH_PLANNER_H */
