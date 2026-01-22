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
 * xpatch_seq_cache.h - Shared caches for sequence number lookups
 *
 * Two separate caches to optimize INSERT and READ operations:
 * 
 * 1. Group Max Seq Cache: (relid, group_value) -> max_seq
 *    - Used during INSERT to quickly get the next sequence number
 *    - One entry per group
 *    - Uses BLAKE3 hashing for collision-resistant key storage
 *
 * 2. TID Seq Cache: (relid, tid) -> seq
 *    - Used during READ to quickly get seq for a tuple
 *    - One entry per tuple accessed
 *
 * Both caches are populated lazily on first access (one-time scan)
 * and persist until server restart or cache eviction.
 */

#ifndef XPATCH_SEQ_CACHE_H
#define XPATCH_SEQ_CACHE_H

#include "pg_xpatch.h"
#include "storage/itemptr.h"

/*
 * Request shared memory space for seq caches.
 * Must be called from _PG_init() before shmem_startup_hook.
 */
void xpatch_seq_cache_request_shmem(void);

/*
 * Initialize the seq caches in shared memory.
 * Called from shmem_startup_hook.
 */
void xpatch_seq_cache_init(void);

/* ================================================================
 * Group Max Seq Cache - for INSERT optimization
 * ================================================================ */

/*
 * Get the maximum sequence number for a group.
 * Returns 0 if group not in cache (caller should scan and populate).
 * 
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value (use 0 if no group_by)
 *   typid       - Type OID of the group column (for proper hashing)
 *   found       - Output: true if found in cache, false if cache miss
 */
int32 xpatch_seq_cache_get_max_seq(Oid relid, Datum group_value, Oid typid, bool *found);

/*
 * Set the maximum sequence number for a group.
 * Called after a successful INSERT or after populating from a scan.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value (use 0 if no group_by)
 *   typid       - Type OID of the group column (for proper hashing)
 *   max_seq     - The maximum sequence number
 */
void xpatch_seq_cache_set_max_seq(Oid relid, Datum group_value, Oid typid, int32 max_seq);

/*
 * Increment and return the next sequence number for a group.
 * Atomically increments max_seq by 1 and returns the new value.
 * If group not in cache, sets it to 1 and returns 1.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value (use 0 if no group_by)
 *   typid       - Type OID of the group column (for proper hashing)
 */
int32 xpatch_seq_cache_next_seq(Oid relid, Datum group_value, Oid typid);

/* ================================================================
 * TID Seq Cache - for READ optimization
 * ================================================================ */

/*
 * Get the sequence number for a tuple by its TID.
 * Returns 0 if TID not in cache (caller should scan and populate).
 *
 * Parameters:
 *   relid - Relation OID
 *   tid   - Tuple ItemPointer
 *   found - Output: true if found in cache, false if cache miss
 */
int32 xpatch_seq_cache_get_tid_seq(Oid relid, ItemPointer tid, bool *found);

/*
 * Set the sequence number for a tuple.
 * Called after determining seq from a scan.
 */
void xpatch_seq_cache_set_tid_seq(Oid relid, ItemPointer tid, int32 seq);

/*
 * Batch populate TID seq cache for an entire group.
 * Called when we need to scan a group anyway.
 * This populates entries for all TIDs in the group in one pass.
 */
void xpatch_seq_cache_populate_group_tids(Oid relid, Datum group_value,
                                          ItemPointer *tids, int32 *seqs,
                                          int count);

/* ================================================================
 * Seq-to-TID Cache - for fetch_by_seq optimization
 * ================================================================ */

/*
 * Get the TID for a tuple by its (group, seq) key.
 * Returns true if found (and populates *tid), false if cache miss.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value (use 0 if no group_by)
 *   typid       - Type OID of the group column (for proper hashing)
 *   seq         - Sequence number within group
 *   tid         - Output: TID if found
 */
bool xpatch_seq_cache_get_seq_tid(Oid relid, Datum group_value, Oid typid,
                                  int32 seq, ItemPointer tid);

/*
 * Set the TID for a (group, seq) key.
 * Called after finding a tuple to cache its location for future lookups.
 */
void xpatch_seq_cache_set_seq_tid(Oid relid, Datum group_value, Oid typid,
                                  int32 seq, ItemPointer tid);

/* ================================================================
 * Cache Invalidation
 * ================================================================ */

/*
 * Invalidate all seq cache entries for a relation.
 * Called when a relation is dropped or truncated.
 */
void xpatch_seq_cache_invalidate_rel(Oid relid);

/*
 * Get seq cache statistics.
 */
typedef struct XPatchSeqCacheStats
{
    /* Group Max Seq Cache stats */
    int64       group_cache_entries;
    int64       group_cache_max;
    int64       group_cache_hits;
    int64       group_cache_misses;
    
    /* TID Seq Cache stats (TID -> seq) */
    int64       tid_cache_entries;
    int64       tid_cache_max;
    int64       tid_cache_hits;
    int64       tid_cache_misses;
    
    /* Seq TID Cache stats (group+seq -> TID) - for fetch_by_seq optimization */
    int64       seq_tid_cache_entries;
    int64       seq_tid_cache_max;
    int64       seq_tid_cache_hits;
    int64       seq_tid_cache_misses;
} XPatchSeqCacheStats;

void xpatch_seq_cache_get_stats(XPatchSeqCacheStats *stats);

#endif /* XPATCH_SEQ_CACHE_H */
