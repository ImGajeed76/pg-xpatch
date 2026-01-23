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
 * xpatch_insert_cache.h - Per-group FIFO insert cache
 *
 * Implements a shared memory FIFO ring buffer cache that holds the last
 * compression_depth reconstructed row contents per (table, group) pair.
 * This eliminates the need to reconstruct previous rows during INSERT,
 * since the bases for delta encoding are always pre-materialized.
 *
 * Architecture:
 * - Fixed number of slot headers in shared memory (lightweight, ~80 bytes each)
 * - Each slot's ring buffer is dynamically allocated in DSA, sized exactly
 *   to the table's compress_depth — no artificial cap
 * - Variable-length content (raw column data) is also in DSA
 * - Slots are evicted based on activity (least-active slot is reused)
 *
 * On cold start (first insert to a group):
 * - Evict the least-active FIFO slot
 * - Allocate a ring buffer in DSA sized to compress_depth
 * - Reconstruct the last min(compress_depth, current_seq-1) rows
 * - Store in the new FIFO slot
 *
 * On warm insert:
 * - Read compress_depth bases directly from FIFO (O(1) per base)
 * - After encoding, push new row content into FIFO ring
 */

#ifndef XPATCH_INSERT_CACHE_H
#define XPATCH_INSERT_CACHE_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

/* Forward declaration */
struct XPatchConfig;

/* Maximum delta columns per table (matching xpatch_config.h) */
#define XPATCH_MAX_DELTA_COLUMNS    32

/* Default number of FIFO slots */
#define XPATCH_DEFAULT_INSERT_CACHE_SLOTS   16

/* GUC variable (defined in xpatch_insert_cache.c) */
extern int xpatch_insert_cache_slots;

/*
 * One base entry returned from the FIFO cache.
 */
typedef struct InsertCacheBase
{
    int32           seq;            /* Sequence number of this base */
    int             tag;            /* Delta tag (new_seq - base_seq) */
    const uint8    *data;           /* Pointer to raw content (palloc'd copy) */
    Size            size;           /* Content size */
} InsertCacheBase;

/*
 * Result of getting bases from the FIFO cache.
 * Dynamically allocated via palloc — sized to actual compress_depth.
 * Use InsertCacheBasesAlloc() to create, pfree() to destroy.
 */
typedef struct InsertCacheBases
{
    int             count;          /* Number of valid bases returned */
    int             capacity;       /* Allocated capacity (= compress_depth) */
    InsertCacheBase bases[];        /* C99 flexible array member */
} InsertCacheBases;

/*
 * Allocate an InsertCacheBases struct sized to the given depth.
 * Returns a zeroed struct with capacity set.
 */
static inline InsertCacheBases *
InsertCacheBasesAlloc(int depth)
{
    InsertCacheBases *b = palloc0(offsetof(InsertCacheBases, bases) +
                                  (Size) depth * sizeof(InsertCacheBase));
    b->capacity = depth;
    return b;
}

/*
 * Request shared memory space for the insert cache.
 * Must be called from _PG_init() during shared_preload_libraries.
 */
void xpatch_insert_cache_request_shmem(void);

/*
 * Initialize the insert cache in shared memory.
 * Called from shmem_startup_hook.
 */
void xpatch_insert_cache_init(void);

/*
 * Get the FIFO slot for a (table, group) pair.
 * If no slot exists, evicts the least-active slot and returns it empty.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value (use 0 if no group_by)
 *   typid       - Type OID of the group column
 *   depth       - Desired ring buffer depth (compression_depth)
 *   num_delta_cols - Number of delta columns in the table
 *   is_new      - Output: true if this is a freshly allocated slot (cold start)
 *
 * Returns the slot index, or -1 if insert cache is not available.
 */
int xpatch_insert_cache_get_slot(Oid relid, Datum group_value, Oid typid,
                                 int depth, int num_delta_cols, bool *is_new);

/*
 * Get base contents from a FIFO slot for delta encoding.
 * Returns up to depth bases ordered by proximity (closest first: tag=1, 2, ...).
 *
 * The returned data pointers are palloc'd copies valid in the current
 * memory context. Caller must pfree them when done.
 *
 * Parameters:
 *   slot_idx    - Slot index from xpatch_insert_cache_get_slot()
 *   new_seq     - Sequence number of the row being inserted
 *   col_idx     - Delta column index (0-based)
 *   bases       - Output: filled with base information
 */
void xpatch_insert_cache_get_bases(int slot_idx, int32 new_seq,
                                   int col_idx, InsertCacheBases *bases);

/*
 * Push new row content into the FIFO ring buffer for one column.
 * Evicts the oldest entry if the ring is full.
 *
 * Parameters:
 *   slot_idx    - Slot index
 *   seq         - Sequence number of the new row
 *   col_idx     - Delta column index (0-based)
 *   data        - Raw content to store
 *   size        - Content size in bytes
 */
void xpatch_insert_cache_push(int slot_idx, int32 seq,
                              int col_idx, const uint8 *data, Size size);

/*
 * Mark a FIFO entry as complete (all columns written).
 * Called after all delta columns for a row have been pushed.
 *
 * Parameters:
 *   slot_idx    - Slot index
 *   seq         - Sequence number that was just completed
 */
void xpatch_insert_cache_commit_entry(int slot_idx, int32 seq);

/*
 * Populate a FIFO slot with reconstructed content (cold start).
 * Called when a slot is newly allocated and needs to be filled with
 * the last compression_depth rows from the table.
 *
 * Parameters:
 *   slot_idx    - Slot index
 *   rel         - Relation
 *   config      - Table configuration
 *   group_value - Group column value
 *   current_max_seq - Current maximum sequence in the group
 */
void xpatch_insert_cache_populate(int slot_idx, Relation rel,
                                  struct XPatchConfig *config,
                                  Datum group_value, int32 current_max_seq);

/*
 * Invalidate all FIFO slots for a relation.
 * Called on TRUNCATE, DROP, etc.
 */
void xpatch_insert_cache_invalidate_rel(Oid relid);

/*
 * Get insert cache statistics.
 */
typedef struct InsertCacheStats
{
    int64       slots_in_use;
    int64       total_slots;
    int64       hits;           /* FIFO was already warm */
    int64       misses;         /* Cold start required */
    int64       evictions;      /* Slots evicted for reuse */
} InsertCacheStats;

void xpatch_insert_cache_get_stats(InsertCacheStats *stats);

#endif /* XPATCH_INSERT_CACHE_H */
