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
 * xpatch_l2_cache.h - L2 shared memory cache for compressed deltas
 *
 * Stores raw compressed delta bytes in shared memory. Architecturally
 * identical to the L1 content cache (striped LWLocks, linear probing
 * hash table, LRU eviction, chained content slots) but uses 512-byte
 * slots and a separate shared memory region.
 *
 * The cache key is (relid, group_hash, seq, attnum) — same as L1.
 *
 * On put: sets CHAIN_BIT_L2 in the chain index.
 * On eviction: clears CHAIN_BIT_L2 in the chain index.
 */

#ifndef XPATCH_L2_CACHE_H
#define XPATCH_L2_CACHE_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

/* Default configuration */
#define XPATCH_L2_DEFAULT_SIZE_MB       1024
#define XPATCH_L2_DEFAULT_MAX_ENTRIES   4194304   /* 4M */
#define XPATCH_L2_DEFAULT_SLOT_SIZE     512       /* bytes */
#define XPATCH_L2_DEFAULT_PARTITIONS    16
#define XPATCH_L2_DEFAULT_MAX_ENTRY_KB  64

/*
 * Request shared memory space for L2 cache.
 * Must be called from _PG_init().
 */
void xpatch_l2_cache_request_shmem(void);

/*
 * Look up a compressed delta in L2 cache.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_hash  - 128-bit BLAKE3 hash of group value
 *   seq         - Sequence number (_xp_seq)
 *   attnum      - Attribute number of delta column
 *
 * Returns a palloc'd copy of the compressed delta (raw bytes including
 * varlena header), or NULL if not found. Caller must pfree().
 */
bytea *xpatch_l2_cache_get(Oid relid, XPatchGroupHash group_hash,
                           int64 seq, AttrNumber attnum);

/*
 * Store a compressed delta in L2 cache.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_hash  - 128-bit BLAKE3 hash of group value
 *   seq         - Sequence number (_xp_seq)
 *   attnum      - Attribute number of delta column
 *   delta       - Compressed delta bytes (varlena with header, will be copied)
 *
 * Also sets CHAIN_BIT_L2 in the chain index for this entry.
 */
void xpatch_l2_cache_put(Oid relid, XPatchGroupHash group_hash,
                         int64 seq, AttrNumber attnum, bytea *delta);

/*
 * Invalidate all L2 cache entries for a relation.
 * Called on DELETE/TRUNCATE/DROP. Also clears CHAIN_BIT_L2 in chain index.
 */
void xpatch_l2_cache_invalidate_rel(Oid relid);

/*
 * Check if L2 cache is initialized and available.
 */
bool xpatch_l2_cache_is_ready(void);

/*
 * L2 cache statistics (aggregated across all stripes).
 */
typedef struct XPatchL2CacheStats
{
    int64       size_bytes;         /* Current size in bytes */
    int64       max_bytes;          /* Maximum size */
    int64       entries_count;      /* Number of entries */
    int64       hit_count;          /* Cache hits */
    int64       miss_count;         /* Cache misses */
    int64       eviction_count;     /* Evictions */
    int64       skip_count;         /* Entries rejected by size limit */
} XPatchL2CacheStats;

void xpatch_l2_cache_get_stats(XPatchL2CacheStats *stats);

#endif /* XPATCH_L2_CACHE_H */
