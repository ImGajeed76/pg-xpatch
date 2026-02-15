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
 * xpatch_cache.h - Shared LRU cache for decoded content
 *
 * Implements a shared memory LRU cache that stores reconstructed
 * delta column content to avoid repeated decompression.
 */

#ifndef XPATCH_CACHE_H
#define XPATCH_CACHE_H

#include "pg_xpatch.h"

/*
 * Request shared memory space for the cache.
 * Must be called from _PG_init() before shmem_startup_hook.
 */
void xpatch_cache_request_shmem(void);

/*
 * Initialize the cache in shared memory.
 * Called from shmem_startup_hook.
 */
void xpatch_cache_init(void);

/*
 * Look up content in the cache.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value
 *   typid       - Type OID of the group column (for proper hashing)
 *   seq         - Sequence number
 *   attnum      - Attribute number of delta column
 *
 * Returns a palloc'd copy of the cached content, or NULL if not found.
 */
bytea *xpatch_cache_get(Oid relid, Datum group_value, Oid typid, int64 seq,
                        AttrNumber attnum);

/*
 * Store content in the cache.
 *
 * Parameters:
 *   relid       - Relation OID
 *   group_value - Group column value
 *   typid       - Type OID of the group column (for proper hashing)
 *   seq         - Sequence number
 *   attnum      - Attribute number of delta column
 *   content     - Content to cache (will be copied)
 */
void xpatch_cache_put(Oid relid, Datum group_value, Oid typid, int64 seq,
                      AttrNumber attnum, bytea *content);

/*
 * Invalidate all cache entries for a relation.
 * Called when a relation is dropped or truncated.
 */
void xpatch_cache_invalidate_rel(Oid relid);

/*
 * Get cache statistics.
 */
typedef struct XPatchCacheStats
{
    int64       size_bytes;         /* Current size in bytes */
    int64       max_bytes;          /* Maximum size */
    int64       entries_count;      /* Number of entries */
    int64       hit_count;          /* Cache hits */
    int64       miss_count;         /* Cache misses */
    int64       eviction_count;     /* Evictions */
    int64       skip_count;         /* Entries rejected by size limit */
} XPatchCacheStats;

void xpatch_cache_get_stats(XPatchCacheStats *stats);

#endif /* XPATCH_CACHE_H */
