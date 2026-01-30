/*
 * pg-xpatch - PostgreSQL Table Access Method for delta-compressed data
 * Copyright (c) 2025 Oliver Seifert
 *
 * xpatch_stats_cache.h - Stats cache for O(1) lookups
 *
 * Provides cached statistics for xpatch tables, stored in xpatch.group_stats
 * and xpatch.table_stats catalog tables. Updated incrementally on INSERT,
 * invalidated on DELETE.
 */

#ifndef XPATCH_STATS_CACHE_H
#define XPATCH_STATS_CACHE_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

/*
 * Update group stats after a successful INSERT.
 * Called from the TAM insert path.
 *
 * Parameters:
 *   relid           - Table OID
 *   group_hash      - BLAKE3 hash of group value
 *   group_value_text - Human-readable group value (can be NULL)
 *   is_keyframe     - True if this row is a keyframe
 *   max_seq         - New max sequence number for this group
 *   max_version_typid - Type OID of order_by column
 *   max_version_data - Serialized max version value (can be NULL)
 *   max_version_len  - Length of max_version_data
 *   raw_size        - Uncompressed size of this row's delta columns
 *   compressed_size - Compressed size of this row's delta columns
 */
void xpatch_stats_cache_update_group(
    Oid relid,
    XPatchGroupHash group_hash,
    const char *group_value_text,
    bool is_keyframe,
    int32 max_seq,
    Oid max_version_typid,
    const uint8 *max_version_data,
    Size max_version_len,
    int64 raw_size,
    int64 compressed_size
);

/*
 * Invalidate group stats after DELETE.
 * The stats will be recomputed on next read or explicit refresh.
 */
void xpatch_stats_cache_invalidate_group(Oid relid, XPatchGroupHash group_hash);

/*
 * Invalidate all stats for a table (called on TRUNCATE).
 */
void xpatch_stats_cache_invalidate_table(Oid relid);

/*
 * Get max_seq for a group from cache.
 * Returns -1 if not found or invalid.
 */
int32 xpatch_stats_cache_get_max_seq(Oid relid, XPatchGroupHash group_hash);

/*
 * Check if table stats are valid (no invalidated groups).
 */
bool xpatch_stats_cache_is_valid(Oid relid);

#endif /* XPATCH_STATS_CACHE_H */
