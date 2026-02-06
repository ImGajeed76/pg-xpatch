/*
 * pg-xpatch - PostgreSQL Table Access Method for delta-compressed data
 * Copyright (c) 2025 Oliver Seifert
 *
 * xpatch_stats_cache.h - Stats cache for O(1) lookups
 *
 * Provides cached statistics for xpatch tables, stored in xpatch.group_stats.
 * Updated incrementally on INSERT and DELETE.
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
 *   is_keyframe     - True if this row is a keyframe
 *   max_seq         - New sequence number for this row
 *   raw_size        - Uncompressed size of this row's delta columns
 *   compressed_size - Compressed size of this row's delta columns
 *   avg_delta_tag   - Average tag across delta columns (0 for keyframe)
 */
void xpatch_stats_cache_update_group(
    Oid relid,
    XPatchGroupHash group_hash,
    bool is_keyframe,
    int64 max_seq,
    int64 raw_size,
    int64 compressed_size,
    double avg_delta_tag
);

/*
 * Delete stats for a specific group.
 * Used internally; prefer refresh_groups for DELETE operations.
 */
void xpatch_stats_cache_delete_group(Oid relid, XPatchGroupHash group_hash);

/*
 * Delete all stats for a table (called on TRUNCATE).
 */
void xpatch_stats_cache_delete_table(Oid relid);

/*
 * Get max_seq for a group from stats cache.
 * Returns -1 if not found.
 */
int64 xpatch_stats_cache_get_max_seq(Oid relid, XPatchGroupHash group_hash);

/*
 * Check if stats exist for a table.
 */
bool xpatch_stats_cache_exists(Oid relid);

/*
 * Get aggregated stats for a table from cache.
 * Returns true if stats exist, false if cache miss.
 */
bool xpatch_stats_cache_get_table_stats(
    Oid relid,
    int64 *total_rows,
    int64 *total_groups,
    int64 *keyframe_count,
    int64 *raw_size_bytes,
    int64 *compressed_size_bytes,
    double *sum_avg_delta_tags
);

/*
 * Refresh stats for specific groups that are missing from cache.
 * Scans only the rows belonging to those groups.
 * 
 * Parameters:
 *   relid        - Table OID
 *   group_hashes - Array of group hashes to refresh (NULL-terminated bytea array)
 *   num_groups   - Number of groups to refresh
 *
 * Returns the number of rows scanned.
 */
int64 xpatch_stats_cache_refresh_groups(Oid relid, XPatchGroupHash *group_hashes, int num_groups);

#endif /* XPATCH_STATS_CACHE_H */
