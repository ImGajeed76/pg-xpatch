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
 * xpatch_l3_cache.h — Persistent disk cache (L3) for decompressed content
 *
 * L3 stores decompressed column content in per-table PostgreSQL heap tables
 * in the xpatch schema. It survives restarts and is populated:
 *   - On reconstruction (when path planner produces decompressed content)
 *   - On INSERT/COPY (content is already available)
 *
 * Schema per xpatch table (e.g., for table "my_table"):
 *
 *   CREATE TABLE xpatch.my_table_xp_l3 (
 *       group_hash_h1   int8        NOT NULL,
 *       group_hash_h2   int8        NOT NULL,
 *       seq             int8        NOT NULL,
 *       attnum          int2        NOT NULL,
 *       content         bytea,
 *       cached_at       timestamptz NOT NULL DEFAULT now(),
 *       PRIMARY KEY (group_hash_h1, group_hash_h2, seq, attnum)
 *   );
 *   CREATE INDEX ON xpatch.my_table_xp_l3 (cached_at);
 *
 * L3 is enabled per-table via xpatch.configure():
 *   SELECT xpatch.configure('my_table', l3_cache_enabled => true);
 *
 * Configuration is stored in xpatch.table_config:
 *   l3_cache_enabled    BOOLEAN DEFAULT false
 *   l3_cache_max_size_mb INT DEFAULT 1024
 *
 * All L3 operations use SPI for catalog access. Callers must be in a
 * valid transaction context (which is always the case during tuple
 * deform / scan / INSERT).
 */

#ifndef XPATCH_L3_CACHE_H
#define XPATCH_L3_CACHE_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

/*
 * Get decompressed content from L3 cache.
 *
 * Returns a palloc'd bytea with the decompressed content, or NULL if
 * not cached. The caller takes ownership of the returned bytea.
 *
 * Uses SPI to SELECT from the L3 table. If the L3 table doesn't exist
 * or L3 is not enabled for this table, returns NULL.
 */
bytea *xpatch_l3_cache_get(Oid relid, XPatchGroupHash group_hash,
                           int64 seq, AttrNumber attnum);

/*
 * Store decompressed content in L3 cache.
 *
 * Inserts or updates the L3 table with the given content. If the L3
 * table doesn't exist yet, creates it (with IF NOT EXISTS + advisory
 * lock to handle concurrent creation).
 *
 * No-op if L3 is not enabled for this table.
 *
 * Uses SPI for the INSERT. Content is stored as bytea.
 */
void xpatch_l3_cache_put(Oid relid, XPatchGroupHash group_hash,
                         int64 seq, AttrNumber attnum,
                         bytea *content);

/*
 * Invalidate (delete) all L3 entries for a relation.
 *
 * Called on table DROP or TRUNCATE. Drops the entire L3 table if it
 * exists, and clears CHAIN_BIT_L3 for all entries in the chain index.
 */
void xpatch_l3_cache_invalidate_rel(Oid relid);

/*
 * Invalidate a specific L3 entry.
 *
 * Called on row DELETE. Removes the specific entry from the L3 table.
 */
void xpatch_l3_cache_invalidate(Oid relid, XPatchGroupHash group_hash,
                                int64 seq, AttrNumber attnum);

/*
 * Invalidate L3 entries for a group with seq >= from_seq.
 *
 * Called on cascade DELETE. Removes matching entries from the L3 table.
 */
void xpatch_l3_cache_invalidate_group(Oid relid, XPatchGroupHash group_hash,
                                       int64 from_seq);

/*
 * Drop the L3 cache table for a relation.
 *
 * Called by xpatch.drop_l3_cache() SQL function. Drops the L3 table
 * and clears all L3 bits in the chain index.
 *
 * Returns true if the table existed and was dropped, false otherwise.
 */
bool xpatch_l3_cache_drop(Oid relid);

/*
 * Check if L3 cache is enabled for a relation.
 *
 * Reads the l3_cache_enabled flag from xpatch.table_config.
 * Caches the result per-backend to avoid repeated SPI calls.
 */
bool xpatch_l3_cache_is_enabled(Oid relid);

/*
 * Get the L3 cache table name for a relation.
 *
 * Returns a palloc'd string like "xpatch.my_table_xp_l3".
 * Returns NULL if the relation name cannot be resolved.
 */
char *xpatch_l3_cache_table_name(Oid relid);

/*
 * Ensure the L3 cache table exists for a relation.
 *
 * Creates the table if it doesn't exist. Uses advisory lock to
 * prevent concurrent creation by multiple backends.
 *
 * Returns true if the table exists (created or already existed),
 * false on failure.
 */
bool xpatch_l3_cache_ensure_table(Oid relid);

#endif /* XPATCH_L3_CACHE_H */
