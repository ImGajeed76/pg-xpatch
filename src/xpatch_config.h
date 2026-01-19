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
 * xpatch_config.h - Configuration parsing and storage
 *
 * Uses auto-detection by default, with optional explicit configuration
 * via xpatch.table_config catalog table (populated by xpatch.configure()).
 */

#ifndef XPATCH_CONFIG_H
#define XPATCH_CONFIG_H

#include "pg_xpatch.h"

/*
 * Configuration for an xpatch table.
 * Auto-detected or read from xpatch.table_config catalog, cached per-relation.
 */
typedef struct XPatchConfig
{
    /* Column identifiers (from catalog or auto-detection) */
    char       *group_by;           /* Column for grouping (optional, NULL if none) */
    char       *order_by;           /* Column for ordering (required) */
    char      **delta_columns;      /* Array of delta column names */
    int         num_delta_columns;

    /* Compression settings */
    int         keyframe_every;     /* Create keyframe every N rows */
    int         compress_depth;     /* How many previous versions to try */
    bool        enable_zstd;        /* Enable zstd on top of delta encoding */

    /* Resolved attribute numbers (populated on first use) */
    AttrNumber  group_by_attnum;    /* InvalidAttrNumber if no group_by */
    AttrNumber  order_by_attnum;
    AttrNumber *delta_attnums;      /* Array of attribute numbers */

    /* Physical column mapping */
    AttrNumber  xp_seq_attnum;      /* _xp_seq column */
} XPatchConfig;

/*
 * Get the configuration for an xpatch table.
 * Returns cached config, auto-detecting or reading from catalog as needed.
 */
XPatchConfig *xpatch_get_config(Relation rel);

/*
 * Free configuration structure.
 */
void xpatch_free_config(XPatchConfig *config);

/*
 * Parse/detect configuration for a relation.
 * Called internally by xpatch_get_config.
 */
XPatchConfig *xpatch_parse_reloptions(Relation rel);

/*
 * Validate that a table schema is compatible with xpatch.
 * Raises ERROR if validation fails.
 */
void xpatch_validate_schema(Relation rel, XPatchConfig *config);

/*
 * Initialize xpatch config subsystem.
 * Called during _PG_init. No-op since we use auto-detection.
 */
void xpatch_init_reloptions(void);

/*
 * Invalidate cached config for a relation.
 * Called when a table is dropped or altered.
 */
void xpatch_invalidate_config(Oid relid);

#endif /* XPATCH_CONFIG_H */
