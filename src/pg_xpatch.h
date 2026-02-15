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
 * pg_xpatch.h - Main header for pg_xpatch extension
 *
 * PostgreSQL Table Access Method for delta-compressed versioned data.
 */

#ifndef PG_XPATCH_H
#define PG_XPATCH_H

#include "postgres.h"
#include "access/tableam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "commands/vacuum.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Extension version */
#define PG_XPATCH_VERSION "0.5.1"

/* Default configuration values */
#define XPATCH_DEFAULT_KEYFRAME_EVERY   100
#define XPATCH_DEFAULT_COMPRESS_DEPTH   1
#define XPATCH_DEFAULT_ENABLE_ZSTD      true
#define XPATCH_DEFAULT_CACHE_SIZE_MB    64
#define XPATCH_DEFAULT_MAX_ENTRY_KB     256

/* GUC variables (declared in pg_xpatch.c) */
extern int xpatch_cache_size_mb;
extern int xpatch_cache_max_entry_kb;

/* GUC variables for insert cache (declared in xpatch_insert_cache.c) */
extern int xpatch_insert_cache_slots;

/* GUC variables for encode pool (declared in xpatch_encode_pool.c) */
extern int xpatch_encode_threads;

#endif /* PG_XPATCH_H */
