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
 * xpatch_startup_warm.h -- Startup warming background worker
 *
 * Registers a one-shot static background worker that runs once after
 * recovery finishes (BGW_NEVER_RESTART). The worker connects to the
 * "postgres" database and performs a single-pass scan of every xpatch
 * table found in xpatch.table_config, building:
 *
 *   1. Chain index -- insert(relid, group_hash, attnum, seq, base_offset,
 *                           CHAIN_BIT_DISK)
 *   2. L2 cache    -- put(relid, group_hash, seq, attnum, compressed_blob),
 *                     which also sets CHAIN_BIT_L2
 *
 * After the table scan, if L3 is enabled for the table, the worker scans
 * the L3 table primary keys via SPI and sets CHAIN_BIT_L3 in the chain
 * index.
 *
 * The worker is interruptible (CHECK_FOR_INTERRUPTS between blocks) and
 * handles SIGTERM for clean shutdown.
 *
 * No GUCs -- the worker is always registered when the extension is loaded
 * via shared_preload_libraries.
 */

#ifndef XPATCH_STARTUP_WARM_H
#define XPATCH_STARTUP_WARM_H

#include "pg_xpatch.h"

/*
 * Register the startup warming background worker.
 *
 * Must be called from _PG_init() during shared_preload_libraries.
 * The worker starts once after recovery finishes and never restarts.
 */
void xpatch_startup_warm_register_bgw(void);

/*
 * Background worker entry point.
 * Exported so PostgreSQL can call it via bgw_function_name.
 */
PGDLLEXPORT void xpatch_startup_warm_worker_main(Datum main_arg);

#endif /* XPATCH_STARTUP_WARM_H */
