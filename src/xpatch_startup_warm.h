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
 * xpatch_startup_warm.h -- Multi-database startup warming background worker
 *
 * Architecture:
 *
 * 1. A static coordinator BGW starts after recovery, connects to "postgres"
 * 2. It enumerates all connectable databases via pg_database
 * 3. For each database it launches a dynamic per-DB BGW via
 *    RegisterDynamicBackgroundWorker (passing the database OID as main_arg)
 * 4. Each per-DB worker connects to its database and warms all xpatch tables:
 *      a) Direct-buffer scan: build chain index + populate L2
 *      b) SPI scan of L3 PKs: set CHAIN_BIT_L3
 * 5. The coordinator waits for all per-DB workers to finish, then exits
 *
 * Graceful degradation:
 * - No connection blocking.  Queries arriving before warming finishes
 *   fall back to the recursive disk reconstruction path (correct but slower).
 * - As warming progresses, subsequent queries benefit automatically.
 */

#ifndef XPATCH_STARTUP_WARM_H
#define XPATCH_STARTUP_WARM_H

#include "pg_xpatch.h"

/*
 * Register the startup warming coordinator BGW.
 * Must be called from _PG_init() during shared_preload_libraries.
 */
void xpatch_startup_warm_register_bgw(void);

/*
 * Coordinator BGW entry point (static worker, connects to "postgres").
 */
PGDLLEXPORT void xpatch_startup_warm_worker_main(Datum main_arg);

/*
 * Per-database worker entry point (dynamic worker).
 * main_arg is the database OID to connect to.
 */
PGDLLEXPORT void xpatch_startup_warm_db_worker_main(Datum main_arg);

#endif /* XPATCH_STARTUP_WARM_H */
