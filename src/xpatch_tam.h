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
 * xpatch_tam.h - Table Access Method declarations
 *
 * Declares the TAM routine structure and all callback functions.
 */

#ifndef XPATCH_TAM_H
#define XPATCH_TAM_H

#include "pg_xpatch.h"

/*
 * Get the TableAmRoutine for the xpatch access method.
 * Called by xpatch_tam_handler() in pg_xpatch.c
 */
const TableAmRoutine *xpatch_get_table_am_routine(void);

#endif /* XPATCH_TAM_H */
