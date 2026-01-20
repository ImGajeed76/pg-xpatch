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
 * xpatch_storage.h - Physical tuple handling
 *
 * Converts between logical tuples (user view) and physical tuples
 * (actual storage with delta-compressed columns).
 */

#ifndef XPATCH_STORAGE_H
#define XPATCH_STORAGE_H

#include "pg_xpatch.h"
#include "xpatch_config.h"

/*
 * Convert a logical tuple (from user INSERT) to physical format.
 * This performs delta compression on configured columns.
 *
 * Returns a HeapTuple that must be freed with heap_freetuple().
 */
HeapTuple xpatch_logical_to_physical(Relation rel, XPatchConfig *config,
                                     TupleTableSlot *slot);

/*
 * Convert a physical tuple to logical format.
 * This reconstructs delta-compressed columns.
 *
 * Populates the provided slot with reconstructed values.
 */
void xpatch_physical_to_logical(Relation rel, XPatchConfig *config,
                                HeapTuple physical_tuple,
                                TupleTableSlot *slot);

/*
 * Get the maximum sequence number for a group.
 * Returns 0 if the group is empty.
 */
int32 xpatch_get_max_seq(Relation rel, XPatchConfig *config, Datum group_value);

/*
 * Get the maximum version value for a group.
 * Used to validate that new versions are strictly increasing.
 */
Datum xpatch_get_max_version(Relation rel, XPatchConfig *config,
                             Datum group_value, bool *is_null);

/*
 * Fetch a physical row by group and sequence number.
 * Returns NULL if not found.
 */
HeapTuple xpatch_fetch_by_seq(Relation rel, XPatchConfig *config,
                              Datum group_value, int32 seq);

/*
 * Reconstruct the content of a delta column for a specific version.
 * This may recursively fetch and decode previous versions.
 *
 * Returns a palloc'd bytea value.
 */
bytea *xpatch_reconstruct_column(Relation rel, XPatchConfig *config,
                                 Datum group_value, int32 seq,
                                 int delta_col_index);

/*
 * Reconstruct a delta column when we already have the physical tuple.
 * This is the fast path - avoids re-fetching the tuple we already have.
 *
 * Returns a palloc'd bytea value.
 */
bytea *xpatch_reconstruct_column_with_tuple(Relation rel, XPatchConfig *config,
                                            HeapTuple physical_tuple,
                                            Datum group_value, int32 seq,
                                            int delta_col_index);

/*
 * Convert bytea back to original Datum type.
 * Used for reconstructing delta columns.
 */
Datum bytea_to_datum(bytea *data, Oid typid);

/*
 * Compare two Datums for equality using the type's equality operator.
 * This handles collation-sensitive types like TEXT correctly.
 * typid is the OID of the type being compared.
 * collation is the collation OID (use DEFAULT_COLLATION_OID if unknown).
 *
 * Returns true if values are equal, false otherwise.
 */
bool xpatch_datums_equal(Datum d1, Datum d2, Oid typid, Oid collation);

#endif /* XPATCH_STORAGE_H */
