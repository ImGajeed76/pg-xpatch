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
 * xpatch_hash.h - BLAKE3-based hashing for group keys
 *
 * Provides 128-bit BLAKE3 hashing for group column values.
 * Used by both the content cache and the sequence cache to create
 * collision-resistant keys from any PostgreSQL data type.
 *
 * BLAKE3 provides:
 * - ~2^64 collision resistance (birthday bound on 128-bit output)
 * - Excellent distribution for any input type
 * - Fast computation with SIMD acceleration (SSE2/AVX2/AVX512/NEON)
 */

#ifndef XPATCH_HASH_H
#define XPATCH_HASH_H

#include "postgres.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "blake3.h"

/*
 * 128-bit group hash for collision-resistant key storage.
 */
typedef struct XPatchGroupHash
{
    uint64  h1;     /* First 64 bits of BLAKE3 output */
    uint64  h2;     /* Second 64 bits of BLAKE3 output */
} XPatchGroupHash;

/*
 * Compute 128-bit BLAKE3 hash of a group value.
 * Handles both pass-by-value and pass-by-reference Datum types.
 *
 * Parameters:
 *   group_value - The Datum to hash
 *   typid       - Type OID of the value (used to determine pass-by-value/ref)
 *   isnull      - True if the value is NULL
 *
 * Returns:
 *   128-bit hash stored in XPatchGroupHash struct
 */
static inline XPatchGroupHash
xpatch_compute_group_hash(Datum group_value, Oid typid, bool isnull)
{
    XPatchGroupHash result;
    blake3_hasher hasher;
    uint8_t output[16];  /* 128 bits = 16 bytes */
    const unsigned char *data;
    Size len;
    
    if (isnull || typid == InvalidOid)
    {
        /* NULL group or no group_by column - use a fixed hash (all zeros) */
        result.h1 = 0;
        result.h2 = 0;
        return result;
    }
    
    /* Determine data pointer and length based on type */
    switch (typid)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
        case FLOAT4OID:
        case FLOAT8OID:
        case BOOLOID:
        case CHAROID:
            /* Pass-by-value types: hash the Datum directly */
            data = (const unsigned char *) &group_value;
            len = sizeof(Datum);
            break;
            
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        case BYTEAOID:
        case NAMEOID:
            /* Variable-length types: detoast and hash the content */
            {
                struct varlena *val = PG_DETOAST_DATUM_PACKED(group_value);
                Pointer orig = DatumGetPointer(group_value);
                data = (const unsigned char *) VARDATA_ANY(val);
                len = VARSIZE_ANY_EXHDR(val);
                
                /* Compute hash before potentially freeing detoasted copy */
                blake3_hasher_init(&hasher);
                blake3_hasher_update(&hasher, data, len);
                blake3_hasher_finalize(&hasher, output, sizeof(output));
                
                /* Free detoasted copy if one was made */
                if ((Pointer)val != orig)
                    pfree(val);
                
                /* Copy to result */
                memcpy(&result.h1, output, 8);
                memcpy(&result.h2, output + 8, 8);
                
                return result;
            }
            break;
            
        case UUIDOID:
            /* UUID is 16 bytes, fixed size but pass-by-reference */
            data = (const unsigned char *) DatumGetPointer(group_value);
            len = 16;
            break;
            
        default:
            {
                /* Handle both pass-by-value and pass-by-reference types correctly.
                 * For pass-by-value types, hash the Datum value directly.
                 * For pass-by-reference types (e.g. NUMERIC), detoast and hash
                 * the actual varlena content to avoid hashing pointer addresses (M1 fix). */
                bool typbyval;
                int16 typlen;
                
                get_typlenbyval(typid, &typlen, &typbyval);
                
                if (typbyval)
                {
                    /* Pass-by-value: hash the Datum directly */
                    data = (const unsigned char *) &group_value;
                    len = sizeof(Datum);
                }
                else if (typlen == -1)
                {
                    /* Variable-length (varlena) pass-by-ref type like NUMERIC */
                    struct varlena *val = PG_DETOAST_DATUM_PACKED(group_value);
                    Pointer orig = DatumGetPointer(group_value);
                    data = (const unsigned char *) VARDATA_ANY(val);
                    len = VARSIZE_ANY_EXHDR(val);
                    
                    blake3_hasher_init(&hasher);
                    blake3_hasher_update(&hasher, data, len);
                    blake3_hasher_finalize(&hasher, output, sizeof(output));
                    
                    if ((Pointer)val != orig)
                        pfree(val);
                    
                    memcpy(&result.h1, output, 8);
                    memcpy(&result.h2, output + 8, 8);
                    return result;
                }
                else if (typlen == -2)
                {
                    /* C-string type */
                    data = (const unsigned char *) DatumGetCString(group_value);
                    len = strlen((const char *) data);
                }
                else
                {
                    /* Fixed-length pass-by-ref type */
                    data = (const unsigned char *) DatumGetPointer(group_value);
                    len = typlen;
                }
            }
            break;
    }
    
    /* Compute BLAKE3 hash (128 bits) */
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, data, len);
    blake3_hasher_finalize(&hasher, output, sizeof(output));
    
    /* Extract two 64-bit values from the hash output (little-endian) */
    memcpy(&result.h1, output, sizeof(uint64));
    memcpy(&result.h2, output + sizeof(uint64), sizeof(uint64));
    
    return result;
}

/* Compare two group hashes for equality */
static inline bool
xpatch_group_hash_equals(XPatchGroupHash a, XPatchGroupHash b)
{
    return a.h1 == b.h1 && a.h2 == b.h2;
}

/* Hash a group hash to a 32-bit value for hash table indexing */
static inline uint32
xpatch_group_hash_to_uint32(XPatchGroupHash hash, uint32 max_entries)
{
    /* Use FNV-1a to combine the two 64-bit values */
    uint32 h = 2166136261u;
    h ^= (uint32) (hash.h1 & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (hash.h1 >> 32);
    h *= 16777619u;
    h ^= (uint32) (hash.h2 & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (hash.h2 >> 32);
    h *= 16777619u;
    
    return h % max_entries;
}

/*
 * Compute a 64-bit lock ID for group-level advisory locking.
 * Combines relation OID with group hash to create unique lock ID per group.
 * Used to prevent concurrent modifications to the same group.
 *
 * Takes XPatchGroupHash (already computed via BLAKE3) rather than raw Datum
 * to ensure consistent hashing for all group value types.
 */
static inline uint64
xpatch_compute_group_lock_id(Oid relid, XPatchGroupHash group_hash)
{
    /* Combine relid with the BLAKE3 group hash */
    uint64 h = group_hash.h1;
    h ^= (uint64) relid;
    h ^= group_hash.h2;
    return h;
}

#endif /* XPATCH_HASH_H */
