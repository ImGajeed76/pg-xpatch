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
 * xpatch_compress.c - Compression/decompression wrappers
 *
 * Bridges between PostgreSQL and the libxpatch_c library.
 */

#include "xpatch_compress.h"
#include "xpatch.h"  /* Generated header from libxpatch_c */

/* Type aliases for cleaner code */
typedef struct xpatch_XPatchBuffer XPatchBuffer;
typedef struct xpatch_XPatchResult XPatchResult;

/*
 * Encode content as a delta against a base
 */
bytea *
xpatch_encode_delta(size_t tag,
                    const uint8 *base_data, size_t base_len,
                    const uint8 *new_data, size_t new_len,
                    bool enable_zstd)
{
    XPatchBuffer result;
    bytea *output;

    /* Call the xpatch library */
    result = xpatch_encode(tag,
                           base_data, base_len,
                           new_data, new_len,
                           enable_zstd);

    if (result.data == NULL)
    {
        elog(WARNING, "xpatch_encode returned NULL");
        return NULL;
    }

    /* Copy result to PostgreSQL memory */
    output = (bytea *) palloc(VARHDRSZ + result.len);
    SET_VARSIZE(output, VARHDRSZ + result.len);
    memcpy(VARDATA(output), result.data, result.len);

    /* Free the xpatch-allocated buffer */
    xpatch_free_buffer(result);

    return output;
}

/*
 * Decode a delta to reconstruct content
 */
bytea *
xpatch_decode_delta(const uint8 *base_data, size_t base_len,
                    const uint8 *delta, size_t delta_len)
{
    XPatchResult result;
    bytea *output;

    /* Call the xpatch library */
    result = xpatch_decode(base_data, base_len, delta, delta_len);

    /* Check for errors */
    if (result.error_message != NULL)
    {
        char err_buf[256];
        snprintf(err_buf, sizeof(err_buf), "xpatch decode error: %s",
                 (char *) result.error_message);
        xpatch_free_error(result.error_message);

        ereport(ERROR,
                (errcode(ERRCODE_DATA_CORRUPTED),
                 errmsg("%s", err_buf)));
    }

    if (result.buffer.data == NULL && result.buffer.len == 0)
    {
        /* Empty result is valid (empty content) */
        output = (bytea *) palloc(VARHDRSZ);
        SET_VARSIZE(output, VARHDRSZ);
        return output;
    }

    /* Copy result to PostgreSQL memory */
    output = (bytea *) palloc(VARHDRSZ + result.buffer.len);
    SET_VARSIZE(output, VARHDRSZ + result.buffer.len);
    memcpy(VARDATA(output), result.buffer.data, result.buffer.len);

    /* Free the xpatch-allocated buffer */
    xpatch_free_buffer(result.buffer);

    return output;
}

/*
 * Extract the tag from a delta
 * 
 * Thread-safe: Returns palloc'd error string (caller must pfree if non-NULL)
 * or NULL on success.
 */
const char *
xpatch_get_delta_tag(const uint8 *delta, size_t delta_len, size_t *tag_out)
{
    int8_t *error;

    error = xpatch_get_tag(delta, delta_len, tag_out);

    if (error != NULL)
    {
        /* 
         * Allocate error in PostgreSQL memory context for thread safety.
         * Caller is responsible for checking and handling the error.
         * The error string lives until end of transaction/statement.
         */
        char *pg_error = pstrdup((char *) error);
        xpatch_free_error(error);
        return pg_error;
    }

    return NULL;
}

/*
 * Get the xpatch library version string
 */
const char *
xpatch_lib_version(void)
{
    return (const char *) xpatch_version();
}
