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
 * xpatch_compress.h - Compression/decompression wrappers
 *
 * Wraps the libxpatch_c library for use within PostgreSQL.
 */

#ifndef XPATCH_COMPRESS_H
#define XPATCH_COMPRESS_H

#include "pg_xpatch.h"

/*
 * Tag value conventions:
 *   tag=0: Keyframe (encoded against empty base)
 *   tag=1: Delta against previous row (1 row back)
 *   tag=2: Delta against 2 rows back
 *   tag=N: Delta against N rows back
 *
 * Tags 0-15 have zero overhead in xpatch encoding (fit in 4 bits).
 * Larger tags use varint encoding. Max tag is 16-bit (65535).
 * compress_depth is limited to 65535 (using tags 1-65535 for deltas).
 */
#define XPATCH_KEYFRAME_TAG         ((size_t) 0)
#define XPATCH_MAX_COMPRESS_DEPTH   65535

/*
 * Encode content as a delta against a base.
 *
 * Parameters:
 *   tag         - Tag value (0-15 have zero overhead)
 *   base_data   - Base content (NULL for keyframe)
 *   base_len    - Length of base content
 *   new_data    - New content to encode
 *   new_len     - Length of new content
 *   enable_zstd - Whether to enable zstd compression
 *
 * Returns a palloc'd bytea containing the encoded delta, or NULL on error.
 */
bytea *xpatch_encode_delta(size_t tag,
                           const uint8 *base_data, size_t base_len,
                           const uint8 *new_data, size_t new_len,
                           bool enable_zstd);

/*
 * Decode a delta to reconstruct content.
 *
 * Parameters:
 *   base_data - Base content (NULL if decoding a keyframe)
 *   base_len  - Length of base content
 *   delta     - Delta to decode
 *   delta_len - Length of delta
 *
 * Returns a palloc'd bytea containing the decoded content, or NULL on error.
 * Raises ERROR on decode failure.
 */
bytea *xpatch_decode_delta(const uint8 *base_data, size_t base_len,
                           const uint8 *delta, size_t delta_len);

/*
 * Extract the tag from a delta.
 *
 * Parameters:
 *   delta     - Delta data
 *   delta_len - Length of delta
 *   tag_out   - Output: extracted tag value
 *
 * Returns NULL on success, or a palloc'd error message string on failure.
 * The error string lives until end of memory context (typically transaction).
 */
const char *xpatch_get_delta_tag(const uint8 *delta, size_t delta_len,
                                 size_t *tag_out);

/*
 * Get the xpatch library version string.
 */
const char *xpatch_lib_version(void);

#endif /* XPATCH_COMPRESS_H */
