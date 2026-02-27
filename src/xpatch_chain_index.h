/*
 * xpatch_chain_index.h — Always-on in-memory chain index for path planning
 *
 * The chain index stores one 4-byte entry per version per delta column per
 * group per xpatch table. It tracks:
 *   - base_offset (3 bytes): the delta tag value (0 = keyframe)
 *   - cache_bits  (1 byte):  bitmap of which cache levels hold data
 *
 * Structure:
 *   - Group directory: DSA-backed hash table with striped LWLocks (16 stripes)
 *     Maps (relid, attnum, group_hash) → GroupChainHeader
 *   - Per-group entry arrays: DSA-allocated, indexed by (seq - base_seq)
 *     Auto-growing with 2x capacity doubling
 *
 * The chain index is always on, always complete (every version is tracked),
 * never evicted. It's populated at INSERT time and on startup warming.
 * The path planner reads it to find the cheapest reconstruction path.
 *
 * Locking:
 *   - Directory reads (chain walks): shared lock on stripe
 *   - Directory writes (INSERT/DELETE): exclusive lock on stripe
 *   - cache_bits updates: lockless single-byte atomic writes (x86)
 */

#ifndef XPATCH_CHAIN_INDEX_H
#define XPATCH_CHAIN_INDEX_H

#include "pg_xpatch.h"
#include "xpatch_hash.h"

/* ---------------------------------------------------------------------------
 * Cache level bitmap (1 byte per entry)
 * ---------------------------------------------------------------------------
 * A version can exist in multiple cache levels simultaneously.
 */
#define CHAIN_BIT_DISK  0x01    /* Compressed delta in xpatch heap table */
#define CHAIN_BIT_L1    0x02    /* Decompressed content in L1 shmem cache */
#define CHAIN_BIT_L2    0x04    /* Compressed delta in L2 shmem cache */
#define CHAIN_BIT_L3    0x08    /* Decompressed content in L3 disk table */

/* ---------------------------------------------------------------------------
 * Path planner cost constants (calibrated from benchmarks)
 * ---------------------------------------------------------------------------
 * Tuned for typical 1-4KB content. When enable_zstd is true, L2 costs
 * are multiplied by COST_ZSTD_MULTIPLIER.
 */
#define COST_L1_NS              200     /* L1 shmem fetch (~4KB) */
#define COST_L2_APPLY_NS        150     /* Delta decode (~4KB, no zstd) */
#define COST_L2_KEYFRAME_NS     80      /* Keyframe decode (~4KB) */
#define COST_L3_NS              22000   /* SPI PK lookup (buffer-warm) */
#define COST_DISK_NS            22000   /* SPI PK lookup (same path) */
#define COST_ZSTD_MULTIPLIER    2       /* Applied to L2 costs when zstd on */

/* ---------------------------------------------------------------------------
 * Chain index entry (4 bytes, naturally aligned)
 * ---------------------------------------------------------------------------
 * base_offset: delta tag value. 0 = keyframe, N = delta against N seqs back.
 * cache_bits:  bitmap of cache levels holding data for this version.
 *
 * Sentinel: base_offset=0 AND cache_bits=0 means unused slot (gap).
 * Keyframe: base_offset=0 AND cache_bits has at least CHAIN_BIT_DISK set.
 */
typedef struct ChainIndexEntry
{
    uint8   base_offset_lo;     /* bits 0-7 of base_offset (delta tag) */
    uint8   base_offset_mid;    /* bits 8-15 */
    uint8   base_offset_hi;     /* bits 16-23 */
    uint8   cache_bits;         /* bitmap: DISK | L1 | L2 | L3 */
} ChainIndexEntry;

/* Pack/unpack helpers for the 3-byte base_offset */
static inline uint32
chain_entry_get_base_offset(const ChainIndexEntry *e)
{
    return (uint32)e->base_offset_lo |
           ((uint32)e->base_offset_mid << 8) |
           ((uint32)e->base_offset_hi << 16);
}

static inline void
chain_entry_set_base_offset(ChainIndexEntry *e, uint32 offset)
{
    e->base_offset_lo  = (uint8)(offset & 0xFF);
    e->base_offset_mid = (uint8)((offset >> 8) & 0xFF);
    e->base_offset_hi  = (uint8)((offset >> 16) & 0xFF);
}

static inline bool
chain_entry_is_sentinel(const ChainIndexEntry *e)
{
    return e->base_offset_lo == 0 &&
           e->base_offset_mid == 0 &&
           e->base_offset_hi == 0 &&
           e->cache_bits == 0;
}

/* ---------------------------------------------------------------------------
 * Group chain header — stored in the directory hash table
 * ---------------------------------------------------------------------------
 * One per (relid, attnum, group_hash) combination. Tracks the DSA-allocated
 * entry array and its bounds.
 */
typedef struct GroupChainHeader
{
    dsa_pointer     entries_ptr;    /* DSA → ChainIndexEntry[capacity] */
    int32           count;          /* Number of versions (including gaps) */
    int32           capacity;       /* Allocated array capacity */
    int64           base_seq;       /* Lowest _xp_seq in this group */
    int64           max_seq;        /* Highest _xp_seq in this group */
} GroupChainHeader;

/* ---------------------------------------------------------------------------
 * Directory key — identifies a group+column
 * ---------------------------------------------------------------------------
 */
typedef struct ChainGroupKey
{
    Oid             relid;
    AttrNumber      attnum;
    int16           padding;
    XPatchGroupHash group_hash;
} ChainGroupKey;

/* ---------------------------------------------------------------------------
 * Chain walk result — returned by chain_index_get_chain()
 * ---------------------------------------------------------------------------
 * Returns a snapshot of the chain from base_seq to max_seq for a group.
 * The entries array is palloc'd (caller must pfree) and contains copies
 * of the chain index entries. Index by (seq - base_seq).
 */
typedef struct ChainWalkResult
{
    ChainIndexEntry *entries;       /* palloc'd copy, caller frees */
    int32           count;          /* Number of entries */
    int64           base_seq;       /* Lowest seq in the chain */
    int64           max_seq;        /* Highest seq in the chain */
} ChainWalkResult;

/* ---------------------------------------------------------------------------
 * GUC variable
 * ---------------------------------------------------------------------------
 */
extern int xpatch_chain_index_initial_capacity;

/* ---------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------------
 */

/* Shmem hook registration — call from _PG_init after insert_cache */
extern void xpatch_chain_index_request_shmem(void);

/*
 * Insert a new entry into the chain index.
 * Called from the INSERT path after delta encoding is complete.
 *
 * base_offset: the delta tag (0 for keyframes, N for delta-against-N-back)
 * cache_bits:  initial bitmap (typically CHAIN_BIT_DISK, possibly | L1 | L2)
 */
extern void xpatch_chain_index_insert(Oid relid, XPatchGroupHash group_hash,
                                       AttrNumber attnum, int64 seq,
                                       uint32 base_offset, uint8 cache_bits);

/*
 * Look up a single entry. Returns false if not found.
 * entry_out is filled with a copy of the entry.
 */
extern bool xpatch_chain_index_lookup(Oid relid, XPatchGroupHash group_hash,
                                       AttrNumber attnum, int64 seq,
                                       ChainIndexEntry *entry_out);

/*
 * Get the full chain for a group+column.
 * Returns a palloc'd copy of the entry array (caller frees result->entries).
 * Returns false if the group is not found in the index.
 *
 * This is the main entry point for the path planner. Takes a shared lock
 * on the directory stripe for the duration of the copy (sub-microsecond).
 */
extern bool xpatch_chain_index_get_chain(Oid relid, XPatchGroupHash group_hash,
                                          AttrNumber attnum,
                                          ChainWalkResult *result);

/*
 * Update cache_bits for a specific entry. Lockless single-byte write.
 * Used by L1/L2/L3 caches on put/evict to keep the index current.
 *
 * set_bits:   bits to OR into cache_bits  (e.g., CHAIN_BIT_L1)
 * clear_bits: bits to AND-NOT from cache_bits (e.g., CHAIN_BIT_L1)
 *
 * Only one of set_bits/clear_bits should be non-zero per call.
 */
extern void xpatch_chain_index_update_bits(Oid relid, XPatchGroupHash group_hash,
                                            AttrNumber attnum, int64 seq,
                                            uint8 set_bits, uint8 clear_bits);

/*
 * Mark entries as sentinels for a seq range (DELETE path).
 * Marks all entries where seq >= from_seq in this group+column.
 */
extern void xpatch_chain_index_delete(Oid relid, XPatchGroupHash group_hash,
                                       AttrNumber attnum, int64 from_seq);

/*
 * Invalidate all entries for a relation (TRUNCATE/DROP).
 * Frees all DSA memory for the relation's groups.
 */
extern void xpatch_chain_index_invalidate_rel(Oid relid);

/*
 * Check if chain index is initialized and ready.
 * Returns false during early startup before shmem hooks have run.
 */
extern bool xpatch_chain_index_is_ready(void);

#endif /* XPATCH_CHAIN_INDEX_H */
