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
 * xpatch_seq_cache.c - Shared caches for sequence number lookups
 *
 * Implements two fixed-size shared memory caches:
 * 1. Group Max Seq Cache - for INSERT optimization
 * 2. TID Seq Cache - for READ optimization
 *
 * Uses BLAKE3 for 128-bit hashing of group keys to support any PostgreSQL
 * data type (TEXT, UUID, BIGINT, etc.) with extremely low collision probability.
 */

#include "xpatch_seq_cache.h"
#include "xpatch_hash.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

/* Default cache sizes (configurable via GUC) */
int xpatch_group_cache_size_mb = 8;  /* 8MB default */
int xpatch_tid_cache_size_mb = 8;    /* 8MB default */
int xpatch_seq_tid_cache_size_mb = 8; /* 8MB default - for seq->TID lookups */

/*
 * Hash index special values:
 * 0 = empty (never used)
 * -1 = tombstone (was used, now deleted - continue probing)
 * >0 = entry_index + 1
 */
#define HASH_EMPTY      0
#define HASH_TOMBSTONE  (-1)

/* ================================================================
 * Group Max Seq Cache
 * ================================================================ */

/* Entry for group max seq cache */
typedef struct GroupSeqEntry
{
    /* Key - using 128-bit BLAKE3 hash for collision resistance */
    Oid             relid;
    XPatchGroupHash group_hash;     /* 128-bit hash of group value */
    
    /* Value */
    int64       max_seq;
    
    /* LRU list links */
    int32       lru_prev;       /* Previous entry in LRU (-1 = head) */
    int32       lru_next;       /* Next entry in LRU (-1 = tail) */
    int32       hash_slot;      /* Which hash slot points to this entry */
    bool        in_use;         /* Entry is valid */
    char        padding[3];
} GroupSeqEntry;

/* Calculate max entries based on size */
#define GROUP_ENTRY_SIZE    sizeof(GroupSeqEntry)

/* Header for group cache */
typedef struct GroupSeqCache
{
    LWLock         *lock;
    int32           num_entries;
    int32           max_entries;
    int32           hash_size;      /* Size of hash index array */
    int32           lru_head;       /* Most recently used entry index (-1 = empty) */
    int32           lru_tail;       /* Least recently used entry index (-1 = empty) */
    int32           free_head;      /* Free list head (-1 = none) */
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    pg_atomic_uint64 eviction_count;
    
    /* 
     * Hash index: hash_index[hash % hash_size] = entry index + 1 (0 = empty)
     * Followed by entries array
     */
} GroupSeqCache;

/* FNV-1a hash constants for combining into 32-bit hash slot index */
#define FNV_OFFSET_BASIS_32  2166136261U
#define FNV_PRIME_32         16777619U

/* Hash function for group cache key - uses 128-bit hash */
static uint32
hash_group_key(Oid relid, XPatchGroupHash group_hash)
{
    uint32 h = FNV_OFFSET_BASIS_32;
    unsigned char *p;
    int i;
    
    p = (unsigned char *) &relid;
    for (i = 0; i < (int) sizeof(Oid); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    /* Mix in the 128-bit group hash */
    p = (unsigned char *) &group_hash.h1;
    for (i = 0; i < (int) sizeof(uint64); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    p = (unsigned char *) &group_hash.h2;
    for (i = 0; i < (int) sizeof(uint64); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    return h;
}

/* ================================================================
 * TID Seq Cache
 * ================================================================ */

/* Entry for TID seq cache */
typedef struct TidSeqEntry
{
    /* Key */
    Oid             relid;
    ItemPointerData tid;
    
    /* Value */
    int64           seq;
    
    /* LRU list links */
    int32           lru_prev;       /* Previous entry in LRU (-1 = head) */
    int32           lru_next;       /* Next entry in LRU (-1 = tail) */
    int32           hash_slot;      /* Which hash slot points to this entry */
    bool            in_use;         /* Entry is valid */
    char            padding[3];
} TidSeqEntry;

#define TID_ENTRY_SIZE  sizeof(TidSeqEntry)

/* Header for TID cache */
typedef struct TidSeqCache
{
    LWLock         *lock;
    int32           num_entries;
    int32           max_entries;
    int32           hash_size;      /* Size of hash index array */
    int32           lru_head;       /* Most recently used entry index (-1 = empty) */
    int32           lru_tail;       /* Least recently used entry index (-1 = empty) */
    int32           free_head;      /* Free list head (-1 = none) */
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    pg_atomic_uint64 eviction_count;
    
    /*
     * Hash index: hash_index[hash % hash_size] = entry index + 1 (0 = empty)
     * Followed by entries array
     */
} TidSeqCache;

/* ================================================================
 * Seq-to-TID Cache (reverse lookup: group+seq -> TID)
 * ================================================================ */

/* Entry for seq-to-TID cache */
typedef struct SeqTidEntry
{
    /* Key - using 128-bit BLAKE3 hash for collision resistance */
    Oid             relid;
    XPatchGroupHash group_hash;     /* 128-bit hash of group value */
    int64           seq;            /* Sequence number */
    
    /* Value */
    ItemPointerData tid;            /* Physical location of the tuple */
    
    /* LRU list links */
    int32       lru_prev;       /* Previous entry in LRU (-1 = head) */
    int32       lru_next;       /* Next entry in LRU (-1 = tail) */
    int32       hash_slot;      /* Which hash slot points to this entry */
    bool        in_use;         /* Entry is valid */
    char        padding[3];
} SeqTidEntry;

#define SEQ_TID_ENTRY_SIZE  sizeof(SeqTidEntry)

/* Header for seq-to-TID cache */
typedef struct SeqTidCache
{
    LWLock         *lock;
    int32           num_entries;
    int32           max_entries;
    int32           hash_size;      /* Size of hash index array */
    int32           lru_head;       /* Most recently used entry index (-1 = empty) */
    int32           lru_tail;       /* Least recently used entry index (-1 = empty) */
    int32           free_head;      /* Free list head (-1 = none) */
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    pg_atomic_uint64 eviction_count;
    
    /*
     * Hash index: hash_index[hash % hash_size] = entry index + 1 (0 = empty)
     * Followed by entries array
     */
} SeqTidCache;

/* Hash function for seq-to-TID cache key */
static uint32
hash_seq_tid_key(Oid relid, XPatchGroupHash group_hash, int64 seq)
{
    uint32 h = FNV_OFFSET_BASIS_32;
    unsigned char *p;
    int i;
    
    p = (unsigned char *) &relid;
    for (i = 0; i < (int) sizeof(Oid); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    /* Mix in the 128-bit group hash */
    p = (unsigned char *) &group_hash.h1;
    for (i = 0; i < (int) sizeof(uint64); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    p = (unsigned char *) &group_hash.h2;
    for (i = 0; i < (int) sizeof(uint64); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    /* Mix in the sequence number */
    p = (unsigned char *) &seq;
    for (i = 0; i < (int) sizeof(int64); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    return h;
}

/* Hash function for TID cache key */
static uint32
hash_tid_key(Oid relid, ItemPointer tid)
{
    uint32 h = FNV_OFFSET_BASIS_32;
    unsigned char *p;
    int i;
    BlockNumber blk = ItemPointerGetBlockNumber(tid);
    OffsetNumber off = ItemPointerGetOffsetNumber(tid);
    
    p = (unsigned char *) &relid;
    for (i = 0; i < (int) sizeof(Oid); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    p = (unsigned char *) &blk;
    for (i = 0; i < (int) sizeof(BlockNumber); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    p = (unsigned char *) &off;
    for (i = 0; i < (int) sizeof(OffsetNumber); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    return h;
}

/* ================================================================
 * Shared Memory Globals
 * ================================================================ */

static GroupSeqCache *group_cache = NULL;
static TidSeqCache *tid_cache = NULL;
static SeqTidCache *seq_tid_cache = NULL;
static bool seq_cache_initialized = false;

/*
 * Backend exit callback - clears per-backend pointers to shared memory.
 * This is a defensive measure; PostgreSQL handles shared memory detachment
 * automatically, but clearing these helps catch bugs where code tries to
 * access the cache after the backend has started shutting down.
 */
static void
xpatch_seq_cache_shmem_exit(int code, Datum arg)
{
    group_cache = NULL;
    tid_cache = NULL;
    seq_tid_cache = NULL;
    seq_cache_initialized = false;
}

/* ================================================================
 * LRU List Management - Group Cache
 * ================================================================ */

/* Remove entry from group cache LRU list */
static void
group_lru_remove(GroupSeqEntry *entries, int32 entry_idx)
{
    GroupSeqEntry *entry = &entries[entry_idx];
    
    if (entry->lru_prev >= 0)
        entries[entry->lru_prev].lru_next = entry->lru_next;
    else
        group_cache->lru_head = entry->lru_next;
    
    if (entry->lru_next >= 0)
        entries[entry->lru_next].lru_prev = entry->lru_prev;
    else
        group_cache->lru_tail = entry->lru_prev;
    
    entry->lru_prev = -1;
    entry->lru_next = -1;
}

/* Add entry to front of group cache LRU list (most recently used) */
static void
group_lru_push_front(GroupSeqEntry *entries, int32 entry_idx)
{
    GroupSeqEntry *entry = &entries[entry_idx];
    
    entry->lru_prev = -1;
    entry->lru_next = group_cache->lru_head;
    
    if (group_cache->lru_head >= 0)
        entries[group_cache->lru_head].lru_prev = entry_idx;
    else
        group_cache->lru_tail = entry_idx;
    
    group_cache->lru_head = entry_idx;
}

/* Move entry to front of LRU (on access) */
static void
group_lru_touch(GroupSeqEntry *entries, int32 entry_idx)
{
    if (group_cache->lru_head == entry_idx)
        return;  /* Already at front */
    
    group_lru_remove(entries, entry_idx);
    group_lru_push_front(entries, entry_idx);
}

/* Evict least recently used entry from group cache */
static int32
group_evict_lru(int32 *hash_index, GroupSeqEntry *entries)
{
    int32 victim_idx = group_cache->lru_tail;
    GroupSeqEntry *victim;
    
    if (victim_idx < 0)
        return -1;
    
    victim = &entries[victim_idx];
    
    /* Remove from LRU list */
    group_lru_remove(entries, victim_idx);
    
    /* 
     * Mark slot as tombstone instead of empty.
     * This preserves the linear probe chain for lookups.
     */
    if (victim->hash_slot >= 0)
        hash_index[victim->hash_slot] = HASH_TOMBSTONE;
    
    /* Mark entry as free */
    victim->in_use = false;
    victim->hash_slot = -1;
    
    /* Add to free list */
    victim->lru_next = group_cache->free_head;
    group_cache->free_head = victim_idx;
    
    group_cache->num_entries--;
    pg_atomic_fetch_add_u64(&group_cache->eviction_count, 1);
    
    return victim_idx;
}

/* Get a free entry from group cache (evicting if necessary) */
static int32
group_alloc_entry(int32 *hash_index, GroupSeqEntry *entries)
{
    int32 idx;
    
    /* Try free list first */
    if (group_cache->free_head >= 0)
    {
        idx = group_cache->free_head;
        group_cache->free_head = entries[idx].lru_next;
        entries[idx].lru_prev = -1;
        entries[idx].lru_next = -1;
        return idx;
    }
    
    /* Evict LRU entry */
    return group_evict_lru(hash_index, entries);
}

/* ================================================================
 * LRU List Management - TID Cache
 * ================================================================ */

/* Remove entry from TID cache LRU list */
static void
tid_lru_remove(TidSeqEntry *entries, int32 entry_idx)
{
    TidSeqEntry *entry = &entries[entry_idx];
    
    if (entry->lru_prev >= 0)
        entries[entry->lru_prev].lru_next = entry->lru_next;
    else
        tid_cache->lru_head = entry->lru_next;
    
    if (entry->lru_next >= 0)
        entries[entry->lru_next].lru_prev = entry->lru_prev;
    else
        tid_cache->lru_tail = entry->lru_prev;
    
    entry->lru_prev = -1;
    entry->lru_next = -1;
}

/* Add entry to front of TID cache LRU list (most recently used) */
static void
tid_lru_push_front(TidSeqEntry *entries, int32 entry_idx)
{
    TidSeqEntry *entry = &entries[entry_idx];
    
    entry->lru_prev = -1;
    entry->lru_next = tid_cache->lru_head;
    
    if (tid_cache->lru_head >= 0)
        entries[tid_cache->lru_head].lru_prev = entry_idx;
    else
        tid_cache->lru_tail = entry_idx;
    
    tid_cache->lru_head = entry_idx;
}

/* Move entry to front of LRU (on access) */
static void
tid_lru_touch(TidSeqEntry *entries, int32 entry_idx)
{
    if (tid_cache->lru_head == entry_idx)
        return;  /* Already at front */
    
    tid_lru_remove(entries, entry_idx);
    tid_lru_push_front(entries, entry_idx);
}

/* Evict least recently used entry from TID cache */
static int32
tid_evict_lru(int32 *hash_index, TidSeqEntry *entries)
{
    int32 victim_idx = tid_cache->lru_tail;
    TidSeqEntry *victim;
    
    if (victim_idx < 0)
        return -1;
    
    victim = &entries[victim_idx];
    
    /* Remove from LRU list */
    tid_lru_remove(entries, victim_idx);
    
    /* 
     * Mark slot as tombstone instead of empty.
     * This preserves the linear probe chain for lookups.
     */
    if (victim->hash_slot >= 0)
        hash_index[victim->hash_slot] = HASH_TOMBSTONE;
    
    /* Mark entry as free */
    victim->in_use = false;
    victim->hash_slot = -1;
    
    /* Add to free list */
    victim->lru_next = tid_cache->free_head;
    tid_cache->free_head = victim_idx;
    
    tid_cache->num_entries--;
    pg_atomic_fetch_add_u64(&tid_cache->eviction_count, 1);
    
    return victim_idx;
}

/* Get a free entry from TID cache (evicting if necessary) */
static int32
tid_alloc_entry(int32 *hash_index, TidSeqEntry *entries)
{
    int32 idx;
    
    /* Try free list first */
    if (tid_cache->free_head >= 0)
    {
        idx = tid_cache->free_head;
        tid_cache->free_head = entries[idx].lru_next;
        entries[idx].lru_prev = -1;
        entries[idx].lru_next = -1;
        return idx;
    }
    
    /* Evict LRU entry */
    return tid_evict_lru(hash_index, entries);
}

/* ================================================================
 * LRU List Management - Seq-to-TID Cache
 * ================================================================ */

/* Remove entry from seq-to-TID cache LRU list */
static void
seq_tid_lru_remove(SeqTidEntry *entries, int32 entry_idx)
{
    SeqTidEntry *entry = &entries[entry_idx];
    
    if (entry->lru_prev >= 0)
        entries[entry->lru_prev].lru_next = entry->lru_next;
    else
        seq_tid_cache->lru_head = entry->lru_next;
    
    if (entry->lru_next >= 0)
        entries[entry->lru_next].lru_prev = entry->lru_prev;
    else
        seq_tid_cache->lru_tail = entry->lru_prev;
    
    entry->lru_prev = -1;
    entry->lru_next = -1;
}

/* Add entry to front of seq-to-TID cache LRU list (most recently used) */
static void
seq_tid_lru_push_front(SeqTidEntry *entries, int32 entry_idx)
{
    SeqTidEntry *entry = &entries[entry_idx];
    
    entry->lru_prev = -1;
    entry->lru_next = seq_tid_cache->lru_head;
    
    if (seq_tid_cache->lru_head >= 0)
        entries[seq_tid_cache->lru_head].lru_prev = entry_idx;
    else
        seq_tid_cache->lru_tail = entry_idx;
    
    seq_tid_cache->lru_head = entry_idx;
}

/* Move entry to front of LRU (on access) */
static void
seq_tid_lru_touch(SeqTidEntry *entries, int32 entry_idx)
{
    if (seq_tid_cache->lru_head == entry_idx)
        return;  /* Already at front */
    
    seq_tid_lru_remove(entries, entry_idx);
    seq_tid_lru_push_front(entries, entry_idx);
}

/* Evict least recently used entry from seq-to-TID cache */
static int32
seq_tid_evict_lru(int32 *hash_index, SeqTidEntry *entries)
{
    int32 victim_idx = seq_tid_cache->lru_tail;
    SeqTidEntry *victim;
    
    if (victim_idx < 0)
        return -1;
    
    victim = &entries[victim_idx];
    
    /* Remove from LRU list */
    seq_tid_lru_remove(entries, victim_idx);
    
    /* 
     * Mark slot as tombstone instead of empty.
     * This preserves the linear probe chain for lookups.
     */
    if (victim->hash_slot >= 0)
        hash_index[victim->hash_slot] = HASH_TOMBSTONE;
    
    /* Mark entry as free */
    victim->in_use = false;
    victim->hash_slot = -1;
    
    /* Add to free list */
    victim->lru_next = seq_tid_cache->free_head;
    seq_tid_cache->free_head = victim_idx;
    
    seq_tid_cache->num_entries--;
    pg_atomic_fetch_add_u64(&seq_tid_cache->eviction_count, 1);
    
    return victim_idx;
}

/* Get a free entry from seq-to-TID cache (evicting if necessary) */
static int32
seq_tid_alloc_entry(int32 *hash_index, SeqTidEntry *entries)
{
    int32 idx;
    
    /* Try free list first */
    if (seq_tid_cache->free_head >= 0)
    {
        idx = seq_tid_cache->free_head;
        seq_tid_cache->free_head = entries[idx].lru_next;
        entries[idx].lru_prev = -1;
        entries[idx].lru_next = -1;
        return idx;
    }
    
    /* Evict LRU entry */
    return seq_tid_evict_lru(hash_index, entries);
}

/* Hooks for shared memory */
static shmem_request_hook_type prev_seq_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_seq_shmem_startup_hook = NULL;

/* ================================================================
 * Shared Memory Setup
 * ================================================================ */

static int
group_cache_max_entries(void)
{
    int max_entries = (xpatch_group_cache_size_mb * 1024 * 1024) / (GROUP_ENTRY_SIZE + sizeof(int32));
    if (max_entries < 1000)
        max_entries = 1000;
    return max_entries;
}

static Size
group_cache_shmem_size(void)
{
    int max_entries = group_cache_max_entries();
    int hash_size = max_entries * 2;  /* 50% load factor */
    
    return sizeof(GroupSeqCache) + 
           hash_size * sizeof(int32) +          /* hash index */
           max_entries * sizeof(GroupSeqEntry); /* entries */
}

static int
tid_cache_max_entries(void)
{
    int max_entries = (xpatch_tid_cache_size_mb * 1024 * 1024) / (TID_ENTRY_SIZE + sizeof(int32));
    if (max_entries < 10000)
        max_entries = 10000;
    return max_entries;
}

static Size
tid_cache_shmem_size(void)
{
    int max_entries = tid_cache_max_entries();
    int hash_size = max_entries * 2;  /* 50% load factor */
    
    return sizeof(TidSeqCache) +
           hash_size * sizeof(int32) +        /* hash index */
           max_entries * sizeof(TidSeqEntry); /* entries */
}

/* Get pointer to hash index array */
static inline int32 *
group_cache_hash_index(GroupSeqCache *cache)
{
    return (int32 *) ((char *) cache + sizeof(GroupSeqCache));
}

static inline GroupSeqEntry *
group_cache_entries(GroupSeqCache *cache)
{
    return (GroupSeqEntry *) ((char *) cache + sizeof(GroupSeqCache) + 
                               cache->hash_size * sizeof(int32));
}

static inline int32 *
tid_cache_hash_index(TidSeqCache *cache)
{
    return (int32 *) ((char *) cache + sizeof(TidSeqCache));
}

static inline TidSeqEntry *
tid_cache_entries(TidSeqCache *cache)
{
    return (TidSeqEntry *) ((char *) cache + sizeof(TidSeqCache) +
                             cache->hash_size * sizeof(int32));
}

static int
seq_tid_cache_max_entries(void)
{
    int max_entries = (xpatch_seq_tid_cache_size_mb * 1024 * 1024) / (SEQ_TID_ENTRY_SIZE + sizeof(int32));
    if (max_entries < 10000)
        max_entries = 10000;
    return max_entries;
}

static Size
seq_tid_cache_shmem_size(void)
{
    int max_entries = seq_tid_cache_max_entries();
    int hash_size = max_entries * 2;  /* 50% load factor */
    
    return sizeof(SeqTidCache) +
           hash_size * sizeof(int32) +          /* hash index */
           max_entries * sizeof(SeqTidEntry);   /* entries */
}

static inline int32 *
seq_tid_cache_hash_index(SeqTidCache *cache)
{
    return (int32 *) ((char *) cache + sizeof(SeqTidCache));
}

static inline SeqTidEntry *
seq_tid_cache_entries(SeqTidCache *cache)
{
    return (SeqTidEntry *) ((char *) cache + sizeof(SeqTidCache) +
                             cache->hash_size * sizeof(int32));
}

static void
xpatch_seq_shmem_request(void)
{
    if (prev_seq_shmem_request_hook)
        prev_seq_shmem_request_hook();
    
    RequestAddinShmemSpace(group_cache_shmem_size());
    RequestAddinShmemSpace(tid_cache_shmem_size());
    RequestAddinShmemSpace(seq_tid_cache_shmem_size());
    RequestNamedLWLockTranche("pg_xpatch_seq", 3);  /* 3 locks now */
}

static void
xpatch_seq_shmem_startup(void)
{
    bool found;
    Size size;
    int max_entries;
    int hash_size;
    LWLockPadded *locks;
    
    if (prev_seq_shmem_startup_hook)
        prev_seq_shmem_startup_hook();
    
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    
    locks = GetNamedLWLockTranche("pg_xpatch_seq");
    
    /* Initialize Group Max Seq Cache */
    size = group_cache_shmem_size();
    max_entries = group_cache_max_entries();
    hash_size = max_entries * 2;
    
    group_cache = ShmemInitStruct("pg_xpatch group seq cache", size, &found);
    
    if (!found)
    {
        int i;
        GroupSeqEntry *g_entries;
        
        memset(group_cache, 0, size);
        group_cache->lock = &(locks[0].lock);
        group_cache->num_entries = 0;
        group_cache->max_entries = max_entries;
        group_cache->hash_size = hash_size;
        group_cache->lru_head = -1;
        group_cache->lru_tail = -1;
        pg_atomic_init_u64(&group_cache->hit_count, 0);
        pg_atomic_init_u64(&group_cache->miss_count, 0);
        pg_atomic_init_u64(&group_cache->eviction_count, 0);
        
        /* Initialize free list */
        g_entries = group_cache_entries(group_cache);
        group_cache->free_head = 0;
        for (i = 0; i < max_entries - 1; i++)
        {
            g_entries[i].in_use = false;
            g_entries[i].lru_prev = -1;
            g_entries[i].lru_next = i + 1;
            g_entries[i].hash_slot = -1;
        }
        g_entries[max_entries - 1].in_use = false;
        g_entries[max_entries - 1].lru_prev = -1;
        g_entries[max_entries - 1].lru_next = -1;
        g_entries[max_entries - 1].hash_slot = -1;
        
        elog(LOG, "pg_xpatch: group seq cache initialized with BLAKE3 hashing (%d max entries, %d hash slots, %zu bytes)",
             max_entries, hash_size, size);
    }
    
    /* Initialize TID Seq Cache */
    size = tid_cache_shmem_size();
    max_entries = tid_cache_max_entries();
    hash_size = max_entries * 2;
    
    tid_cache = ShmemInitStruct("pg_xpatch tid seq cache", size, &found);
    
    if (!found)
    {
        int i;
        TidSeqEntry *t_entries;
        
        memset(tid_cache, 0, size);
        tid_cache->lock = &(locks[1].lock);
        tid_cache->num_entries = 0;
        tid_cache->max_entries = max_entries;
        tid_cache->hash_size = hash_size;
        tid_cache->lru_head = -1;
        tid_cache->lru_tail = -1;
        pg_atomic_init_u64(&tid_cache->hit_count, 0);
        pg_atomic_init_u64(&tid_cache->miss_count, 0);
        pg_atomic_init_u64(&tid_cache->eviction_count, 0);
        
        /* Initialize free list */
        t_entries = tid_cache_entries(tid_cache);
        tid_cache->free_head = 0;
        for (i = 0; i < max_entries - 1; i++)
        {
            t_entries[i].in_use = false;
            t_entries[i].lru_prev = -1;
            t_entries[i].lru_next = i + 1;
            t_entries[i].hash_slot = -1;
        }
        t_entries[max_entries - 1].in_use = false;
        t_entries[max_entries - 1].lru_prev = -1;
        t_entries[max_entries - 1].lru_next = -1;
        t_entries[max_entries - 1].hash_slot = -1;
        
        elog(LOG, "pg_xpatch: tid seq cache initialized with LRU (%d max entries, %d hash slots, %zu bytes)",
             max_entries, hash_size, size);
    }
    
    /* Initialize Seq-to-TID Cache (reverse lookup for fetch_by_seq) */
    size = seq_tid_cache_shmem_size();
    max_entries = seq_tid_cache_max_entries();
    hash_size = max_entries * 2;
    
    seq_tid_cache = ShmemInitStruct("pg_xpatch seq tid cache", size, &found);
    
    if (!found)
    {
        int i;
        SeqTidEntry *st_entries;
        
        memset(seq_tid_cache, 0, size);
        seq_tid_cache->lock = &(locks[2].lock);
        seq_tid_cache->num_entries = 0;
        seq_tid_cache->max_entries = max_entries;
        seq_tid_cache->hash_size = hash_size;
        seq_tid_cache->lru_head = -1;
        seq_tid_cache->lru_tail = -1;
        pg_atomic_init_u64(&seq_tid_cache->hit_count, 0);
        pg_atomic_init_u64(&seq_tid_cache->miss_count, 0);
        pg_atomic_init_u64(&seq_tid_cache->eviction_count, 0);
        
        /* Initialize free list */
        st_entries = seq_tid_cache_entries(seq_tid_cache);
        seq_tid_cache->free_head = 0;
        for (i = 0; i < max_entries - 1; i++)
        {
            st_entries[i].in_use = false;
            st_entries[i].lru_prev = -1;
            st_entries[i].lru_next = i + 1;
            st_entries[i].hash_slot = -1;
        }
        st_entries[max_entries - 1].in_use = false;
        st_entries[max_entries - 1].lru_prev = -1;
        st_entries[max_entries - 1].lru_next = -1;
        st_entries[max_entries - 1].hash_slot = -1;
        
        elog(LOG, "pg_xpatch: seq-to-tid cache initialized with LRU (%d max entries, %d hash slots, %zu bytes)",
             max_entries, hash_size, size);
    }
    
    LWLockRelease(AddinShmemInitLock);
    
    seq_cache_initialized = true;
    
    /* Register exit callback to clear per-backend pointers */
    on_shmem_exit(xpatch_seq_cache_shmem_exit, (Datum) 0);
}

void
xpatch_seq_cache_request_shmem(void)
{
    prev_seq_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = xpatch_seq_shmem_request;
    
    prev_seq_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = xpatch_seq_shmem_startup;
}

void
xpatch_seq_cache_init(void)
{
    /* Actual initialization happens in shmem_startup_hook */
}

/* ================================================================
 * Group Max Seq Cache Operations
 * ================================================================ */

/* Find entry index using hash lookup with linear probing, returns -1 if not found */
static int32
group_cache_find(Oid relid, XPatchGroupHash group_hash, int32 *out_slot)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    GroupSeqEntry *entries;
    int probe_count;
    
    if (out_slot)
        *out_slot = -1;
    
    if (group_cache->num_entries == 0)
        return -1;
    
    h = hash_group_key(relid, group_hash);
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    
    /* Linear probing */
    for (probe_count = 0; probe_count < group_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % group_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == HASH_EMPTY)
            return -1;  /* Empty slot (never used) - not found */
        
        if (idx == HASH_TOMBSTONE)
            continue;   /* Deleted slot - continue probing */
        
        idx--;  /* Convert from 1-based to 0-based */
        if (entries[idx].in_use && 
            entries[idx].relid == relid && 
            xpatch_group_hash_equals(entries[idx].group_hash, group_hash))
        {
            if (out_slot)
                *out_slot = slot;
            return idx;
        }
    }
    
    return -1;
}

/* Find free slot for insertion */
static int32
group_cache_find_slot_for_key(Oid relid, XPatchGroupHash group_hash)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    GroupSeqEntry *entries;
    int probe_count;
    int32 first_tombstone = -1;  /* Track first tombstone for reuse */
    
    h = hash_group_key(relid, group_hash);
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    
    /* Linear probing to find empty slot, tombstone, or existing entry */
    for (probe_count = 0; probe_count < group_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % group_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == HASH_EMPTY)
        {
            /* Prefer tombstone if we found one earlier */
            return (first_tombstone >= 0) ? first_tombstone : slot;
        }
        
        if (idx == HASH_TOMBSTONE)
        {
            /* Remember first tombstone for potential reuse */
            if (first_tombstone < 0)
                first_tombstone = slot;
            continue;
        }
        
        idx--;
        if (entries[idx].in_use &&
            entries[idx].relid == relid && 
            xpatch_group_hash_equals(entries[idx].group_hash, group_hash))
            return slot;  /* Existing entry */
    }
    
    /* If we found a tombstone during probing, use it */
    if (first_tombstone >= 0)
        return first_tombstone;
    
    return -1;  /* Cache full */
}

int64
xpatch_seq_cache_get_max_seq(Oid relid, Datum group_value, Oid typid, bool *found)
{
    int32 idx;
    int64 result = 0;
    GroupSeqEntry *entries;
    XPatchGroupHash group_hash;
    
    *found = false;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return 0;
    
    /* Compute hash outside the lock */
    group_hash = xpatch_compute_group_hash(group_value, typid, false);
    
    LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);  /* Need exclusive for LRU update */
    
    idx = group_cache_find(relid, group_hash, NULL);
    
    if (idx >= 0)
    {
        entries = group_cache_entries(group_cache);
        result = entries[idx].max_seq;
        *found = true;
        
        /* Update LRU - move to front */
        group_lru_touch(entries, idx);
        
        pg_atomic_fetch_add_u64(&group_cache->hit_count, 1);
    }
    else
    {
        pg_atomic_fetch_add_u64(&group_cache->miss_count, 1);
    }
    
    LWLockRelease(group_cache->lock);
    
    return result;
}

void
xpatch_seq_cache_set_max_seq(Oid relid, Datum group_value, Oid typid, int64 max_seq)
{
    int32 idx;
    int32 slot;
    int32 *hash_index;
    GroupSeqEntry *entries;
    XPatchGroupHash group_hash;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return;
    
    /* Compute hash outside the lock */
    group_hash = xpatch_compute_group_hash(group_value, typid, false);
    
    LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
    
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    
    /* Check if already exists */
    idx = group_cache_find(relid, group_hash, &slot);
    
    if (idx >= 0)
    {
        /* Update existing entry */
        entries[idx].max_seq = max_seq;
        group_lru_touch(entries, idx);
        LWLockRelease(group_cache->lock);
        return;
    }
    
    /* Find slot for new entry */
    slot = group_cache_find_slot_for_key(relid, group_hash);
    
    if (slot < 0)
    {
        /* Hash table full - need to evict to make room in hash */
        group_evict_lru(hash_index, entries);
        slot = group_cache_find_slot_for_key(relid, group_hash);
        if (slot < 0)
        {
            /* Still no slot - give up */
            LWLockRelease(group_cache->lock);
            return;
        }
    }
    
    /* Allocate entry (evicting if necessary) */
    idx = group_alloc_entry(hash_index, entries);
    
    if (idx < 0)
    {
        /* No entry available */
        LWLockRelease(group_cache->lock);
        return;
    }
    
    /* Initialize new entry */
    entries[idx].relid = relid;
    entries[idx].group_hash = group_hash;
    entries[idx].max_seq = max_seq;
    entries[idx].hash_slot = slot;
    entries[idx].in_use = true;
    
    /* Add to hash table */
    hash_index[slot] = idx + 1;  /* Store 1-based index */
    
    /* Add to LRU front */
    group_lru_push_front(entries, idx);
    
    group_cache->num_entries++;
    
    LWLockRelease(group_cache->lock);
}

int64
xpatch_seq_cache_next_seq(Oid relid, Datum group_value, Oid typid)
{
    int32 idx;
    int32 slot;
    int64 new_seq;
    GroupSeqEntry *entries;
    XPatchGroupHash group_hash;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return 0;  /* Caller should fall back to scan */
    
    /* Compute hash outside the lock */
    group_hash = xpatch_compute_group_hash(group_value, typid, false);
    
    LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
    
    entries = group_cache_entries(group_cache);
    
    /* Check if entry exists */
    idx = group_cache_find(relid, group_hash, &slot);
    
    if (idx >= 0)
    {
        /* Increment existing entry and update LRU */
        new_seq = ++entries[idx].max_seq;
        group_lru_touch(entries, idx);
        pg_atomic_fetch_add_u64(&group_cache->hit_count, 1);
    }
    else
    {
        /*
         * New group not in cache - return 0 to trigger fallback scan.
         * The caller will scan the table to find the actual max_seq,
         * then call xpatch_seq_cache_set_max_seq() to populate the cache.
         * 
         * We don't create the entry here because we don't know the actual
         * max_seq value - it could be 0 (empty group) or higher if there
         * are existing rows from before a server restart or cache eviction.
         */
        pg_atomic_fetch_add_u64(&group_cache->miss_count, 1);
        LWLockRelease(group_cache->lock);
        return 0;
    }
    
    LWLockRelease(group_cache->lock);
    
    return new_seq;
}

bool
xpatch_seq_cache_rollback_seq(Oid relid, Datum group_value, Oid typid, int64 expected_seq)
{
    int32 idx;
    int32 slot;
    bool success = false;
    GroupSeqEntry *entries;
    XPatchGroupHash group_hash;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return false;  /* Cache not available - can't rollback */
    
    /* Compute hash outside the lock */
    group_hash = xpatch_compute_group_hash(group_value, typid, false);
    
    LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
    
    entries = group_cache_entries(group_cache);
    
    /* Find the entry */
    idx = group_cache_find(relid, group_hash, &slot);
    
    if (idx >= 0)
    {
        /*
         * Only rollback if the current value matches what we expect.
         * This handles the case where another concurrent insert succeeded
         * after our failed one - we shouldn't decrement in that case.
         */
        if (entries[idx].max_seq == expected_seq)
        {
            entries[idx].max_seq--;
            success = true;
            elog(DEBUG1, "xpatch: rolled back seq " INT64_FORMAT " for group", expected_seq);
        }
        else
        {
            elog(DEBUG1, "xpatch: seq rollback skipped - current " INT64_FORMAT " != expected " INT64_FORMAT,
                 entries[idx].max_seq, expected_seq);
        }
    }
    
    LWLockRelease(group_cache->lock);
    
    return success;
}

/* ================================================================
 * TID Seq Cache Operations
 * ================================================================ */

/* Find entry index using hash lookup with linear probing, returns -1 if not found */
static int32
tid_cache_find(Oid relid, ItemPointer tid, int32 *out_slot)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    TidSeqEntry *entries;
    int probe_count;
    
    if (out_slot)
        *out_slot = -1;
    
    if (tid_cache->num_entries == 0)
        return -1;
    
    h = hash_tid_key(relid, tid);
    hash_index = tid_cache_hash_index(tid_cache);
    entries = tid_cache_entries(tid_cache);
    
    /* Linear probing */
    for (probe_count = 0; probe_count < tid_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % tid_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == HASH_EMPTY)
            return -1;  /* Empty slot (never used) - not found */
        
        if (idx == HASH_TOMBSTONE)
            continue;   /* Deleted slot - continue probing */
        
        idx--;  /* Convert from 1-based to 0-based */
        if (entries[idx].in_use &&
            entries[idx].relid == relid && 
            ItemPointerEquals(&entries[idx].tid, tid))
        {
            if (out_slot)
                *out_slot = slot;
            return idx;
        }
    }
    
    return -1;
}

/* Find free slot for insertion */
static int32
tid_cache_find_slot_for_key(Oid relid, ItemPointer tid)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    TidSeqEntry *entries;
    int probe_count;
    int32 first_tombstone = -1;  /* Track first tombstone for reuse */
    
    h = hash_tid_key(relid, tid);
    hash_index = tid_cache_hash_index(tid_cache);
    entries = tid_cache_entries(tid_cache);
    
    /* Linear probing to find empty slot, tombstone, or existing entry */
    for (probe_count = 0; probe_count < tid_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % tid_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == HASH_EMPTY)
        {
            /* Prefer tombstone if we found one earlier */
            return (first_tombstone >= 0) ? first_tombstone : slot;
        }
        
        if (idx == HASH_TOMBSTONE)
        {
            /* Remember first tombstone for potential reuse */
            if (first_tombstone < 0)
                first_tombstone = slot;
            continue;
        }
        
        idx--;
        if (entries[idx].relid == relid && ItemPointerEquals(&entries[idx].tid, tid))
            return slot;  /* Existing entry */
    }
    
    /* If we found a tombstone during probing, use it */
    if (first_tombstone >= 0)
        return first_tombstone;
    
    return -1;  /* Cache full */
}

int64
xpatch_seq_cache_get_tid_seq(Oid relid, ItemPointer tid, bool *found)
{
    int32 idx;
    int64 result = 0;
    TidSeqEntry *entries;
    
    *found = false;
    
    if (!seq_cache_initialized || tid_cache == NULL)
        return 0;
    
    LWLockAcquire(tid_cache->lock, LW_EXCLUSIVE);  /* Need exclusive for LRU update */
    
    idx = tid_cache_find(relid, tid, NULL);
    
    if (idx >= 0)
    {
        entries = tid_cache_entries(tid_cache);
        result = entries[idx].seq;
        *found = true;
        
        /* Update LRU - move to front */
        tid_lru_touch(entries, idx);
        
        pg_atomic_fetch_add_u64(&tid_cache->hit_count, 1);
    }
    else
    {
        pg_atomic_fetch_add_u64(&tid_cache->miss_count, 1);
    }
    
    LWLockRelease(tid_cache->lock);
    
    return result;
}

void
xpatch_seq_cache_set_tid_seq(Oid relid, ItemPointer tid, int64 seq)
{
    int32 idx;
    int32 slot;
    int32 *hash_index;
    TidSeqEntry *entries;
    
    if (!seq_cache_initialized || tid_cache == NULL)
        return;
    
    LWLockAcquire(tid_cache->lock, LW_EXCLUSIVE);
    
    hash_index = tid_cache_hash_index(tid_cache);
    entries = tid_cache_entries(tid_cache);
    
    /* Check if already exists */
    idx = tid_cache_find(relid, tid, &slot);
    
    if (idx >= 0)
    {
        /* Update existing entry */
        entries[idx].seq = seq;
        tid_lru_touch(entries, idx);
        LWLockRelease(tid_cache->lock);
        return;
    }
    
    /* Find slot for new entry */
    slot = tid_cache_find_slot_for_key(relid, tid);
    
    if (slot < 0)
    {
        /* Hash table full - need to evict to make room in hash */
        tid_evict_lru(hash_index, entries);
        slot = tid_cache_find_slot_for_key(relid, tid);
        if (slot < 0)
        {
            /* Still no slot - give up */
            LWLockRelease(tid_cache->lock);
            return;
        }
    }
    
    /* Allocate entry (evicting if necessary) */
    idx = tid_alloc_entry(hash_index, entries);
    
    if (idx < 0)
    {
        /* No entry available */
        LWLockRelease(tid_cache->lock);
        return;
    }
    
    /* Initialize new entry */
    entries[idx].relid = relid;
    ItemPointerCopy(tid, &entries[idx].tid);
    entries[idx].seq = seq;
    entries[idx].hash_slot = slot;
    entries[idx].in_use = true;
    
    /* Add to hash table */
    hash_index[slot] = idx + 1;  /* Store 1-based index */
    
    /* Add to LRU front */
    tid_lru_push_front(entries, idx);
    
    tid_cache->num_entries++;
    
    LWLockRelease(tid_cache->lock);
}

void
xpatch_seq_cache_populate_group_tids(Oid relid, Datum group_value,
                                     ItemPointer *tids, int64 *seqs,
                                     int count)
{
    int i;
    int32 idx;
    int32 slot;
    int32 *hash_index;
    TidSeqEntry *entries;
    
    if (!seq_cache_initialized || tid_cache == NULL)
        return;
    
    LWLockAcquire(tid_cache->lock, LW_EXCLUSIVE);
    
    hash_index = tid_cache_hash_index(tid_cache);
    entries = tid_cache_entries(tid_cache);
    
    for (i = 0; i < count; i++)
    {
        /* Check if already exists */
        idx = tid_cache_find(relid, tids[i], &slot);
        
        if (idx >= 0)
        {
            /* Update existing and touch LRU */
            entries[idx].seq = seqs[i];
            tid_lru_touch(entries, idx);
            continue;
        }
        
        /* Find slot for new entry */
        slot = tid_cache_find_slot_for_key(relid, tids[i]);
        
        if (slot < 0)
        {
            /* Hash table full - evict to make room */
            tid_evict_lru(hash_index, entries);
            slot = tid_cache_find_slot_for_key(relid, tids[i]);
            if (slot < 0)
                continue;  /* Still no slot - skip this entry */
        }
        
        /* Allocate entry (evicting if necessary) */
        idx = tid_alloc_entry(hash_index, entries);
        
        if (idx < 0)
            continue;  /* No entry available */
        
        /* Initialize new entry */
        entries[idx].relid = relid;
        ItemPointerCopy(tids[i], &entries[idx].tid);
        entries[idx].seq = seqs[i];
        entries[idx].hash_slot = slot;
        entries[idx].in_use = true;
        
        hash_index[slot] = idx + 1;
        tid_lru_push_front(entries, idx);
        tid_cache->num_entries++;
    }
    
    LWLockRelease(tid_cache->lock);
}

/* ================================================================
 * Seq-to-TID Cache Operations
 * ================================================================ */

/* Find entry index using hash lookup with linear probing, returns -1 if not found */
static int32
seq_tid_cache_find(Oid relid, XPatchGroupHash group_hash, int64 seq, int32 *out_slot)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    SeqTidEntry *entries;
    int probe_count;
    
    if (out_slot)
        *out_slot = -1;
    
    if (seq_tid_cache->num_entries == 0)
        return -1;
    
    h = hash_seq_tid_key(relid, group_hash, seq);
    hash_index = seq_tid_cache_hash_index(seq_tid_cache);
    entries = seq_tid_cache_entries(seq_tid_cache);
    
    /* Linear probing */
    for (probe_count = 0; probe_count < seq_tid_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % seq_tid_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == HASH_EMPTY)
            return -1;  /* Empty slot (never used) - not found */
        
        if (idx == HASH_TOMBSTONE)
            continue;   /* Deleted slot - continue probing */
        
        idx--;  /* Convert from 1-based to 0-based */
        if (entries[idx].in_use && 
            entries[idx].relid == relid && 
            entries[idx].seq == seq &&
            xpatch_group_hash_equals(entries[idx].group_hash, group_hash))
        {
            if (out_slot)
                *out_slot = slot;
            return idx;
        }
    }
    
    return -1;
}

/* Find free slot for insertion */
static int32
seq_tid_cache_find_slot_for_key(Oid relid, XPatchGroupHash group_hash, int64 seq)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    SeqTidEntry *entries;
    int probe_count;
    int32 first_tombstone = -1;  /* Track first tombstone for reuse */
    
    h = hash_seq_tid_key(relid, group_hash, seq);
    hash_index = seq_tid_cache_hash_index(seq_tid_cache);
    entries = seq_tid_cache_entries(seq_tid_cache);
    
    /* Linear probing to find empty slot, tombstone, or existing entry */
    for (probe_count = 0; probe_count < seq_tid_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % seq_tid_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == HASH_EMPTY)
        {
            /* Prefer tombstone if we found one earlier */
            return (first_tombstone >= 0) ? first_tombstone : slot;
        }
        
        if (idx == HASH_TOMBSTONE)
        {
            /* Remember first tombstone for potential reuse */
            if (first_tombstone < 0)
                first_tombstone = slot;
            continue;
        }
        
        idx--;
        if (entries[idx].in_use &&
            entries[idx].relid == relid && 
            entries[idx].seq == seq &&
            xpatch_group_hash_equals(entries[idx].group_hash, group_hash))
            return slot;  /* Existing entry */
    }
    
    /* If we found a tombstone during probing, use it */
    if (first_tombstone >= 0)
        return first_tombstone;
    
    return -1;  /* Cache full */
}

bool
xpatch_seq_cache_get_seq_tid(Oid relid, Datum group_value, Oid typid,
                              int64 seq, ItemPointer tid)
{
    int32 idx;
    SeqTidEntry *entries;
    XPatchGroupHash group_hash;
    
    if (!seq_cache_initialized || seq_tid_cache == NULL)
        return false;
    
    /* Compute hash outside the lock */
    group_hash = xpatch_compute_group_hash(group_value, typid, false);
    
    LWLockAcquire(seq_tid_cache->lock, LW_EXCLUSIVE);  /* Need exclusive for LRU update */
    
    idx = seq_tid_cache_find(relid, group_hash, seq, NULL);
    
    if (idx >= 0)
    {
        entries = seq_tid_cache_entries(seq_tid_cache);
        ItemPointerCopy(&entries[idx].tid, tid);
        
        /* Update LRU - move to front */
        seq_tid_lru_touch(entries, idx);
        
        pg_atomic_fetch_add_u64(&seq_tid_cache->hit_count, 1);
        LWLockRelease(seq_tid_cache->lock);
        return true;
    }
    else
    {
        pg_atomic_fetch_add_u64(&seq_tid_cache->miss_count, 1);
    }
    
    LWLockRelease(seq_tid_cache->lock);
    return false;
}

void
xpatch_seq_cache_set_seq_tid(Oid relid, Datum group_value, Oid typid,
                              int64 seq, ItemPointer tid)
{
    int32 idx;
    int32 slot;
    int32 *hash_index;
    SeqTidEntry *entries;
    XPatchGroupHash group_hash;
    
    if (!seq_cache_initialized || seq_tid_cache == NULL)
        return;
    
    /* Compute hash outside the lock */
    group_hash = xpatch_compute_group_hash(group_value, typid, false);
    
    LWLockAcquire(seq_tid_cache->lock, LW_EXCLUSIVE);
    
    hash_index = seq_tid_cache_hash_index(seq_tid_cache);
    entries = seq_tid_cache_entries(seq_tid_cache);
    
    /* Check if already exists */
    idx = seq_tid_cache_find(relid, group_hash, seq, &slot);
    
    if (idx >= 0)
    {
        /* Update existing entry */
        ItemPointerCopy(tid, &entries[idx].tid);
        seq_tid_lru_touch(entries, idx);
        LWLockRelease(seq_tid_cache->lock);
        return;
    }
    
    /* Find slot for new entry */
    slot = seq_tid_cache_find_slot_for_key(relid, group_hash, seq);
    
    if (slot < 0)
    {
        /* Hash table full - need to evict to make room in hash */
        seq_tid_evict_lru(hash_index, entries);
        slot = seq_tid_cache_find_slot_for_key(relid, group_hash, seq);
        if (slot < 0)
        {
            /* Still no slot - give up */
            LWLockRelease(seq_tid_cache->lock);
            return;
        }
    }
    
    /* Allocate entry (evicting if necessary) */
    idx = seq_tid_alloc_entry(hash_index, entries);
    
    if (idx < 0)
    {
        /* No entry available */
        LWLockRelease(seq_tid_cache->lock);
        return;
    }
    
    /* Initialize new entry */
    entries[idx].relid = relid;
    entries[idx].group_hash = group_hash;
    entries[idx].seq = seq;
    ItemPointerCopy(tid, &entries[idx].tid);
    entries[idx].hash_slot = slot;
    entries[idx].in_use = true;
    
    /* Add to hash table */
    hash_index[slot] = idx + 1;  /* Store 1-based index */
    
    /* Add to LRU front */
    seq_tid_lru_push_front(entries, idx);
    
    seq_tid_cache->num_entries++;
    
    LWLockRelease(seq_tid_cache->lock);
}

/* ================================================================
 * Cache Invalidation
 * ================================================================ */

void
xpatch_seq_cache_invalidate_rel(Oid relid)
{
    int i;
    int32 *hash_index;
    GroupSeqEntry *g_entries;
    TidSeqEntry *t_entries;
    SeqTidEntry *st_entries;
    
    if (!seq_cache_initialized)
        return;
    
    /* 
     * For invalidation, we need to mark entries as invalid and set tombstones
     * in the hash table. We iterate through all entries and invalidate those
     * matching the relid.
     */
    
    /* Invalidate group cache entries */
    if (group_cache != NULL)
    {
        LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
        
        hash_index = group_cache_hash_index(group_cache);
        g_entries = group_cache_entries(group_cache);
        
        for (i = 0; i < group_cache->max_entries; i++)
        {
            if (g_entries[i].in_use && g_entries[i].relid == relid)
            {
                /* Set tombstone in hash table to preserve probe chains */
                if (g_entries[i].hash_slot >= 0)
                    hash_index[g_entries[i].hash_slot] = HASH_TOMBSTONE;
                
                /* Remove from LRU list */
                group_lru_remove(g_entries, i);
                
                /* Mark as free and add to free list */
                g_entries[i].in_use = false;
                g_entries[i].hash_slot = -1;
                g_entries[i].lru_next = group_cache->free_head;
                group_cache->free_head = i;
                
                group_cache->num_entries--;
            }
        }
        
        LWLockRelease(group_cache->lock);
    }
    
    /* Invalidate TID cache entries */
    if (tid_cache != NULL)
    {
        LWLockAcquire(tid_cache->lock, LW_EXCLUSIVE);
        
        hash_index = tid_cache_hash_index(tid_cache);
        t_entries = tid_cache_entries(tid_cache);
        
        for (i = 0; i < tid_cache->max_entries; i++)
        {
            if (t_entries[i].in_use && t_entries[i].relid == relid)
            {
                /* Set tombstone in hash table to preserve probe chains */
                if (t_entries[i].hash_slot >= 0)
                    hash_index[t_entries[i].hash_slot] = HASH_TOMBSTONE;
                
                /* Remove from LRU list */
                tid_lru_remove(t_entries, i);
                
                /* Mark as free and add to free list */
                t_entries[i].in_use = false;
                t_entries[i].hash_slot = -1;
                t_entries[i].lru_next = tid_cache->free_head;
                tid_cache->free_head = i;
                
                tid_cache->num_entries--;
            }
        }
        
        LWLockRelease(tid_cache->lock);
    }
    
    /* Invalidate seq-to-TID cache entries */
    if (seq_tid_cache != NULL)
    {
        LWLockAcquire(seq_tid_cache->lock, LW_EXCLUSIVE);
        
        hash_index = seq_tid_cache_hash_index(seq_tid_cache);
        st_entries = seq_tid_cache_entries(seq_tid_cache);
        
        for (i = 0; i < seq_tid_cache->max_entries; i++)
        {
            if (st_entries[i].in_use && st_entries[i].relid == relid)
            {
                /* Set tombstone in hash table to preserve probe chains */
                if (st_entries[i].hash_slot >= 0)
                    hash_index[st_entries[i].hash_slot] = HASH_TOMBSTONE;
                
                /* Remove from LRU list */
                seq_tid_lru_remove(st_entries, i);
                
                /* Mark as free and add to free list */
                st_entries[i].in_use = false;
                st_entries[i].hash_slot = -1;
                st_entries[i].lru_next = seq_tid_cache->free_head;
                seq_tid_cache->free_head = i;
                
                seq_tid_cache->num_entries--;
            }
        }
        
        LWLockRelease(seq_tid_cache->lock);
    }
}

/* ================================================================
 * Statistics
 * ================================================================ */

void
xpatch_seq_cache_get_stats(XPatchSeqCacheStats *stats)
{
    memset(stats, 0, sizeof(*stats));
    
    if (!seq_cache_initialized)
        return;
    
    if (group_cache != NULL)
    {
        LWLockAcquire(group_cache->lock, LW_SHARED);
        stats->group_cache_entries = group_cache->num_entries;
        stats->group_cache_max = group_cache->max_entries;
        stats->group_cache_hits = pg_atomic_read_u64(&group_cache->hit_count);
        stats->group_cache_misses = pg_atomic_read_u64(&group_cache->miss_count);
        LWLockRelease(group_cache->lock);
    }
    
    if (tid_cache != NULL)
    {
        LWLockAcquire(tid_cache->lock, LW_SHARED);
        stats->tid_cache_entries = tid_cache->num_entries;
        stats->tid_cache_max = tid_cache->max_entries;
        stats->tid_cache_hits = pg_atomic_read_u64(&tid_cache->hit_count);
        stats->tid_cache_misses = pg_atomic_read_u64(&tid_cache->miss_count);
        LWLockRelease(tid_cache->lock);
    }
    
    if (seq_tid_cache != NULL)
    {
        LWLockAcquire(seq_tid_cache->lock, LW_SHARED);
        stats->seq_tid_cache_entries = seq_tid_cache->num_entries;
        stats->seq_tid_cache_max = seq_tid_cache->max_entries;
        stats->seq_tid_cache_hits = pg_atomic_read_u64(&seq_tid_cache->hit_count);
        stats->seq_tid_cache_misses = pg_atomic_read_u64(&seq_tid_cache->miss_count);
        LWLockRelease(seq_tid_cache->lock);
    }
}
