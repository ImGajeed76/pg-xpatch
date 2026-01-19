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
 */

#include "xpatch_seq_cache.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

/* Default cache sizes (configurable via GUC) */
int xpatch_group_cache_size_mb = 8;  /* 8MB default */
int xpatch_tid_cache_size_mb = 8;    /* 8MB default */

/* FNV-1a hash constants */
#define FNV_OFFSET_BASIS_32  2166136261U
#define FNV_PRIME_32         16777619U

/* ================================================================
 * Group Max Seq Cache
 * ================================================================ */

/* Entry for group max seq cache */
typedef struct GroupSeqEntry
{
    /* Key */
    Oid         relid;
    int64       group_value;    /* Datum cast to int64 for fixed size */
    
    /* Value */
    int32       max_seq;
    int32       padding;
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
    int32           padding2;
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    
    /* 
     * Hash index: hash_index[hash % hash_size] = entry index + 1 (0 = empty)
     * Followed by entries array
     */
} GroupSeqCache;

/* Hash function for group cache key */
static uint32
hash_group_key(Oid relid, int64 group_value)
{
    uint32 h = FNV_OFFSET_BASIS_32;
    unsigned char *p;
    int i;
    
    p = (unsigned char *) &relid;
    for (i = 0; i < (int) sizeof(Oid); i++)
        h = (h ^ p[i]) * FNV_PRIME_32;
    
    p = (unsigned char *) &group_value;
    for (i = 0; i < (int) sizeof(int64); i++)
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
    int16           padding;
    
    /* Value */
    int32           seq;
} TidSeqEntry;

#define TID_ENTRY_SIZE  sizeof(TidSeqEntry)

/* Header for TID cache */
typedef struct TidSeqCache
{
    LWLock         *lock;
    int32           num_entries;
    int32           max_entries;
    int32           hash_size;      /* Size of hash index array */
    int32           padding2;
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    
    /*
     * Hash index: hash_index[hash % hash_size] = entry index + 1 (0 = empty)
     * Followed by entries array
     */
} TidSeqCache;

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
static bool seq_cache_initialized = false;

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

static void
xpatch_seq_shmem_request(void)
{
    if (prev_seq_shmem_request_hook)
        prev_seq_shmem_request_hook();
    
    RequestAddinShmemSpace(group_cache_shmem_size());
    RequestAddinShmemSpace(tid_cache_shmem_size());
    RequestNamedLWLockTranche("pg_xpatch_seq", 2);
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
        memset(group_cache, 0, size);
        group_cache->lock = &(locks[0].lock);
        group_cache->num_entries = 0;
        group_cache->max_entries = max_entries;
        group_cache->hash_size = hash_size;
        pg_atomic_init_u64(&group_cache->hit_count, 0);
        pg_atomic_init_u64(&group_cache->miss_count, 0);
        
        elog(LOG, "pg_xpatch: group seq cache initialized (%d max entries, %d hash slots, %zu bytes)",
             max_entries, hash_size, size);
    }
    
    /* Initialize TID Seq Cache */
    size = tid_cache_shmem_size();
    max_entries = tid_cache_max_entries();
    hash_size = max_entries * 2;
    
    tid_cache = ShmemInitStruct("pg_xpatch tid seq cache", size, &found);
    
    if (!found)
    {
        memset(tid_cache, 0, size);
        tid_cache->lock = &(locks[1].lock);
        tid_cache->num_entries = 0;
        tid_cache->max_entries = max_entries;
        tid_cache->hash_size = hash_size;
        pg_atomic_init_u64(&tid_cache->hit_count, 0);
        pg_atomic_init_u64(&tid_cache->miss_count, 0);
        
        elog(LOG, "pg_xpatch: tid seq cache initialized (%d max entries, %d hash slots, %zu bytes)",
             max_entries, hash_size, size);
    }
    
    LWLockRelease(AddinShmemInitLock);
    
    seq_cache_initialized = true;
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
group_cache_find(Oid relid, int64 group_value)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    GroupSeqEntry *entries;
    int probe_count;
    
    if (group_cache->num_entries == 0)
        return -1;
    
    h = hash_group_key(relid, group_value);
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    
    /* Linear probing */
    for (probe_count = 0; probe_count < group_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % group_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == 0)
            return -1;  /* Empty slot - not found */
        
        idx--;  /* Convert from 1-based to 0-based */
        if (entries[idx].relid == relid && entries[idx].group_value == group_value)
            return idx;
    }
    
    return -1;
}

/* Find free slot for insertion */
static int32
group_cache_find_slot_for_key(Oid relid, int64 group_value)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    GroupSeqEntry *entries;
    int probe_count;
    
    h = hash_group_key(relid, group_value);
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    
    /* Linear probing to find empty slot or existing entry */
    for (probe_count = 0; probe_count < group_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % group_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == 0)
            return slot;  /* Empty slot */
        
        idx--;
        if (entries[idx].relid == relid && entries[idx].group_value == group_value)
            return slot;  /* Existing entry */
    }
    
    return -1;  /* Cache full */
}

int32
xpatch_seq_cache_get_max_seq(Oid relid, Datum group_value, bool *found)
{
    int32 idx;
    int32 result = 0;
    GroupSeqEntry *entries;
    
    *found = false;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return 0;
    
    LWLockAcquire(group_cache->lock, LW_SHARED);
    
    idx = group_cache_find(relid, (int64) group_value);
    
    if (idx >= 0)
    {
        entries = group_cache_entries(group_cache);
        result = entries[idx].max_seq;
        *found = true;
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
xpatch_seq_cache_set_max_seq(Oid relid, Datum group_value, int32 max_seq)
{
    int32 idx;
    int32 slot;
    int32 *hash_index;
    GroupSeqEntry *entries;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return;
    
    LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
    
    slot = group_cache_find_slot_for_key(relid, (int64) group_value);
    
    if (slot < 0)
    {
        /* Cache full */
        LWLockRelease(group_cache->lock);
        return;
    }
    
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    idx = hash_index[slot];
    
    if (idx > 0)
    {
        /* Update existing entry */
        entries[idx - 1].max_seq = max_seq;
    }
    else if (group_cache->num_entries < group_cache->max_entries)
    {
        /* Add new entry */
        idx = group_cache->num_entries;
        entries[idx].relid = relid;
        entries[idx].group_value = (int64) group_value;
        entries[idx].max_seq = max_seq;
        hash_index[slot] = idx + 1;  /* Store 1-based index */
        group_cache->num_entries++;
    }
    
    LWLockRelease(group_cache->lock);
}

int32
xpatch_seq_cache_next_seq(Oid relid, Datum group_value)
{
    int32 idx;
    int32 slot;
    int32 new_seq;
    int32 *hash_index;
    GroupSeqEntry *entries;
    
    if (!seq_cache_initialized || group_cache == NULL)
        return 0;  /* Caller should fall back to scan */
    
    LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
    
    slot = group_cache_find_slot_for_key(relid, (int64) group_value);
    
    if (slot < 0)
    {
        /* Cache full */
        pg_atomic_fetch_add_u64(&group_cache->miss_count, 1);
        LWLockRelease(group_cache->lock);
        return 0;
    }
    
    hash_index = group_cache_hash_index(group_cache);
    entries = group_cache_entries(group_cache);
    idx = hash_index[slot];
    
    if (idx > 0)
    {
        /* Increment and return */
        new_seq = ++entries[idx - 1].max_seq;
        pg_atomic_fetch_add_u64(&group_cache->hit_count, 1);
    }
    else if (group_cache->num_entries < group_cache->max_entries)
    {
        /* New group - start at seq 1 */
        idx = group_cache->num_entries;
        entries[idx].relid = relid;
        entries[idx].group_value = (int64) group_value;
        entries[idx].max_seq = 1;
        hash_index[slot] = idx + 1;
        group_cache->num_entries++;
        new_seq = 1;
        pg_atomic_fetch_add_u64(&group_cache->miss_count, 1);
    }
    else
    {
        /* Cache full, return 0 to trigger fallback */
        new_seq = 0;
        pg_atomic_fetch_add_u64(&group_cache->miss_count, 1);
    }
    
    LWLockRelease(group_cache->lock);
    
    return new_seq;
}

/* ================================================================
 * TID Seq Cache Operations
 * ================================================================ */

/* Find entry index using hash lookup with linear probing, returns -1 if not found */
static int32
tid_cache_find(Oid relid, ItemPointer tid)
{
    uint32 h;
    int32 slot;
    int32 idx;
    int32 *hash_index;
    TidSeqEntry *entries;
    int probe_count;
    
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
        
        if (idx == 0)
            return -1;  /* Empty slot - not found */
        
        idx--;  /* Convert from 1-based to 0-based */
        if (entries[idx].relid == relid && ItemPointerEquals(&entries[idx].tid, tid))
            return idx;
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
    
    h = hash_tid_key(relid, tid);
    hash_index = tid_cache_hash_index(tid_cache);
    entries = tid_cache_entries(tid_cache);
    
    /* Linear probing to find empty slot or existing entry */
    for (probe_count = 0; probe_count < tid_cache->hash_size; probe_count++)
    {
        slot = (h + probe_count) % tid_cache->hash_size;
        idx = hash_index[slot];
        
        if (idx == 0)
            return slot;  /* Empty slot */
        
        idx--;
        if (entries[idx].relid == relid && ItemPointerEquals(&entries[idx].tid, tid))
            return slot;  /* Existing entry */
    }
    
    return -1;  /* Cache full */
}

int32
xpatch_seq_cache_get_tid_seq(Oid relid, ItemPointer tid, bool *found)
{
    int32 idx;
    int32 result = 0;
    TidSeqEntry *entries;
    
    *found = false;
    
    if (!seq_cache_initialized || tid_cache == NULL)
        return 0;
    
    LWLockAcquire(tid_cache->lock, LW_SHARED);
    
    idx = tid_cache_find(relid, tid);
    
    if (idx >= 0)
    {
        entries = tid_cache_entries(tid_cache);
        result = entries[idx].seq;
        *found = true;
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
xpatch_seq_cache_set_tid_seq(Oid relid, ItemPointer tid, int32 seq)
{
    int32 idx;
    int32 slot;
    int32 *hash_index;
    TidSeqEntry *entries;
    
    if (!seq_cache_initialized || tid_cache == NULL)
        return;
    
    LWLockAcquire(tid_cache->lock, LW_EXCLUSIVE);
    
    slot = tid_cache_find_slot_for_key(relid, tid);
    
    if (slot < 0)
    {
        /* Cache full */
        LWLockRelease(tid_cache->lock);
        return;
    }
    
    hash_index = tid_cache_hash_index(tid_cache);
    entries = tid_cache_entries(tid_cache);
    idx = hash_index[slot];
    
    if (idx > 0)
    {
        /* Update existing entry */
        entries[idx - 1].seq = seq;
    }
    else if (tid_cache->num_entries < tid_cache->max_entries)
    {
        /* Add new entry */
        idx = tid_cache->num_entries;
        entries[idx].relid = relid;
        ItemPointerCopy(tid, &entries[idx].tid);
        entries[idx].seq = seq;
        hash_index[slot] = idx + 1;  /* Store 1-based index */
        tid_cache->num_entries++;
    }
    
    LWLockRelease(tid_cache->lock);
}

void
xpatch_seq_cache_populate_group_tids(Oid relid, Datum group_value,
                                     ItemPointer *tids, int32 *seqs,
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
    
    for (i = 0; i < count && tid_cache->num_entries < tid_cache->max_entries; i++)
    {
        slot = tid_cache_find_slot_for_key(relid, tids[i]);
        
        if (slot < 0)
            break;  /* Cache full */
        
        idx = hash_index[slot];
        
        if (idx == 0)
        {
            /* Add new entry */
            idx = tid_cache->num_entries;
            entries[idx].relid = relid;
            ItemPointerCopy(tids[i], &entries[idx].tid);
            entries[idx].seq = seqs[i];
            hash_index[slot] = idx + 1;
            tid_cache->num_entries++;
        }
    }
    
    LWLockRelease(tid_cache->lock);
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
    int new_count;
    
    if (!seq_cache_initialized)
        return;
    
    /* 
     * For invalidation, we need to rebuild the hash index.
     * This is expensive but invalidation is rare (DDL operations).
     * We compact entries and rebuild the hash index.
     */
    
    /* Invalidate group cache entries */
    if (group_cache != NULL)
    {
        LWLockAcquire(group_cache->lock, LW_EXCLUSIVE);
        
        hash_index = group_cache_hash_index(group_cache);
        g_entries = group_cache_entries(group_cache);
        
        /* Clear hash index */
        memset(hash_index, 0, group_cache->hash_size * sizeof(int32));
        
        /* Compact entries and rebuild hash index */
        new_count = 0;
        for (i = 0; i < group_cache->num_entries; i++)
        {
            if (g_entries[i].relid != relid)
            {
                if (i != new_count)
                    g_entries[new_count] = g_entries[i];
                
                /* Reinsert into hash index */
                {
                    uint32 h = hash_group_key(g_entries[new_count].relid, 
                                               g_entries[new_count].group_value);
                    int32 slot;
                    int probe;
                    
                    for (probe = 0; probe < group_cache->hash_size; probe++)
                    {
                        slot = (h + probe) % group_cache->hash_size;
                        if (hash_index[slot] == 0)
                        {
                            hash_index[slot] = new_count + 1;
                            break;
                        }
                    }
                }
                new_count++;
            }
        }
        group_cache->num_entries = new_count;
        
        LWLockRelease(group_cache->lock);
    }
    
    /* Invalidate TID cache entries */
    if (tid_cache != NULL)
    {
        LWLockAcquire(tid_cache->lock, LW_EXCLUSIVE);
        
        hash_index = tid_cache_hash_index(tid_cache);
        t_entries = tid_cache_entries(tid_cache);
        
        /* Clear hash index */
        memset(hash_index, 0, tid_cache->hash_size * sizeof(int32));
        
        /* Compact entries and rebuild hash index */
        new_count = 0;
        for (i = 0; i < tid_cache->num_entries; i++)
        {
            if (t_entries[i].relid != relid)
            {
                if (i != new_count)
                    t_entries[new_count] = t_entries[i];
                
                /* Reinsert into hash index */
                {
                    uint32 h = hash_tid_key(t_entries[new_count].relid, 
                                             &t_entries[new_count].tid);
                    int32 slot;
                    int probe;
                    
                    for (probe = 0; probe < tid_cache->hash_size; probe++)
                    {
                        slot = (h + probe) % tid_cache->hash_size;
                        if (hash_index[slot] == 0)
                        {
                            hash_index[slot] = new_count + 1;
                            break;
                        }
                    }
                }
                new_count++;
            }
        }
        tid_cache->num_entries = new_count;
        
        LWLockRelease(tid_cache->lock);
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
}
