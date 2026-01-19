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
 * xpatch_cache.c - Shared LRU cache implementation
 *
 * Implements a shared memory LRU cache for decoded content.
 * The cache is shared across all PostgreSQL backends for better hit rates.
 *
 * Architecture:
 * - Fixed-size shared memory region allocated at startup
 * - Hash table for O(1) key lookup
 * - Doubly-linked LRU list for eviction
 * - LWLock for concurrent access
 * - Content stored in dynamically-sized slots within shmem
 *
 * Memory Layout:
 * [XPatchSharedCache header]
 * [Hash table entries - fixed count]
 * [Content buffer pool - remaining space]
 */

#include "xpatch_cache.h"
#include "xpatch_hash.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* Maximum entries in shared cache (fixed at startup) 
 * This should be large enough to hold all reconstructed values
 * for active workloads. Each entry is small (~64 bytes), so we
 * can afford many entries. The content slots hold the actual data.
 */
#define XPATCH_SHMEM_MAX_ENTRIES    65536

/* Maximum content size for a single entry (64KB) */
#define XPATCH_MAX_ENTRY_SIZE       (64 * 1024)

/* Content slot size (fixed to simplify memory management) */
#define XPATCH_SLOT_SIZE            4096

/* Cache entry key - must be fixed size for shared memory hash */
typedef struct XPatchCacheKey
{
    Oid             relid;
    XPatchGroupHash group_hash;     /* 128-bit BLAKE3 hash of group value */
    int32           seq;
    AttrNumber      attnum;
    int16           padding;        /* Alignment padding */
} XPatchCacheKey;

/* Cache entry - stored in shared hash table */
typedef struct XPatchCacheEntry
{
    XPatchCacheKey  key;
    int32           slot_index;     /* Index into content buffer (-1 if none) */
    int32           content_size;   /* Actual content size */
    int32           num_slots;      /* Number of slots used */
    int32           lru_prev;       /* Previous entry in LRU (index, -1 = head) */
    int32           lru_next;       /* Next entry in LRU (index, -1 = tail) */
    bool            in_use;         /* Entry is valid and holds data */
    bool            tombstone;      /* Entry was deleted, continue probing */
} XPatchCacheEntry;

/* Content slot header */
typedef struct XPatchContentSlot
{
    int32           next_slot;      /* Next slot in chain (-1 if last) */
    char            data[XPATCH_SLOT_SIZE - sizeof(int32)];
} XPatchContentSlot;

/* Shared cache header in shmem */
typedef struct XPatchSharedCache
{
    LWLock         *lock;               /* Protects all cache operations */
    int32           lru_head;           /* Most recently used entry index */
    int32           lru_tail;           /* Least recently used entry index */
    int32           num_entries;        /* Current entry count */
    int32           max_entries;        /* Maximum entries */
    int32           num_slots;          /* Total content slots */
    int32           free_slot_head;     /* Free slot list head */
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    pg_atomic_uint64 eviction_count;
    
    /* Hash table entries follow (fixed array) */
    XPatchCacheEntry entries[FLEXIBLE_ARRAY_MEMBER];
} XPatchSharedCache;

/* Pointers to shared memory structures */
static XPatchSharedCache *shared_cache = NULL;
static XPatchContentSlot *content_slots = NULL;
static bool shmem_initialized = false;

/* Hooks for shared memory */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * Backend exit callback - clears per-backend pointers to shared memory.
 * This is a defensive measure; PostgreSQL handles shared memory detachment
 * automatically, but clearing these helps catch bugs where code tries to
 * access the cache after the backend has started shutting down.
 */
static void
xpatch_cache_shmem_exit(int code, Datum arg)
{
    /* Clear per-backend pointers to shared memory */
    shared_cache = NULL;
    content_slots = NULL;
    shmem_initialized = false;
}

/*
 * Calculate required shared memory size
 */
static Size
xpatch_cache_shmem_size(void)
{
    Size size = 0;
    int num_slots;
    
    /* Header + entry array */
    size = offsetof(XPatchSharedCache, entries);
    size = add_size(size, mul_size(sizeof(XPatchCacheEntry), XPATCH_SHMEM_MAX_ENTRIES));
    
    /* Content slots - use remaining space up to cache_size_mb */
    num_slots = (xpatch_cache_size_mb * 1024 * 1024 - size) / sizeof(XPatchContentSlot);
    if (num_slots < 1000)
        num_slots = 1000;  /* Minimum slots */
    
    size = add_size(size, mul_size(sizeof(XPatchContentSlot), num_slots));
    
    return size;
}

/*
 * Shared memory request hook
 */
static void
xpatch_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();
    
    RequestAddinShmemSpace(xpatch_cache_shmem_size());
    RequestNamedLWLockTranche("pg_xpatch", 1);
}

/*
 * Initialize content slot free list
 */
static void
init_free_slots(int num_slots)
{
    int i;
    
    for (i = 0; i < num_slots - 1; i++)
    {
        content_slots[i].next_slot = i + 1;
    }
    content_slots[num_slots - 1].next_slot = -1;
    
    shared_cache->free_slot_head = 0;
    shared_cache->num_slots = num_slots;
}

/*
 * Allocate slots for content
 * Returns first slot index, or -1 if not enough space
 */
static int32
alloc_slots(int num_needed)
{
    int32 first_slot = -1;
    int32 prev_slot = -1;
    int i;
    
    for (i = 0; i < num_needed; i++)
    {
        int32 slot = shared_cache->free_slot_head;
        if (slot < 0)
        {
            /* Not enough slots - free what we allocated */
            while (first_slot >= 0)
            {
                int32 next = content_slots[first_slot].next_slot;
                content_slots[first_slot].next_slot = shared_cache->free_slot_head;
                shared_cache->free_slot_head = first_slot;
                first_slot = next;
            }
            return -1;
        }
        
        shared_cache->free_slot_head = content_slots[slot].next_slot;
        content_slots[slot].next_slot = -1;
        
        if (first_slot < 0)
            first_slot = slot;
        else
            content_slots[prev_slot].next_slot = slot;
        
        prev_slot = slot;
    }
    
    return first_slot;
}

/*
 * Free slots back to free list
 */
static void
free_slots(int32 first_slot)
{
    while (first_slot >= 0)
    {
        int32 next = content_slots[first_slot].next_slot;
        content_slots[first_slot].next_slot = shared_cache->free_slot_head;
        shared_cache->free_slot_head = first_slot;
        first_slot = next;
    }
}

/*
 * Remove entry from LRU list
 */
static void
lru_remove(XPatchCacheEntry *entry, int entry_idx)
{
    if (entry->lru_prev >= 0)
        shared_cache->entries[entry->lru_prev].lru_next = entry->lru_next;
    else
        shared_cache->lru_head = entry->lru_next;
    
    if (entry->lru_next >= 0)
        shared_cache->entries[entry->lru_next].lru_prev = entry->lru_prev;
    else
        shared_cache->lru_tail = entry->lru_prev;
    
    entry->lru_prev = -1;
    entry->lru_next = -1;
}

/*
 * Add entry to front of LRU list (most recently used)
 */
static void
lru_push_front(XPatchCacheEntry *entry, int entry_idx)
{
    entry->lru_prev = -1;
    entry->lru_next = shared_cache->lru_head;
    
    if (shared_cache->lru_head >= 0)
        shared_cache->entries[shared_cache->lru_head].lru_prev = entry_idx;
    else
        shared_cache->lru_tail = entry_idx;
    
    shared_cache->lru_head = entry_idx;
}

/*
 * Evict least recently used entry
 * Sets a tombstone marker so linear probing continues past this slot.
 */
static void
evict_lru_entry(void)
{
    int32 victim_idx = shared_cache->lru_tail;
    XPatchCacheEntry *victim;
    
    if (victim_idx < 0)
        return;
    
    victim = &shared_cache->entries[victim_idx];
    
    /* Remove from LRU list */
    lru_remove(victim, victim_idx);
    
    /* Free content slots */
    if (victim->slot_index >= 0)
        free_slots(victim->slot_index);
    
    /* 
     * Mark entry as tombstone (not in_use but also not empty).
     * This is crucial for linear probing - we must continue probing
     * past deleted entries to find entries that were inserted after
     * this one and probed past it during insertion.
     */
    victim->in_use = false;
    victim->tombstone = true;  /* Keep probing past this slot */
    victim->slot_index = -1;
    victim->content_size = 0;
    victim->num_slots = 0;
    
    shared_cache->num_entries--;
    pg_atomic_fetch_add_u64(&shared_cache->eviction_count, 1);
}

/*
 * Hash function for cache key - fast O(1) lookup
 * Uses FNV-1a to combine the 128-bit group hash with other key fields.
 */
static inline uint32
hash_cache_key(const XPatchCacheKey *key)
{
    uint32 h;
    
    /* FNV-1a style hash combining all key fields */
    h = 2166136261u;
    h ^= (uint32) key->relid;
    h *= 16777619u;
    /* Incorporate 128-bit group hash */
    h ^= (uint32) (key->group_hash.h1 & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h1 >> 32);
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h2 & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h2 >> 32);
    h *= 16777619u;
    h ^= (uint32) key->seq;
    h *= 16777619u;
    h ^= (uint32) key->attnum;
    h *= 16777619u;
    
    return h % XPATCH_SHMEM_MAX_ENTRIES;
}

/*
 * Find entry by key using hash table with linear probing - O(1) average
 * Properly handles tombstones to maintain correct probing behavior.
 */
static int32
find_entry(const XPatchCacheKey *key)
{
    uint32 hash = hash_cache_key(key);
    int probes = 0;
    
    while (probes < XPATCH_SHMEM_MAX_ENTRIES)
    {
        int32 idx = (hash + probes) % XPATCH_SHMEM_MAX_ENTRIES;
        XPatchCacheEntry *entry = &shared_cache->entries[idx];
        
        if (!entry->in_use && !entry->tombstone)
        {
            /* Empty slot (never used) - key not found */
            return -1;
        }
        
        /* Skip tombstones but continue probing */
        if (entry->tombstone)
        {
            probes++;
            continue;
        }
        
        /* Check if this is the entry we're looking for */
        if (entry->key.relid == key->relid &&
            xpatch_group_hash_equals(entry->key.group_hash, key->group_hash) &&
            entry->key.seq == key->seq &&
            entry->key.attnum == key->attnum)
        {
            return idx;
        }
        
        probes++;
    }
    
    return -1;
}

/*
 * Find free entry slot using hash-based placement
 * Uses the same hash as find_entry for consistency.
 * Can reuse tombstone slots to reclaim space.
 */
static int32
find_free_entry_for_key(const XPatchCacheKey *key)
{
    uint32 hash = hash_cache_key(key);
    int probes = 0;
    int32 first_tombstone = -1;
    
    while (probes < XPATCH_SHMEM_MAX_ENTRIES)
    {
        int32 idx = (hash + probes) % XPATCH_SHMEM_MAX_ENTRIES;
        XPatchCacheEntry *entry = &shared_cache->entries[idx];
        
        /* Empty slot (never used) - can use it */
        if (!entry->in_use && !entry->tombstone)
            return idx;
        
        /* 
         * Tombstone slot - remember the first one we see.
         * We prefer the first tombstone to maintain good probe locality.
         */
        if (entry->tombstone && first_tombstone < 0)
            first_tombstone = idx;
        
        probes++;
    }
    
    /* If we found a tombstone, we can reuse it */
    return first_tombstone;
}

/*
 * Copy content to slots
 */
static void
copy_to_slots(int32 first_slot, const bytea *content)
{
    Size content_len = VARSIZE(content);
    const char *src = (const char *) content;
    Size remaining = content_len;
    int32 slot = first_slot;
    Size slot_data_size = sizeof(content_slots[0].data);
    
    while (remaining > 0 && slot >= 0)
    {
        Size to_copy = Min(remaining, slot_data_size);
        memcpy(content_slots[slot].data, src, to_copy);
        src += to_copy;
        remaining -= to_copy;
        slot = content_slots[slot].next_slot;
    }
}

/*
 * Copy content from slots to palloc'd bytea
 */
static bytea *
copy_from_slots(int32 first_slot, Size content_size)
{
    bytea *result;
    char *dst;
    Size remaining = content_size;
    int32 slot = first_slot;
    Size slot_data_size = sizeof(content_slots[0].data);
    
    result = (bytea *) palloc(content_size);
    dst = (char *) result;
    
    while (remaining > 0 && slot >= 0)
    {
        Size to_copy = Min(remaining, slot_data_size);
        memcpy(dst, content_slots[slot].data, to_copy);
        dst += to_copy;
        remaining -= to_copy;
        slot = content_slots[slot].next_slot;
    }
    
    return result;
}

/*
 * Shared memory startup hook
 */
static void
xpatch_shmem_startup(void)
{
    bool found;
    Size cache_size;
    Size slots_offset;
    int num_slots;
    
    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();
    
    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
    
    cache_size = xpatch_cache_shmem_size();
    
    shared_cache = ShmemInitStruct("pg_xpatch cache",
                                    cache_size,
                                    &found);
    
    if (!found)
    {
        int i;
        
        /* Initialize cache header */
        memset(shared_cache, 0, cache_size);
        
        shared_cache->lock = &(GetNamedLWLockTranche("pg_xpatch"))->lock;
        shared_cache->lru_head = -1;
        shared_cache->lru_tail = -1;
        shared_cache->num_entries = 0;
        shared_cache->max_entries = XPATCH_SHMEM_MAX_ENTRIES;
        pg_atomic_init_u64(&shared_cache->hit_count, 0);
        pg_atomic_init_u64(&shared_cache->miss_count, 0);
        pg_atomic_init_u64(&shared_cache->eviction_count, 0);
        
        /* Initialize entry array */
        for (i = 0; i < XPATCH_SHMEM_MAX_ENTRIES; i++)
        {
            shared_cache->entries[i].in_use = false;
            shared_cache->entries[i].tombstone = false;
            shared_cache->entries[i].slot_index = -1;
            shared_cache->entries[i].lru_prev = -1;
            shared_cache->entries[i].lru_next = -1;
        }
        
        /* Calculate content slots location */
        slots_offset = offsetof(XPatchSharedCache, entries);
        slots_offset += sizeof(XPatchCacheEntry) * XPATCH_SHMEM_MAX_ENTRIES;
        
        content_slots = (XPatchContentSlot *) ((char *) shared_cache + slots_offset);
        
        num_slots = (cache_size - slots_offset) / sizeof(XPatchContentSlot);
        init_free_slots(num_slots);
        
        elog(LOG, "pg_xpatch: shared cache initialized (%d entries, %d content slots, %zu MB)",
             XPATCH_SHMEM_MAX_ENTRIES, num_slots, cache_size / (1024 * 1024));
    }
    else
    {
        /* Attach to existing cache - just set content_slots pointer */
        slots_offset = offsetof(XPatchSharedCache, entries);
        slots_offset += sizeof(XPatchCacheEntry) * XPATCH_SHMEM_MAX_ENTRIES;
        content_slots = (XPatchContentSlot *) ((char *) shared_cache + slots_offset);
    }
    
    LWLockRelease(AddinShmemInitLock);
    
    shmem_initialized = true;
    
    /* Register exit callback to clear per-backend pointers */
    on_shmem_exit(xpatch_cache_shmem_exit, (Datum) 0);
}

/*
 * Request shared memory space
 * Must be called from _PG_init()
 */
void
xpatch_cache_request_shmem(void)
{
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = xpatch_shmem_request;
    
    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = xpatch_shmem_startup;
}

/*
 * Initialize the cache (called from _PG_init, but actual init is in startup hook)
 */
void
xpatch_cache_init(void)
{
    /* Actual initialization happens in shmem_startup_hook */
}

/*
 * Look up content in the cache
 */
bytea *
xpatch_cache_get(Oid relid, Datum group_value, Oid typid, int32 seq, AttrNumber attnum)
{
    XPatchCacheKey key;
    int32 entry_idx;
    XPatchCacheEntry *entry;
    bytea *result = NULL;
    
    if (!shmem_initialized || shared_cache == NULL)
        return NULL;
    
    /* Build key with 128-bit BLAKE3 hash of group value */
    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.group_hash = xpatch_compute_group_hash(group_value, typid, false);
    key.seq = seq;
    key.attnum = attnum;
    
    LWLockAcquire(shared_cache->lock, LW_SHARED);
    
    entry_idx = find_entry(&key);
    
    if (entry_idx >= 0)
    {
        entry = &shared_cache->entries[entry_idx];
        
        /* Copy content to caller's memory */
        if (entry->slot_index >= 0 && entry->content_size > 0)
        {
            result = copy_from_slots(entry->slot_index, entry->content_size);
        }
        
        pg_atomic_fetch_add_u64(&shared_cache->hit_count, 1);
        
        /* Move to front of LRU - need exclusive lock */
        LWLockRelease(shared_cache->lock);
        LWLockAcquire(shared_cache->lock, LW_EXCLUSIVE);
        
        /* Re-check entry is still valid after lock upgrade */
        if (entry->in_use)
        {
            lru_remove(entry, entry_idx);
            lru_push_front(entry, entry_idx);
        }
    }
    else
    {
        pg_atomic_fetch_add_u64(&shared_cache->miss_count, 1);
    }
    
    LWLockRelease(shared_cache->lock);
    
    return result;
}

/*
 * Store content in the cache
 */
void
xpatch_cache_put(Oid relid, Datum group_value, Oid typid, int32 seq,
                 AttrNumber attnum, bytea *content)
{
    XPatchCacheKey key;
    int32 entry_idx;
    XPatchCacheEntry *entry;
    Size content_size;
    int num_slots_needed;
    int32 first_slot;
    
    if (!shmem_initialized || shared_cache == NULL || content == NULL)
        return;
    
    content_size = VARSIZE(content);
    
    /* Don't cache very large entries */
    if (content_size > XPATCH_MAX_ENTRY_SIZE)
        return;
    
    /* Calculate slots needed */
    num_slots_needed = (content_size + sizeof(content_slots[0].data) - 1) / 
                       sizeof(content_slots[0].data);
    
    /* Build key with 128-bit BLAKE3 hash of group value */
    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.group_hash = xpatch_compute_group_hash(group_value, typid, false);
    key.seq = seq;
    key.attnum = attnum;
    
    LWLockAcquire(shared_cache->lock, LW_EXCLUSIVE);
    
    /* Check if already cached */
    entry_idx = find_entry(&key);
    if (entry_idx >= 0)
    {
        /* Already cached - just move to front of LRU */
        entry = &shared_cache->entries[entry_idx];
        lru_remove(entry, entry_idx);
        lru_push_front(entry, entry_idx);
        LWLockRelease(shared_cache->lock);
        return;
    }
    
    /* Allocate content slots */
    first_slot = alloc_slots(num_slots_needed);
    
    /* If not enough slots, evict until we have space */
    while (first_slot < 0 && shared_cache->num_entries > 0)
    {
        evict_lru_entry();
        first_slot = alloc_slots(num_slots_needed);
    }
    
    if (first_slot < 0)
    {
        /* Still no space - give up */
        LWLockRelease(shared_cache->lock);
        return;
    }
    
    /* Find free entry at appropriate hash position */
    entry_idx = find_free_entry_for_key(&key);
    
    /* If no free entry, evict until we have space */
    while (entry_idx < 0 && shared_cache->num_entries > 0)
    {
        evict_lru_entry();
        entry_idx = find_free_entry_for_key(&key);
    }
    
    if (entry_idx < 0)
    {
        /* No entry available - free slots and give up */
        free_slots(first_slot);
        LWLockRelease(shared_cache->lock);
        return;
    }
    
    /* Initialize entry */
    entry = &shared_cache->entries[entry_idx];
    memcpy(&entry->key, &key, sizeof(key));
    entry->slot_index = first_slot;
    entry->content_size = content_size;
    entry->num_slots = num_slots_needed;
    entry->in_use = true;
    entry->tombstone = false;  /* Clear tombstone when reusing slot */
    
    /* Copy content to slots */
    copy_to_slots(first_slot, content);
    
    /* Add to LRU */
    lru_push_front(entry, entry_idx);
    shared_cache->num_entries++;
    
    LWLockRelease(shared_cache->lock);
}

/*
 * Invalidate all cache entries for a relation
 */
void
xpatch_cache_invalidate_rel(Oid relid)
{
    int i;
    
    if (!shmem_initialized || shared_cache == NULL)
        return;
    
    LWLockAcquire(shared_cache->lock, LW_EXCLUSIVE);
    
    for (i = 0; i < XPATCH_SHMEM_MAX_ENTRIES; i++)
    {
        XPatchCacheEntry *entry = &shared_cache->entries[i];
        
        if (entry->in_use && entry->key.relid == relid)
        {
            lru_remove(entry, i);
            
            if (entry->slot_index >= 0)
                free_slots(entry->slot_index);
            
            entry->in_use = false;
            entry->slot_index = -1;
            entry->content_size = 0;
            entry->num_slots = 0;
            
            shared_cache->num_entries--;
        }
    }
    
    LWLockRelease(shared_cache->lock);
}

/*
 * Get cache statistics
 */
void
xpatch_cache_get_stats(XPatchCacheStats *stats)
{
    if (!shmem_initialized || shared_cache == NULL)
    {
        memset(stats, 0, sizeof(*stats));
        return;
    }
    
    LWLockAcquire(shared_cache->lock, LW_SHARED);
    
    stats->entries_count = shared_cache->num_entries;
    stats->max_bytes = xpatch_cache_size_mb * 1024 * 1024;
    stats->hit_count = pg_atomic_read_u64(&shared_cache->hit_count);
    stats->miss_count = pg_atomic_read_u64(&shared_cache->miss_count);
    stats->eviction_count = pg_atomic_read_u64(&shared_cache->eviction_count);
    
    /* Estimate current size from entries */
    {
        int i;
        Size total = 0;
        for (i = 0; i < XPATCH_SHMEM_MAX_ENTRIES; i++)
        {
            if (shared_cache->entries[i].in_use)
                total += shared_cache->entries[i].content_size;
        }
        stats->size_bytes = total;
    }
    
    LWLockRelease(shared_cache->lock);
}
