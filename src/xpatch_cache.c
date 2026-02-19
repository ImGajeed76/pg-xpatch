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
 * xpatch_cache.c - Shared LRU cache with lock striping
 *
 * Implements a shared memory LRU cache for decoded content.
 * The cache is shared across all PostgreSQL backends for better hit rates.
 *
 * Lock Striping (v0.6.0):
 * The cache is partitioned into N = xpatch_cache_partitions independent
 * stripes. Each stripe has its own LWLock, LRU list, entry hash table,
 * and content slot free list. Key hash determines stripe assignment:
 *   stripe_idx = hash(key) % num_stripes
 *
 * This allows N concurrent backends to access the cache without contention,
 * as long as they access different stripes.
 *
 * Memory Layout (per stripe):
 *   [XPatchCacheStripe header]
 *   ...all stripe headers contiguous...
 *   [Entry arrays - one per stripe, contiguous]
 *   [Content slot buffer - one pool per stripe, contiguous]
 *
 * Global layout in shared memory:
 *   [XPatchSharedCache header with stripe array]
 *   [All entry arrays]
 *   [All content slot buffers]
 */

#include "xpatch_cache.h"
#include "xpatch_hash.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

/* GUC variables (registered in pg_xpatch.c, defaults set here) */
int xpatch_cache_max_entries = 65536;
int xpatch_cache_slot_size_kb = 4;    /* in KB, converted to bytes at startup */
int xpatch_cache_partitions = 32;

/* Cache entry key - must be fixed size for shared memory hash */
typedef struct XPatchCacheKey
{
    Oid             relid;
    XPatchGroupHash group_hash;     /* 128-bit BLAKE3 hash of group value */
    int64           seq;
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

/*
 * Content slots: variable-size, computed from xpatch_cache_slot_size_kb GUC.
 *
 * Each slot is laid out as:
 *   [int32 next_slot][char data[slot_data_size]]
 *
 * Total slot size = xpatch_cache_slot_size_kb * 1024 bytes.
 * Data area = total - sizeof(int32).
 *
 * These statics are computed once in shmem_startup and remain fixed
 * for the lifetime of the postmaster (PGC_POSTMASTER GUC).
 */
static int   slot_total_size;            /* xpatch_cache_slot_size_kb * 1024 */
static int   slot_data_size;             /* slot_total_size - sizeof(int32) */

/*
 * Per-stripe cache partition.
 *
 * Each stripe independently manages its own entries, LRU list, slot pool,
 * and statistics. This eliminates contention between backends accessing
 * different stripes.
 */
typedef struct XPatchCacheStripe
{
    LWLock             *lock;
    int32               lru_head;           /* Most recently used entry index (stripe-local) */
    int32               lru_tail;           /* Least recently used entry index (stripe-local) */
    int32               num_entries;        /* Current entry count in this stripe */
    int32               max_entries;        /* Max entries for this stripe */
    int32               free_slot_head;     /* Free slot list head (stripe-local index) */
    int32               num_slots;          /* Total content slots in this stripe */
    pg_atomic_uint64    hit_count;
    pg_atomic_uint64    miss_count;
    pg_atomic_uint64    eviction_count;
    pg_atomic_uint64    skip_count;         /* entries rejected by size limit */
} XPatchCacheStripe;

/*
 * Shared cache header in shmem.
 *
 * The stripe array is embedded at the end via FLEXIBLE_ARRAY_MEMBER.
 * Entry arrays and slot buffers follow after the header in shmem,
 * computed via offsets.
 */
typedef struct XPatchSharedCache
{
    int32               num_stripes;
    int32               total_entries;      /* Sum of all stripe max_entries */
    int32               total_slots;        /* Sum of all stripe num_slots */
    XPatchCacheStripe   stripes[FLEXIBLE_ARRAY_MEMBER];
} XPatchSharedCache;

/* Pointers to shared memory structures */
static XPatchSharedCache *shared_cache = NULL;

/*
 * Per-stripe base pointers into shmem (computed once per backend).
 * stripe_entries_base[i] points to the start of stripe i's entry array.
 * stripe_slots_base[i] points to the start of stripe i's slot buffer.
 */
static XPatchCacheEntry **stripe_entries_base = NULL;
static char            **stripe_slots_base = NULL;

static bool shmem_initialized = false;

/* Hooks for shared memory */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* --- Slot access helpers (per-stripe) --- */

static inline int32 *
slot_next_ptr(char *slots_base, int32 idx)
{
    return (int32 *)(slots_base + (Size)idx * slot_total_size);
}

static inline char *
slot_data_ptr(char *slots_base, int32 idx)
{
    return slots_base + (Size)idx * slot_total_size + sizeof(int32);
}

/* --- Hash function --- */

/*
 * Compute a 32-bit hash of the cache key using FNV-1a.
 *
 * The group_hash field already contains 128 bits of BLAKE3 output,
 * so FNV-1a on the composite key gives excellent distribution.
 *
 * Returns the raw 32-bit hash (caller applies modulo).
 */
static inline uint32
hash_cache_key_raw(const XPatchCacheKey *key)
{
    uint32 h;

    h = 2166136261u;
    h ^= (uint32) key->relid;
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h1 & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h1 >> 32);
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h2 & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (key->group_hash.h2 >> 32);
    h *= 16777619u;
    h ^= (uint32) (key->seq & 0xFFFFFFFF);
    h *= 16777619u;
    h ^= (uint32) (key->seq >> 32);
    h *= 16777619u;
    h ^= (uint32) key->attnum;
    h *= 16777619u;

    return h;
}

/*
 * Determine which stripe a key belongs to.
 */
static inline int
key_to_stripe(const XPatchCacheKey *key)
{
    return (int)(hash_cache_key_raw(key) % (uint32)shared_cache->num_stripes);
}

/*
 * Hash a key to a probe start index within a stripe's entry array.
 */
static inline uint32
hash_cache_key_for_stripe(const XPatchCacheKey *key, int32 max_entries)
{
    return hash_cache_key_raw(key) % (uint32)max_entries;
}

/*
 * Backend exit callback - clears per-backend pointers to shared memory.
 */
static void
xpatch_cache_shmem_exit(int code, Datum arg)
{
    shared_cache = NULL;
    if (stripe_entries_base)
    {
        pfree(stripe_entries_base);
        stripe_entries_base = NULL;
    }
    if (stripe_slots_base)
    {
        pfree(stripe_slots_base);
        stripe_slots_base = NULL;
    }
    shmem_initialized = false;
}

/* --- Per-stripe internal operations --- */

/*
 * Initialize content slot free list for a stripe.
 */
static void
init_free_slots(XPatchCacheStripe *stripe, char *slots_base, int num_slots)
{
    int i;

    for (i = 0; i < num_slots - 1; i++)
    {
        *slot_next_ptr(slots_base, i) = i + 1;
    }
    *slot_next_ptr(slots_base, num_slots - 1) = -1;

    stripe->free_slot_head = 0;
    stripe->num_slots = num_slots;
}

/*
 * Allocate slots from a stripe's free list.
 * Returns first slot index (stripe-local), or -1 if not enough space.
 */
static int32
alloc_slots(XPatchCacheStripe *stripe, char *slots_base, int num_needed)
{
    int32 first_slot = -1;
    int32 prev_slot = -1;
    int i;

    for (i = 0; i < num_needed; i++)
    {
        int32 slot = stripe->free_slot_head;
        if (slot < 0)
        {
            /* Not enough slots - free what we allocated */
            while (first_slot >= 0)
            {
                int32 next = *slot_next_ptr(slots_base, first_slot);
                *slot_next_ptr(slots_base, first_slot) = stripe->free_slot_head;
                stripe->free_slot_head = first_slot;
                first_slot = next;
            }
            return -1;
        }

        stripe->free_slot_head = *slot_next_ptr(slots_base, slot);
        *slot_next_ptr(slots_base, slot) = -1;

        if (first_slot < 0)
            first_slot = slot;
        else
            *slot_next_ptr(slots_base, prev_slot) = slot;

        prev_slot = slot;
    }

    return first_slot;
}

/*
 * Free slots back to a stripe's free list.
 */
static void
free_slots(XPatchCacheStripe *stripe, char *slots_base, int32 first_slot)
{
    while (first_slot >= 0)
    {
        int32 next = *slot_next_ptr(slots_base, first_slot);
        *slot_next_ptr(slots_base, first_slot) = stripe->free_slot_head;
        stripe->free_slot_head = first_slot;
        first_slot = next;
    }
}

/*
 * Remove entry from a stripe's LRU list.
 */
static void
lru_remove(XPatchCacheStripe *stripe, XPatchCacheEntry *entries,
           XPatchCacheEntry *entry, int entry_idx)
{
    if (entry->lru_prev >= 0)
        entries[entry->lru_prev].lru_next = entry->lru_next;
    else
        stripe->lru_head = entry->lru_next;

    if (entry->lru_next >= 0)
        entries[entry->lru_next].lru_prev = entry->lru_prev;
    else
        stripe->lru_tail = entry->lru_prev;

    entry->lru_prev = -1;
    entry->lru_next = -1;
}

/*
 * Add entry to front of a stripe's LRU list (most recently used).
 */
static void
lru_push_front(XPatchCacheStripe *stripe, XPatchCacheEntry *entries,
               XPatchCacheEntry *entry, int entry_idx)
{
    entry->lru_prev = -1;
    entry->lru_next = stripe->lru_head;

    if (stripe->lru_head >= 0)
        entries[stripe->lru_head].lru_prev = entry_idx;
    else
        stripe->lru_tail = entry_idx;

    stripe->lru_head = entry_idx;
}

/*
 * Evict least recently used entry from a stripe.
 */
static void
evict_lru_entry(XPatchCacheStripe *stripe, XPatchCacheEntry *entries,
                char *slots_base)
{
    int32 victim_idx = stripe->lru_tail;
    XPatchCacheEntry *victim;

    if (victim_idx < 0)
        return;

    victim = &entries[victim_idx];

    /* Remove from LRU list */
    lru_remove(stripe, entries, victim, victim_idx);

    /* Free content slots */
    if (victim->slot_index >= 0)
        free_slots(stripe, slots_base, victim->slot_index);

    /*
     * Mark entry as tombstone (not in_use but also not empty).
     * Crucial for linear probing correctness.
     */
    victim->in_use = false;
    victim->tombstone = true;
    victim->slot_index = -1;
    victim->content_size = 0;
    victim->num_slots = 0;

    stripe->num_entries--;
    pg_atomic_fetch_add_u64(&stripe->eviction_count, 1);
}

/*
 * Find entry by key in a stripe using linear probing.
 */
static int32
find_entry(XPatchCacheEntry *entries, int32 max_entries,
           const XPatchCacheKey *key)
{
    uint32 hash = hash_cache_key_for_stripe(key, max_entries);
    int probes = 0;

    while (probes < max_entries)
    {
        int32 idx = (hash + probes) % max_entries;
        XPatchCacheEntry *entry = &entries[idx];

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
 * Find free entry slot in a stripe using hash-based placement.
 * Can reuse tombstone slots.
 */
static int32
find_free_entry_for_key(XPatchCacheEntry *entries, int32 max_entries,
                        const XPatchCacheKey *key)
{
    uint32 hash = hash_cache_key_for_stripe(key, max_entries);
    int probes = 0;
    int32 first_tombstone = -1;

    while (probes < max_entries)
    {
        int32 idx = (hash + probes) % max_entries;
        XPatchCacheEntry *entry = &entries[idx];

        /* Empty slot (never used) - can use it */
        if (!entry->in_use && !entry->tombstone)
            return idx;

        /* Tombstone slot - remember the first one */
        if (entry->tombstone && first_tombstone < 0)
            first_tombstone = idx;

        probes++;
    }

    /* If we found a tombstone, we can reuse it */
    return first_tombstone;
}

/*
 * Copy content to slots within a stripe's slot buffer.
 */
static void
copy_to_slots(char *slots_base, int32 first_slot, const bytea *content)
{
    Size content_len = VARSIZE(content);
    const char *src = (const char *) content;
    Size remaining = content_len;
    int32 slot = first_slot;

    while (remaining > 0 && slot >= 0)
    {
        Size to_copy = Min(remaining, (Size)slot_data_size);
        memcpy(slot_data_ptr(slots_base, slot), src, to_copy);
        src += to_copy;
        remaining -= to_copy;
        slot = *slot_next_ptr(slots_base, slot);
    }
}

/*
 * Copy content from slots to palloc'd bytea.
 */
static bytea *
copy_from_slots(char *slots_base, int32 first_slot, Size content_size)
{
    bytea *result;
    char *dst;
    Size remaining = content_size;
    int32 slot = first_slot;

    result = (bytea *) palloc(content_size);
    dst = (char *) result;

    while (remaining > 0 && slot >= 0)
    {
        Size to_copy = Min(remaining, (Size)slot_data_size);
        memcpy(dst, slot_data_ptr(slots_base, slot), to_copy);
        dst += to_copy;
        remaining -= to_copy;
        slot = *slot_next_ptr(slots_base, slot);
    }

    return result;
}

/* --- Shared memory size calculation --- */

/*
 * Calculate required shared memory size.
 *
 * Layout:
 *   [XPatchSharedCache header + stripe array]
 *   [Entry arrays: num_stripes * entries_per_stripe * sizeof(XPatchCacheEntry)]
 *   [Slot buffers: remaining space up to cache_size_mb]
 */
static Size
xpatch_cache_shmem_size(void)
{
    Size size = 0;
    Size header_size;
    Size entries_size;
    Size slot_bytes;
    int num_stripes = xpatch_cache_partitions;
    int entries_per_stripe;
    int total_slots;

    /* Header + stripe array */
    header_size = offsetof(XPatchSharedCache, stripes);
    header_size = add_size(header_size, mul_size(sizeof(XPatchCacheStripe), num_stripes));
    header_size = MAXALIGN(header_size);

    /* Entry arrays for all stripes */
    entries_per_stripe = xpatch_cache_max_entries / num_stripes;
    if (entries_per_stripe < 64)
        entries_per_stripe = 64;  /* Minimum entries per stripe */
    entries_size = mul_size(sizeof(XPatchCacheEntry),
                            mul_size(entries_per_stripe, num_stripes));
    entries_size = MAXALIGN(entries_size);

    size = add_size(header_size, entries_size);

    /* Content slots - use remaining space up to cache_size_mb */
    slot_bytes = (Size)xpatch_cache_slot_size_kb * 1024;
    total_slots = ((Size)xpatch_cache_size_mb * 1024 * 1024 - size) / slot_bytes;
    if (total_slots < num_stripes)
        total_slots = num_stripes;  /* At least 1 slot per stripe */

    size = add_size(size, mul_size(slot_bytes, total_slots));

    return size;
}

/*
 * Shared memory request hook.
 */
static void
xpatch_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(xpatch_cache_shmem_size());
    RequestNamedLWLockTranche("pg_xpatch", xpatch_cache_partitions);
}

/*
 * Shared memory startup hook.
 */
static void
xpatch_shmem_startup(void)
{
    bool found;
    Size cache_size;
    Size header_size;
    Size entries_size;
    Size slot_bytes;
    int num_stripes = xpatch_cache_partitions;
    int entries_per_stripe;
    int total_slots;
    int slots_per_stripe;
    int extra_slots;
    char *entries_start;
    char *slots_start;
    int s;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    cache_size = xpatch_cache_shmem_size();

    shared_cache = ShmemInitStruct("pg_xpatch cache",
                                    cache_size,
                                    &found);

    /* Compute slot layout statics (same for postmaster and children) */
    slot_total_size = xpatch_cache_slot_size_kb * 1024;
    slot_data_size = slot_total_size - (int)sizeof(int32);

    /* Compute layout parameters (needed by both init and attach paths) */
    header_size = offsetof(XPatchSharedCache, stripes);
    header_size = add_size(header_size, mul_size(sizeof(XPatchCacheStripe), num_stripes));
    header_size = MAXALIGN(header_size);

    entries_per_stripe = xpatch_cache_max_entries / num_stripes;
    if (entries_per_stripe < 64)
        entries_per_stripe = 64;
    entries_size = mul_size(sizeof(XPatchCacheEntry),
                            mul_size(entries_per_stripe, num_stripes));
    entries_size = MAXALIGN(entries_size);

    slot_bytes = (Size)xpatch_cache_slot_size_kb * 1024;
    total_slots = (cache_size - header_size - entries_size) / slot_bytes;
    if (total_slots < num_stripes)
        total_slots = num_stripes;

    slots_per_stripe = total_slots / num_stripes;
    extra_slots = total_slots % num_stripes;

    /* Compute base pointers into shmem */
    entries_start = (char *) shared_cache + header_size;
    slots_start = entries_start + entries_size;

    if (!found)
    {
        LWLockPadded *locks;

        /* Zero out entire region */
        memset(shared_cache, 0, cache_size);

        shared_cache->num_stripes = num_stripes;
        shared_cache->total_entries = entries_per_stripe * num_stripes;
        shared_cache->total_slots = total_slots;

        locks = GetNamedLWLockTranche("pg_xpatch");

        /* Initialize each stripe */
        for (s = 0; s < num_stripes; s++)
        {
            XPatchCacheStripe *stripe = &shared_cache->stripes[s];
            XPatchCacheEntry *entries;
            char *sslots;
            int this_stripe_slots;
            int this_stripe_entries = entries_per_stripe;
            int i;

            stripe->lock = &locks[s].lock;
            stripe->lru_head = -1;
            stripe->lru_tail = -1;
            stripe->num_entries = 0;
            stripe->max_entries = this_stripe_entries;
            pg_atomic_init_u64(&stripe->hit_count, 0);
            pg_atomic_init_u64(&stripe->miss_count, 0);
            pg_atomic_init_u64(&stripe->eviction_count, 0);
            pg_atomic_init_u64(&stripe->skip_count, 0);

            /* Entry array for this stripe */
            entries = (XPatchCacheEntry *)(entries_start +
                        (Size)s * entries_per_stripe * sizeof(XPatchCacheEntry));
            for (i = 0; i < this_stripe_entries; i++)
            {
                entries[i].in_use = false;
                entries[i].tombstone = false;
                entries[i].slot_index = -1;
                entries[i].lru_prev = -1;
                entries[i].lru_next = -1;
            }

            /* Slot buffer for this stripe */
            /* Distribute extra slots to first stripes (1 extra each) */
            this_stripe_slots = slots_per_stripe + (s < extra_slots ? 1 : 0);

            /* Compute slot offset: sum of slots for all previous stripes */
            {
                int slot_offset = 0;
                int j;
                for (j = 0; j < s; j++)
                    slot_offset += slots_per_stripe + (j < extra_slots ? 1 : 0);

                sslots = slots_start + (Size)slot_offset * slot_bytes;
            }

            stripe->num_slots = this_stripe_slots;
            if (this_stripe_slots > 0)
                init_free_slots(stripe, sslots, this_stripe_slots);
            else
                stripe->free_slot_head = -1;
        }

        elog(LOG, "pg_xpatch: shared cache initialized (%d stripes, %d entries/stripe, "
             "%d total slots, %zu MB)",
             num_stripes, entries_per_stripe, total_slots,
             cache_size / (1024 * 1024));
    }

    /* Build per-backend pointer arrays (both init and attach paths) */
    {
        MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
        stripe_entries_base = palloc(sizeof(XPatchCacheEntry *) * num_stripes);
        stripe_slots_base = palloc(sizeof(char *) * num_stripes);
        MemoryContextSwitchTo(old_ctx);

        for (s = 0; s < num_stripes; s++)
        {
            int slot_offset = 0;
            int j;

            stripe_entries_base[s] = (XPatchCacheEntry *)(entries_start +
                        (Size)s * entries_per_stripe * sizeof(XPatchCacheEntry));

            for (j = 0; j < s; j++)
                slot_offset += slots_per_stripe + (j < extra_slots ? 1 : 0);
            stripe_slots_base[s] = slots_start + (Size)slot_offset * slot_bytes;
        }
    }

    LWLockRelease(AddinShmemInitLock);

    shmem_initialized = true;

    /* Register exit callback to clear per-backend pointers */
    on_shmem_exit(xpatch_cache_shmem_exit, (Datum) 0);
}

/*
 * Request shared memory space.
 * Must be called from _PG_init().
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
 * Initialize the cache (called from _PG_init, actual init in startup hook).
 */
void
xpatch_cache_init(void)
{
    /* Actual initialization happens in shmem_startup_hook */
}

/* --- Public API --- */

/*
 * Look up content in the cache.
 */
bytea *
xpatch_cache_get(Oid relid, Datum group_value, Oid typid, int64 seq, AttrNumber attnum)
{
    XPatchCacheKey key;
    int stripe_idx;
    XPatchCacheStripe *stripe;
    XPatchCacheEntry *entries;
    char *slots_base;
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

    /* Determine stripe */
    stripe_idx = key_to_stripe(&key);
    stripe = &shared_cache->stripes[stripe_idx];
    entries = stripe_entries_base[stripe_idx];
    slots_base = stripe_slots_base[stripe_idx];

    LWLockAcquire(stripe->lock, LW_SHARED);

    entry_idx = find_entry(entries, stripe->max_entries, &key);

    if (entry_idx >= 0)
    {
        entry = &entries[entry_idx];

        /* Copy content to caller's memory */
        if (entry->slot_index >= 0 && entry->content_size > 0)
        {
            result = copy_from_slots(slots_base, entry->slot_index,
                                     entry->content_size);
        }

        pg_atomic_fetch_add_u64(&stripe->hit_count, 1);

        /* Move to front of LRU - need exclusive lock */
        LWLockRelease(stripe->lock);
        LWLockAcquire(stripe->lock, LW_EXCLUSIVE);

        /*
         * Re-check entry is still valid after lock upgrade.
         * Between releasing shared and acquiring exclusive lock, another
         * backend could have evicted this entry and reused the slot.
         */
        if (entry->in_use &&
            memcmp(&entry->key, &key, sizeof(XPatchCacheKey)) == 0)
        {
            lru_remove(stripe, entries, entry, entry_idx);
            lru_push_front(stripe, entries, entry, entry_idx);
        }
    }
    else
    {
        pg_atomic_fetch_add_u64(&stripe->miss_count, 1);
    }

    LWLockRelease(stripe->lock);

    return result;
}

/*
 * Store content in the cache.
 */
void
xpatch_cache_put(Oid relid, Datum group_value, Oid typid, int64 seq,
                 AttrNumber attnum, bytea *content)
{
    XPatchCacheKey key;
    int stripe_idx;
    XPatchCacheStripe *stripe;
    XPatchCacheEntry *entries;
    char *slots_base;
    int32 entry_idx;
    XPatchCacheEntry *entry;
    Size content_size;
    int num_slots_needed;
    int32 first_slot;

    if (!shmem_initialized || shared_cache == NULL || content == NULL)
        return;

    content_size = VARSIZE(content);

    /* Don't cache entries that exceed the configurable size limit */
    if (content_size > (Size) xpatch_cache_max_entry_kb * 1024)
    {
        static bool warned = false;

        /* Use stripe 0 for skip_count when we haven't computed key yet */
        if (shared_cache->num_stripes > 0)
            pg_atomic_fetch_add_u64(&shared_cache->stripes[0].skip_count, 1);

        if (!warned)
        {
            elog(WARNING, "pg_xpatch: cache entry of %zu bytes exceeds limit of %d KB; "
                 "consider increasing pg_xpatch.cache_max_entry_kb",
                 content_size, xpatch_cache_max_entry_kb);
            warned = true;
        }
        else
        {
            elog(DEBUG1, "pg_xpatch: cache skip %zu bytes (limit %d KB)",
                 content_size, xpatch_cache_max_entry_kb);
        }
        return;
    }

    /* Calculate slots needed */
    num_slots_needed = (content_size + slot_data_size - 1) / slot_data_size;

    /* Build key with 128-bit BLAKE3 hash of group value */
    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.group_hash = xpatch_compute_group_hash(group_value, typid, false);
    key.seq = seq;
    key.attnum = attnum;

    /* Determine stripe */
    stripe_idx = key_to_stripe(&key);
    stripe = &shared_cache->stripes[stripe_idx];
    entries = stripe_entries_base[stripe_idx];
    slots_base = stripe_slots_base[stripe_idx];

    LWLockAcquire(stripe->lock, LW_EXCLUSIVE);

    /* Check if already cached */
    entry_idx = find_entry(entries, stripe->max_entries, &key);
    if (entry_idx >= 0)
    {
        /* Already cached - just move to front of LRU */
        entry = &entries[entry_idx];
        lru_remove(stripe, entries, entry, entry_idx);
        lru_push_front(stripe, entries, entry, entry_idx);
        LWLockRelease(stripe->lock);
        return;
    }

    /* Allocate content slots */
    first_slot = alloc_slots(stripe, slots_base, num_slots_needed);

    /* If not enough slots, evict until we have space */
    while (first_slot < 0 && stripe->num_entries > 0)
    {
        evict_lru_entry(stripe, entries, slots_base);
        first_slot = alloc_slots(stripe, slots_base, num_slots_needed);
    }

    if (first_slot < 0)
    {
        /* Still no space - give up */
        LWLockRelease(stripe->lock);
        return;
    }

    /* Find free entry at appropriate hash position */
    entry_idx = find_free_entry_for_key(entries, stripe->max_entries, &key);

    /* If no free entry, evict until we have space */
    while (entry_idx < 0 && stripe->num_entries > 0)
    {
        evict_lru_entry(stripe, entries, slots_base);
        entry_idx = find_free_entry_for_key(entries, stripe->max_entries, &key);
    }

    if (entry_idx < 0)
    {
        /* No entry available - free slots and give up */
        free_slots(stripe, slots_base, first_slot);
        LWLockRelease(stripe->lock);
        return;
    }

    /* Initialize entry */
    entry = &entries[entry_idx];
    memcpy(&entry->key, &key, sizeof(key));
    entry->slot_index = first_slot;
    entry->content_size = content_size;
    entry->num_slots = num_slots_needed;
    entry->in_use = true;
    entry->tombstone = false;

    /* Copy content to slots */
    copy_to_slots(slots_base, first_slot, content);

    /* Add to LRU */
    lru_push_front(stripe, entries, entry, entry_idx);
    stripe->num_entries++;

    LWLockRelease(stripe->lock);
}

/*
 * Invalidate all cache entries for a relation.
 * Iterates all stripes sequentially.
 */
void
xpatch_cache_invalidate_rel(Oid relid)
{
    int s;

    if (!shmem_initialized || shared_cache == NULL)
        return;

    for (s = 0; s < shared_cache->num_stripes; s++)
    {
        XPatchCacheStripe *stripe = &shared_cache->stripes[s];
        XPatchCacheEntry *entries = stripe_entries_base[s];
        char *slots_base = stripe_slots_base[s];
        int i;

        LWLockAcquire(stripe->lock, LW_EXCLUSIVE);

        for (i = 0; i < stripe->max_entries; i++)
        {
            XPatchCacheEntry *entry = &entries[i];

            if (entry->in_use && entry->key.relid == relid)
            {
                lru_remove(stripe, entries, entry, i);

                if (entry->slot_index >= 0)
                    free_slots(stripe, slots_base, entry->slot_index);

                entry->in_use = false;
                entry->slot_index = -1;
                entry->content_size = 0;
                entry->num_slots = 0;

                stripe->num_entries--;
            }
        }

        LWLockRelease(stripe->lock);
    }
}

/*
 * Get cache statistics.
 * Aggregates across all stripes.
 */
void
xpatch_cache_get_stats(XPatchCacheStats *stats)
{
    int s;

    if (!shmem_initialized || shared_cache == NULL)
    {
        memset(stats, 0, sizeof(*stats));
        return;
    }

    memset(stats, 0, sizeof(*stats));
    stats->max_bytes = (int64)xpatch_cache_size_mb * 1024 * 1024;

    for (s = 0; s < shared_cache->num_stripes; s++)
    {
        XPatchCacheStripe *stripe = &shared_cache->stripes[s];
        XPatchCacheEntry *entries = stripe_entries_base[s];
        int i;

        LWLockAcquire(stripe->lock, LW_SHARED);

        stats->entries_count += stripe->num_entries;
        stats->hit_count += pg_atomic_read_u64(&stripe->hit_count);
        stats->miss_count += pg_atomic_read_u64(&stripe->miss_count);
        stats->eviction_count += pg_atomic_read_u64(&stripe->eviction_count);
        stats->skip_count += pg_atomic_read_u64(&stripe->skip_count);

        /* Estimate current size from entries */
        for (i = 0; i < stripe->max_entries; i++)
        {
            if (entries[i].in_use)
                stats->size_bytes += entries[i].content_size;
        }

        LWLockRelease(stripe->lock);
    }
}
