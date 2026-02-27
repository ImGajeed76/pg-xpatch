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
 * xpatch_l2_cache.c - L2 shared memory cache for compressed deltas
 *
 * Stores raw compressed delta bytes in shared memory. Same architecture as
 * the L1 content cache (xpatch_cache.c): striped LWLocks, open-addressing
 * hash table with linear probing + tombstones, LRU eviction per stripe,
 * variable-size content via chained slots.
 *
 * Key differences from L1:
 *   - 512-byte slots (L1 uses 4KB) — deltas are typically small
 *   - 16 lock stripes by default (L1 uses 32)
 *   - Separate shared memory region and LWLock tranche
 *   - On put/evict: updates chain index cache_bits (CHAIN_BIT_L2)
 *
 * Memory Layout (in shared memory):
 *   [L2SharedCache header + stripe array]
 *   [Entry arrays: num_stripes * entries_per_stripe * sizeof(L2CacheEntry)]
 *   [Content slot buffers: remaining space up to l2_cache_size_mb]
 *
 * Locking:
 *   - Per-stripe LWLock for all stripe operations
 *   - Shared lock for reads (GET + LRU promotion)
 *   - Exclusive lock for writes (PUT, eviction, invalidation)
 *   - cache_bits updates on the chain index are lockless byte writes
 *
 * Lock ordering (global):
 *   L1 stripe lock > chain index stripe lock (shared) > L2 stripe lock
 *
 *   In practice, L2 operations never hold L1 locks, and chain index
 *   cache_bits updates are lockless. The only concern is: never acquire
 *   an L1 lock while holding an L2 lock.
 */

#include "xpatch_l2_cache.h"
#include "xpatch_chain_index.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"

/* GUC variables (registered in pg_xpatch.c, defaults set here) */
int xpatch_l2_cache_size_mb = XPATCH_L2_DEFAULT_SIZE_MB;
int xpatch_l2_cache_max_entries = XPATCH_L2_DEFAULT_MAX_ENTRIES;
int xpatch_l2_cache_slot_size = XPATCH_L2_DEFAULT_SLOT_SIZE;
int xpatch_l2_cache_partitions = XPATCH_L2_DEFAULT_PARTITIONS;
int xpatch_l2_cache_max_entry_kb = XPATCH_L2_DEFAULT_MAX_ENTRY_KB;

/* LWLock tranche name (must differ from L1's "pg_xpatch") */
#define L2_LOCK_TRANCHE_NAME "pg_xpatch_l2"

/* ---------------------------------------------------------------------------
 * Data structures
 * ---------------------------------------------------------------------------
 */

/* Cache entry key — same layout as L1's XPatchCacheKey */
typedef struct L2CacheKey
{
    Oid             relid;
    XPatchGroupHash group_hash;
    int64           seq;
    AttrNumber      attnum;
    int16           padding;
} L2CacheKey;

/* Cache entry — stored in open-addressing hash table */
typedef struct L2CacheEntry
{
    L2CacheKey  key;
    int32       slot_index;         /* First slot in content chain (-1 = none) */
    int32       content_size;       /* Actual content size in bytes */
    int32       num_slots;          /* Number of chained slots */
    int32       lru_prev;           /* LRU doubly-linked list (index, -1 = head) */
    int32       lru_next;           /* LRU doubly-linked list (index, -1 = tail) */
    bool        in_use;
    bool        tombstone;
} L2CacheEntry;

/* Per-stripe partition */
typedef struct L2CacheStripe
{
    LWLock             *lock;
    int32               lru_head;
    int32               lru_tail;
    int32               num_entries;
    int32               max_entries;
    int32               free_slot_head;
    int32               num_slots;
    pg_atomic_uint64    hit_count;
    pg_atomic_uint64    miss_count;
    pg_atomic_uint64    eviction_count;
    pg_atomic_uint64    skip_count;
} L2CacheStripe;

/* Shared cache header */
typedef struct L2SharedCache
{
    int32           num_stripes;
    int32           total_entries;
    int32           total_slots;
    L2CacheStripe   stripes[FLEXIBLE_ARRAY_MEMBER];
} L2SharedCache;

/* ---------------------------------------------------------------------------
 * Static state
 * ---------------------------------------------------------------------------
 */

static L2SharedCache   *l2_cache = NULL;
static L2CacheEntry   **l2_stripe_entries = NULL;
static char           **l2_stripe_slots = NULL;
static bool             l2_initialized = false;

/*
 * Slot layout statics, computed once in shmem_startup.
 *
 * Each slot: [int32 next_slot][char data[l2_slot_data_size]]
 * Total slot size = xpatch_l2_cache_slot_size bytes.
 */
static int  l2_slot_total_size;
static int  l2_slot_data_size;

/* Hooks for chaining shared memory requests */
static shmem_request_hook_type l2_prev_shmem_request_hook = NULL;
static shmem_startup_hook_type l2_prev_shmem_startup_hook = NULL;

/* ---------------------------------------------------------------------------
 * Slot access helpers
 * ---------------------------------------------------------------------------
 */

static inline int32 *
l2_slot_next(char *base, int32 idx)
{
    return (int32 *)(base + (Size)idx * l2_slot_total_size);
}

static inline char *
l2_slot_data(char *base, int32 idx)
{
    return base + (Size)idx * l2_slot_total_size + sizeof(int32);
}

/* ---------------------------------------------------------------------------
 * Hash function (FNV-1a, same algorithm as L1)
 * ---------------------------------------------------------------------------
 */

static inline uint32
l2_hash_key(const L2CacheKey *key)
{
    uint32 h = 2166136261u;

    h ^= (uint32) key->relid;              h *= 16777619u;
    h ^= (uint32)(key->group_hash.h1);     h *= 16777619u;
    h ^= (uint32)(key->group_hash.h1 >> 32); h *= 16777619u;
    h ^= (uint32)(key->group_hash.h2);     h *= 16777619u;
    h ^= (uint32)(key->group_hash.h2 >> 32); h *= 16777619u;
    h ^= (uint32)(key->seq);               h *= 16777619u;
    h ^= (uint32)(key->seq >> 32);         h *= 16777619u;
    h ^= (uint32) key->attnum;             h *= 16777619u;

    return h;
}

static inline int
l2_key_to_stripe(const L2CacheKey *key)
{
    return (int)(l2_hash_key(key) % (uint32)l2_cache->num_stripes);
}

static inline uint32
l2_key_to_probe(const L2CacheKey *key, int32 max_entries)
{
    return l2_hash_key(key) % (uint32)max_entries;
}

/* ---------------------------------------------------------------------------
 * Key comparison
 * ---------------------------------------------------------------------------
 */

static inline bool
l2_keys_equal(const L2CacheKey *a, const L2CacheKey *b)
{
    return a->relid == b->relid &&
           xpatch_group_hash_equals(a->group_hash, b->group_hash) &&
           a->seq == b->seq &&
           a->attnum == b->attnum;
}

/* ---------------------------------------------------------------------------
 * Per-stripe slot management
 * ---------------------------------------------------------------------------
 */

static void
l2_init_free_slots(L2CacheStripe *stripe, char *base, int num_slots)
{
    int i;

    for (i = 0; i < num_slots - 1; i++)
        *l2_slot_next(base, i) = i + 1;
    *l2_slot_next(base, num_slots - 1) = -1;

    stripe->free_slot_head = 0;
    stripe->num_slots = num_slots;
}

static int32
l2_alloc_slots(L2CacheStripe *stripe, char *base, int needed)
{
    int32 first = -1;
    int32 prev = -1;
    int i;

    for (i = 0; i < needed; i++)
    {
        int32 s = stripe->free_slot_head;
        if (s < 0)
        {
            /* Rollback partial allocation */
            while (first >= 0)
            {
                int32 next = *l2_slot_next(base, first);
                *l2_slot_next(base, first) = stripe->free_slot_head;
                stripe->free_slot_head = first;
                first = next;
            }
            return -1;
        }
        stripe->free_slot_head = *l2_slot_next(base, s);
        *l2_slot_next(base, s) = -1;

        if (first < 0)
            first = s;
        else
            *l2_slot_next(base, prev) = s;
        prev = s;
    }
    return first;
}

static void
l2_free_slots(L2CacheStripe *stripe, char *base, int32 first)
{
    while (first >= 0)
    {
        int32 next = *l2_slot_next(base, first);
        *l2_slot_next(base, first) = stripe->free_slot_head;
        stripe->free_slot_head = first;
        first = next;
    }
}

/* ---------------------------------------------------------------------------
 * LRU list management
 * ---------------------------------------------------------------------------
 */

static void
l2_lru_remove(L2CacheStripe *stripe, L2CacheEntry *entries,
              L2CacheEntry *entry, int entry_idx)
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

static void
l2_lru_push_front(L2CacheStripe *stripe, L2CacheEntry *entries,
                  L2CacheEntry *entry, int entry_idx)
{
    entry->lru_prev = -1;
    entry->lru_next = stripe->lru_head;

    if (stripe->lru_head >= 0)
        entries[stripe->lru_head].lru_prev = entry_idx;
    else
        stripe->lru_tail = entry_idx;

    stripe->lru_head = entry_idx;
}

/* ---------------------------------------------------------------------------
 * Entry lookup (linear probing)
 * ---------------------------------------------------------------------------
 */

static int32
l2_find_entry(L2CacheEntry *entries, int32 max_entries, const L2CacheKey *key)
{
    uint32 hash = l2_key_to_probe(key, max_entries);
    int probes = 0;

    while (probes < max_entries)
    {
        int32 idx = (hash + probes) % max_entries;
        L2CacheEntry *e = &entries[idx];

        if (!e->in_use && !e->tombstone)
            return -1;     /* Empty slot — key not found */

        if (e->tombstone)
        {
            probes++;
            continue;       /* Skip tombstone, keep probing */
        }

        if (l2_keys_equal(&e->key, key))
            return idx;

        probes++;
    }
    return -1;
}

static int32
l2_find_free_entry(L2CacheEntry *entries, int32 max_entries, const L2CacheKey *key)
{
    uint32 hash = l2_key_to_probe(key, max_entries);
    int probes = 0;
    int32 first_tombstone = -1;

    while (probes < max_entries)
    {
        int32 idx = (hash + probes) % max_entries;
        L2CacheEntry *e = &entries[idx];

        if (!e->in_use && !e->tombstone)
            return idx;

        if (e->tombstone && first_tombstone < 0)
            first_tombstone = idx;

        probes++;
    }
    return first_tombstone;
}

/* ---------------------------------------------------------------------------
 * Eviction
 * ---------------------------------------------------------------------------
 */

static void
l2_evict_lru(L2CacheStripe *stripe, L2CacheEntry *entries, char *slots_base)
{
    int32 victim_idx = stripe->lru_tail;
    L2CacheEntry *victim;

    if (victim_idx < 0)
        return;

    victim = &entries[victim_idx];

    /* Remove from LRU */
    l2_lru_remove(stripe, entries, victim, victim_idx);

    /* Free content slots */
    if (victim->slot_index >= 0)
        l2_free_slots(stripe, slots_base, victim->slot_index);

    /*
     * Clear CHAIN_BIT_L2 in chain index for evicted entry.
     * This is a lockless byte write — safe to do while holding stripe lock.
     */
    if (xpatch_chain_index_is_ready())
    {
        xpatch_chain_index_update_bits(victim->key.relid,
                                       victim->key.group_hash,
                                       victim->key.attnum,
                                       victim->key.seq,
                                       0, CHAIN_BIT_L2);
    }

    /* Mark as tombstone */
    victim->in_use = false;
    victim->tombstone = true;
    victim->slot_index = -1;
    victim->content_size = 0;
    victim->num_slots = 0;

    stripe->num_entries--;
    pg_atomic_fetch_add_u64(&stripe->eviction_count, 1);
}

/* ---------------------------------------------------------------------------
 * Content copy helpers
 * ---------------------------------------------------------------------------
 */

static void
l2_copy_to_slots(char *base, int32 first_slot, const bytea *content)
{
    Size total = VARSIZE(content);
    const char *src = (const char *) content;
    Size remaining = total;
    int32 slot = first_slot;

    while (remaining > 0 && slot >= 0)
    {
        Size to_copy = Min(remaining, (Size)l2_slot_data_size);
        memcpy(l2_slot_data(base, slot), src, to_copy);
        src += to_copy;
        remaining -= to_copy;
        slot = *l2_slot_next(base, slot);
    }
}

static bytea *
l2_copy_from_slots(char *base, int32 first_slot, Size content_size)
{
    bytea *result;
    char *dst;
    Size remaining = content_size;
    int32 slot = first_slot;

    result = (bytea *) palloc(content_size);
    dst = (char *) result;

    while (remaining > 0 && slot >= 0)
    {
        Size to_copy = Min(remaining, (Size)l2_slot_data_size);
        memcpy(dst, l2_slot_data(base, slot), to_copy);
        dst += to_copy;
        remaining -= to_copy;
        slot = *l2_slot_next(base, slot);
    }

    return result;
}

/* ---------------------------------------------------------------------------
 * Shared memory sizing
 * ---------------------------------------------------------------------------
 *
 * Layout:
 *   [L2SharedCache header + stripe array]
 *   [Entry arrays: all stripes contiguous]
 *   [Slot buffers: remaining space]
 */

static Size
l2_shmem_size(void)
{
    Size size;
    Size header_size;
    Size entries_size;
    int num_stripes = xpatch_l2_cache_partitions;
    int entries_per_stripe;
    int total_slots;

    /* Header + stripe array */
    header_size = offsetof(L2SharedCache, stripes);
    header_size = add_size(header_size, mul_size(sizeof(L2CacheStripe), num_stripes));
    header_size = MAXALIGN(header_size);

    /* Entry arrays */
    entries_per_stripe = xpatch_l2_cache_max_entries / num_stripes;
    if (entries_per_stripe < 64)
        entries_per_stripe = 64;
    entries_size = mul_size(sizeof(L2CacheEntry),
                            mul_size(entries_per_stripe, num_stripes));
    entries_size = MAXALIGN(entries_size);

    size = add_size(header_size, entries_size);

    /* Slot buffers — fill remaining space up to l2_cache_size_mb */
    total_slots = ((Size)xpatch_l2_cache_size_mb * 1024 * 1024 - size) /
                  (Size)xpatch_l2_cache_slot_size;
    if (total_slots < num_stripes)
        total_slots = num_stripes;

    size = add_size(size, mul_size((Size)xpatch_l2_cache_slot_size, total_slots));

    return size;
}

/* ---------------------------------------------------------------------------
 * Shared memory hooks
 * ---------------------------------------------------------------------------
 */

static void
l2_shmem_request(void)
{
    if (l2_prev_shmem_request_hook)
        l2_prev_shmem_request_hook();

    RequestAddinShmemSpace(l2_shmem_size());
    RequestNamedLWLockTranche(L2_LOCK_TRANCHE_NAME, xpatch_l2_cache_partitions);
}

static void
l2_shmem_exit(int code, Datum arg)
{
    l2_cache = NULL;
    if (l2_stripe_entries)
    {
        pfree(l2_stripe_entries);
        l2_stripe_entries = NULL;
    }
    if (l2_stripe_slots)
    {
        pfree(l2_stripe_slots);
        l2_stripe_slots = NULL;
    }
    l2_initialized = false;
}

static void
l2_shmem_startup(void)
{
    bool found;
    Size cache_size;
    Size header_size;
    Size entries_size;
    int num_stripes = xpatch_l2_cache_partitions;
    int entries_per_stripe;
    int total_slots;
    int slots_per_stripe;
    int extra_slots;
    char *entries_start;
    char *slots_start;
    int s;

    if (l2_prev_shmem_startup_hook)
        l2_prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    cache_size = l2_shmem_size();

    l2_cache = ShmemInitStruct("pg_xpatch_l2 cache", cache_size, &found);

    /* Compute slot layout statics */
    l2_slot_total_size = xpatch_l2_cache_slot_size;
    l2_slot_data_size = l2_slot_total_size - (int)sizeof(int32);

    /* Layout parameters */
    header_size = offsetof(L2SharedCache, stripes);
    header_size = add_size(header_size, mul_size(sizeof(L2CacheStripe), num_stripes));
    header_size = MAXALIGN(header_size);

    entries_per_stripe = xpatch_l2_cache_max_entries / num_stripes;
    if (entries_per_stripe < 64)
        entries_per_stripe = 64;
    entries_size = mul_size(sizeof(L2CacheEntry),
                            mul_size(entries_per_stripe, num_stripes));
    entries_size = MAXALIGN(entries_size);

    total_slots = (cache_size - header_size - entries_size) /
                  (Size)xpatch_l2_cache_slot_size;
    if (total_slots < num_stripes)
        total_slots = num_stripes;

    slots_per_stripe = total_slots / num_stripes;
    extra_slots = total_slots % num_stripes;

    entries_start = (char *)l2_cache + header_size;
    slots_start = entries_start + entries_size;

    if (!found)
    {
        LWLockPadded *locks;

        memset(l2_cache, 0, cache_size);

        l2_cache->num_stripes = num_stripes;
        l2_cache->total_entries = entries_per_stripe * num_stripes;
        l2_cache->total_slots = total_slots;

        locks = GetNamedLWLockTranche(L2_LOCK_TRANCHE_NAME);

        for (s = 0; s < num_stripes; s++)
        {
            L2CacheStripe *stripe = &l2_cache->stripes[s];
            L2CacheEntry *entries;
            char *sslots;
            int this_stripe_slots;
            int i;

            stripe->lock = &locks[s].lock;
            stripe->lru_head = -1;
            stripe->lru_tail = -1;
            stripe->num_entries = 0;
            stripe->max_entries = entries_per_stripe;
            pg_atomic_init_u64(&stripe->hit_count, 0);
            pg_atomic_init_u64(&stripe->miss_count, 0);
            pg_atomic_init_u64(&stripe->eviction_count, 0);
            pg_atomic_init_u64(&stripe->skip_count, 0);

            /* Entry array for this stripe */
            entries = (L2CacheEntry *)(entries_start +
                        (Size)s * entries_per_stripe * sizeof(L2CacheEntry));
            for (i = 0; i < entries_per_stripe; i++)
            {
                entries[i].in_use = false;
                entries[i].tombstone = false;
                entries[i].slot_index = -1;
                entries[i].lru_prev = -1;
                entries[i].lru_next = -1;
            }

            /* Slot buffer for this stripe */
            this_stripe_slots = slots_per_stripe + (s < extra_slots ? 1 : 0);
            {
                int slot_offset = 0;
                int j;
                for (j = 0; j < s; j++)
                    slot_offset += slots_per_stripe + (j < extra_slots ? 1 : 0);
                sslots = slots_start + (Size)slot_offset * xpatch_l2_cache_slot_size;
            }

            stripe->num_slots = this_stripe_slots;
            if (this_stripe_slots > 0)
                l2_init_free_slots(stripe, sslots, this_stripe_slots);
            else
                stripe->free_slot_head = -1;
        }

        elog(LOG, "pg_xpatch: L2 cache initialized (%d stripes, %d entries/stripe, "
             "%d total slots @ %d bytes, %zu MB)",
             num_stripes, entries_per_stripe, total_slots,
             xpatch_l2_cache_slot_size, cache_size / (1024 * 1024));
    }

    /* Build per-backend pointer arrays */
    {
        MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);
        l2_stripe_entries = palloc(sizeof(L2CacheEntry *) * num_stripes);
        l2_stripe_slots = palloc(sizeof(char *) * num_stripes);
        MemoryContextSwitchTo(old_ctx);

        for (s = 0; s < num_stripes; s++)
        {
            int slot_offset = 0;
            int j;

            l2_stripe_entries[s] = (L2CacheEntry *)(entries_start +
                        (Size)s * entries_per_stripe * sizeof(L2CacheEntry));

            for (j = 0; j < s; j++)
                slot_offset += slots_per_stripe + (j < extra_slots ? 1 : 0);
            l2_stripe_slots[s] = slots_start +
                        (Size)slot_offset * xpatch_l2_cache_slot_size;
        }
    }

    LWLockRelease(AddinShmemInitLock);

    l2_initialized = true;
    on_shmem_exit(l2_shmem_exit, (Datum) 0);
}

/* ---------------------------------------------------------------------------
 * Public API: request shmem
 * ---------------------------------------------------------------------------
 */

void
xpatch_l2_cache_request_shmem(void)
{
    l2_prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = l2_shmem_request;

    l2_prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = l2_shmem_startup;
}

bool
xpatch_l2_cache_is_ready(void)
{
    return l2_initialized && l2_cache != NULL;
}

/* ---------------------------------------------------------------------------
 * Public API: get
 * ---------------------------------------------------------------------------
 */

bytea *
xpatch_l2_cache_get(Oid relid, XPatchGroupHash group_hash,
                    int64 seq, AttrNumber attnum)
{
    L2CacheKey key;
    int stripe_idx;
    L2CacheStripe *stripe;
    L2CacheEntry *entries;
    char *slots_base;
    int32 entry_idx;
    bytea *result = NULL;

    if (!l2_initialized || l2_cache == NULL)
        return NULL;

    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.group_hash = group_hash;
    key.seq = seq;
    key.attnum = attnum;

    stripe_idx = l2_key_to_stripe(&key);
    stripe = &l2_cache->stripes[stripe_idx];
    entries = l2_stripe_entries[stripe_idx];
    slots_base = l2_stripe_slots[stripe_idx];

    LWLockAcquire(stripe->lock, LW_SHARED);

    entry_idx = l2_find_entry(entries, stripe->max_entries, &key);

    if (entry_idx >= 0)
    {
        L2CacheEntry *e = &entries[entry_idx];

        if (e->slot_index >= 0 && e->content_size > 0)
        {
            result = l2_copy_from_slots(slots_base, e->slot_index,
                                        e->content_size);
        }

        pg_atomic_fetch_add_u64(&stripe->hit_count, 1);

        /* Promote in LRU — requires exclusive lock */
        LWLockRelease(stripe->lock);
        LWLockAcquire(stripe->lock, LW_EXCLUSIVE);

        /*
         * Re-validate after lock upgrade: another backend may have evicted
         * this entry between releasing shared and acquiring exclusive.
         */
        if (e->in_use && l2_keys_equal(&e->key, &key))
        {
            l2_lru_remove(stripe, entries, e, entry_idx);
            l2_lru_push_front(stripe, entries, e, entry_idx);
        }
    }
    else
    {
        pg_atomic_fetch_add_u64(&stripe->miss_count, 1);
    }

    LWLockRelease(stripe->lock);
    return result;
}

/* ---------------------------------------------------------------------------
 * Public API: put
 * ---------------------------------------------------------------------------
 */

void
xpatch_l2_cache_put(Oid relid, XPatchGroupHash group_hash,
                    int64 seq, AttrNumber attnum, bytea *delta)
{
    L2CacheKey key;
    int stripe_idx;
    L2CacheStripe *stripe;
    L2CacheEntry *entries;
    char *slots_base;
    int32 entry_idx;
    Size content_size;
    int num_slots_needed;
    int32 first_slot;

    if (!l2_initialized || l2_cache == NULL || delta == NULL)
        return;

    content_size = VARSIZE(delta);

    /* Reject entries exceeding size limit */
    if (content_size > (Size)xpatch_l2_cache_max_entry_kb * 1024)
    {
        if (l2_cache->num_stripes > 0)
            pg_atomic_fetch_add_u64(&l2_cache->stripes[0].skip_count, 1);

        elog(DEBUG1, "pg_xpatch: L2 cache skip %zu bytes (limit %d KB)",
             content_size, xpatch_l2_cache_max_entry_kb);
        return;
    }

    num_slots_needed = (content_size + l2_slot_data_size - 1) / l2_slot_data_size;

    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.group_hash = group_hash;
    key.seq = seq;
    key.attnum = attnum;

    stripe_idx = l2_key_to_stripe(&key);
    stripe = &l2_cache->stripes[stripe_idx];
    entries = l2_stripe_entries[stripe_idx];
    slots_base = l2_stripe_slots[stripe_idx];

    LWLockAcquire(stripe->lock, LW_EXCLUSIVE);

    /* Already cached? Move to LRU front. */
    entry_idx = l2_find_entry(entries, stripe->max_entries, &key);
    if (entry_idx >= 0)
    {
        L2CacheEntry *e = &entries[entry_idx];
        l2_lru_remove(stripe, entries, e, entry_idx);
        l2_lru_push_front(stripe, entries, e, entry_idx);
        LWLockRelease(stripe->lock);
        return;
    }

    /* Allocate slots, evicting as needed */
    first_slot = l2_alloc_slots(stripe, slots_base, num_slots_needed);
    while (first_slot < 0 && stripe->num_entries > 0)
    {
        l2_evict_lru(stripe, entries, slots_base);
        first_slot = l2_alloc_slots(stripe, slots_base, num_slots_needed);
    }
    if (first_slot < 0)
    {
        LWLockRelease(stripe->lock);
        return;
    }

    /* Find free entry slot, evicting as needed */
    entry_idx = l2_find_free_entry(entries, stripe->max_entries, &key);
    while (entry_idx < 0 && stripe->num_entries > 0)
    {
        l2_evict_lru(stripe, entries, slots_base);
        entry_idx = l2_find_free_entry(entries, stripe->max_entries, &key);
    }
    if (entry_idx < 0)
    {
        l2_free_slots(stripe, slots_base, first_slot);
        LWLockRelease(stripe->lock);
        return;
    }

    /* Initialize entry */
    {
        L2CacheEntry *e = &entries[entry_idx];
        memcpy(&e->key, &key, sizeof(key));
        e->slot_index = first_slot;
        e->content_size = content_size;
        e->num_slots = num_slots_needed;
        e->in_use = true;
        e->tombstone = false;

        l2_copy_to_slots(slots_base, first_slot, delta);
        l2_lru_push_front(stripe, entries, e, entry_idx);
        stripe->num_entries++;
    }

    LWLockRelease(stripe->lock);

    /* Set CHAIN_BIT_L2 — lockless byte write, safe outside stripe lock */
    if (xpatch_chain_index_is_ready())
    {
        xpatch_chain_index_update_bits(relid, group_hash, attnum, seq,
                                       CHAIN_BIT_L2, 0);
    }
}

/* ---------------------------------------------------------------------------
 * Public API: invalidate all entries for a relation
 * ---------------------------------------------------------------------------
 */

void
xpatch_l2_cache_invalidate_rel(Oid relid)
{
    int s;

    if (!l2_initialized || l2_cache == NULL)
        return;

    for (s = 0; s < l2_cache->num_stripes; s++)
    {
        L2CacheStripe *stripe = &l2_cache->stripes[s];
        L2CacheEntry *entries = l2_stripe_entries[s];
        char *slots_base = l2_stripe_slots[s];
        int i;

        LWLockAcquire(stripe->lock, LW_EXCLUSIVE);

        for (i = 0; i < stripe->max_entries; i++)
        {
            L2CacheEntry *e = &entries[i];

            if (e->in_use && e->key.relid == relid)
            {
                l2_lru_remove(stripe, entries, e, i);

                if (e->slot_index >= 0)
                    l2_free_slots(stripe, slots_base, e->slot_index);

                /*
                 * No chain index update here — the caller
                 * (xpatch_tam.c) handles chain index invalidation
                 * separately via xpatch_chain_index_invalidate_rel().
                 */
                e->in_use = false;
                e->slot_index = -1;
                e->content_size = 0;
                e->num_slots = 0;

                stripe->num_entries--;
            }
        }

        LWLockRelease(stripe->lock);
    }
}

/* ---------------------------------------------------------------------------
 * Public API: statistics
 * ---------------------------------------------------------------------------
 */

void
xpatch_l2_cache_get_stats(XPatchL2CacheStats *stats)
{
    int s;

    if (!l2_initialized || l2_cache == NULL)
    {
        memset(stats, 0, sizeof(*stats));
        return;
    }

    memset(stats, 0, sizeof(*stats));
    stats->max_bytes = (int64)xpatch_l2_cache_size_mb * 1024 * 1024;

    for (s = 0; s < l2_cache->num_stripes; s++)
    {
        L2CacheStripe *stripe = &l2_cache->stripes[s];
        L2CacheEntry *entries = l2_stripe_entries[s];
        int i;

        LWLockAcquire(stripe->lock, LW_SHARED);

        stats->entries_count += stripe->num_entries;
        stats->hit_count += pg_atomic_read_u64(&stripe->hit_count);
        stats->miss_count += pg_atomic_read_u64(&stripe->miss_count);
        stats->eviction_count += pg_atomic_read_u64(&stripe->eviction_count);
        stats->skip_count += pg_atomic_read_u64(&stripe->skip_count);

        for (i = 0; i < stripe->max_entries; i++)
        {
            if (entries[i].in_use)
                stats->size_bytes += entries[i].content_size;
        }

        LWLockRelease(stripe->lock);
    }
}
