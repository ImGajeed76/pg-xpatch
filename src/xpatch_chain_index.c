/*
 * xpatch_chain_index.c — Always-on in-memory chain index for path planning
 *
 * Implements a DSA-backed group directory with per-group entry arrays.
 * See xpatch_chain_index.h for the full design description.
 *
 * Memory layout:
 *   Fixed shmem (ShmemInitStruct):
 *     - ChainIndexShmem: directory hash table + stripe headers + DSA handle
 *   DSA (dynamic shared memory):
 *     - Per-group ChainIndexEntry[] arrays (auto-growing, 2x capacity)
 *
 * Locking:
 *   - 16 striped LWLocks on the directory (ChainGroupKey → stripe via hash)
 *   - Shared lock for reads (chain walks, lookups)
 *   - Exclusive lock for writes (insert, delete, grow)
 *   - cache_bits: lockless single-byte atomic writes
 *   - Meta lock (lock[NUM_STRIPES]): for DSA creation/attachment
 */

#include "pg_xpatch.h"
#include "xpatch_chain_index.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/* ---------------------------------------------------------------------------
 * Configuration
 * ---------------------------------------------------------------------------
 */
#define CHAIN_INDEX_NUM_STRIPES     16
#define CHAIN_INDEX_DIR_SLOTS       4096    /* Initial directory hash slots */
#define CHAIN_INDEX_LOAD_FACTOR_PCT 75      /* Grow directory at 75% full */

/* GUC variable */
int xpatch_chain_index_initial_capacity = 64;

/* ---------------------------------------------------------------------------
 * Directory entry — one slot in the open-addressing hash table
 * ---------------------------------------------------------------------------
 */
typedef struct ChainDirEntry
{
    ChainGroupKey       key;
    GroupChainHeader    header;
    bool                in_use;
    bool                tombstone;
} ChainDirEntry;

/* ---------------------------------------------------------------------------
 * Shared memory layout
 * ---------------------------------------------------------------------------
 */
typedef struct ChainIndexShmem
{
    /* DSA handle — set by first backend to create it */
    dsm_handle          dsa_hdl;

    /* Directory */
    int32               dir_capacity;   /* Number of hash table slots */
    int32               dir_count;      /* Number of live entries */

    /* Stats */
    pg_atomic_uint64    insert_count;
    pg_atomic_uint64    lookup_count;
    pg_atomic_uint64    lookup_miss_count;
    pg_atomic_uint64    grow_count;

    /*
     * Directory entries follow in shared memory.
     * Indexed as: entries[0..dir_capacity-1]
     */
    ChainDirEntry       dir_entries[FLEXIBLE_ARRAY_MEMBER];
} ChainIndexShmem;

/* ---------------------------------------------------------------------------
 * Per-backend state
 * ---------------------------------------------------------------------------
 */
static ChainIndexShmem  *chain_index = NULL;
static dsa_area          *chain_index_dsa = NULL;
static bool              chain_index_initialized = false;
static LWLockPadded      *chain_index_locks = NULL;
static LWLock            *chain_index_meta_lock = NULL;

/* Hook chaining */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ---------------------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------------------
 */

/* Hash a ChainGroupKey to a uint32 for directory probing */
static inline uint32
chain_group_key_hash(const ChainGroupKey *key)
{
    uint32 h = 0x811c9dc5;   /* FNV-1a offset basis */
    const uint8 *p;
    int i;

    /* Hash relid */
    p = (const uint8 *)&key->relid;
    for (i = 0; i < (int)sizeof(Oid); i++)
        h = (h ^ p[i]) * 0x01000193;

    /* Hash attnum */
    p = (const uint8 *)&key->attnum;
    for (i = 0; i < (int)sizeof(AttrNumber); i++)
        h = (h ^ p[i]) * 0x01000193;

    /* Hash group_hash (128 bits) */
    p = (const uint8 *)&key->group_hash;
    for (i = 0; i < (int)sizeof(XPatchGroupHash); i++)
        h = (h ^ p[i]) * 0x01000193;

    return h;
}

/* Get stripe index for a key */
static inline int
chain_stripe_for_key(const ChainGroupKey *key)
{
    return (int)(chain_group_key_hash(key) % CHAIN_INDEX_NUM_STRIPES);
}

/* Compare two ChainGroupKeys */
static inline bool
chain_group_key_equals(const ChainGroupKey *a, const ChainGroupKey *b)
{
    return a->relid == b->relid &&
           a->attnum == b->attnum &&
           xpatch_group_hash_equals(a->group_hash, b->group_hash);
}

/* ---------------------------------------------------------------------------
 * DSA management
 * ---------------------------------------------------------------------------
 */
static void
ensure_dsa_attached(void)
{
    MemoryContext oldcxt;

    if (chain_index_dsa != NULL)
        return;

    oldcxt = MemoryContextSwitchTo(TopMemoryContext);

    LWLockAcquire(chain_index_meta_lock, LW_EXCLUSIVE);

    if (chain_index->dsa_hdl == DSM_HANDLE_INVALID)
    {
        /* First backend — create DSA */
        int tranche_id = chain_index_meta_lock->tranche;
        chain_index_dsa = dsa_create(tranche_id);
        dsa_pin(chain_index_dsa);
        chain_index->dsa_hdl = dsa_get_handle(chain_index_dsa);
    }
    else
    {
        /* Subsequent backends — attach */
        chain_index_dsa = dsa_attach(chain_index->dsa_hdl);
    }

    LWLockRelease(chain_index_meta_lock);

    dsa_pin_mapping(chain_index_dsa);

    MemoryContextSwitchTo(oldcxt);
}

/* ---------------------------------------------------------------------------
 * Directory hash table operations
 * ---------------------------------------------------------------------------
 * Open-addressing with linear probing and tombstones.
 * Same pattern as xpatch_cache.c.
 *
 * Caller MUST hold the appropriate stripe lock.
 */

/* Find an existing entry by key. Returns index or -1 if not found. */
static int
dir_find(const ChainGroupKey *key)
{
    uint32 start = chain_group_key_hash(key) % (uint32)chain_index->dir_capacity;
    uint32 idx = start;

    do
    {
        ChainDirEntry *e = &chain_index->dir_entries[idx];

        if (!e->in_use && !e->tombstone)
            return -1;  /* Empty slot — key not in table */

        if (e->in_use && chain_group_key_equals(&e->key, key))
            return (int)idx;

        /* Continue past tombstones and non-matching entries */
        idx = (idx + 1) % (uint32)chain_index->dir_capacity;
    } while (idx != start);

    return -1;
}

/*
 * Find or create a slot for key. Returns index.
 * If the key already exists, returns its index.
 * If not, inserts a new entry with an empty header and returns its index.
 *
 * Caller MUST hold the stripe lock in EXCLUSIVE mode.
 */
static int
dir_find_or_create(const ChainGroupKey *key)
{
    uint32 start = chain_group_key_hash(key) % (uint32)chain_index->dir_capacity;
    uint32 idx = start;
    int first_tombstone = -1;

    do
    {
        ChainDirEntry *e = &chain_index->dir_entries[idx];

        if (!e->in_use && !e->tombstone)
        {
            /* Empty slot — key doesn't exist, insert here (or at tombstone) */
            int ins_idx = (first_tombstone >= 0) ? first_tombstone : (int)idx;
            ChainDirEntry *ins = &chain_index->dir_entries[ins_idx];

            ins->key = *key;
            ins->in_use = true;
            ins->tombstone = false;
            memset(&ins->header, 0, sizeof(GroupChainHeader));
            ins->header.entries_ptr = InvalidDsaPointer;
            chain_index->dir_count++;
            return ins_idx;
        }

        if (e->tombstone && first_tombstone < 0)
            first_tombstone = (int)idx;

        if (e->in_use && chain_group_key_equals(&e->key, key))
            return (int)idx;

        idx = (idx + 1) % (uint32)chain_index->dir_capacity;
    } while (idx != start);

    /*
     * Table is full — this should not happen if we size it properly
     * and the load factor check is working. But handle gracefully.
     */
    if (first_tombstone >= 0)
    {
        ChainDirEntry *ins = &chain_index->dir_entries[first_tombstone];
        ins->key = *key;
        ins->in_use = true;
        ins->tombstone = false;
        memset(&ins->header, 0, sizeof(GroupChainHeader));
        ins->header.entries_ptr = InvalidDsaPointer;
        chain_index->dir_count++;
        return first_tombstone;
    }

    elog(WARNING, "xpatch_chain_index: directory full (%d/%d)",
         chain_index->dir_count, chain_index->dir_capacity);
    return -1;
}

/* ---------------------------------------------------------------------------
 * Entry array management (DSA-allocated)
 * ---------------------------------------------------------------------------
 */

/*
 * Allocate or grow the entry array for a group.
 * Called when we need to insert a seq that doesn't fit in the current array.
 *
 * new_count: the minimum number of entries needed.
 * Caller MUST hold the stripe lock in EXCLUSIVE mode.
 */
static bool
ensure_entry_capacity(GroupChainHeader *header, int32 new_count)
{
    int32 new_capacity;
    dsa_pointer new_ptr;
    ChainIndexEntry *old_entries;
    ChainIndexEntry *new_entries;

    if (new_count <= header->capacity)
        return true;

    ensure_dsa_attached();

    /* Double capacity until sufficient */
    new_capacity = header->capacity > 0 ? header->capacity : xpatch_chain_index_initial_capacity;
    while (new_capacity < new_count)
        new_capacity *= 2;

    new_ptr = dsa_allocate_extended(chain_index_dsa,
                                     (Size)new_capacity * sizeof(ChainIndexEntry),
                                     DSA_ALLOC_NO_OOM | DSA_ALLOC_ZERO);
    if (!DsaPointerIsValid(new_ptr))
    {
        elog(WARNING, "xpatch_chain_index: DSA allocation failed for %d entries", new_capacity);
        return false;
    }

    new_entries = (ChainIndexEntry *)dsa_get_address(chain_index_dsa, new_ptr);

    /* Copy old entries if any */
    if (DsaPointerIsValid(header->entries_ptr) && header->capacity > 0)
    {
        old_entries = (ChainIndexEntry *)dsa_get_address(chain_index_dsa, header->entries_ptr);
        memcpy(new_entries, old_entries, (Size)header->count * sizeof(ChainIndexEntry));

        /* Free old array */
        dsa_free(chain_index_dsa, header->entries_ptr);
    }

    header->entries_ptr = new_ptr;
    header->capacity = new_capacity;

    pg_atomic_fetch_add_u64(&chain_index->grow_count, 1);

    return true;
}

/*
 * Get a pointer to the entry for a given seq in a group.
 * Returns NULL if seq is out of range or the entry array is not allocated.
 *
 * Caller MUST hold the stripe lock (shared or exclusive).
 */
static ChainIndexEntry *
get_entry_ptr(GroupChainHeader *header, int64 seq)
{
    int32 idx;
    ChainIndexEntry *entries;

    if (!DsaPointerIsValid(header->entries_ptr))
        return NULL;

    if (seq < header->base_seq || seq > header->max_seq)
        return NULL;

    idx = (int32)(seq - header->base_seq);
    if (idx < 0 || idx >= header->count)
        return NULL;

    ensure_dsa_attached();
    entries = (ChainIndexEntry *)dsa_get_address(chain_index_dsa, header->entries_ptr);
    return &entries[idx];
}

/* ---------------------------------------------------------------------------
 * Shmem hooks
 * ---------------------------------------------------------------------------
 */

static Size
chain_index_shmem_size(void)
{
    return offsetof(ChainIndexShmem, dir_entries) +
           (Size)CHAIN_INDEX_DIR_SLOTS * sizeof(ChainDirEntry);
}

static void
chain_index_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(chain_index_shmem_size());
    /* +1 for meta lock */
    RequestNamedLWLockTranche("pg_xpatch_chain_index",
                              CHAIN_INDEX_NUM_STRIPES + 1);
}

static void
chain_index_shmem_exit(int code, Datum arg)
{
    if (chain_index_dsa)
    {
        dsa_detach(chain_index_dsa);
        chain_index_dsa = NULL;
    }
    chain_index = NULL;
    chain_index_locks = NULL;
    chain_index_meta_lock = NULL;
    chain_index_initialized = false;
}

static void
chain_index_shmem_startup(void)
{
    bool found;
    int i;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    chain_index = ShmemInitStruct("pg_xpatch_chain_index",
                                   chain_index_shmem_size(),
                                   &found);

    if (!found)
    {
        /* Postmaster: initialize */
        memset(chain_index, 0, chain_index_shmem_size());
        chain_index->dsa_hdl = DSM_HANDLE_INVALID;
        chain_index->dir_capacity = CHAIN_INDEX_DIR_SLOTS;
        chain_index->dir_count = 0;

        pg_atomic_init_u64(&chain_index->insert_count, 0);
        pg_atomic_init_u64(&chain_index->lookup_count, 0);
        pg_atomic_init_u64(&chain_index->lookup_miss_count, 0);
        pg_atomic_init_u64(&chain_index->grow_count, 0);

        for (i = 0; i < CHAIN_INDEX_DIR_SLOTS; i++)
        {
            chain_index->dir_entries[i].in_use = false;
            chain_index->dir_entries[i].tombstone = false;
        }
    }

    chain_index_locks = GetNamedLWLockTranche("pg_xpatch_chain_index");
    chain_index_meta_lock = &chain_index_locks[CHAIN_INDEX_NUM_STRIPES].lock;

    LWLockRelease(AddinShmemInitLock);

    chain_index_initialized = true;
    on_shmem_exit(chain_index_shmem_exit, (Datum)0);

    elog(LOG, "pg_xpatch: chain index initialized (%d directory slots, %d stripes)",
         CHAIN_INDEX_DIR_SLOTS, CHAIN_INDEX_NUM_STRIPES);
}

/* ---------------------------------------------------------------------------
 * Public API: hook registration
 * ---------------------------------------------------------------------------
 */
void
xpatch_chain_index_request_shmem(void)
{
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = chain_index_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = chain_index_shmem_startup;
}

/* ---------------------------------------------------------------------------
 * Public API: readiness check
 * ---------------------------------------------------------------------------
 */
bool
xpatch_chain_index_is_ready(void)
{
    return chain_index_initialized && chain_index != NULL;
}

/* ---------------------------------------------------------------------------
 * Public API: insert
 * ---------------------------------------------------------------------------
 */
void
xpatch_chain_index_insert(Oid relid, XPatchGroupHash group_hash,
                           AttrNumber attnum, int64 seq,
                           uint32 base_offset, uint8 cache_bits)
{
    ChainGroupKey key;
    int stripe;
    int dir_idx;
    ChainDirEntry *dir_entry;
    GroupChainHeader *header;
    ChainIndexEntry *entry;
    ChainIndexEntry *entries;

    if (!chain_index_initialized)
        return;

    /* Build key */
    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.attnum = attnum;
    key.group_hash = group_hash;

    stripe = chain_stripe_for_key(&key);

    LWLockAcquire(&chain_index_locks[stripe].lock, LW_EXCLUSIVE);

    dir_idx = dir_find_or_create(&key);
    if (dir_idx < 0)
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        return;
    }

    dir_entry = &chain_index->dir_entries[dir_idx];
    header = &dir_entry->header;

    /*
     * First entry for this group — set base_seq.
     * Subsequent entries: extend array as needed.
     */
    if (header->count == 0)
    {
        /* First version in this group */
        header->base_seq = seq;
        header->max_seq = seq;

        if (!ensure_entry_capacity(header, 1))
        {
            LWLockRelease(&chain_index_locks[stripe].lock);
            return;
        }

        header->count = 1;
    }
    else if (seq >= header->base_seq && seq <= header->max_seq)
    {
        /* Seq falls within existing range (re-insert or fill gap) */
        /* Nothing to do — get_entry_ptr() will find it */
    }
    else if (seq > header->max_seq)
    {
        /* Extend array forward */
        int32 new_count = (int32)(seq - header->base_seq + 1);

        if (!ensure_entry_capacity(header, new_count))
        {
            LWLockRelease(&chain_index_locks[stripe].lock);
            return;
        }

        /* Zero-fill new slots (sentinels) between old max_seq+1 and seq-1 */
        if (new_count > header->count)
        {
            ensure_dsa_attached();
            entries = (ChainIndexEntry *)dsa_get_address(
                chain_index_dsa, header->entries_ptr);
            memset(&entries[header->count], 0,
                   (Size)(new_count - header->count) * sizeof(ChainIndexEntry));
        }

        header->count = new_count;
        header->max_seq = seq;
    }
    else
    {
        /*
         * seq < base_seq — need to prepend.
         * This is rare (only in restore mode with out-of-order inserts).
         * Shift existing entries right and update base_seq.
         */
        int32 shift = (int32)(header->base_seq - seq);
        int32 new_count = header->count + shift;

        if (!ensure_entry_capacity(header, new_count))
        {
            LWLockRelease(&chain_index_locks[stripe].lock);
            return;
        }

        ensure_dsa_attached();
        entries = (ChainIndexEntry *)dsa_get_address(
            chain_index_dsa, header->entries_ptr);

        /* Shift existing entries right */
        memmove(&entries[shift], &entries[0],
                (Size)header->count * sizeof(ChainIndexEntry));

        /* Zero-fill the new prefix slots */
        memset(&entries[0], 0, (Size)shift * sizeof(ChainIndexEntry));

        header->count = new_count;
        header->base_seq = seq;
    }

    /* Write the entry */
    entry = get_entry_ptr(header, seq);
    if (entry != NULL)
    {
        chain_entry_set_base_offset(entry, base_offset);
        entry->cache_bits = cache_bits;
    }

    LWLockRelease(&chain_index_locks[stripe].lock);

    pg_atomic_fetch_add_u64(&chain_index->insert_count, 1);
}

/* ---------------------------------------------------------------------------
 * Public API: lookup single entry
 * ---------------------------------------------------------------------------
 */
bool
xpatch_chain_index_lookup(Oid relid, XPatchGroupHash group_hash,
                           AttrNumber attnum, int64 seq,
                           ChainIndexEntry *entry_out)
{
    ChainGroupKey key;
    int stripe;
    int dir_idx;
    ChainDirEntry *dir_entry;
    ChainIndexEntry *entry;

    if (!chain_index_initialized)
        return false;

    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.attnum = attnum;
    key.group_hash = group_hash;

    stripe = chain_stripe_for_key(&key);

    LWLockAcquire(&chain_index_locks[stripe].lock, LW_SHARED);

    dir_idx = dir_find(&key);
    if (dir_idx < 0)
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        pg_atomic_fetch_add_u64(&chain_index->lookup_miss_count, 1);
        return false;
    }

    dir_entry = &chain_index->dir_entries[dir_idx];
    entry = get_entry_ptr(&dir_entry->header, seq);

    if (entry == NULL || chain_entry_is_sentinel(entry))
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        pg_atomic_fetch_add_u64(&chain_index->lookup_miss_count, 1);
        return false;
    }

    *entry_out = *entry;

    LWLockRelease(&chain_index_locks[stripe].lock);

    pg_atomic_fetch_add_u64(&chain_index->lookup_count, 1);
    return true;
}

/* ---------------------------------------------------------------------------
 * Public API: get full chain
 * ---------------------------------------------------------------------------
 */
bool
xpatch_chain_index_get_chain(Oid relid, XPatchGroupHash group_hash,
                              AttrNumber attnum,
                              ChainWalkResult *result)
{
    ChainGroupKey key;
    int stripe;
    int dir_idx;
    ChainDirEntry *dir_entry;
    GroupChainHeader *header;
    ChainIndexEntry *entries;

    if (!chain_index_initialized)
        return false;

    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.attnum = attnum;
    key.group_hash = group_hash;

    stripe = chain_stripe_for_key(&key);

    LWLockAcquire(&chain_index_locks[stripe].lock, LW_SHARED);

    dir_idx = dir_find(&key);
    if (dir_idx < 0)
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        return false;
    }

    dir_entry = &chain_index->dir_entries[dir_idx];
    header = &dir_entry->header;

    if (header->count == 0 || !DsaPointerIsValid(header->entries_ptr))
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        return false;
    }

    ensure_dsa_attached();
    entries = (ChainIndexEntry *)dsa_get_address(chain_index_dsa, header->entries_ptr);

    /* Copy under shared lock — fast, bounded by count */
    result->entries = (ChainIndexEntry *)palloc((Size)header->count * sizeof(ChainIndexEntry));
    memcpy(result->entries, entries, (Size)header->count * sizeof(ChainIndexEntry));
    result->count = header->count;
    result->base_seq = header->base_seq;
    result->max_seq = header->max_seq;

    LWLockRelease(&chain_index_locks[stripe].lock);

    return true;
}

/* ---------------------------------------------------------------------------
 * Public API: update cache bits (lockless)
 * ---------------------------------------------------------------------------
 */
void
xpatch_chain_index_update_bits(Oid relid, XPatchGroupHash group_hash,
                                AttrNumber attnum, int64 seq,
                                uint8 set_bits, uint8 clear_bits)
{
    ChainGroupKey key;
    int stripe;
    int dir_idx;
    ChainDirEntry *dir_entry;
    ChainIndexEntry *entry;

    if (!chain_index_initialized)
        return;

    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.attnum = attnum;
    key.group_hash = group_hash;

    stripe = chain_stripe_for_key(&key);

    /*
     * Take shared lock just to find the entry pointer. The actual
     * byte write to cache_bits is atomic on x86 (single byte store).
     * A stale read by another backend would cause a suboptimal but
     * correct path choice.
     */
    LWLockAcquire(&chain_index_locks[stripe].lock, LW_SHARED);

    dir_idx = dir_find(&key);
    if (dir_idx < 0)
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        return;
    }

    dir_entry = &chain_index->dir_entries[dir_idx];
    entry = get_entry_ptr(&dir_entry->header, seq);

    if (entry != NULL && !chain_entry_is_sentinel(entry))
    {
        if (set_bits)
            entry->cache_bits |= set_bits;
        if (clear_bits)
            entry->cache_bits &= ~clear_bits;
    }

    LWLockRelease(&chain_index_locks[stripe].lock);
}

/* ---------------------------------------------------------------------------
 * Public API: delete (mark sentinels from from_seq onward)
 * ---------------------------------------------------------------------------
 */
void
xpatch_chain_index_delete(Oid relid, XPatchGroupHash group_hash,
                           AttrNumber attnum, int64 from_seq)
{
    ChainGroupKey key;
    int stripe;
    int dir_idx;
    ChainDirEntry *dir_entry;
    GroupChainHeader *header;
    ChainIndexEntry *entries;
    int32 start_idx;
    int32 i;

    if (!chain_index_initialized)
        return;

    memset(&key, 0, sizeof(key));
    key.relid = relid;
    key.attnum = attnum;
    key.group_hash = group_hash;

    stripe = chain_stripe_for_key(&key);

    LWLockAcquire(&chain_index_locks[stripe].lock, LW_EXCLUSIVE);

    dir_idx = dir_find(&key);
    if (dir_idx < 0)
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        return;
    }

    dir_entry = &chain_index->dir_entries[dir_idx];
    header = &dir_entry->header;

    if (header->count == 0 || !DsaPointerIsValid(header->entries_ptr))
    {
        LWLockRelease(&chain_index_locks[stripe].lock);
        return;
    }

    if (from_seq < header->base_seq)
        start_idx = 0;
    else
        start_idx = (int32)(from_seq - header->base_seq);

    if (start_idx < header->count)
    {
        ensure_dsa_attached();
        entries = (ChainIndexEntry *)dsa_get_address(chain_index_dsa, header->entries_ptr);

        /* Zero out entries from start_idx onward (sentinel = all zeros) */
        for (i = start_idx; i < header->count; i++)
            memset(&entries[i], 0, sizeof(ChainIndexEntry));

        /* Update max_seq */
        if (from_seq <= header->base_seq)
        {
            /* Everything deleted — free the array */
            dsa_free(chain_index_dsa, header->entries_ptr);
            header->entries_ptr = InvalidDsaPointer;
            header->count = 0;
            header->capacity = 0;
            header->base_seq = 0;
            header->max_seq = 0;

            /* Mark directory entry as tombstone */
            dir_entry->in_use = false;
            dir_entry->tombstone = true;
            chain_index->dir_count--;
        }
        else
        {
            header->max_seq = from_seq - 1;
            header->count = start_idx;
        }
    }

    LWLockRelease(&chain_index_locks[stripe].lock);
}

/* ---------------------------------------------------------------------------
 * Public API: invalidate all entries for a relation (TRUNCATE/DROP)
 * ---------------------------------------------------------------------------
 * Walks the entire directory — expensive but rare (only on TRUNCATE/DROP).
 */
void
xpatch_chain_index_invalidate_rel(Oid relid)
{
    int i, s;

    if (!chain_index_initialized)
        return;

    /*
     * Walk all directory slots. We need exclusive locks on all stripes
     * to safely remove entries. Do stripe-by-stripe to minimize contention.
     */
    for (s = 0; s < CHAIN_INDEX_NUM_STRIPES; s++)
    {
        LWLockAcquire(&chain_index_locks[s].lock, LW_EXCLUSIVE);

        for (i = 0; i < chain_index->dir_capacity; i++)
        {
            ChainDirEntry *e = &chain_index->dir_entries[i];

            if (!e->in_use)
                continue;

            if (e->key.relid != relid)
                continue;

            /* Only process entries that belong to this stripe */
            if (chain_stripe_for_key(&e->key) != s)
                continue;

            /* Free DSA array */
            if (DsaPointerIsValid(e->header.entries_ptr))
            {
                ensure_dsa_attached();
                dsa_free(chain_index_dsa, e->header.entries_ptr);
            }

            /* Mark as tombstone */
            memset(&e->header, 0, sizeof(GroupChainHeader));
            e->header.entries_ptr = InvalidDsaPointer;
            e->in_use = false;
            e->tombstone = true;
            chain_index->dir_count--;
        }

        LWLockRelease(&chain_index_locks[s].lock);
    }
}
