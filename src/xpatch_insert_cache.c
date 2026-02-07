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
 * xpatch_insert_cache.c - Per-group FIFO insert cache with dynamic ring buffer
 *
 * Architecture:
 * - Fixed number of slot headers in shared memory (lightweight, ~64 bytes each)
 * - Each slot's ring buffer (seqs, valid flags, DSA pointers, sizes) is
 *   dynamically allocated in DSA, sized exactly to compress_depth
 * - Variable-length content (raw column data) is also in DSA
 * - No artificial cap on compress_depth — ring buffer grows to match
 *
 * Ring buffer DSA layout (single contiguous allocation):
 *   int64           entry_seqs[depth]
 *   bool            entry_valid[depth]         (padded to MAXALIGN)
 *   dsa_pointer     entry_ptrs[depth * ncols]
 *   Size            entry_sizes[depth * ncols]
 */

#include "xpatch_insert_cache.h"
#include "xpatch_config.h"
#include "xpatch_storage.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

/* GUC variable */
int xpatch_insert_cache_slots = XPATCH_DEFAULT_INSERT_CACHE_SLOTS;

/*
 * Slot structure in shared memory — fixed size, lightweight.
 * The actual ring buffer data lives in DSA pointed to by ring_ptr.
 */
typedef struct InsertCacheSlot
{
    /* Key */
    Oid             relid;
    XPatchGroupHash group_hash;

    /* State */
    bool            in_use;
    pg_atomic_uint64 activity;
    int32           depth;          /* Ring buffer capacity (= compress_depth) */
    int32           count;          /* Valid entries in ring (0..depth) */
    int32           head;           /* Next write position (0..depth-1) */
    int32           num_delta_cols;

    /* DSA pointer to the ring buffer arrays */
    dsa_pointer     ring_ptr;
} InsertCacheSlot;

/* Shared memory header */
typedef struct InsertCacheShmem
{
    int32           num_slots;
    dsa_handle      dsa_hdl;
    pg_atomic_uint64 hit_count;
    pg_atomic_uint64 miss_count;
    pg_atomic_uint64 eviction_count;
    pg_atomic_uint64 eviction_miss_count;  /* Slot evicted while in use */
    InsertCacheSlot slots[FLEXIBLE_ARRAY_MEMBER];
} InsertCacheShmem;

/* Per-backend state */
static InsertCacheShmem *insert_cache = NULL;
static dsa_area *insert_cache_dsa = NULL;
static bool insert_cache_initialized = false;

/* LWLock tranche */
static LWLockPadded *insert_cache_locks = NULL;
static LWLock *insert_cache_meta_lock = NULL;

/* Hooks */
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* ----------------------------------------------------------------
 * Ring buffer layout helpers
 *
 * Given a base pointer (from dsa_get_address on ring_ptr), these
 * return pointers into the contiguous ring buffer allocation.
 * ----------------------------------------------------------------
 */

static inline Size
ring_alloc_size(int depth, int num_delta_cols)
{
    Size size = 0;
    size += MAXALIGN(sizeof(int64) * depth);                /* seqs (aligned) */
    size += MAXALIGN(sizeof(bool) * depth);                 /* valid (aligned) */
    size += MAXALIGN(sizeof(dsa_pointer) * depth * num_delta_cols);  /* ptrs (aligned) */
    size += sizeof(Size) * depth * num_delta_cols;          /* sizes */
    return MAXALIGN(size);
}

static inline int64 *
ring_seqs(void *ring_base)
{
    return (int64 *) ring_base;
}

static inline bool *
ring_valid(void *ring_base, int depth)
{
    return (bool *) ((char *) ring_base + MAXALIGN(sizeof(int64) * depth));
}

static inline dsa_pointer *
ring_ptrs(void *ring_base, int depth)
{
    Size offset = MAXALIGN(sizeof(int64) * depth);
    offset += MAXALIGN(sizeof(bool) * depth);
    return (dsa_pointer *) ((char *) ring_base + offset);
}

static inline Size *
ring_sizes(void *ring_base, int depth, int num_delta_cols)
{
    Size offset = MAXALIGN(sizeof(int64) * depth);
    offset += MAXALIGN(sizeof(bool) * depth);
    offset += MAXALIGN(sizeof(dsa_pointer) * depth * num_delta_cols);
    return (Size *) ((char *) ring_base + offset);
}

/* ----------------------------------------------------------------
 * Shared memory management
 * ----------------------------------------------------------------
 */

static Size
insert_cache_shmem_size(void)
{
    Size size;
    size = offsetof(InsertCacheShmem, slots);
    size = add_size(size, mul_size(sizeof(InsertCacheSlot), xpatch_insert_cache_slots));
    return size;
}

static void
insert_cache_shmem_exit(int code, Datum arg)
{
    if (insert_cache_dsa)
    {
        dsa_detach(insert_cache_dsa);
        insert_cache_dsa = NULL;
    }
    insert_cache = NULL;
    insert_cache_locks = NULL;
    insert_cache_meta_lock = NULL;
    insert_cache_initialized = false;
}

static void
insert_cache_shmem_request(void)
{
    if (prev_shmem_request_hook)
        prev_shmem_request_hook();

    RequestAddinShmemSpace(insert_cache_shmem_size());
    RequestNamedLWLockTranche("pg_xpatch_insert_cache",
                              xpatch_insert_cache_slots + 1);
}

static void
init_slot(InsertCacheSlot *slot)
{
    slot->relid = InvalidOid;
    memset(&slot->group_hash, 0, sizeof(XPatchGroupHash));
    slot->in_use = false;
    pg_atomic_init_u64(&slot->activity, 0);
    slot->depth = 0;
    slot->count = 0;
    slot->head = 0;
    slot->num_delta_cols = 0;
    slot->ring_ptr = InvalidDsaPointer;
}

static void
insert_cache_shmem_startup(void)
{
    bool found;
    Size cache_size;
    int i;

    if (prev_shmem_startup_hook)
        prev_shmem_startup_hook();

    LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

    cache_size = insert_cache_shmem_size();

    insert_cache = ShmemInitStruct("pg_xpatch_insert_cache",
                                   cache_size,
                                   &found);

    if (!found)
    {
        memset(insert_cache, 0, cache_size);
        insert_cache->num_slots = xpatch_insert_cache_slots;
        insert_cache->dsa_hdl = DSM_HANDLE_INVALID;
        pg_atomic_init_u64(&insert_cache->hit_count, 0);
        pg_atomic_init_u64(&insert_cache->miss_count, 0);
        pg_atomic_init_u64(&insert_cache->eviction_count, 0);
        pg_atomic_init_u64(&insert_cache->eviction_miss_count, 0);

        for (i = 0; i < xpatch_insert_cache_slots; i++)
            init_slot(&insert_cache->slots[i]);

        elog(LOG, "pg_xpatch: insert cache initialized (%d slots, dynamic depth)",
             xpatch_insert_cache_slots);
    }

    insert_cache_locks = GetNamedLWLockTranche("pg_xpatch_insert_cache");
    insert_cache_meta_lock = &insert_cache_locks[xpatch_insert_cache_slots].lock;

    LWLockRelease(AddinShmemInitLock);

    insert_cache_initialized = true;
    on_shmem_exit(insert_cache_shmem_exit, (Datum) 0);
}

/* ----------------------------------------------------------------
 * DSA management
 * ----------------------------------------------------------------
 */

static void
ensure_dsa_attached(void)
{
    MemoryContext oldcxt;

    if (insert_cache_dsa != NULL)
        return;

    /*
     * Switch to TopMemoryContext for DSA creation/attachment.
     * The DSA control structures must live for the entire backend lifetime,
     * not just the current INSERT's memory context.
     */
    oldcxt = MemoryContextSwitchTo(TopMemoryContext);

    LWLockAcquire(insert_cache_meta_lock, LW_EXCLUSIVE);

    if (insert_cache->dsa_hdl == DSM_HANDLE_INVALID)
    {
        int tranche_id = insert_cache_meta_lock->tranche;
        insert_cache_dsa = dsa_create(tranche_id);
        dsa_pin(insert_cache_dsa);
        insert_cache->dsa_hdl = dsa_get_handle(insert_cache_dsa);
        elog(DEBUG1, "xpatch: insert cache DSA created (handle=%u)", insert_cache->dsa_hdl);
    }
    else
    {
        insert_cache_dsa = dsa_attach(insert_cache->dsa_hdl);
        elog(DEBUG1, "xpatch: insert cache DSA attached (handle=%u)", insert_cache->dsa_hdl);
    }

    LWLockRelease(insert_cache_meta_lock);

    dsa_pin_mapping(insert_cache_dsa);

    MemoryContextSwitchTo(oldcxt);
}

/* ----------------------------------------------------------------
 * Slot content management
 * ----------------------------------------------------------------
 */

/*
 * Free all DSA content in a slot's ring buffer, then free the ring itself.
 * Caller must hold the slot's LWLock in exclusive mode.
 */
static void
free_slot_content(InsertCacheSlot *slot)
{
    void *ring_base;
    bool *valid;
    dsa_pointer *ptrs;
    int i, j;

    if (!insert_cache_dsa || !DsaPointerIsValid(slot->ring_ptr))
        return;

    ring_base = dsa_get_address(insert_cache_dsa, slot->ring_ptr);
    valid = ring_valid(ring_base, slot->depth);
    ptrs = ring_ptrs(ring_base, slot->depth);

    for (i = 0; i < slot->depth; i++)
    {
        if (!valid[i])
            continue;

        for (j = 0; j < slot->num_delta_cols; j++)
        {
            dsa_pointer p = ptrs[i * slot->num_delta_cols + j];
            if (DsaPointerIsValid(p))
                dsa_free(insert_cache_dsa, p);
        }
    }

    dsa_free(insert_cache_dsa, slot->ring_ptr);
    slot->ring_ptr = InvalidDsaPointer;
    slot->count = 0;
    slot->head = 0;
}

/*
 * Allocate and initialize a ring buffer in DSA for the given depth/ncols.
 * Returns the dsa_pointer to the ring, or InvalidDsaPointer on failure.
 */
static dsa_pointer
alloc_ring(int depth, int num_delta_cols)
{
    Size alloc_size;
    dsa_pointer ptr;
    void *ring_base;
    int64 *seqs;
    bool *valid;
    dsa_pointer *ptrs;
    Size *sizes;
    int i, total_ptrs;

    alloc_size = ring_alloc_size(depth, num_delta_cols);
    ptr = dsa_allocate_extended(insert_cache_dsa, alloc_size,
                                DSA_ALLOC_NO_OOM | DSA_ALLOC_ZERO);

    if (!DsaPointerIsValid(ptr))
        return InvalidDsaPointer;

    /* Initialize all arrays to zero/invalid */
    ring_base = dsa_get_address(insert_cache_dsa, ptr);
    seqs = ring_seqs(ring_base);
    valid = ring_valid(ring_base, depth);
    ptrs = ring_ptrs(ring_base, depth);
    sizes = ring_sizes(ring_base, depth, num_delta_cols);

    total_ptrs = depth * num_delta_cols;

    for (i = 0; i < depth; i++)
    {
        seqs[i] = 0;
        valid[i] = false;
    }
    for (i = 0; i < total_ptrs; i++)
    {
        ptrs[i] = InvalidDsaPointer;
        sizes[i] = 0;
    }

    return ptr;
}

/* ----------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------
 */

void
xpatch_insert_cache_request_shmem(void)
{
    prev_shmem_request_hook = shmem_request_hook;
    shmem_request_hook = insert_cache_shmem_request;

    prev_shmem_startup_hook = shmem_startup_hook;
    shmem_startup_hook = insert_cache_shmem_startup;
}

void
xpatch_insert_cache_init(void)
{
    /* Actual initialization happens in shmem_startup_hook */
}

/*
 * Find the least-active slot for eviction.
 */
static int
find_least_active_slot(void)
{
    int i;
    int best_idx = 0;
    uint64 best_activity = PG_UINT64_MAX;

    for (i = 0; i < insert_cache->num_slots; i++)
    {
        InsertCacheSlot *slot = &insert_cache->slots[i];
        uint64 act;

        if (!slot->in_use)
            return i;

        act = pg_atomic_read_u64(&slot->activity);
        if (act < best_activity)
        {
            best_activity = act;
            best_idx = i;
        }
    }

    return best_idx;
}

/*
 * Get or allocate a FIFO slot for a (table, group) pair.
 */
int
xpatch_insert_cache_get_slot(Oid relid, Datum group_value, Oid typid,
                             int depth, int num_delta_cols, bool *is_new,
                             XPatchGroupHash *out_hash)
{
    XPatchGroupHash ghash;
    int i;
    int slot_idx;
    InsertCacheSlot *slot;

    if (!insert_cache_initialized || !insert_cache)
        return -1;

    if (depth < 1)
        depth = 1;
    if (num_delta_cols > XPATCH_MAX_DELTA_COLUMNS)
        num_delta_cols = XPATCH_MAX_DELTA_COLUMNS;

    ensure_dsa_attached();

    ghash = xpatch_compute_group_hash(group_value, typid, false);

    /* Return the hash to caller for ownership validation */
    if (out_hash)
        *out_hash = ghash;

    *is_new = false;

    /* Search for existing slot — verify under shared lock to prevent race with eviction */
    for (i = 0; i < insert_cache->num_slots; i++)
    {
        slot = &insert_cache->slots[i];
        if (slot->in_use &&
            slot->relid == relid &&
            xpatch_group_hash_equals(slot->group_hash, ghash))
        {
            /* Re-verify under lock to prevent race with concurrent eviction */
            LWLockAcquire(&insert_cache_locks[i].lock, LW_SHARED);
            if (slot->in_use &&
                slot->relid == relid &&
                xpatch_group_hash_equals(slot->group_hash, ghash))
            {
                pg_atomic_fetch_add_u64(&slot->activity, 1);
                pg_atomic_fetch_add_u64(&insert_cache->hit_count, 1);
                LWLockRelease(&insert_cache_locks[i].lock);
                return i;
            }
            LWLockRelease(&insert_cache_locks[i].lock);
            /* Slot was evicted between our check and lock — continue scanning */
        }
    }

    /* Not found - allocate new slot (evict least active) */
    pg_atomic_fetch_add_u64(&insert_cache->miss_count, 1);

    slot_idx = find_least_active_slot();
    slot = &insert_cache->slots[slot_idx];

    LWLockAcquire(&insert_cache_locks[slot_idx].lock, LW_EXCLUSIVE);

    /* Double-check after acquiring lock */
    if (slot->in_use &&
        slot->relid == relid &&
        xpatch_group_hash_equals(slot->group_hash, ghash))
    {
        pg_atomic_fetch_add_u64(&slot->activity, 1);
        LWLockRelease(&insert_cache_locks[slot_idx].lock);
        return slot_idx;
    }

    /* Evict existing content */
    if (slot->in_use)
    {
        free_slot_content(slot);
        pg_atomic_fetch_add_u64(&insert_cache->eviction_count, 1);
    }

    /* Initialize for new owner */
    slot->relid = relid;
    slot->group_hash = ghash;
    slot->in_use = true;
    pg_atomic_write_u64(&slot->activity, 1);
    slot->depth = depth;
    slot->count = 0;
    slot->head = 0;
    slot->num_delta_cols = num_delta_cols;

    /* Allocate ring buffer in DSA */
    slot->ring_ptr = alloc_ring(depth, num_delta_cols);
    if (!DsaPointerIsValid(slot->ring_ptr))
    {
        /* DSA allocation failed - mark slot as unusable */
        slot->in_use = false;
        LWLockRelease(&insert_cache_locks[slot_idx].lock);
        elog(WARNING, "xpatch: failed to allocate ring buffer (depth=%d, cols=%d)",
             depth, num_delta_cols);
        return -1;
    }

    *is_new = true;

    LWLockRelease(&insert_cache_locks[slot_idx].lock);

    return slot_idx;
}

/* Per-backend flag to avoid spamming eviction warnings */
static bool eviction_miss_warned = false;

/*
 * Record an eviction miss and warn once per backend.
 */
static void
record_eviction_miss(void)
{
    pg_atomic_fetch_add_u64(&insert_cache->eviction_miss_count, 1);

    if (!eviction_miss_warned)
    {
        elog(WARNING, "xpatch: insert cache slot evicted during use "
             "(consider increasing pg_xpatch.insert_cache_slots from %d "
             "or reducing concurrent writers to the same table)",
             xpatch_insert_cache_slots);
        eviction_miss_warned = true;
    }
}

/*
 * Get base contents from a FIFO slot for delta encoding.
 * Returns up to depth bases, each palloc'd in CurrentMemoryContext.
 *
 * If the slot has been evicted and reused by another group, returns count=0
 * and the caller should fall back to reconstruction.
 */
void
xpatch_insert_cache_get_bases(int slot_idx, Oid relid,
                              XPatchGroupHash expected_hash,
                              int64 new_seq, int col_idx,
                              InsertCacheBases *bases)
{
    InsertCacheSlot *slot;
    void *ring_base;
    int64 *seqs;
    bool *valid;
    dsa_pointer *ptrs;
    Size *sizes;
    int i, ring_idx;
    int bases_found = 0;

    bases->count = 0;

    if (!insert_cache_initialized || slot_idx < 0 ||
        slot_idx >= insert_cache->num_slots)
        return;

    slot = &insert_cache->slots[slot_idx];

    if (!slot->in_use || col_idx >= slot->num_delta_cols ||
        !DsaPointerIsValid(slot->ring_ptr))
        return;

    LWLockAcquire(&insert_cache_locks[slot_idx].lock, LW_SHARED);

    /*
     * Ownership check: verify slot still belongs to our (relid, group).
     * Another process may have evicted and reused this slot since get_slot().
     */
    if (!slot->in_use ||
        slot->relid != relid ||
        !xpatch_group_hash_equals(slot->group_hash, expected_hash))
    {
        LWLockRelease(&insert_cache_locks[slot_idx].lock);
        record_eviction_miss();
        return;  /* Cache miss - caller falls back to reconstruction */
    }

    ring_base = dsa_get_address(insert_cache_dsa, slot->ring_ptr);
    seqs = ring_seqs(ring_base);
    valid = ring_valid(ring_base, slot->depth);
    ptrs = ring_ptrs(ring_base, slot->depth);
    sizes = ring_sizes(ring_base, slot->depth, slot->num_delta_cols);

    /*
     * Walk the ring buffer backwards from head, collecting valid entries.
     * Tag = new_seq - entry_seq (how many rows back this base is).
     */
    for (i = 0; i < slot->count && i < slot->depth; i++)
    {
        int tag;
        int ptr_offset;
        dsa_pointer content_ptr;
        Size content_size;

        ring_idx = (slot->head - 1 - i + slot->depth) % slot->depth;

        if (!valid[ring_idx])
            continue;

        tag = new_seq - seqs[ring_idx];
        if (tag < 1 || tag > slot->depth)
            continue;

        ptr_offset = ring_idx * slot->num_delta_cols + col_idx;
        content_ptr = ptrs[ptr_offset];
        content_size = sizes[ptr_offset];

        if (!DsaPointerIsValid(content_ptr) || content_size == 0)
            continue;

        /* Copy content to caller's palloc'd memory */
        {
            void *dsa_mem = dsa_get_address(insert_cache_dsa, content_ptr);

            bases->bases[bases_found].seq = seqs[ring_idx];
            bases->bases[bases_found].tag = tag;
            bases->bases[bases_found].data = palloc(content_size);
            memcpy((void *) bases->bases[bases_found].data, dsa_mem, content_size);
            bases->bases[bases_found].size = content_size;
            bases_found++;

            if (bases_found >= bases->capacity)
                break;
        }
    }

    LWLockRelease(&insert_cache_locks[slot_idx].lock);

    bases->count = bases_found;

    /* Sort by tag ascending (closest base first) */
    {
        int si, sj;
        for (si = 1; si < bases->count; si++)
        {
            sj = si;
            while (sj > 0 && bases->bases[sj].tag < bases->bases[sj - 1].tag)
            {
                int64 tmp_seq = bases->bases[sj].seq;
                int tmp_tag = bases->bases[sj].tag;
                const uint8 *tmp_data = bases->bases[sj].data;
                Size tmp_size = bases->bases[sj].size;

                bases->bases[sj].seq = bases->bases[sj - 1].seq;
                bases->bases[sj].tag = bases->bases[sj - 1].tag;
                bases->bases[sj].data = bases->bases[sj - 1].data;
                bases->bases[sj].size = bases->bases[sj - 1].size;

                bases->bases[sj - 1].seq = tmp_seq;
                bases->bases[sj - 1].tag = tmp_tag;
                bases->bases[sj - 1].data = tmp_data;
                bases->bases[sj - 1].size = tmp_size;
                sj--;
            }
        }
    }
}

/*
 * Push new row content into the FIFO ring buffer for one column.
 * If the slot has been evicted and reused by another group, this is a no-op.
 */
void
xpatch_insert_cache_push(int slot_idx, Oid relid,
                         XPatchGroupHash expected_hash,
                         int64 seq, int col_idx,
                         const uint8 *data, Size size)
{
    InsertCacheSlot *slot;
    void *ring_base;
    dsa_pointer *ptrs;
    Size *sizes;
    int64 *seqs;
    int write_pos;
    int ptr_offset;
    dsa_pointer new_ptr;
    void *dsa_mem;

    if (!insert_cache_initialized || slot_idx < 0 ||
        slot_idx >= insert_cache->num_slots || !insert_cache_dsa)
        return;

    slot = &insert_cache->slots[slot_idx];

    if (!slot->in_use || col_idx >= slot->num_delta_cols ||
        !DsaPointerIsValid(slot->ring_ptr))
        return;

    if (size == 0 || data == NULL)
        return;

    LWLockAcquire(&insert_cache_locks[slot_idx].lock, LW_EXCLUSIVE);

    /*
     * Ownership check: verify slot still belongs to our (relid, group).
     * If not, silently skip - the data was already written to heap correctly.
     */
    if (!slot->in_use ||
        slot->relid != relid ||
        !xpatch_group_hash_equals(slot->group_hash, expected_hash))
    {
        LWLockRelease(&insert_cache_locks[slot_idx].lock);
        record_eviction_miss();
        return;
    }

    ring_base = dsa_get_address(insert_cache_dsa, slot->ring_ptr);
    ptrs = ring_ptrs(ring_base, slot->depth);
    sizes = ring_sizes(ring_base, slot->depth, slot->num_delta_cols);
    seqs = ring_seqs(ring_base);

    write_pos = slot->head;
    ptr_offset = write_pos * slot->num_delta_cols + col_idx;

    /* Free old content at this position if any */
    if (DsaPointerIsValid(ptrs[ptr_offset]))
    {
        dsa_free(insert_cache_dsa, ptrs[ptr_offset]);
        ptrs[ptr_offset] = InvalidDsaPointer;
        sizes[ptr_offset] = 0;
    }

    /* Allocate DSA memory and copy content (NO_OOM to avoid throwing with lock held) */
    new_ptr = dsa_allocate_extended(insert_cache_dsa, size, DSA_ALLOC_NO_OOM);
    if (!DsaPointerIsValid(new_ptr))
    {
        /* OOM — leave slot in consistent state (old entry freed, new not written) */
        LWLockRelease(&insert_cache_locks[slot_idx].lock);
        return;
    }
    dsa_mem = dsa_get_address(insert_cache_dsa, new_ptr);
    memcpy(dsa_mem, data, size);

    ptrs[ptr_offset] = new_ptr;
    sizes[ptr_offset] = size;
    seqs[write_pos] = seq;

    LWLockRelease(&insert_cache_locks[slot_idx].lock);
}

/*
 * Mark a FIFO entry as complete (all columns written).
 * If the slot has been evicted and reused by another group, this is a no-op.
 */
void
xpatch_insert_cache_commit_entry(int slot_idx, Oid relid,
                                 XPatchGroupHash expected_hash,
                                 int64 seq)
{
    InsertCacheSlot *slot;
    void *ring_base;
    bool *valid;
    int64 *seqs;
    int write_pos;

    if (!insert_cache_initialized || slot_idx < 0 ||
        slot_idx >= insert_cache->num_slots)
        return;

    slot = &insert_cache->slots[slot_idx];
    if (!slot->in_use || !DsaPointerIsValid(slot->ring_ptr))
        return;

    LWLockAcquire(&insert_cache_locks[slot_idx].lock, LW_EXCLUSIVE);

    /*
     * Ownership check: verify slot still belongs to our (relid, group).
     * If not, silently skip.
     */
    if (!slot->in_use ||
        slot->relid != relid ||
        !xpatch_group_hash_equals(slot->group_hash, expected_hash))
    {
        LWLockRelease(&insert_cache_locks[slot_idx].lock);
        record_eviction_miss();
        return;
    }

    ring_base = dsa_get_address(insert_cache_dsa, slot->ring_ptr);
    valid = ring_valid(ring_base, slot->depth);
    seqs = ring_seqs(ring_base);

    write_pos = slot->head;

    valid[write_pos] = true;
    seqs[write_pos] = seq;

    /* Advance head (ring buffer wrap) */
    slot->head = (slot->head + 1) % slot->depth;

    /* Track count (caps at depth) */
    if (slot->count < slot->depth)
        slot->count++;

    LWLockRelease(&insert_cache_locks[slot_idx].lock);
}

/*
 * Populate a FIFO slot with reconstructed content (cold start).
 * This is called right after get_slot() when we own the slot, so we use
 * the slot's own relid/hash for the ownership checks.
 */
void
xpatch_insert_cache_populate(int slot_idx, Relation rel,
                             struct XPatchConfig *config,
                             Datum group_value, int64 current_max_seq)
{
    InsertCacheSlot *slot;
    Oid relid;
    XPatchGroupHash slot_hash;
    int depth;
    int num_to_populate;
    int i, j;
    int64 seq;

    if (!insert_cache_initialized || slot_idx < 0 ||
        slot_idx >= insert_cache->num_slots)
        return;

    slot = &insert_cache->slots[slot_idx];
    if (!slot->in_use || !DsaPointerIsValid(slot->ring_ptr))
        return;

    ensure_dsa_attached();

    /* Capture ownership info from slot (we own it, just got it from get_slot) */
    relid = slot->relid;
    slot_hash = slot->group_hash;

    depth = slot->depth;

    /* Only populate what we can (may have fewer rows than depth) */
    num_to_populate = Min(depth, current_max_seq);

    if (num_to_populate <= 0)
        return;

    /*
     * Reconstruct the last num_to_populate rows and push into FIFO.
     * Start from oldest to newest so FIFO ordering is correct.
     */
    for (i = num_to_populate - 1; i >= 0; i--)
    {
        seq = current_max_seq - i;

        for (j = 0; j < config->num_delta_columns && j < XPATCH_MAX_DELTA_COLUMNS; j++)
        {
            bytea *content;

            content = xpatch_reconstruct_column(rel, config, group_value,
                                                seq, j);
            if (content != NULL)
            {
                xpatch_insert_cache_push(slot_idx, relid, slot_hash,
                                         seq, j,
                                         (const uint8 *) VARDATA_ANY(content),
                                         VARSIZE_ANY_EXHDR(content));
                pfree(content);
            }
        }

        xpatch_insert_cache_commit_entry(slot_idx, relid, slot_hash, seq);
    }
}

/*
 * Invalidate all FIFO slots for a relation.
 */
void
xpatch_insert_cache_invalidate_rel(Oid relid)
{
    int i;

    if (!insert_cache_initialized || !insert_cache)
        return;

    if (insert_cache->dsa_hdl != DSM_HANDLE_INVALID)
        ensure_dsa_attached();

    for (i = 0; i < insert_cache->num_slots; i++)
    {
        InsertCacheSlot *slot = &insert_cache->slots[i];

        if (slot->in_use && slot->relid == relid)
        {
            LWLockAcquire(&insert_cache_locks[i].lock, LW_EXCLUSIVE);

            if (slot->in_use && slot->relid == relid)
            {
                free_slot_content(slot);
                slot->in_use = false;
            }

            LWLockRelease(&insert_cache_locks[i].lock);
        }
    }
}

/*
 * Get insert cache statistics.
 */
void
xpatch_insert_cache_get_stats(InsertCacheStats *stats)
{
    int i;

    memset(stats, 0, sizeof(*stats));

    if (!insert_cache_initialized || !insert_cache)
        return;

    stats->total_slots = insert_cache->num_slots;
    stats->hits = pg_atomic_read_u64(&insert_cache->hit_count);
    stats->misses = pg_atomic_read_u64(&insert_cache->miss_count);
    stats->evictions = pg_atomic_read_u64(&insert_cache->eviction_count);
    stats->eviction_misses = pg_atomic_read_u64(&insert_cache->eviction_miss_count);

    for (i = 0; i < insert_cache->num_slots; i++)
    {
        if (insert_cache->slots[i].in_use)
            stats->slots_in_use++;
    }
}
