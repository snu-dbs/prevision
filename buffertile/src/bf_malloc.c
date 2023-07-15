#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include <bf.h>

#include "bf_malloc.h"

#if BF_MALLOC_DEBUG
#include <assert.h>
#endif

#if BF_MALLOC_DEBUG
static int malloc_count = 0;
static int realloc_count = 0;
static int free_count = 0;
#endif


// list util function
static inline void* ptr(BFmspace *mspace, BFmspace_offset_t offset) {
    return (offset != oNULL) ? (void *) ((char *) mspace + offset) : NULL;
}

static inline BFmspace_offset_t offset(BFmspace *mspace, BFmspace_chunk_hdr *chunk) {
    return (BFmspace_offset_t) ((char *) chunk - (char *) mspace);
}

static inline BFmspace_chunk_hdr* next(BFmspace *mspace, BFmspace_chunk_hdr *chunk) {
    return ptr(mspace, chunk->next);
}

static inline BFmspace_chunk_hdr* next_free(BFmspace *mspace, BFmspace_chunk_hdr *chunk) {
    return ptr(mspace, chunk->next_free);
}

static inline BFmspace_chunk_hdr* prev(BFmspace *mspace, BFmspace_chunk_hdr *chunk) {
    return ptr(mspace, chunk->prev);
}

static inline BFmspace_chunk_hdr* prev_free(BFmspace *mspace, BFmspace_chunk_hdr *chunk) {
    return ptr(mspace, chunk->prev_free);
}

static inline void attemp_set_search_start(BFmspace *mspace, BFmspace_offset_t offset) {
    if (offset == oNULL) mspace->search_start = mspace->free_head;
    else mspace->search_start = offset;
}

static inline void insert_to_free_list(BFmspace *mspace, BFmspace_chunk_hdr *hdr) {
    hdr->prev_free = oNULL;
    hdr->next_free = mspace->free_head;
    mspace->free_head = offset(mspace, hdr);
    BFmspace_chunk_hdr *next_free_hdr = next_free(mspace, hdr);
    if (next_free_hdr != NULL) next_free_hdr->prev_free = mspace->free_head;
}

static inline void remove_from_free_list(BFmspace *mspace, BFmspace_chunk_hdr *hdr) {
    if (hdr->prev_free == oNULL) {
        // head
        mspace->free_head = hdr->next_free;

        // adjust header's prev
        BFmspace_chunk_hdr *n = next_free(mspace, hdr);
        if (n != NULL) n->prev_free = oNULL;

    } else if (hdr->next_free == oNULL) {
        // tail
        // adjust prev's next
        BFmspace_chunk_hdr *p = prev_free(mspace, hdr);
        if (p != NULL) p->next_free = oNULL;
    } else {
        // middle
        // next and prev should exists
        BFmspace_chunk_hdr *n = next_free(mspace, hdr);
        BFmspace_chunk_hdr *p = prev_free(mspace, hdr);

        p->next_free = offset(mspace, n);
        n->prev_free = offset(mspace, p);
    }

    hdr->next_free = oNULL;
    hdr->prev_free = oNULL;
}

static inline void debug_chunk(BFmspace *mspace, BFmspace_chunk_hdr *chunk) {
// #if BF_MALLOC_DEBUG
    if (mspace == NULL || chunk == NULL) return;

    fprintf(stderr, "[BF_MALLOC]\tmspace: %p\n", (void *) mspace);
    fprintf(stderr, "[BF_MALLOC]\tchunk: %p (offset= %ld)\n", 
        (void *) chunk, ((char *) chunk - (char *) mspace));
    fprintf(stderr, "[BF_MALLOC]\t\tsize: %ld\n", chunk->size);
    fprintf(stderr, "[BF_MALLOC]\t\tnext: %p\n", (void *) next(mspace, chunk));
    fprintf(stderr, "[BF_MALLOC]\t\tprev: %p\n", (void *) prev(mspace, chunk));
    fprintf(stderr, "[BF_MALLOC]\t\tnext_free: %p\n", (void *) next_free(mspace, chunk));
    fprintf(stderr, "[BF_MALLOC]\t\tprev_free: %p\n", (void *) prev_free(mspace, chunk));
    fprintf(stderr, "[BF_MALLOC]\t\tused: %d\n", chunk->used);
// #endif
}

BFmspace* BF_shm_create_mspace(void *base, size_t size) {
#if BF_MALLOC_DEBUG
    fprintf(stderr, "[BF_MALLOC] create_mspace(size= %ld)\n", size);
#endif
    // Initialize BF mspace
    // The BFmspace structure may be padded automatically.
    BFmspace *mspace = (BFmspace*) BF_SHM_ALIGNED_MEMORY(base);
    mspace->mspace_meta_size = BF_SHM_ALIGNED_SIZEOF(sizeof(BFmspace));
    mspace->base = mspace->free_head = mspace->search_start \
        = mspace->mspace_meta_size;
    mspace->size = size - mspace->mspace_meta_size;
    mspace->hdr_size = BF_SHM_ALIGNED_SIZEOF(sizeof(BFmspace_chunk_hdr));
    mspace->unique = random() % 65535;        // no srand.

    // Create new large empty chunk
    BFmspace_chunk_hdr *hdr = 
        (BFmspace_chunk_hdr*) ((char *) mspace + mspace->base);
    hdr->unique = mspace->unique;
    hdr->size = mspace->size - mspace->hdr_size;
    hdr->used = 0;
    hdr->next = oNULL;
    hdr->prev = oNULL;
    hdr->next_free = oNULL;
    hdr->prev_free = oNULL;

    // debugging
#if BF_MALLOC_DEBUG
    fprintf(stderr, "[BF_MALLOC]\tmspace created\n");
    debug_chunk(mspace, hdr);
#endif

    return mspace;
}


void* BF_shm_malloc(BFmspace* mspace, size_t size) {
#if BF_MALLOC_DEBUG
    fprintf(stderr, "[BF_MALLOC] malloc(size= %ld)\n", size);
    malloc_count++;
#endif
    // align the size of the size, argument.
    // I think it would not a matter to change the size directly.
    size = BF_SHM_ALIGNED_SIZEOF(size);
    size_t required_size = size + mspace->hdr_size;
    
    // Quick Fix of avoiding cascaded eviction
    if (mspace == _mspace_data || mspace == _mspace_idata) {
        int res = 0;
        if ((res = BF_claim_memory(mspace, size)) != 0) {
            // fprintf(stderr, "err=%d , size=%lu\n", res, size);
            fprintf(stderr, "[BufferTile] Memory Claim Failed. It probably means that no memory available.\n");
            // BF_ShowLists();
            return NULL;
        }
    }

    // find a chunk in the free list
    BFmspace_chunk_hdr *hdr = BF_shm_find_fittable_chunk_bestfit(mspace, size);
    
    // no chunk found
    if (hdr == NULL) {
        fprintf(stderr, "[BF_MALLOC] None of chunks are fittable.\n");
        return NULL;
    }

#if BF_MALLOC_DEBUG
    fprintf(stderr, "[BF_MALLOC]\tchunk found\n");
    // debug_chunk(mspace, hdr);
#endif

    // set next search_start
    mspace->search_start = hdr->next_free;

    // remove chunk from free list
    remove_from_free_list(mspace, hdr);

    // split the chunk if it has too enough space
    if (hdr->size > required_size) {
        // create new chunk
        BFmspace_chunk_hdr *new_hdr = 
            (BFmspace_chunk_hdr*) ((char *) hdr + required_size);
        new_hdr->unique = mspace->unique;
        new_hdr->size = hdr->size - required_size;
        new_hdr->used = 0;
        new_hdr->prev = offset(mspace, hdr);
        new_hdr->next = hdr->next;

        // adjust next chunk's prev
        BFmspace_chunk_hdr *next_hdr = next(mspace, hdr);
        if (next_hdr != NULL) next_hdr->prev = offset(mspace, new_hdr);

        // put new chunk into free list
        insert_to_free_list(mspace, new_hdr);

        // set next search_start
        mspace->search_start = offset(mspace, new_hdr);

        // adjust original chunk
        hdr->size = size;
        hdr->next = offset(mspace, new_hdr);

#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\tchunk split\n");
        fprintf(stderr, "[BF_MALLOC]\thdr:\n"); debug_chunk(mspace, hdr);
        fprintf(stderr, "[BF_MALLOC]\tnew_hdr:\n"); debug_chunk(mspace, new_hdr);
        fprintf(stderr, "[BF_MALLOC]\tnext_hdr:\n"); debug_chunk(mspace, next_hdr);
#endif
    }

    hdr->used = 1;

    void *ret = (void*) ((char *) hdr + mspace->hdr_size);
#if BF_MALLOC_DEBUG
        fprintf(stderr, "\tret:\n");
        fprintf(stderr, "\thdr\n"); debug_chunk(mspace, hdr);
        fprintf(stderr, "\tret: %p\n", (void *) ret);
        fprintf(stderr, "====== %ld\n", ((char *) ret - (char *) mspace));
#endif
    return ret;
}

void* BF_shm_realloc(BFmspace* mspace, void* ptr, size_t size) {
#if BF_MALLOC_DEBUG
    fprintf(stderr, "[BF_MALLOC] realloc(size= %ld)\n", size);
    realloc_count++;
#endif
    // align the size of the size, argument.
    // I think it would not a matter to change the size directly.
    size = BF_SHM_ALIGNED_SIZEOF(size);

    BFmspace_chunk_hdr *hdr = 
        (BFmspace_chunk_hdr*) ((char *) ptr - mspace->hdr_size);
    // fprintf(stderr, "\thdr: %p (size= %d, used= %d)\n", hdr, hdr->size, hdr->used);

    // if the size is enlarged, free this and malloc new one.
    if (hdr->size < size) {
#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\talloc new\n");
#endif
        void *new = BF_shm_malloc(mspace, size);
        memcpy(new, ptr, hdr->size);
        BF_shm_free(mspace, ptr);
        return new;
    }

    // if new size is too small, split the chunk
    size_t required_size = size + mspace->hdr_size;
    if (hdr->size > required_size) {
        // create new chunk
        BFmspace_chunk_hdr *new_hdr = 
            (BFmspace_chunk_hdr*) ((char *) hdr + required_size);
        new_hdr->unique = mspace->unique;
        new_hdr->size = hdr->size - required_size;
        new_hdr->used = 0;
        new_hdr->prev = offset(mspace, hdr);
        new_hdr->next = hdr->next;

        // adjust next chunk's prev
        BFmspace_chunk_hdr *next_hdr = next(mspace, hdr);
        if (next_hdr != NULL) next_hdr->prev = offset(mspace, new_hdr);

        // put new chunk into free list
        insert_to_free_list(mspace, new_hdr);

        // adjust original chunk
        hdr->size = size;
        hdr->next = offset(mspace, new_hdr);

#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\tchunk split\n");
        // fprintf(stderr, "\thdr\n"); debug_chunk(mspace, hdr);
        // fprintf(stderr, "\tnew_hdr\n"); debug_chunk(mspace, new_hdr);
        // fprintf(stderr, "\tnext_hdr\n"); debug_chunk(mspace, next_hdr);
#endif
    }

    // return original one without modification
    return ptr;
}

void BF_shm_free(BFmspace* mspace, void* ptr) {
#if BF_MALLOC_DEBUG
    fprintf(stderr, "[BF_MALLOC] free()\n");
    free_count++;

    // check if ptr is in the mspace
    assert((char *) mspace + mspace->base <= (char *) ptr && 
        (char *) ptr < ((char *) mspace + mspace->base + mspace->size));
#endif
    BFmspace_chunk_hdr *hdr = 
        (BFmspace_chunk_hdr*) ((char *) ptr - mspace->hdr_size);

    assert(hdr->unique == mspace->unique);

    uint8_t merged = 0;     // is hdr merged? if not, need to update free list
    hdr->used = 0;

    // merge the previous chunk of freed one
    BFmspace_chunk_hdr *prev_hdr = prev(mspace, hdr);
    if (prev_hdr != NULL && prev_hdr->used == 0) {
#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\tchunk merge (previous)\n");
        fprintf(stderr, "[BF_MALLOC]\thdr\n"); debug_chunk(mspace, hdr);
        fprintf(stderr, "[BF_MALLOC]\tprev_hdr\n"); debug_chunk(mspace, prev_hdr);
#endif
        // merge to the previous chunk
        prev_hdr->size += hdr->size + mspace->hdr_size;
        prev_hdr->next = hdr->next;

        // adjust the next chunk's prev
        BFmspace_chunk_hdr *next_hdr = next(mspace, hdr);
        if (next_hdr != NULL) next_hdr->prev = offset(mspace, prev_hdr);

        // no need to adjust free list 
        //      since prev_hdr is already in the free list.

#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\tnext_hdr\n"); debug_chunk(mspace, next_hdr);
#endif

        // change hdr to previous chunk for next prodecure
        hdr = prev_hdr;

        // update merged
        merged = 1;
    }

    // merge the next chunk of freed one
    BFmspace_chunk_hdr *next_hdr = next(mspace, hdr);
    if (next_hdr != NULL && next_hdr->used == 0) {
#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\tchunk merge (next)\n");
        fprintf(stderr, "[BF_MALLOC]\thdr\n"); debug_chunk(mspace, hdr);
        fprintf(stderr, "[BF_MALLOC]\tnext_hdr\n"); debug_chunk(mspace, next_hdr);
#endif
        // merge to hdr
        hdr->size += next_hdr->size + mspace->hdr_size;
        hdr->next = next_hdr->next;

        // adjust the next chunk's prev
        BFmspace_chunk_hdr *next_next_hdr = next(mspace, next_hdr);
        if (next_next_hdr != NULL) next_next_hdr->prev = offset(mspace, hdr);

        // next_chunk must deleted from free list 
        remove_from_free_list(mspace, next_hdr);

        // set next search_start
        mspace->search_start = next_hdr->next_free;

#if BF_MALLOC_DEBUG
        fprintf(stderr, "[BF_MALLOC]\tnext_next_hdr\n");debug_chunk(mspace, next_next_hdr);
#endif
    }

    if (merged == 0) {
        // put freed chunk into free list
        insert_to_free_list(mspace, hdr);
    }
}


// #if BF_MALLOC_DEBUG

void BF_shm_get_stats(BFmspace *mspace, size_t *num_chunks, size_t *num_used,
    size_t *num_unused, size_t *unused_size, size_t *used_size, bool_t print) {
    *num_chunks = 0;
    *num_unused = 0;
    *num_used = 0;
    *unused_size = 0;
    *used_size = 0;

    // iterate all chunks
    BFmspace_chunk_hdr *hdr = ptr(mspace, mspace->base);
    while (hdr != NULL) {     
        (*num_chunks) += 1;
        if (hdr->used == 0) {
            (*num_unused)++;
            (*unused_size) += hdr->size + mspace->hdr_size;
        } else {
            (*num_used)++;
            (*used_size) += hdr->size + mspace->hdr_size;
        }

        if (print) debug_chunk(mspace, hdr);
        hdr = next(mspace, hdr);
    }
}

void BF_shm_validity_check(BFmspace *mspace) {
    size_t num_chunks, num_used, num_unused, unused_size, used_size;
    BF_shm_get_stats(
        mspace, &num_chunks, &num_used, &num_unused, &unused_size, &used_size, false);
    assert((used_size + unused_size) == mspace->size);
}

void BF_shm_print_stats(BFmspace *mspace) {
    size_t num_chunks, num_used, num_unused, unused_size, used_size;
    fprintf(stderr, "[BF_MALLOC] ============================== \n");
    fprintf(stderr, "[BF_MALLOC] ===== BF_shm_print_stats ===== \n");
    fprintf(stderr, "[BF_MALLOC] ============================== \n");

    fprintf(stderr, "[BF_MALLOC] =========== mspace =========== \n");
    fprintf(stderr, "[BF_MALLOC] mspace->base = %ld (%p)\n",
        mspace->base, ptr(mspace, mspace->base));
    fprintf(stderr, "[BF_MALLOC] mspace->size = %ld\n", mspace->size);
    fprintf(stderr, "[BF_MALLOC] mspace->free_head = %ld(%p)\n",
        mspace->free_head, ptr(mspace, mspace->free_head));
    fprintf(stderr, "[BF_MALLOC] mspace->search_start = %ld(%p)\n",
        mspace->search_start, ptr(mspace, mspace->search_start));

    fprintf(stderr, "[BF_MALLOC] =========== chunks =========== \n");
    BF_shm_get_stats(mspace, &num_chunks, &num_used, &num_unused, &unused_size,
        &used_size, true);
    fprintf(stderr, "[BF_MALLOC] ======= chunks summary ======= \n");    
    fprintf(stderr, "[BF_MALLOC] num_chunks = %ld\n", num_chunks);
    fprintf(stderr, "[BF_MALLOC] num_used = %ld\n", num_used);
    fprintf(stderr, "[BF_MALLOC] num_unused = %ld\n", num_unused);
    fprintf(stderr, "[BF_MALLOC] used_size = %ld\n", used_size);
    fprintf(stderr, "[BF_MALLOC] unused_size = %ld\n", unused_size);
    fprintf(stderr, "[BF_MALLOC] valid = %d\n", (used_size + unused_size) == mspace->size);
}
// #endif

bool_t BF_shm_does_fittable_chunk_exists(
        BFmspace *mspace,
        size_t estimated_size) {
    
    return BF_shm_find_fittable_chunk(mspace, estimated_size) != NULL;
}


BFmspace_chunk_hdr* BF_shm_find_fittable_chunk(
        BFmspace *mspace,
        size_t size) {
    // Fisrt-fit

    // find a chunk in the free list
    // the search_start can be oNULL.
    BFmspace_offset_t starting_point = (mspace->search_start != oNULL) ? \
        mspace->search_start : mspace->free_head;

    if (starting_point == 0) {
        // fprintf(stderr, "[BF_MALLOC] No available memory error!\n");
        return NULL;
    }
    
    BFmspace_chunk_hdr *hdr = ptr(mspace, starting_point);
    BFmspace_chunk_hdr *origin = hdr;
    uint8_t failed = 0;
    while (1) {
        // find chunk
        // IMPORTANT NOTE: If you see a segfault error at the below line,
        //      the memory region may be overwritten or there are no free memory
        //      in the malloc system
        if (hdr->used == 0 && hdr->size >= size) {
		    break;
        }
	
        // the chunk is not good
        // find next free chunk
        hdr = next_free(mspace, hdr);
        if (hdr == NULL) {
            // if hdr == NULL, we reached the end of the free list.
            // we should iterate all chunks in the free circular list,
            //    so go to the head of the free list.
            hdr = ptr(mspace, mspace->free_head);
        }

        // if we meet the chunk of search_start, it means we iterate all chunks,
        //    so we failed to find a chunk.
        if (hdr == origin) {
            failed = 1;
            break;
        }
    }
    if (failed) return NULL;
    
    return hdr;
}

BFmspace_chunk_hdr* BF_shm_find_fittable_chunk_bestfit(
        BFmspace *mspace,
        size_t size) {
    // Best-fit

    // find a chunk in the free list
    // the search_start can be oNULL.
    BFmspace_offset_t starting_point = (mspace->search_start != oNULL) ? \
        mspace->search_start : mspace->free_head;

    if (starting_point == 0) {
        // fprintf(stderr, "[BF_MALLOC] No available memory error!\n");
        return NULL;
    }
    
    BFmspace_chunk_hdr *hdr = ptr(mspace, starting_point);
    BFmspace_chunk_hdr *origin = hdr;
    BFmspace_chunk_hdr *best = NULL;
    uint8_t failed = 0;
    while (1) {
        // find chunk
        // IMPORTANT NOTE: If you see a segfault error at the below line,
        //      the memory region may be overwritten or there are no free memory
        //      in the malloc system
        if (hdr->used == 0 && hdr->size >= size) {
            if (best == oNULL || hdr->size < best->size) {
                best = hdr;
            }
        }
	
        // the chunk is not good
        // find next free chunk
        hdr = next_free(mspace, hdr);
        if (hdr == NULL) {
            // if hdr == NULL, we reached the end of the free list.
            // we should iterate all chunks in the free circular list,
            //    so go to the head of the free list.
            hdr = ptr(mspace, mspace->free_head);
        }

        if (hdr == origin) {
            break;
        }
    }
    if (failed) return NULL;
    
    return best;
}
