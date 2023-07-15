#ifndef __BF_MALLOC_H__
#define __BF_MALLOC_H__

#include <stddef.h>
#include "bf_type.h"

#define BF_MALLOC_DEBUG     0

#define MEMORY_ALIGN    sizeof(void*) * 8
#define BF_SHM_ALIGNED_MEMORY(offset) ((void*) (((u_int64_t) offset + (MEMORY_ALIGN - 1)) & -MEMORY_ALIGN))
#define BF_SHM_ALIGNED_SIZEOF(size) ((size + (MEMORY_ALIGN - 1)) & -MEMORY_ALIGN)

#define oNULL               0
typedef ptrdiff_t BFmspace_offset_t;

typedef struct BFmspace {
    BFmspace_offset_t   base;       // (relative) base address of the first chunk
    // BFmspace_offset_t   current;    // (relative) current brk
    BFmspace_offset_t   free_head;     // (relative) head of the free list
    BFmspace_offset_t   search_start;  // (relative) the starting point of search
    size_t              size;       // allowed size of memory chunks

    uint8_t             unique;     // uniquecheck

    // pre-computed attributes
    size_t              mspace_meta_size;   // size of mspace meta size
    size_t              hdr_size;           // size of hdr size 
} BFmspace;

typedef struct BFmspace_chunk_hdr {
    unsigned short      unique;     // to check invalid free()
    size_t              size;       // data size
    BFmspace_offset_t   next;       // next chunk offset
    BFmspace_offset_t   prev;       // previous chunk offset
    BFmspace_offset_t   next_free;  // next free chunk offset
    BFmspace_offset_t   prev_free;  // previous free chunk offset
    int                 used;       // is this chunk used?
} BFmspace_chunk_hdr;

BFmspace* BF_shm_create_mspace(void *base, size_t size);
void* BF_shm_malloc(BFmspace *mspace, size_t size);
void* BF_shm_realloc(BFmspace *mspace, void* ptr, size_t size);
void BF_shm_free(BFmspace *mspace, void* ptr);

void BF_shm_print_stats(BFmspace *mspace);
void BF_shm_validity_check(BFmspace *mspace);

bool_t BF_shm_does_fittable_chunk_exists(BFmspace *_mspace, \
    size_t estimated_size);
BFmspace_chunk_hdr* BF_shm_find_fittable_chunk(BFmspace *mspace, size_t size);
BFmspace_chunk_hdr* BF_shm_find_fittable_chunk_bestfit(BFmspace *mspace, size_t size);

#if BF_MALLOC_DEBUG
void BF_shm_get_stats(BFmspace *mspace, size_t *num_chunks, size_t *num_used,
    size_t *num_unused, size_t *unused_size, size_t *used_size);
void BF_shm_print_stats(BFmspace* mspace);
#endif

#endif
