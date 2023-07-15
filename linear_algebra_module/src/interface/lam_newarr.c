#include <stdio.h>

#include "bf.h"
#include "utils.h"

#include "lam_interface.h"
#include "lam_internals.h"

#include <sys/time.h>
// #include <math.h>

// array-array opeartion
unsigned long long __lam_newarr_cnt = 0;
unsigned long long __lam_newarr_time = 0;

#define __LAM_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_newarr_time += diff;                                                                       \
    __lam_newarr_cnt++;

#define min(a, b) ((a > b) ? b : a)
#define max(a, b) ((a < b) ? b : a)

void _print_sparse(PFpage *page) {
    int printsize = 4;
    fprintf(stderr, "\n_print_sparse\n");
    fprintf(stderr, "type: %d\n", page->type);

    uint64_t *tilesize = bf_util_get_tile_extents(page);
    fprintf(stderr, "tilesize: %lu x %lu\n", tilesize[0], tilesize[1]);

    double *data = (double*) bf_util_get_pagebuf(page);
    uint64_t data_len = page->pagebuf_len / sizeof(double);
    fprintf(stderr, "data(%lu):[", data_len);
    for (uint64_t i = 0; i < min(printsize, data_len); i++) {
        fprintf(stderr, "%lf,", data[i]);
    }
    fprintf(stderr, "..., ");
    for (uint64_t i = max(data_len - printsize, 0); i < data_len; i++) {
        fprintf(stderr, "%lf,", data[i]);
    }
    fprintf(stderr, "]\n");

    uint64_t *indptr = bf_util_pagebuf_get_coords(page, 0);
    uint64_t *indicies = bf_util_pagebuf_get_coords(page, 1);
    uint64_t *lens = bf_util_pagebuf_get_coords_lens(page);
    uint64_t indptr_len = lens[0] / sizeof(uint64_t);
    uint64_t indices_len = lens[1] / sizeof(uint64_t);
    fprintf(stderr, "indptr(%lu):[", indptr_len);
    for (uint64_t i = 0; i < min(printsize, indptr_len); i++) {
        fprintf(stderr, "%lu,", indptr[i]);
    }
    fprintf(stderr, "..., ");
    for (uint64_t i = max(indptr_len - printsize, 0); i < indptr_len; i++) {
        fprintf(stderr, "%lu,", indptr[i]);
    }
    fprintf(stderr, "]\n");
    fprintf(stderr, "indices(%lu):[", indices_len);
    for (uint64_t i = 0; i < min(printsize, indices_len); i++) {
        fprintf(stderr, "%lu,", indicies[i]);
    }
    fprintf(stderr, "..., ");
    for (uint64_t i = max(indices_len - printsize, 0); i < indices_len; i++) {
        fprintf(stderr, "%lu,", indicies[i]);
    }
    fprintf(stderr, "]\n");
}

void _lam_sparse_newarray(Chunk *chunk, double density, double value, uint64_t *tilesize) {
    // assert(density == 1);

    chunk_getbuf(chunk, BF_EMPTYTILE_SPARSE_CSR);

    uint64_t num_of_elems = tilesize[0] * tilesize[1];
    uint64_t nnz = num_of_elems * density;

    PFpage* page = chunk->curpage;
    BF_ResizeBuf(page, nnz);

    uint64_t *indptr = bf_util_pagebuf_get_coords(page, 0);
    uint64_t *indices = bf_util_pagebuf_get_coords(page, 1);
    double *values = bf_util_get_pagebuf(page);

    bf_util_pagebuf_set_unfilled_idx(page, nnz);
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, nnz * sizeof(double));

    // make candidates
    // TODO: hmm...
    bool *bitmap = BF_shm_malloc(_mspace_data, sizeof(bool) * num_of_elems);
    if (density > 0.5) {
        memset(bitmap, true, sizeof(bool) * num_of_elems);
        for (uint64_t i = 0; i < (num_of_elems - nnz); i++) {
            uint64_t idx = rand() % num_of_elems;
            if (bitmap[idx] == 0) {
                i--;
                continue;
            }
            bitmap[idx] = 0;
        }
    } else {
        for (uint64_t i = 0; i < nnz; i++) {
            uint64_t idx = rand() % num_of_elems;
            if (bitmap[idx] == 1) {
                i--;
                continue;
            }
            bitmap[idx] = 1;
        }
    }

    // fill in page
    uint64_t i = 0;
    for (uint64_t idx = 0; idx < num_of_elems; idx++) {
        if (bitmap[idx] == 0) continue;

        uint64_t row = idx / tilesize[1];
        uint64_t col = idx % tilesize[1];
        
        indptr[row + 1]++;
        indices[i] = col;
        values[i] = value;
        i++;
    }

    // complete indptr
    for (uint64_t idx = 0; idx < chunk->tile_extents[0]; idx++) {
        indptr[idx + 1] += indptr[idx];
    }

    BF_shm_free(_mspace_data, bitmap);
}

void _lam_dense_newarray(Chunk *chunk, double value, uint64_t *tilesize) {
    chunk_getbuf(chunk, BF_EMPTYTILE_DENSE);

    uint64_t num_of_elems = 1;
    for (uint32_t d = 0; d < chunk->dim_len; d++) {
        num_of_elems *= chunk->tile_extents[d];
    }

    PFpage* page = chunk->curpage;

    double *values = bf_util_get_pagebuf(page);

    // fill in page
    for (uint64_t idx = 0; idx < num_of_elems; idx++) {
        uint64_t *coords;
        bf_util_calculate_nd_from_1d_row_major(idx, chunk->tile_extents, chunk->dim_len, &coords);

        // checking if it is out of valid tilesize
        bool_t out = false;
        for (uint32_t d = 0; d < chunk->dim_len; d++) {
            if (coords[d] >= tilesize[d]) {
                out = true;
                break;
            }
        }

        if (out) continue;

        // Fill the values
        values[idx] = value;

        free(coords);
    }
}

Chunk* lam_newarray(Array *out, uint64_t pos) {
    Chunk *chunk;

    uint64_t *coords = NULL;
    uint64_t *coord_lens = NULL;
    uint64_t **dim_domains = out->desc.dim_domains;
    uint32_t dim_len = out->desc.dim_len;

    uint64_t *chunksize = out->desc.tile_extents;

    storage_util_get_dcoord_lens(dim_domains, chunksize, dim_len, &coord_lens);
    bf_util_calculate_nd_from_1d_row_major(pos, coord_lens, dim_len, &coords);

    chunk_get_chunk(
        out->desc.object_name, 
        chunksize, 
        coords, 
        &chunk);

    // out of array which user defined should be filled with zero 
    uint64_t valid_chunksize[] = {chunksize[0], chunksize[1]};
    if (out->is_expanded) {
        for (uint32_t d = 0; d < dim_len; d++) {
            uint64_t remains = (out->desc.real_dim_domains[d][1] - out->desc.real_dim_domains[d][0] + 1) % chunksize[d];
            if (coords[d] + 1 == coord_lens[d] && remains != 0) {
                valid_chunksize[d] = remains;
            }  
        }
    }

    tilestore_format_t format = out->desc.array_type;
    if (format == TILESTORE_DENSE) {
        __LAM_STAT_START();

        _lam_dense_newarray(
            chunk, out->op_param.newarr.default_value, valid_chunksize);
    
        __LAM_STAT_END();

    } else if (format == TILESTORE_SPARSE_CSR) {
        __LAM_STAT_START();
        
        _lam_sparse_newarray(
            chunk, out->op_param.newarr.density, out->op_param.newarr.default_value, valid_chunksize);

        __LAM_STAT_END();
    } else {
        assert(false);
    }

    chunk->dirty = true;

    free(coord_lens);
    free(coords);

    return chunk;
}