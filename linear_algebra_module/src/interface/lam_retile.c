#include <stdio.h>
#include <string.h>
#include <math.h>
// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"

#include "lam_interface.h"

#include <sys/time.h>

unsigned long long __lam_retile_cnt = 0;
unsigned long long __lam_retile_time = 0;

#define __LAM_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_retile_time += diff;                                                                          \
    __lam_retile_cnt++;

void _lam_chunk_retile_int(
    ChunkIterator *input_chunk_iter, Chunk *output_chunk)
{
    int *outbuf = (int*) bf_util_get_pagebuf(output_chunk->curpage);

    for (uint64_t i = 0; i < bf_util_pagebuf_get_len(output_chunk->curpage) / sizeof(int); i++)
    {
        // get value from iterator
        int data = chunk_iterator_get_cell_int(input_chunk_iter);
        
        // write value to output buffer
        outbuf[i] = data;

        // go to the next cell
        input_chunk_iter = chunk_iterator_get_next(input_chunk_iter);
    }
}

void _lam_chunk_retile_float64(
    ChunkIterator *input_chunk_iter, Chunk *output_chunk)
{
    double *outbuf = (double*) bf_util_get_pagebuf(output_chunk->curpage);

    for (uint64_t i = 0; i < bf_util_pagebuf_get_len(output_chunk->curpage) / sizeof(double); i++)
    {
        // get value from iterator
        double data = chunk_iterator_get_cell_double(input_chunk_iter);
        
        // write value to output buffer
        outbuf[i] = data;

        // go to the next cell
        input_chunk_iter = chunk_iterator_get_next(input_chunk_iter);
    }
}

// Find the idx of the value in vector[start:end] (end is inclusive).
// If not found, 
//      1) if is_start is true, return the minimum idx of a value that greater than the input value.
//      2) if false, return the maximum idx of a value that smaller than the input value.
int64_t _get_idx_with_binary_search(
        uint64_t *vector, 
        uint64_t start, 
        uint64_t end,
        uint64_t value,
        bool is_start) {

    int64_t low = (int64_t) start;
    int64_t high = (int64_t) end;

    while (low <= high) {
        int64_t mid = (high + low) / 2;
        
        if (vector[mid] < value) low = mid + 1;
        else if (vector[mid] > value) high = mid - 1; 
        else /* (vector[mid] == value) */ return mid;
    }
    
    // when failed to search
    if (is_start) return low;
    else return high;
}

void _lam_chunk_retile_sparse_float64(Chunk *input_chunk, Chunk *output_chunk) {
    // fprintf(stderr, "retile started\n");

    PFpage *otile = output_chunk->curpage;
    BF_ResizeBuf(otile, output_chunk->tile_extents[0] * output_chunk->tile_extents[1]);

    double *otile_pagebuf = bf_util_get_pagebuf(otile);
    uint64_t *otile_indptr = bf_util_pagebuf_get_coords(otile, 0);
    uint64_t *otile_indices = bf_util_pagebuf_get_coords(otile, 1);
    uint64_t otile_nnz = 0; 
    otile_indptr[0] = otile_indptr[1] = 0;

    uint64_t array_size[2] = {
        input_chunk->array_domains[0][1] - input_chunk->array_domains[0][0] + 1,
        input_chunk->array_domains[1][1] - input_chunk->array_domains[1][0] + 1
    };

    uint64_t itile_target_box[2][2] = {
        {
            output_chunk->chunk_domains[0][0] / input_chunk->tile_extents[0],
            output_chunk->chunk_domains[1][0] / input_chunk->tile_extents[1]
        },
        {
            output_chunk->chunk_domains[0][1] / input_chunk->tile_extents[0],
            output_chunk->chunk_domains[1][1] / input_chunk->tile_extents[1]
        }
    };

    uint64_t row_idx_start = output_chunk->chunk_domains[0][0];
    uint64_t row_idx_end = output_chunk->chunk_domains[0][1];   // inclusive

    // fprintf(stderr, "%lu, %lu / %lu, %lu, %lu, %lu / %lu, %lu\n",
    //     array_size[0], array_size[1], 
    //     itile_target_box[0][0], itile_target_box[0][1], itile_target_box[1][0], itile_target_box[1][1],
    //     row_idx_start, row_idx_end);

    for (uint64_t row_idx = row_idx_start; row_idx <= row_idx_end; row_idx++) {
        uint64_t itile_row_idx = row_idx / input_chunk->tile_extents[0];
     
        uint64_t itile_cell_row_idx = row_idx % input_chunk->tile_extents[0];
        uint64_t otile_cell_row_idx = row_idx % output_chunk->tile_extents[0];
        for (uint64_t itile_col_idx = itile_target_box[0][1]; itile_col_idx <= itile_target_box[1][1]; itile_col_idx++) {
            PFpage *itile;
            array_key key;
            uint64_t itile_coords[2] = {itile_row_idx, itile_col_idx};

            key.arrayname = input_chunk->array_name;
            key.attrname = input_chunk->attr_name;
            key.dim_len = input_chunk->dim_len;
            key.dcoords = itile_coords;

            if (BF_GetBuf(key, &itile) != BFE_OK) {
                fprintf(stderr, "[LAM] getbuf error when sparse retile!!\n");
            }

            double *itile_pagebuf = bf_util_get_pagebuf(itile);
            uint64_t *itile_indptr = bf_util_pagebuf_get_coords(itile, 0);
            uint64_t *itile_indices = bf_util_pagebuf_get_coords(itile, 1);

            uint64_t search_idx_start = itile_indptr[itile_cell_row_idx];
            uint64_t search_idx_end = itile_indptr[itile_cell_row_idx + 1] - 1;     // inclusive

            uint64_t itile_domains[2] = {
                itile_coords[1] * input_chunk->tile_extents[1],
                ((itile_coords[1] + 1) * input_chunk->tile_extents[1]) - 1
            };

            uint64_t target_value_start, target_value_end;
            if (itile_domains[0] <= output_chunk->chunk_domains[1][0]
                   && output_chunk->chunk_domains[1][0] <= itile_domains[1]) {
                target_value_start = output_chunk->chunk_domains[1][0] % input_chunk->tile_extents[1];
            } else {
                target_value_start = 0;
            }

            if (itile_domains[0] <= output_chunk->chunk_domains[1][1]
                   && output_chunk->chunk_domains[1][1] <= itile_domains[1]) {
                target_value_end = output_chunk->chunk_domains[1][1] % input_chunk->tile_extents[1];
            } else {
                target_value_end = input_chunk->tile_extents[1] - 1;
            }

            int64_t itile_start_idx = _get_idx_with_binary_search(
                itile_indices, search_idx_start, search_idx_end, target_value_start, true);
            int64_t itile_end_idx = _get_idx_with_binary_search(
                itile_indices, search_idx_start, search_idx_end, target_value_end, false);

            // fprintf(stderr, "\t{%lu, %lu} / %lu, %lu / %lu, %lu\n",
            //     itile_coords[0], itile_coords[1], 
            //     search_idx_start, search_idx_end,
            //     itile_start_idx, itile_end_idx);

            for (int64_t itile_idx = itile_start_idx; itile_idx <= itile_end_idx; itile_idx++) {
                // calculate a value for indices
                uint64_t col_idx = input_chunk->tile_extents[1] * itile_coords[1] + itile_indices[itile_idx];
                uint64_t otile_col_idx = col_idx % output_chunk->tile_extents[1];

                otile_pagebuf[otile_nnz] = itile_pagebuf[itile_idx];
                otile_indices[otile_nnz] = otile_col_idx;
                otile_indptr[otile_cell_row_idx + 1]++;
                otile_nnz++;
            }

            BF_UnpinBuf(key);
        }

        // fprintf(stderr, "otile: %lu - %lu\n", otile_indptr[otile_cell_row_idx], otile_indptr[otile_cell_row_idx + 1]);

        if (otile_cell_row_idx < (output_chunk->tile_extents[0] - 1)) {
            otile_indptr[otile_cell_row_idx + 2] = otile_indptr[otile_cell_row_idx + 1];
            // fprintf(stderr, "push!! %lu %lu\n", otile_cell_row_idx, (output_chunk->tile_extents[0] - 1));
        }
    }

    bf_util_pagebuf_set_unfilled_idx(otile, otile_nnz);
    bf_util_pagebuf_set_unfilled_pagebuf_offset(otile, otile_nnz * sizeof(double));

    BF_ResizeBuf(otile, otile_nnz);
    return;
}


Chunk* _lam_chunk_retile_sparse(
        Chunk *input_chunk, 
        Chunk *output_chunk, 
        tilestore_datatype_t attrtype)
{
    // retile
    __LAM_STAT_START();

    if (attrtype == TILESTORE_FLOAT64)
        _lam_chunk_retile_sparse_float64(input_chunk, output_chunk);
    else
        assert(false);

    __LAM_STAT_END();

    // fprintf(stderr, "diff=%lu\n", diff);

    output_chunk->dirty = 1;
}

Chunk* _lam_chunk_retile(
        Chunk *input_chunk, 
        Chunk *output_chunk, 
        tilestore_datatype_t attrtype)
{
    // get output iterator
    ChunkIterator *input_iterator = chunk_iterator_init(input_chunk);

    // retile
    __LAM_STAT_START();

    if (attrtype == TILESTORE_INT32)
        _lam_chunk_retile_int(input_iterator, output_chunk);
    else if (attrtype == TILESTORE_FLOAT64)
        _lam_chunk_retile_float64(input_iterator, output_chunk);
    else
        assert(false);

    __LAM_STAT_END();

    output_chunk->dirty = 1;

    // free
    chunk_iterator_free(input_iterator);
}

void chunk_getbuf_origin_tile(Chunk* chunk) {
    // explicit getting buffer
    if (chunk->curpage != NULL) return;

    uint64_t coords[2] = {0, 0};

    array_key key;
    key.arrayname = chunk->array_name;
    key.attrname = chunk->attr_name;
    key.dcoords = coords;
    key.dim_len = chunk->dim_len;
    key.emptytile_template = BF_EMPTYTILE_NONE;
    BF_GetBuf(key, &chunk->curpage);

    memcpy(chunk->tile_coords, coords, chunk->dim_len * sizeof(uint64_t));
    
    for (int d = 0; d < chunk->dim_len; d++)
    {
        chunk->tile_domains[d][0] = chunk->array_domains[d][0] + chunk->tile_coords[d] * chunk->tile_extents[d];
        chunk->tile_domains[d][1] = chunk->tile_domains[d][0] + chunk->tile_extents[d] - 1;
        if (chunk->tile_domains[d][1] > chunk->array_domains[d][1]) // Marginal tile handling
            chunk->tile_domains[d][1] = chunk->array_domains[d][1];
    }
}

Chunk *lam_chunk_retile(
        Chunk *input_chunk,         // should get get_chunk()-ed chunk.
        tilestore_datatype_t attrtype,
        Array *output_array,
        uint64_t output_pos,
        uint64_t *output_chunksize)
{

    /************************************************
    /* prepare variables
    ************************************************/
    uint64_t *output_chunk_coords = NULL;
    uint64_t *output_chunk_coord_lens = NULL;
    uint32_t *dim_order = output_array->op_param.trans.transpose_func;
    uint64_t **dim_domains = output_array->desc.dim_domains;
    uint32_t dim_len = output_array->desc.dim_len;

    /************************************************
    /* get output chunk
    ************************************************/
    // abuse get_dcoord_lens to get output_chunk_coords
    storage_util_get_dcoord_lens(dim_domains, output_chunksize, dim_len, &output_chunk_coord_lens);

    // calculate output_chunk_coords (it seems that the order is ROW_MAJOR)
    bf_util_calculate_nd_from_1d_row_major(output_pos, output_chunk_coord_lens, dim_len, &output_chunk_coords);

    // get output chunk
    Chunk *output_chunk;
    chunk_get_chunk(output_array->desc.object_name, output_chunksize, output_chunk_coords, &output_chunk);
    
    /************************************************
    /* Check
    ************************************************/
    // FIXME: We assume that all tiles are dense so that only check the first tile currently. 
    // Fix it at the moment that we consider sparse tiles and mixed tiles. 
    chunk_getbuf_origin_tile(input_chunk);
    
    bool is_csr = (input_chunk->curpage->type == SPARSE_FIXED);

    /************************************************
     * assert
     ***********************************************/
    // if both chunk_domains are same?
    for (uint32_t d = 0; d < dim_len; d++)
    {
        assert(input_chunk->chunk_domains[d][0] == output_chunk->chunk_domains[d][0]);
        assert(input_chunk->chunk_domains[d][1] == output_chunk->chunk_domains[d][1]);
    }

    // run retile
    if (is_csr) {
        chunk_getbuf(output_chunk, BF_EMPTYTILE_SPARSE_CSR);

        _lam_chunk_retile_sparse(input_chunk, output_chunk, attrtype);
    } else {
        chunk_getbuf(output_chunk, BF_EMPTYTILE_DENSE);

        _lam_chunk_retile(input_chunk, output_chunk, attrtype);
    }

    free(output_chunk_coords); 
    free(output_chunk_coord_lens);

    return output_chunk;
}
