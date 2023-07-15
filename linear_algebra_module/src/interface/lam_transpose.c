#include <stdio.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "utils.h"

#include "lam_interface.h"
#include "lam_internals.h"

#include <sys/time.h>

unsigned long long __lam_trans_cnt = 0;
unsigned long long __lam_trans_time = 0;

#define __LAM_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_trans_time += diff;                                                                          \
    __lam_trans_cnt++;


void _lam_chunk_transpose_opt_copy_values_int(
    Chunk *input_chunk, Chunk *output_chunk, uint32_t dim_len,
    uint64_t *out_current_coords, uint64_t *out_stride, uint64_t *output_chunksize)
{
    for (uint64_t i = 0; i < bf_util_pagebuf_get_len(input_chunk->curpage) / sizeof(int); i++)
    {
        // calculate out_i
        uint64_t out_i = 0;
        for (uint32_t d = 0; d < dim_len; d++)
        {
            out_i += out_stride[d] * out_current_coords[d];
        }

        // write to output_iterator
        // read from input_iterator
        int data = bf_util_pagebuf_get_int(input_chunk->curpage, out_i);
        bf_util_pagebuf_set_int(output_chunk->curpage, i, data);

        // update out_current_coords
        // out_current_coords[dim_len - 1]++;
        for (int64_t d = dim_len - 1; d >= 0; d--)
        {
            out_current_coords[d]++;

            bool_t overflow = out_current_coords[d] == output_chunksize[d];
            if (!overflow)
                break;

            // overflow
            out_current_coords[d] = 0;
        }
    }
}

void _lam_chunk_transpose_opt_copy_values_float64(
    Chunk *input_chunk, Chunk *output_chunk, uint32_t dim_len,
    uint64_t *out_current_coords, uint64_t *out_stride, uint64_t *output_chunksize)
{
    double *in_pagebuf = bf_util_get_pagebuf(input_chunk->curpage);
    double *out_pagebuf = bf_util_get_pagebuf(output_chunk->curpage);

    for (uint64_t i = 0; i < bf_util_pagebuf_get_len(input_chunk->curpage) / sizeof(double); i++)
    {
        // calculate out_i
        uint64_t out_i = 0;
        for (uint32_t d = 0; d < dim_len; d++)
        {
            out_i += out_stride[d] * out_current_coords[d];
        }

        // write to output_iterator
        // read from input_iterator
        out_pagebuf[i] = in_pagebuf[out_i];

        // update out_current_coords
        // out_current_coords[dim_len - 1]++;
        for (int64_t d = dim_len - 1; d >= 0; d--)
        {
            out_current_coords[d]++;

            bool_t overflow = out_current_coords[d] == output_chunksize[d];
            if (!overflow)
                break;

            // overflow
            out_current_coords[d] = 0;
        }
    }
}

void _lam_chunk_transpose_opt(
    Chunk *input_chunk, Chunk *output_chunk, uint32_t dim_len,
    uint32_t *dim_order, tilestore_datatype_t attrtype,
    uint64_t *output_chunk_coords, uint64_t *output_chunksize)
{
    // fprintf(stderr, "optimizable! - new!!\n");
    uint64_t *tile_coords = malloc(sizeof(uint64_t) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++)
    {
        uint64_t chunk_side = input_chunk->chunk_domains[d][1] - input_chunk->chunk_domains[d][0] + 1;
        if (input_chunk->chunk_domains[d][0] == 0)
        {
            tile_coords[d] = 0;
        }
        else
        {
            tile_coords[d] = input_chunk->chunk_domains[d][0] / chunk_side;
        }
    }

    array_key in_key, out_key;

    if (input_chunk->curpage == NULL)
    {
        in_key.arrayname = input_chunk->array_name;
        in_key.attrname = input_chunk->attr_name; // Assume single attribute
        in_key.dcoords = tile_coords;
        in_key.dim_len = dim_len;
        BF_GetBuf(in_key, &input_chunk->curpage);
        input_chunk->dirty = 0; // Initialize the dirty flag
    }

    input_chunk->tile_coords = tile_coords;

    if (output_chunk->curpage == NULL)
    {
        out_key.arrayname = output_chunk->array_name;
        out_key.attrname = output_chunk->attr_name; // Assume single attribute
        out_key.dcoords = output_chunk_coords;
        out_key.dim_len = dim_len;
        BF_GetBuf(out_key, &output_chunk->curpage);
    }

    output_chunk->dirty = 1; // Initialize the dirty flag
    output_chunk->tile_coords = output_chunk_coords;

    uint64_t *out_current_coords = malloc(sizeof(uint64_t) * dim_len);
    uint64_t *in_stride = malloc(sizeof(uint64_t) * dim_len);
    uint64_t *out_stride = malloc(sizeof(uint64_t) * dim_len);

    // only ROW_MAJOR supported now
    assert(output_chunk->cell_order == TILESTORE_ROW_MAJOR);
    assert(output_chunk->cell_order == TILESTORE_ROW_MAJOR);

    uint64_t base = 1;
    for (int64_t d = dim_len - 1; d >= 0; d--)
    {
        out_current_coords[d] = 0;
        in_stride[d] = base;

        uint64_t in_chunk_side = input_chunk->chunk_domains[d][1] - input_chunk->chunk_domains[d][0] + 1;
        base *= in_chunk_side;
    }

    for (uint32_t d = 0; d < dim_len; d++)
    {
        out_stride[d] = in_stride[dim_order[d]];
    }

    __LAM_STAT_START();

    // transpose
    if (attrtype == TILESTORE_INT32)
        _lam_chunk_transpose_opt_copy_values_int(input_chunk, output_chunk, dim_len, out_current_coords, out_stride, output_chunksize);
    else if (attrtype == TILESTORE_FLOAT64)
        _lam_chunk_transpose_opt_copy_values_float64(input_chunk, output_chunk, dim_len, out_current_coords, out_stride, output_chunksize);
    else
        assert(false);

    __LAM_STAT_END();

    BF_TouchBuf(out_key);

    // free
    BF_UnpinBuf(in_key);
    BF_UnpinBuf(out_key);
}

int _lam_chunk_transpose_iterator_copy_values_double(ChunkIterator *input, ChunkIterator *output)
{
    while (1)
    {
        // read from input_iterator
        double data = chunk_iterator_get_cell_double(input);

        // write to output_iterator
        chunk_iterator_write_cell_double(output, data);

        // fprintf(stderr, "\t input=%lu,output=%lu,%d\n", input->tile_idx, output->tile_idx, data);

        // break condition
        if (chunk_iterator_has_next(input) == 0)
            break;

        // move to the next element
        input = chunk_iterator_get_next(input);
        output = chunk_iterator_get_next(output);
    }

    return 0;
}

int _lam_chunk_transpose_iterator_copy_values_int(ChunkIterator *input, ChunkIterator *output)
{
    while (1)
    {
        // read from input_iterator
        int data = chunk_iterator_get_cell_int(input);

        // write to output_iterator
        chunk_iterator_write_cell_int(output, data);

        // fprintf(stderr, "\t input=%lu,output=%lu,%d\n", input->tile_idx, output->tile_idx, data);

        // break condition
        if (chunk_iterator_has_next(input) == 0)
            break;

        // move to the next element
        input = chunk_iterator_get_next(input);
        output = chunk_iterator_get_next(output);
    }

    return 0;
}

void _lam_chunk_transpose_iterator(
    Chunk *input_chunk, Chunk *output_chunk, uint32_t dim_len,
    uint32_t *dim_order, tilestore_datatype_t attrtype)
{
    // fprintf(stderr, "not optimizable!\n");
    // iterator version
    // get iterator
    ChunkIterator *input_iterator = chunk_iterator_init(input_chunk);
    ChunkIterator *output_iterator = chunk_custom_order_iterator_init(output_chunk, dim_order);

    // test size
    assert(chunk_iterator_size(input_iterator) == chunk_iterator_size(output_iterator));

    // transpose
    __LAM_STAT_START();
    if (attrtype == TILESTORE_INT32)
        _lam_chunk_transpose_iterator_copy_values_int(input_iterator, output_iterator);
    else if (attrtype == TILESTORE_FLOAT64)
        _lam_chunk_transpose_iterator_copy_values_double(input_iterator, output_iterator);
    else
        assert(false); // TODO: Not yet!

    __LAM_STAT_END();

    // free
    chunk_iterator_free(input_iterator);
    chunk_iterator_free(output_iterator);
}

void _lam_chunk_transpose_halfopt_copy_values_int(
    Chunk *input_chunk, ChunkIterator *output_chunk_iter)
{
    for (uint64_t i = 0; i < bf_util_pagebuf_get_len(input_chunk->curpage) / sizeof(int); i++)
    {
        // read from input_iterator
        int data = bf_util_pagebuf_get_int(input_chunk->curpage, i);

        // write to output_iterator
        chunk_iterator_write_cell_int(output_chunk_iter, data);

        // fprintf(stderr, "\t input=%lu,output=%lu,%d\n", input->tile_idx, output->tile_idx, data);
        output_chunk_iter = chunk_iterator_get_next(output_chunk_iter);
    }
}

void _lam_chunk_transpose_halfopt_copy_values_float64(
    Chunk *input_chunk, ChunkIterator *output_chunk_iter)
{
    double *in_pagebuf = bf_util_get_pagebuf(input_chunk->curpage);

    for (uint64_t i = 0; i < bf_util_pagebuf_get_len(input_chunk->curpage) / sizeof(double); i++)
    {
        // read from input_iterator
        double data = in_pagebuf[i];

        // write to output_iterator
        chunk_iterator_write_cell_double(output_chunk_iter, data);

        // fprintf(stderr, "\t input=%lu,output=%lu,%d\n", input->tile_idx, output->tile_idx, data);
        output_chunk_iter = chunk_iterator_get_next(output_chunk_iter);
    }
}

void _lam_chunk_transpose_csr_int(
    Chunk *input_chunk, Chunk *output_chunk)
{
    int32_t *in_data = (int32_t *)bf_util_get_pagebuf(input_chunk->curpage);
    uint64_t *in_indptr = bf_util_pagebuf_get_coords(input_chunk->curpage, 0);
    uint64_t *in_indices = bf_util_pagebuf_get_coords(input_chunk->curpage, 1);

    int32_t *out_data = (int32_t *)bf_util_get_pagebuf(output_chunk->curpage);
    uint64_t *out_indptr = bf_util_pagebuf_get_coords(output_chunk->curpage, 0);
    uint64_t *out_indices = bf_util_pagebuf_get_coords(output_chunk->curpage, 1);

    uint64_t nnz = bf_util_pagebuf_get_unfilled_idx(input_chunk->curpage);
    uint64_t in_col_size = (bf_util_pagebuf_get_coords_lens(input_chunk->curpage)[0] / sizeof(uint64_t)) - 1;
    uint64_t out_row_size = bf_util_pagebuf_get_coords_lens(output_chunk->curpage)[0] / sizeof(uint64_t) - 1;

    // phase 1. construct out_indptr
    // record # of cells in each row
    for (uint64_t i = 0; i < nnz; i++)
    {
        uint64_t row_idx = in_indices[i];
        out_indptr[row_idx + 1]++;
    }

    // convert # of cells to pointer
    for (uint64_t i = 1; i < out_row_size; i++)
    {
        out_indptr[i] += out_indptr[i - 1];
    }

    // phase 2. copy values
    for (uint64_t col_idx = 0; col_idx < in_col_size; col_idx++)
    {
        for (uint64_t i = in_indptr[col_idx]; i < in_indptr[col_idx + 1]; i++)
        {
            int32_t value = in_data[i];
            uint64_t row_idx = in_indices[i];

            uint64_t o = out_indptr[row_idx];
            out_indices[o] = col_idx;
            out_data[o] = value;

            out_indptr[row_idx]++;
        }
    }

    // phase 3. restore out_indptr
    for (uint64_t i = 0, last = 0; i <= out_row_size; i++)
    {
        uint64_t temp = out_indptr[i];
        out_indptr[i] = last;
        last = temp;
    }
}

void _lam_chunk_transpose_csr_float64(
    Chunk *input_chunk, Chunk *output_chunk)
{
    double *in_data = (double *)bf_util_get_pagebuf(input_chunk->curpage);
    uint64_t *in_indptr = bf_util_pagebuf_get_coords(input_chunk->curpage, 0);
    uint64_t *in_indices = bf_util_pagebuf_get_coords(input_chunk->curpage, 1);

    double *out_data = (double *)bf_util_get_pagebuf(output_chunk->curpage);
    uint64_t *out_indptr = bf_util_pagebuf_get_coords(output_chunk->curpage, 0);
    uint64_t *out_indices = bf_util_pagebuf_get_coords(output_chunk->curpage, 1);

    uint64_t nnz = bf_util_pagebuf_get_unfilled_idx(input_chunk->curpage);
    uint64_t in_col_size = (bf_util_pagebuf_get_coords_lens(input_chunk->curpage)[0] / sizeof(uint64_t)) - 1;
    uint64_t out_row_size = bf_util_pagebuf_get_coords_lens(output_chunk->curpage)[0] / sizeof(uint64_t) - 1;
    // uint64_t out_row_size = output_chunk->tile_extents[1];

    // phase 1. construct out_indptr
    // record # of cells in each row
    for (uint64_t i = 0; i < nnz; i++)
    {
        uint64_t row_idx = in_indices[i];
        out_indptr[row_idx + 1]++;
    }

    // convert # of cells to pointer
    for (uint64_t i = 1; i < out_row_size; i++)
    {
        out_indptr[i] += out_indptr[i - 1];
    }

    // phase 2. copy values
    for (uint64_t col_idx = 0; col_idx < in_col_size; col_idx++)
    {
        for (uint64_t i = in_indptr[col_idx]; i < in_indptr[col_idx + 1]; i++)
        {
            double value = in_data[i];
            uint64_t row_idx = in_indices[i];

            uint64_t o = out_indptr[row_idx];
            out_indices[o] = col_idx;
            out_data[o] = value;

            out_indptr[row_idx]++;
        }
    }

    // phase 3. restore out_indptr
    for (uint64_t i = 0, last = 0; i <= out_row_size; i++)
    {
        uint64_t temp = out_indptr[i];
        out_indptr[i] = last;
        last = temp;
    }
}

void _lam_chunk_transpose_csr(
    Chunk *input_chunk, Chunk *output_chunk, tilestore_datatype_t attrtype)
{
    // We can see the input chunk as CSC of the transposed chunk.
    // Thus, a transpose algorithm equals CSC-to-CSR conversion.
    // The CSC-to-CSR conversion algorithm below is almost identical to the CSR-to-CSC conversion of SciPy
    //      (https://github.com/scipy/scipy/blob/main/scipy/sparse/sparsetools/csr.h).

    // enlarge page size
    uint64_t nnz = bf_util_pagebuf_get_unfilled_idx(input_chunk->curpage);

    // make room
    BF_ResizeBuf(output_chunk->curpage, nnz);

    bf_util_pagebuf_set_unfilled_idx(output_chunk->curpage, nnz);
    bf_util_pagebuf_set_unfilled_pagebuf_offset(output_chunk->curpage, nnz * tilestore_datatype_size(attrtype));

    memset(
        bf_util_pagebuf_get_coords(output_chunk->curpage, 0), 
        0, 
        bf_util_pagebuf_get_coords_lens(output_chunk->curpage)[0]);

    // transpose
    __LAM_STAT_START();

    if (attrtype == TILESTORE_INT32)
    {
        _lam_chunk_transpose_csr_int(input_chunk, output_chunk);
    }
    else if (attrtype == TILESTORE_FLOAT64)
    {
        _lam_chunk_transpose_csr_float64(input_chunk, output_chunk);
    }
    else
        assert(false);

    __LAM_STAT_END();

    output_chunk->dirty = 1;
}

void _lam_chunk_transpose_halfopt(
    Chunk *input_chunk, Chunk *output_chunk, uint32_t dim_len,
    uint32_t *dim_order, tilestore_datatype_t attrtype)
{
    // get output iterator
    ChunkIterator *output_iterator = chunk_custom_order_iterator_init(output_chunk, dim_order);

    // transpose
    __LAM_STAT_START();

    if (attrtype == TILESTORE_INT32)
        _lam_chunk_transpose_halfopt_copy_values_int(input_chunk, output_iterator);
    else if (attrtype == TILESTORE_FLOAT64)
        _lam_chunk_transpose_halfopt_copy_values_float64(input_chunk, output_iterator);
    else
        assert(false);

    __LAM_STAT_END();

    // free
    chunk_iterator_free(output_iterator);
}

Chunk *lam_chunk_transpose(Chunk *input_chunk,
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
     * assert
     ***********************************************/
    // if tile_extents == chunk_size ?
    for (uint32_t d = 0; d < dim_len; d++)
    {
        uint64_t input_chunk_size = input_chunk->chunk_domains[d][1] - input_chunk->chunk_domains[d][0] + 1;
        uint64_t output_chunk_size = output_chunk->chunk_domains[d][1] - output_chunk->chunk_domains[d][0] + 1;

        assert(input_chunk->tile_extents[d] == input_chunk_size);
        assert(output_chunk->tile_extents[d] == output_chunk_size);
    }

    /************************************************
    /* getbuf for input and output chunks
    ************************************************/
    PFpage *input_original = NULL;
    chunk_getbuf(input_chunk, BF_EMPTYTILE_NONE);

    bool is_output_csr = (input_chunk->curpage->type == SPARSE_FIXED);

    /************************************************
    /* transpose
    ************************************************/

    assert(input_chunk->curpage != NULL);       // TODO: Not implemented yet
    
    if (is_output_csr) {
        // sparse
        chunk_getbuf(output_chunk, BF_EMPTYTILE_SPARSE_CSR);

        _lam_chunk_transpose_csr(input_chunk, output_chunk, attrtype);

        fprintf(stderr, "[LAM] trans - sparse op\n");
    } else {
        // dense
        chunk_getbuf(output_chunk, BF_EMPTYTILE_DENSE);

        _lam_chunk_transpose_halfopt(input_chunk, output_chunk, dim_len, dim_order, attrtype);

        fprintf(stderr, "[LAM] trans - dense op\n");
    }

    // free
    free(output_chunk_coord_lens);

    output_chunk->dirty = 1;

    return output_chunk;
}
