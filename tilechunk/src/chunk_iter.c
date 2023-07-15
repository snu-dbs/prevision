#include <stdio.h>
#include <string.h>
#include <math.h>

#include "bf.h"
#include "bf_struct.h"
#include "utils.h"
#include "arraykey.h"
#include "chunk_iter.h"

/**************************************************
 *       DENSE Chunk Iterator                     *
 **************************************************/

void _check_list_init(
    Chunk *chunk,
    uint64_t *chunk_size,
    int d,
    uint64_t num_of_cell_in_lower_dim,
    // returns
    uint64_t **_check_list,
    uint64_t *_check_list_len)
{

    // the maximum size of check_list can be floor(chunk_size / tile_size) + 2.
    uint64_t *check_list =
        malloc(sizeof(uint64_t) *
               ((chunk_size[d] / chunk->tile_extents[d]) + 2));
    uint64_t check_list_length = 0;

    // start coordinates of the chunk and the tile can be different.
    // remaining chunk length to count check_list
    uint64_t remains = chunk_size[d];

    // the first element should be zero
    check_list[0] = 0;
    check_list_length++;

    // if the first tile is cut and smaller than chunk,
    //      we should calculate the tile first.
    uint64_t chunk_relative_start_coord =
        chunk->chunk_domains[d][0] - chunk->array_domains[d][0];
    uint64_t chunk_offset_from_tile =
        chunk_relative_start_coord % chunk->tile_extents[d];
    // negative offset of starting coordinate of tile from chunk
    uint64_t first_tile_start_coord =
        chunk_relative_start_coord - chunk_offset_from_tile;
    uint8_t is_cut = (chunk_offset_from_tile != 0);
    uint8_t is_small =
        (first_tile_start_coord + chunk->tile_extents[d] <
         chunk_relative_start_coord + chunk_size[d]);
    if (is_cut && is_small)
    {
        uint64_t available_tile_length =
            chunk->tile_extents[d] - chunk_offset_from_tile;

        // fill the check list
        check_list[1] = available_tile_length * num_of_cell_in_lower_dim;
        check_list_length++;

        // calculate remains
        remains -= available_tile_length;
    }

    // fill the check_list
    while (remains > chunk->tile_extents[d])
    {
        // fill the check list
        uint64_t start_idx = check_list[check_list_length - 1];
        uint64_t distance_to_next_idx_in_d =
            chunk->tile_extents[d] * num_of_cell_in_lower_dim;

        check_list[check_list_length] = start_idx + distance_to_next_idx_in_d;
        check_list_length++;

        // calculate remains
        remains -= chunk->tile_extents[d];
    }

    (*_check_list) = check_list;
    (*_check_list_len) = check_list_length;
}

uint8_t _is_tile_fitted_into_chunk_for_dimension(Chunk *chunk, uint32_t d)
{
    // chunk_domains - array_domains is because of that tile starts from array_domain
    if (((chunk->chunk_domains[d][0] - chunk->array_domains[d][0]) % chunk->tile_extents[d]) == 0 && // starting coord are aligned?
        ((chunk->chunk_domains[d][1] - chunk->chunk_domains[d][0] + 1) == chunk->tile_extents[d]))
    { // tile side == chunk side?
        return 1;
    }
    return 0;
}

void _calculate_chunk_size(
    Chunk *chunk,
    uint64_t **chunk_size,
    uint64_t *num_of_cells_in_chunk)
{

    (*chunk_size) = malloc(sizeof(uint64_t) * chunk->dim_len);
    (*num_of_cells_in_chunk) = 1; // length can be zero?
    for (int d = 0; d < chunk->dim_len; d++)
    {
        (*chunk_size)[d] =
            chunk->chunk_domains[d][1] - chunk->chunk_domains[d][0] + 1;
        // because chunk_domains is inclusive range.
        (*num_of_cells_in_chunk) *= (*chunk_size)[d];
    }
}

void _calculate_check_list(
    Chunk *chunk,
    uint64_t *chunk_size,
    bool_t except_lowest_dim, // for sparse iter
    // returns
    uint64_t **_check_list,
    uint64_t *_check_list_length,
    uint64_t *_modulo)
{

    // variables that will be returned
    uint64_t *check_list = NULL;
    uint64_t check_list_length;
    uint64_t modulo = 1;

    if (chunk->cell_order == TILESTORE_ROW_MAJOR)
    {
        int start_d = chunk->dim_len - 1;
        if (except_lowest_dim)
            start_d -= 1;
        for (int d = start_d; d >= 0; d--)
        {
            // is tile of the dim d fitted into chunk of the dim d?
            if (!_is_tile_fitted_into_chunk_for_dimension(chunk, (uint32_t)d))
            {
                // No, complete check_list and break the loop.
                _check_list_init(
                    chunk, chunk_size, d, modulo,
                    &check_list, &check_list_length);

                modulo *= chunk_size[d]; // update modulo
                break;
            }

            // update modulo
            modulo *= chunk_size[d];

            // If fit, skip the dimension.
        }
    }
    else
    {
        int start_d = 0;
        if (except_lowest_dim)
            start_d += 1;
        for (int d = start_d; d < chunk->dim_len; d++)
        {
            // is tile of the dim d fitted into chunk of the dim d?
            if (!_is_tile_fitted_into_chunk_for_dimension(chunk, (uint32_t)d))
            {
                // No, complete check_list and break the loop.
                _check_list_init(
                    chunk, chunk_size, d, modulo,
                    &check_list, &check_list_length);

                modulo *= chunk_size[d]; // update modulo
                break;
            }

            // update modulo
            modulo *= chunk_size[d];

            // If fit, skip the dimension.
        }
    }

    // isn't check_list initialized yet?
    if (check_list == NULL)
    {
        // it means that the tile is perfectly fitted into the chunk
        //      so that _is_tile_fitted_into_chunk_for_dimension() always true.

        check_list = malloc(sizeof(uint64_t) * 1);
        check_list[0] = 0;
        check_list_length = 1;
    }

    (*_check_list) = check_list;
    (*_check_list_length) = check_list_length;
    (*_modulo) = modulo;
}

ChunkIterator *chunk_iterator_init(Chunk *chunk)
{
    // initialize and create check_list
    ChunkIterator *iter = malloc(sizeof(ChunkIterator));
    iter->chunk = chunk;
    iter->chunk_idx = 0;
    iter->tile_idx = 0;
    iter->chunk_mod_idx = 0;
    iter->tile_stride = 1;
    iter->dim_order = NULL;
    iter->original_dim = NULL;
    iter->check_list = NULL;

    // calculate valid_chunk_size
    uint64_t *chunk_size;
    uint64_t num_of_cells_in_chunk;

    _calculate_chunk_size(chunk, &chunk_size, &num_of_cells_in_chunk);

    iter->chunk_size = chunk_size;
    iter->length = num_of_cells_in_chunk;

    // calculate check_list
    uint64_t *check_list;
    uint64_t check_list_length;
    uint64_t modulo = 1;

    _calculate_check_list(
        iter->chunk, iter->chunk_size, false,
        &check_list, &check_list_length, &modulo);

    iter->check_list = check_list;
    iter->check_list_length = check_list_length;
    iter->modulo = modulo;

    iter->check_idx = 0;

    // done after get first element
    iter->chunk_idx--;
    iter->tile_idx -= iter->tile_stride;
    iter->chunk_mod_idx--;
    return chunk_iterator_get_next(iter);
}

ChunkIterator *chunk_custom_order_iterator_init(Chunk *chunk, uint32_t *dim_order)
{
    // initialize and create check_list
    ChunkIterator *iter = malloc(sizeof(ChunkIterator));
    iter->chunk = chunk;
    iter->chunk_idx = 0;
    iter->tile_idx = 0;
    iter->tile_stride = 1;
    iter->chunk_mod_idx = 0;

    // copy dim_order
    iter->dim_order = malloc(sizeof(uint32_t) * chunk->dim_len);
    memcpy(iter->dim_order, dim_order, sizeof(uint32_t) * chunk->dim_len);

    // calculate valid_chunk_size
    uint64_t *chunk_size;
    uint64_t num_of_cells_in_chunk;
    _calculate_chunk_size(chunk, &chunk_size, &num_of_cells_in_chunk);
    iter->chunk_size = chunk_size;
    iter->length = num_of_cells_in_chunk;

    // create original_dim:
    //      loop {0, 1, 2, ...} -> original_dim -> dim_order
    uint32_t *original_dim = malloc(sizeof(uint32_t) * chunk->dim_len);
    iter->original_dim = original_dim;
    for (int d = 0; d < chunk->dim_len; d++)
    {
        for (int dd = 0; dd < chunk->dim_len; dd++)
        {
            if (dim_order[dd] == d)
            {
                original_dim[d] = dd;
                break;
            }
        }
    }

    // we can't calculate check_list when custom order access.
    // so we use the lowest dimension to create check_list.
    uint32_t low_dim = (chunk->cell_order == TILESTORE_ROW_MAJOR) ? original_dim[chunk->dim_len - 1] : original_dim[0];
    uint64_t *check_list;
    uint64_t check_list_length;

    _check_list_init(
        chunk, chunk_size, low_dim, 1,
        &check_list, &check_list_length);

    iter->check_list = check_list;
    iter->check_list_length = check_list_length;
    iter->modulo = iter->chunk_size[low_dim];

    // calculate tile_stride
    // tile_stride is the size of the dimensions lower than dim_order[low_dim].
    iter->tile_stride = 1;
    if (chunk->cell_order == TILESTORE_ROW_MAJOR)
    {
        for (int d = chunk->dim_len - 1; d > low_dim; d--)
        {
            uint64_t valid_tile_extent = chunk->tile_domains[d][1] - chunk->tile_domains[d][0] + 1;
            iter->tile_stride *= valid_tile_extent;
        }
    }
    else
    {
        for (int d = 0; d < low_dim; d++)
        {
            uint64_t valid_tile_extent = chunk->tile_domains[d][1] - chunk->tile_domains[d][0] + 1;
            iter->tile_stride *= valid_tile_extent;
        }
    }

    iter->check_idx = 0;

    // done after get first element
    iter->chunk_idx--;
    iter->tile_idx -= iter->tile_stride;
    iter->chunk_mod_idx--;
    return chunk_iterator_get_next(iter);
}

inline void chunk_iterator_free(ChunkIterator *iter)
{
    if (iter->original_dim != NULL)
        free(iter->original_dim);
    if (iter->dim_order != NULL)
        free(iter->dim_order);
    free(iter->chunk_size);
    free(iter->check_list);
    free(iter);
}

inline uint64_t chunk_iterator_size(ChunkIterator *iter)
{
    return iter->length;
}

inline uint8_t chunk_iterator_has_next(ChunkIterator *iter)
{
    return iter->chunk_idx < (iter->length - 1) ? 1 : 0;
}

// the returned ChunkIterator will be the same with the input.
ChunkIterator *chunk_iterator_get_next(ChunkIterator *iter)
{
    // increment
    iter->chunk_idx++;
    iter->tile_idx += iter->tile_stride;

    // do with chunk_mod_idx instead of only using chunk_idx is almost 25% faster.
    iter->chunk_mod_idx++;
    if (iter->chunk_mod_idx >= iter->modulo)
        iter->chunk_mod_idx = 0;

    // check
    if (iter->check_list[iter->check_idx] != iter->chunk_mod_idx)
    {
        // We don't need to do _chunk_process().
        // This function is usually returned from this block.
        return iter;
    }

    // We need to _chunk_process().
    // increment values
    iter->check_idx++;
    if (iter->check_idx >= iter->check_list_length)
        iter->check_idx = 0;

    // calculate coordinates using valid_chunk_size
    uint64_t *cell_coords_chunk = malloc(sizeof(uint64_t) * iter->chunk->dim_len);

    // calculate coordinates in the chunk (local ND coords).
    uint64_t curr = iter->chunk_idx;
    if (iter->dim_order != NULL)
    {
        // if this code block causes slow performance, seperate it to new function.
        // I think it is not.
        if (iter->chunk->cell_order == TILESTORE_ROW_MAJOR)
        {
            for (int d = iter->chunk->dim_len - 1; d >= 0; d--)
            {
                uint32_t dim = iter->original_dim[d];
                cell_coords_chunk[dim] = (curr % iter->chunk_size[dim]);
                curr = curr / iter->chunk_size[dim];
            }
        }
        else
        {
            for (int d = 0; d < iter->chunk->dim_len; d++)
            {
                uint32_t dim = iter->original_dim[d];
                cell_coords_chunk[dim] = (curr % iter->chunk_size[dim]);
                curr = curr / iter->chunk_size[dim];
            }
        }
    }
    else
    {
        if (iter->chunk->cell_order == TILESTORE_ROW_MAJOR)
        {
            for (int d = iter->chunk->dim_len - 1; d >= 0; d--)
            {
                cell_coords_chunk[d] = (curr % iter->chunk_size[d]);
                curr = curr / iter->chunk_size[d];
            }
        }
        else
        {
            for (int d = 0; d < iter->chunk->dim_len; d++)
            {
                cell_coords_chunk[d] = (curr % iter->chunk_size[d]);
                curr = curr / iter->chunk_size[d];
            }
        }
    }

    // _chunk_process
    iter->tile_idx = _chunk_process(iter->chunk, cell_coords_chunk);

    // custom iterator needs to adjust tile_stride as well
    if (iter->dim_order != NULL)
    {
        // if this code block causes slow performance, seperate it to new function.
        // I think it is not.
        if (iter->chunk->cell_order == TILESTORE_ROW_MAJOR)
        {
            uint32_t low_dim = iter->original_dim[iter->chunk->dim_len - 1];
            iter->tile_stride = 1;
            for (int d = iter->chunk->dim_len - 1; d > low_dim; d--)
            {
                uint64_t valid_tile_extent = iter->chunk->tile_domains[d][1] - iter->chunk->tile_domains[d][0] + 1;
                iter->tile_stride *= valid_tile_extent;
            }
        }
        else
        {
            uint32_t low_dim = iter->original_dim[0];
            iter->tile_stride = 1;
            for (int d = 0; d < low_dim; d++)
            {
                uint64_t valid_tile_extent = iter->chunk->tile_domains[d][1] - iter->chunk->tile_domains[d][0] + 1;
                iter->tile_stride *= valid_tile_extent;
            }
        }
    }

    // free
    free(cell_coords_chunk);

    // return
    return iter;
}

inline int chunk_iterator_get_cell_int(ChunkIterator *iter)
{
    return ((int*) bf_util_get_pagebuf(iter->chunk->curpage))[iter->tile_idx];
}

inline float chunk_iterator_get_cell_float(ChunkIterator *iter)
{
    return ((float*) bf_util_get_pagebuf(iter->chunk->curpage))[iter->tile_idx];
}

inline double chunk_iterator_get_cell_double(ChunkIterator *iter)
{
    return ((double*) bf_util_get_pagebuf(iter->chunk->curpage))[iter->tile_idx];
}

inline void chunk_iterator_write_cell_int(ChunkIterator *iter, int val)
{
    iter->chunk->dirty = 1;
    ((int*) bf_util_get_pagebuf(iter->chunk->curpage))[iter->tile_idx] = val;
}

inline void chunk_iterator_write_cell_float(ChunkIterator *iter, float val)
{
    iter->chunk->dirty = 1;
    ((float*) bf_util_get_pagebuf(iter->chunk->curpage))[iter->tile_idx] = val;
}

inline void chunk_iterator_write_cell_double(ChunkIterator *iter, double val)
{
    iter->chunk->dirty = 1;
    ((double*) bf_util_get_pagebuf(iter->chunk->curpage))[iter->tile_idx] = val;
}

/**************************************************
 *       SPARSE Chunk Iterator                    *
 **************************************************/

/* Sparse Fit Iterator */

// init / free
ChunkSparseFitIterator *chunk_sparse_fit_iterator_init(Chunk *chunk)
{
    // _chunk_process() to get the tile
    uint64_t *zerozero = calloc(sizeof(uint64_t *), chunk->dim_len);
    _chunk_process(chunk, zerozero);

    // validity check - check the chunk domains and the tile domains are same
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        bool_t is_same = ((chunk->tile_domains[d][0] == chunk->chunk_domains[d][0]) && (chunk->tile_domains[d][1] == chunk->chunk_domains[d][1]));
        if (!is_same)
        {
            // TODO: use errno instead of the error msg?
            fprintf(stderr, "Failed to init ChunkSparseFitIterator: the tile domains and the chunk domains are different.\n");
            return NULL;
        }
    }

    // initialize
    ChunkSparseFitIterator *iterator = malloc(sizeof(ChunkSparseFitIterator *));
    iterator->chunk = chunk;

    // dim_idx=0 should exist.
    iterator->length = (uint64_t)(bf_util_pagebuf_get_coords_lens(chunk->curpage)[0] / sizeof(int32_t));
    iterator->tile_idx = 0;

    return iterator;
}

void chunk_sparse_fit_iterator_free(ChunkSparseFitIterator *iter)
{
    free(iter);
}

uint64_t chunk_sparse_fit_iterator_size(ChunkSparseFitIterator *iter)
{
    return iter->length;
}

uint8_t chunk_sparse_fit_iterator_has_next(ChunkSparseFitIterator *iter)
{
    return iter->tile_idx < iter->length;
}

uint8_t chunk_sparse_fit_iterator_is_eof(ChunkSparseFitIterator *iter)
{
    return iter->tile_idx >= iter->length;
}

ChunkSparseFitIterator *chunk_sparse_fit_iterator_get_next(ChunkSparseFitIterator *iter)
{
    iter->tile_idx++;
}

int chunk_sparse_fit_iterator_get_cell_int(ChunkSparseFitIterator *iter)
{
    // TODO: duplicated?
    return bf_util_pagebuf_get_int(iter->chunk->curpage, iter->tile_idx);
}

float chunk_sparse_fit_iterator_get_cell_float(ChunkSparseFitIterator *iter)
{
    // TODO: duplicated?
    return bf_util_pagebuf_get_float(iter->chunk->curpage, iter->tile_idx);
}

double chunk_sparse_fit_iterator_get_cell_double(ChunkSparseFitIterator *iter)
{
    // TODO: duplicated?
    return bf_util_pagebuf_get_double(iter->chunk->curpage, iter->tile_idx);
}

uint64_t *chunk_sparse_fit_iterator_get_coords(ChunkSparseFitIterator *iter)
{
    return bf_util_get_coords_of_a_cell_of_idx_uint64(
        iter->chunk->curpage,
        iter->chunk->dim_len,
        iter->tile_idx);
}

uint64_t *chunk_sparse_fit_iterator_get_coords_in_chunk(ChunkSparseFitIterator *iter)
{
    uint64_t *ret = chunk_sparse_fit_iterator_get_coords(iter);
    for (uint64_t d = 0; d < iter->chunk->dim_len; d++)
    {
        ret[d] -= iter->chunk->chunk_domains[d][0];
    }
    return ret;
}

void chunk_sparse_fit_iterator_get_coords_in_chunk_no_malloc(ChunkSparseFitIterator *iter, uint64_t *coords)
{
    assert(0);
    // BFshm_offset_t *page_coords = BF_SHM_PTR(iter->chunk->curpage->coords_o);

    // for (uint32_t d = 0; d < iter->chunk->dim_len; d++)
    // {
    //     int32_t *data = (int32_t *)BF_SHM_PTR(page_coords[d]);
    //     coords[d] = (uint64_t)data[iter->tile_idx] - iter->chunk->chunk_domains[d][0];
    // }
}

/* Sparse Random Order Iterator */
bool_t _is_collide(uint64_t **rect1, uint64_t **rect2, uint32_t dim_len)
{

    // fprintf(stderr, "rect1: {{%lu, %lu}, {%lu, %lu}}, rect2: {{%lu, %lu}, {%lu, %lu}}, ",
    //     rect1[0][0], rect1[1][0], rect1[0][1], rect1[1][1],
    //     rect2[0][0], rect2[1][0], rect2[0][1], rect2[1][1]);

    // do the upper left of rect1 and the lower right of rect2 have intersect?
    for (uint32_t d = 0; d < dim_len; d++)
    {
        if (rect1[d][0] > rect2[d][1])
        {
            return false;
        }
    }

    // do the upper left of rect2 and the lower right of rect1 have intersect?
    for (uint32_t d = 0; d < dim_len; d++)
    {
        if (rect1[d][1] < rect2[d][0])
        {
            return false;
        }
    }

    return true;
}

bool_t _is_in(uint64_t *coords, uint64_t **rect, uint32_t dim_len)
{
    for (uint32_t d = 0; d < dim_len; d++)
    {
        bool_t is_in_range = (rect[d][0] <= coords[d]) && (coords[d] <= rect[d][1]);
        if (!is_in_range)
        {
            return false;
        }
    }

    return true;
}

// build check_tile_coords_list and check_tile_coords_length of
//      ChunkSparseRandomOrderIterator and ChunkSparseIterator.
// Moreover, it returns tile_sequence_len for ChunkSparseIterator.
void _build_check_tile_coords_list(
    Chunk *chunk,
    // returns
    uint64_t ***_check_tile_coords_list,
    uint64_t *_check_tile_coords_length,
    uint64_t *_tile_sequence_len)
{

    uint64_t **check_tile_coords_list;
    uint64_t tile_sequence_len;

    // get num of tiles
    uint64_t num_of_tiles;
    uint64_t *arr_size_in_tile; // it is re-used in near future!
    storage_util_get_dcoord_lens(chunk->array_domains, chunk->tile_extents, chunk->dim_len, &arr_size_in_tile);

    num_of_tiles = 1;
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        num_of_tiles *= arr_size_in_tile[d];
    }

    // malloc check_tile_coords_list.
    // the check_tile_coords_length is roughly calculated.
    uint64_t approximated_check_tile_coords_length = 1;
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        uint64_t arr_size_in_dim = chunk->array_domains[d][1] - chunk->array_domains[d][0] + 1;
        uint64_t chunk_size_in_dim = chunk->chunk_domains[d][1] - chunk->chunk_domains[d][0] + 1;

        // the maximum number of chunks in array in this dimension is
        //      ceil((the number of cells in dim) / (the number of cells in chunk)) + 1.
        uint64_t maximum_num_of_chunk_in_dim = (uint64_t)(ceil(arr_size_in_dim / chunk_size_in_dim) + 1);
        approximated_check_tile_coords_length *= maximum_num_of_chunk_in_dim;
    }
    check_tile_coords_list = malloc(sizeof(uint64_t *) * approximated_check_tile_coords_length);

    // pre-malloc tile_domains (which holds the rect of tile) to avoid frequent malloc
    uint64_t **tile_domains = malloc(sizeof(uint64_t *) * chunk->dim_len);
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        tile_domains[d] = malloc(sizeof(uint64_t) * 2);
    }

    // TODO: Improve to reduce the iteration below
    // iterate over tiles and fill the check_tile_coords_list
    uint64_t check_tile_coords_idx = 0;
    // used for calculating tile_sequence_len
    bool_t is_measuring_tile_sequence = true;
    uint64_t current_tile_sequence_len = 0;
    for (uint64_t i = 0; i < num_of_tiles; i++)
    {
        // calculate tile coords
        uint64_t *tile_coords = NULL;
        bf_util_calculate_nd_from_1d_row_major(i, arr_size_in_tile, chunk->dim_len, &tile_coords);

        // calculate tile domains to compute collision
        for (uint32_t d = 0; d < chunk->dim_len; d++)
        {
            tile_domains[d][0] = (chunk->tile_extents[d] * tile_coords[d]) + chunk->array_domains[d][0];
            tile_domains[d][1] = tile_domains[d][0] + chunk->tile_extents[d] - 1;
        }

        // does it make a collision?
        if (_is_collide(tile_domains, chunk->chunk_domains, chunk->dim_len))
        {
            // fprintf(stderr, "is_collide= true\n");
            // if in the chunk boundary
            // fill check_tile_coords_list
            check_tile_coords_list[check_tile_coords_idx] = malloc(sizeof(uint64_t) * chunk->dim_len);
            for (uint32_t d = 0; d < chunk->dim_len; d++)
            {
                check_tile_coords_list[check_tile_coords_idx][d] = tile_coords[d];
            }
            check_tile_coords_idx++;

            // tile_sequence
            if (is_measuring_tile_sequence)
            {
                current_tile_sequence_len++;
            }
        }
        else
        {
            // if the sequence is ended, finish measuring_tile_sequence
            bool_t is_first_measure_ended =
                (current_tile_sequence_len > 0) && is_measuring_tile_sequence;
            if (is_first_measure_ended)
            {
                is_measuring_tile_sequence = false;
            }
        }
        // else fprintf(stderr, "is_collide= false\n");

        free(tile_coords);
    }

    (*_check_tile_coords_list) = check_tile_coords_list;
    (*_check_tile_coords_length) = check_tile_coords_idx;
    if (_tile_sequence_len != NULL)
        (*_tile_sequence_len) = current_tile_sequence_len;

    // free tile_domains
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        free(tile_domains[d]);
    }
    free(tile_domains);
    free(arr_size_in_tile);
}

// init / free
ChunkSparseRandomOrderIterator *chunk_sparse_random_order_iterator_init(Chunk *chunk)
{
    ChunkSparseRandomOrderIterator *iter = malloc(sizeof(ChunkSparseRandomOrderIterator));
    iter->chunk = chunk;

    // build stuffs regarding check_tile_coords_list..
    _build_check_tile_coords_list(
        chunk,
        &iter->check_tile_coords_list, &iter->check_tile_coords_length, NULL);
    iter->check_tile_coords_idx = 0 - 1;

    iter->idx_in_tile = 0 - 1;
    iter->tile_length = 0;

    chunk_sparse_random_order_iterator_get_next(iter);
    return iter;
}

void chunk_sparse_random_order_iterator_free(ChunkSparseRandomOrderIterator *iter)
{
    for (uint64_t i = 0; i < iter->check_tile_coords_length; i++)
    {
        free(iter->check_tile_coords_list[i]);
    }
    free(iter->check_tile_coords_list);
    free(iter);
}

// is_eof() / get_next()
uint8_t chunk_sparse_random_order_iterator_is_eof(ChunkSparseRandomOrderIterator *iter)
{
    bool_t is_tile_left = iter->check_tile_coords_idx < iter->check_tile_coords_length;
    bool_t is_cell_left = iter->idx_in_tile < iter->tile_length;
    return !is_tile_left && !is_cell_left;
}

uint64_t *_get_upper_left_array_coordinates_of_chunk_in_tile(Chunk *chunk)
{
    uint64_t *coords = malloc(sizeof(uint64_t) * chunk->dim_len);
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        bool_t is_chunk_domain_in_tile_domain =
            chunk->tile_domains[d][0] <= chunk->chunk_domains[d][0];
        if (is_chunk_domain_in_tile_domain)
        {
            coords[d] = chunk->chunk_domains[d][0];
        }
        else
        {
            coords[d] = chunk->tile_domains[d][0];
        }
    }

    return coords;
}

void _chunk_sparse_iterator_replace_tile_and_update_members(
    Chunk *chunk,
    uint64_t **check_tile_coords_list,
    uint64_t check_tile_coords_idx,
    uint64_t *idx_in_tile,
    uint64_t *tile_length)
{
    // convert 1D idx_in_tile to ND idx
    uint64_t *next_tile_coords = check_tile_coords_list[check_tile_coords_idx];
    uint64_t *arr_size_in_tile;
    storage_util_get_dcoord_lens(chunk->array_domains, chunk->tile_extents, chunk->dim_len, &arr_size_in_tile);

    // replace tile
    _chunk_replace_tile(chunk, next_tile_coords);

    // set tile_idx to the lub of the tile
    uint64_t *upper_left_coords =
        _get_upper_left_array_coordinates_of_chunk_in_tile(chunk);
    (*idx_in_tile) = _chunk_sparse_get_idx_for_coordinates(
        chunk, upper_left_coords, RETURN_LUB_IF_NOT_FOUND);
    (*idx_in_tile) -= 1; // idx_in_tile will be incremented by one, so adjust it

    // set tile_length
    (*tile_length) = chunk->curpage->unfilled_idx;

    // Free tile_coords
    free(arr_size_in_tile);
    free(upper_left_coords);

    return;
}

ChunkSparseRandomOrderIterator *chunk_sparse_random_order_iterator_get_next(
    ChunkSparseRandomOrderIterator *iter)
{
    while (1)
    {
        // increment idx_in_tile
        iter->idx_in_tile++;

        // check is there any cell left?
        bool_t is_cell_left = iter->idx_in_tile < iter->tile_length;
        if (!is_cell_left)
        {
            // No, then we have to check next tile.
            iter->check_tile_coords_idx++;

            // before start, check we has any tile left.
            bool_t is_tile_left = iter->check_tile_coords_idx < iter->check_tile_coords_length;
            if (!is_tile_left)
            {
                // if there are no tile, get_next() can't do anything. it is eof.
                return iter;
            }

            /* Replace tile and set tile_idx to the lub of the tile */
            _chunk_sparse_iterator_replace_tile_and_update_members(
                iter->chunk,
                iter->check_tile_coords_list,
                iter->check_tile_coords_idx,
                &iter->idx_in_tile,
                &iter->tile_length);

            // done!!... but we have to iterate until meeting valid cell
            continue;
        }

        // read and check if it is in the chunk region
        uint64_t *tile_coords = bf_util_get_coords_of_a_cell_of_idx_uint64(
            iter->chunk->curpage,
            iter->chunk->dim_len,
            iter->idx_in_tile);
        uint8_t is_in = _is_in(
            tile_coords,
            iter->chunk->chunk_domains,
            iter->chunk->dim_len);
        free(tile_coords);
        if (!is_in)
        {
            // if not, iterate until valid cell..
            continue;
        }

        // congratulation!!!! we finally find a valid cell!
        break;
    }

    return iter;
}

// read cell
int chunk_sparse_random_order_iterator_get_cell_int(ChunkSparseRandomOrderIterator *iter)
{
    return bf_util_pagebuf_get_int(iter->chunk->curpage, iter->idx_in_tile);
}

float chunk_sparse_random_order_iterator_get_cell_float(ChunkSparseRandomOrderIterator *iter)
{
    return bf_util_pagebuf_get_float(iter->chunk->curpage, iter->idx_in_tile);
}

double chunk_sparse_random_order_iterator_get_cell_double(ChunkSparseRandomOrderIterator *iter)
{
    return bf_util_pagebuf_get_double(iter->chunk->curpage, iter->idx_in_tile);
}

uint64_t *chunk_sparse_random_order_iterator_get_coords(ChunkSparseRandomOrderIterator *iter)
{
    return bf_util_get_coords_of_a_cell_of_idx_uint64(
        iter->chunk->curpage,
        iter->chunk->dim_len,
        iter->idx_in_tile);
}

uint64_t *chunk_sparse_random_order_iterator_get_coords_in_chunk(ChunkSparseRandomOrderIterator *iter)
{
    uint64_t *ret = chunk_sparse_random_order_iterator_get_coords(iter);
    for (uint64_t d = 0; d < iter->chunk->dim_len; d++)
    {
        ret[d] -= iter->chunk->chunk_domains[d][0];
    }
    return ret;
}

void _add_tile_of_ccords_to_tile_sequence_list(
    ChunkSparseIterator *iter,
    int32_t *coords)
{

    // cell coords -> tile coords
    uint64_t *dcoords = malloc(iter->chunk->dim_len * sizeof(uint64_t));
    for (uint32_t d = 0; d < iter->chunk->dim_len; d++)
    {
        uint64_t adjusted =
            ((uint64_t)coords[d] - iter->chunk->array_domains[d][0]);
        dcoords[d] =
            (uint64_t)floor(adjusted / iter->chunk->tile_extents[d]);
    }

    // find in the check_tile_coords_list
    // TODO: it is costly?
    uint64_t ans = 0 - 1;
    for (uint64_t i = 0; i < iter->check_tile_coords_length; i++)
    {
        uint64_t *element_dcoords = iter->check_tile_coords_list[i];
        int res = bf_util_compare_coords(
            dcoords, element_dcoords, iter->chunk->dim_len, TILESTORE_ROW_MAJOR);
        if (res == COMP_EQUAL)
        {
            ans = i;
            break;
        }
    }

    // integrity check
    assert(ans != (0 - 1));

    iter->tile_sequence_list[iter->tile_sequence_len] = ans;
    iter->tile_sequence_len += 1;
}

void _update_unseen_tile_information_in_tile_sequence(
    ChunkSparseIterator *iter)
{
    // update tile information that first seen
    //      (is_tile_seen_list, tile_length_list)

    for (uint64_t i = 0; i < iter->tile_sequence_len; i++)
    {
        uint64_t idx = iter->tile_sequence_list[i];
        bool_t is_seen = iter->is_tile_seen_list[idx];
        if (!is_seen)
        {
            // first meeting tile!

            // convert 1D idx_in_tile to ND idx and replace tile
            uint64_t *next_tile_coords = iter->check_tile_coords_list[idx];
            _chunk_replace_tile(iter->chunk, next_tile_coords);

            // set tile_idx to the lub of the tile
            uint64_t *upper_left_coords =
                _get_upper_left_array_coordinates_of_chunk_in_tile(
                    iter->chunk);
            iter->idx_of_cells_in_tile_list[idx] =
                _chunk_sparse_get_idx_for_coordinates(
                    iter->chunk, upper_left_coords, RETURN_LUB_IF_NOT_FOUND) -
                1;
            free(upper_left_coords);

            // get len
            uint64_t len =
                bf_util_pagebuf_get_unfilled_idx(iter->chunk->curpage);
            iter->tile_length_list[idx] = len;

            // update boolean
            iter->is_tile_seen_list[idx] = true;
        }
    }
}

void _update_tile_sequence_list(ChunkSparseIterator *iter)
{
    // get needed variables
    tilestore_layout_t cell_order = iter->chunk->cell_order;
    uint32_t low_dim = 0;
    if (cell_order == TILESTORE_ROW_MAJOR)
        low_dim = iter->chunk->dim_len - 1;
    uint64_t max_len =
        (uint64_t)ceil(iter->chunk_size[low_dim] /
                       iter->chunk->tile_extents[low_dim]);

    // alloc tile_sequence list
    // if tile_sequence_list exists, free it first
    if (iter->tile_sequence_list != NULL)
        free(iter->tile_sequence_list);

    iter->tile_sequence_list = malloc(max_len * sizeof(uint64_t));
    iter->tile_sequence_len = 0;

    // get start coordinates
    size_t coords_size = sizeof(int32_t) * iter->chunk->dim_len;
    int32_t *coords = malloc(coords_size);
    memcpy(coords, iter->cell_sequence_coords, coords_size);
    coords[low_dim] = (int32_t)iter->chunk->chunk_domains[low_dim][0];

    // the tile according to start coordinates should be included
    _add_tile_of_ccords_to_tile_sequence_list(iter, coords);

    // add until coords are out of chunk region
    while (true)
    {
        coords[low_dim] += iter->chunk->tile_extents[low_dim];

        bool_t is_coords_in_chunk =
            (coords[low_dim] <
             ((int32_t)iter->chunk->chunk_domains[low_dim][1] + 1));
        if (!is_coords_in_chunk)
            break;

        _add_tile_of_ccords_to_tile_sequence_list(iter, coords);
    }

    free(coords);

    // update unseen tile info
    _update_unseen_tile_information_in_tile_sequence(iter);

    // update member
    iter->tile_sequence_idx = 0;

    // replace to the first tile
    uint64_t check_til_coords_idx =
        iter->tile_sequence_list[iter->tile_sequence_idx];
    uint64_t *next_tile_coords =
        iter->check_tile_coords_list[check_til_coords_idx];
    _chunk_replace_tile(iter->chunk, next_tile_coords);
}

// it returns whether it iterate all cell sequence or not
bool_t _increment_cell_sequence_coords(ChunkSparseIterator *iter)
{
    tilestore_layout_t cell_order = iter->chunk->cell_order;

    // increment coords with a consideration of chunk region
    bool_t overflow = true;
    if (cell_order == TILESTORE_ROW_MAJOR)
    {
        // row major
        for (int d = iter->chunk->dim_len - 2; d >= 0; d--)
        {
            if (overflow)
            {
                iter->cell_sequence_coords[d]++;
                overflow = false;
            }

            overflow =
                (iter->cell_sequence_coords[d] >
                 iter->chunk->chunk_domains[d][1]);
            if (overflow)
            {
                iter->cell_sequence_coords[d] =
                    iter->chunk->chunk_domains[d][0];
            }
        }
    }
    else
    {
        // col major
        for (int d = 1; d < iter->chunk->dim_len; d++)
        {
            if (overflow)
            {
                iter->cell_sequence_coords[d]++;
                overflow = false;
            }

            overflow =
                (iter->cell_sequence_coords[d] >
                 iter->chunk->chunk_domains[d][1]);
            if (overflow)
            {
                iter->cell_sequence_coords[d] =
                    iter->chunk->chunk_domains[d][0];
            }
        }
    }

    return overflow;
}

bool_t _is_cell_over_cell_seq(ChunkSparseIterator *iter, uint64_t cell_idx)
{
    tilestore_layout_t cell_order = iter->chunk->cell_order;
    int32_t *coords = bf_util_get_coords_of_a_cell_of_idx_int32(
        iter->chunk->curpage, iter->chunk->dim_len, cell_idx);
    bool_t res = false;

    // check coordinates are same except the seq dimension
    if (cell_order == TILESTORE_ROW_MAJOR)
    {
        // row major
        for (uint32_t d = 0; d < (iter->chunk->dim_len - 1); d++)
        {
            bool_t diff = (iter->cell_sequence_coords[d] != coords[d]);
            if (diff)
            {
                res = (iter->cell_sequence_coords[d] < coords[d]);
                break;
            }
        }
    }
    else
    {
        // col major
        for (uint32_t d = 1; d < iter->chunk->dim_len; d++)
        {
            bool_t diff = (iter->cell_sequence_coords[d] != coords[d]);
            if (diff)
            {
                res = (iter->cell_sequence_coords[d] < coords[d]);
                break;
            }
        }
    }

    free(coords);
    return res;
}

bool_t _is_cell_in_cell_seq(ChunkSparseIterator *iter, uint64_t cell_idx)
{
    tilestore_layout_t cell_order = iter->chunk->cell_order;
    int32_t *coords = bf_util_get_coords_of_a_cell_of_idx_int32(
        iter->chunk->curpage, iter->chunk->dim_len, cell_idx);
    bool_t res = true;

    // check coordinates are same except the seq dimension
    if (cell_order == TILESTORE_ROW_MAJOR)
    {
        // row major
        for (uint32_t d = 0; d < (iter->chunk->dim_len - 1); d++)
        {
            bool_t diff = (iter->cell_sequence_coords[d] != coords[d]);
            if (diff)
            {
                res = false;
                break;
            }
        }
    }
    else
    {
        // col major
        for (uint32_t d = 1; d < iter->chunk->dim_len; d++)
        {
            bool_t diff = (iter->cell_sequence_coords[d] != coords[d]);
            if (diff)
            {
                res = false;
                break;
            }
        }
    }

    free(coords);
    return res;
}

// init / free
ChunkSparseIterator *chunk_sparse_iterator_init(Chunk *chunk)
{
    ChunkSparseIterator *iter = malloc(sizeof(ChunkSparseIterator));
    iter->chunk = chunk;

    // calculate valid_chunk_size
    uint64_t *chunk_size;
    uint64_t num_of_cells_in_chunk;

    _calculate_chunk_size(chunk, &chunk_size, &num_of_cells_in_chunk);

    iter->chunk_size = chunk_size;

    // calculate check_list
    uint64_t *check_list;
    uint64_t check_list_length;
    uint64_t modulo = 1;

    _calculate_check_list(
        iter->chunk, iter->chunk_size, true,
        &check_list, &check_list_length, &modulo);

    iter->check_list = check_list;
    iter->check_list_length = check_list_length;
    iter->check_idx = 0;
    iter->modulo = modulo;

    // build stuffs regarding check_tile_coords_list..
    _build_check_tile_coords_list(
        chunk,
        &iter->check_tile_coords_list, &iter->check_tile_coords_length,
        NULL);

    // we don't know the correct values of below yet,
    //    but have to initialize it.
    iter->is_tile_seen_list =
        malloc(sizeof(bool_t) * iter->check_tile_coords_length);
    iter->idx_of_cells_in_tile_list =
        malloc(sizeof(uint64_t) * iter->check_tile_coords_length);
    iter->tile_length_list =
        malloc(sizeof(uint64_t) * iter->check_tile_coords_length);
    for (uint64_t i = 0; i < iter->check_tile_coords_length; i++)
    {
        iter->is_tile_seen_list[i] = false;
        iter->idx_of_cells_in_tile_list[i] = 0 - 1;
        iter->tile_length_list[i] = 0 - 1;
    }

    // init cell_sequence_coords
    // it will be the upper left coordinates of chunk
    iter->cell_sequence_coords = malloc(sizeof(int32_t) * chunk->dim_len);
    for (uint64_t d = 0; d < chunk->dim_len; d++)
    {
        iter->cell_sequence_coords[d] = (int32_t)chunk->chunk_domains[d][0];
    }
    iter->cell_sequence_count = 0;

    // init tile_sequence
    iter->tile_sequence_list = NULL;
    iter->tile_sequence_len = 0;

    _update_tile_sequence_list(iter);

    iter->is_eof = false;

    // sparse chunk iterator init wo using get_next
    //      so we need to manually adjust some members
    if (iter->check_list_length > 1)
        iter->check_idx = 1;

    // set tile_idx to the lub of the tile
    uint64_t *upper_left_coords =
        _get_upper_left_array_coordinates_of_chunk_in_tile(chunk);
    iter->idx_of_cells_in_tile_list[0] = _chunk_sparse_get_idx_for_coordinates(
        chunk, upper_left_coords, RETURN_LUB_IF_NOT_FOUND);
    free(upper_left_coords);

    // adjust cell idx
    while (iter->idx_of_cells_in_tile_list[0] < iter->tile_length_list[0])
    {
        uint64_t current_cell_idx = iter->idx_of_cells_in_tile_list[0];

        // read and check if it is in the chunk region
        uint64_t *tile_coords =
            bf_util_get_coords_of_a_cell_of_idx_uint64(
                iter->chunk->curpage,
                iter->chunk->dim_len,
                current_cell_idx);
        bool_t is_in = _is_in(
            tile_coords,
            iter->chunk->chunk_domains,
            iter->chunk->dim_len);
        free(tile_coords);

        // Debuging prints
        // int32_t *coords = bf_util_get_coords_of_a_cell_of_idx_int32(
        //     iter->chunk->curpage, iter->chunk->dim_len, current_cell_idx);
        // fprintf(stderr, "init => \t\t cell_coord:%d,%d,%d\t\t is_in:%d\n",
        //     coords[0], coords[1], coords[2],
        //     is_in);
        // free(coords);

        // if the cell in the chunk region
        if (is_in)
        {
            break;
        }

        // if not, iterate until valid cell..
        iter->idx_of_cells_in_tile_list[0]++;
    }

    return iter;
}

void chunk_sparse_iterator_free(ChunkSparseIterator *iter)
{
    free(iter->chunk_size);
    free(iter->check_list);
    for (uint64_t i = 0; i < iter->check_tile_coords_length; i++)
    {
        free(iter->check_tile_coords_list[i]);
    }
    free(iter->is_tile_seen_list);
    free(iter->idx_of_cells_in_tile_list);
    free(iter->tile_length_list);
    if (iter->tile_sequence_list != NULL)
        (iter->tile_sequence_list);
    free(iter->cell_sequence_coords);
    free(iter);
}

// is_eof() / get_next()
uint8_t chunk_sparse_iterator_is_eof(ChunkSparseIterator *iter)
{
    return iter->is_eof;
}

ChunkSparseIterator *chunk_sparse_iterator_get_next(
    ChunkSparseIterator *iter)
{
    while (true)
    {
        ////////////////////////////////
        // find valid cell
        ////////////////////////////////
        uint64_t tile_idx =
            iter->tile_sequence_list[iter->tile_sequence_idx];
        uint64_t current_cell_idx = iter->idx_of_cells_in_tile_list[tile_idx];
        uint64_t next_cell_idx = current_cell_idx + 1;

        bool_t is_eof = next_cell_idx >= iter->tile_length_list[tile_idx];
        // bool_t is_in_cell_sequence = _is_cell_in_cell_seq(iter, next_cell_idx);
        bool_t is_over_cell_sequence = _is_cell_over_cell_seq(iter, next_cell_idx);

        // Debuging prints
        // int32_t *coords = bf_util_get_coords_of_a_cell_of_idx_int32(
        //     iter->chunk->curpage, iter->chunk->dim_len, next_cell_idx);
        // fprintf(stderr, "cell_seq:%d,%d\t\t cell_coord:%d,%d,%d\t\t is_over:%d\n",
        //     iter->cell_sequence_coords[0], iter->cell_sequence_coords[1],
        //     coords[0], coords[1], coords[2],
        //     is_over_cell_sequence);
        // free(coords);

        // if eof or out of cell seq
        if (is_eof || is_over_cell_sequence)
        {
            // we have to replace tile or tile seq

            // if current tile is not the last tile in the tile seq,
            //      then just see the next tile.
            bool_t is_last_tile =
                (iter->tile_sequence_idx + 1) == (iter->tile_sequence_len);
            if (!is_last_tile)
            {
                iter->tile_sequence_idx += 1;
            }
            else
            {
                // if current tile is the last tile
                // increment current_coords_of_cell_seq
                bool_t done = _increment_cell_sequence_coords(iter);
                if (done)
                {
                    // if it iterates all, mark eof and just return
                    iter->is_eof = true;
                    return iter;
                }

                // incremental cell_seq_count and check that it is in check_list
                iter->cell_sequence_count++;
                uint64_t chunk_mod_idx = iter->cell_sequence_count % iter->modulo;
                if (iter->check_list[iter->check_idx] == chunk_mod_idx)
                {
                    // We need to replace tile seq.
                    // increment values
                    iter->check_idx++;
                    if (iter->check_idx >= iter->check_list_length)
                        iter->check_idx = 0;

                    // replace tile seq
                    _update_tile_sequence_list(iter);

                    // the function above replace tile, so let's continue loop
                    continue;
                }

                // We don't need to replace tile seq.
                // This function is usually returned from this block.
                iter->tile_sequence_idx = 0;
            }

            // replace tile
            uint64_t check_til_coords_idx =
                iter->tile_sequence_list[iter->tile_sequence_idx];
            uint64_t *next_tile_coords =
                iter->check_tile_coords_list[check_til_coords_idx];
            _chunk_replace_tile(iter->chunk, next_tile_coords);

            continue;
        }

        // update cell idx
        current_cell_idx = next_cell_idx;
        iter->idx_of_cells_in_tile_list[tile_idx] = current_cell_idx;

        // read and check if it is in the chunk region
        uint64_t *tile_coords =
            bf_util_get_coords_of_a_cell_of_idx_uint64(
                iter->chunk->curpage,
                iter->chunk->dim_len,
                current_cell_idx);
        bool_t is_in = _is_in(
            tile_coords,
            iter->chunk->chunk_domains,
            iter->chunk->dim_len);
        free(tile_coords);
        if (!is_in)
        {
            // if not, iterate until valid cell..
            continue;
        }

        // congratulation!!!! we finally find a valid cell!
        break;
    }

    return iter;
}

// read cell
int chunk_sparse_iterator_get_cell_int(ChunkSparseIterator *iter)
{
    uint64_t tile_idx = iter->tile_sequence_list[iter->tile_sequence_idx];
    uint64_t cell_idx = iter->idx_of_cells_in_tile_list[tile_idx];
    return bf_util_pagebuf_get_int(iter->chunk->curpage, cell_idx);
}

float chunk_sparse_iterator_get_cell_float(ChunkSparseIterator *iter)
{
    uint64_t tile_idx = iter->tile_sequence_list[iter->tile_sequence_idx];
    uint64_t cell_idx = iter->idx_of_cells_in_tile_list[tile_idx];
    return bf_util_pagebuf_get_float(iter->chunk->curpage, cell_idx);
}

double chunk_sparse_iterator_get_cell_double(ChunkSparseIterator *iter)
{
    uint64_t tile_idx = iter->tile_sequence_list[iter->tile_sequence_idx];
    uint64_t cell_idx = iter->idx_of_cells_in_tile_list[tile_idx];
    return bf_util_pagebuf_get_double(iter->chunk->curpage, cell_idx);
}

uint64_t *chunk_sparse_iterator_get_coords(ChunkSparseIterator *iter)
{
    uint64_t tile_idx = iter->tile_sequence_list[iter->tile_sequence_idx];
    uint64_t cell_idx = iter->idx_of_cells_in_tile_list[tile_idx];
    return bf_util_get_coords_of_a_cell_of_idx_uint64(
        iter->chunk->curpage,
        iter->chunk->dim_len,
        cell_idx);
}

uint64_t *chunk_sparse_iterator_get_coords_in_chunk(ChunkSparseIterator *iter)
{
    uint64_t *ret = chunk_sparse_iterator_get_coords(iter);
    for (uint64_t d = 0; d < iter->chunk->dim_len; d++)
    {
        ret[d] -= iter->chunk->chunk_domains[d][0];
    }
    return ret;
}
