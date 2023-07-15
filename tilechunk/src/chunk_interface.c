#include <stdio.h>
#include <string.h>
#include <math.h>

#include "tilestore.h"
#include "bf.h"
#include "bf_struct.h"
#include "utils.h"
#include "arraykey.h"
#include "chunk_struct.h"
#include "chunk_interface.h"

/****************************************************************
/*            Chunk internal functions definitions              *
*****************************************************************/

/* Replace current tile */
int _chunk_replace_tile(Chunk *chunk, uint64_t *tile_coords_new)
{
    array_key array_key; // BF-layer request

    /* Unpin the old tile. TouchBuf() if dirty */
    if (chunk->curpage != NULL)
    {
        array_key.arrayname = chunk->array_name;
        array_key.attrname = chunk->attr_name; // Assume single attribute
        array_key.dcoords = chunk->tile_coords;
        array_key.dim_len = chunk->dim_len;
        if (chunk->dirty == 1) // If dirty
            BF_TouchBuf(array_key);
        BF_UnpinBuf(array_key);
    }

    // copy tile_coords_new
    memcpy(chunk->tile_coords, tile_coords_new, sizeof(uint64_t) * chunk->dim_len);

    /* Get new tile from buffer layer */
    array_key.arrayname = chunk->array_name;
    array_key.attrname = chunk->attr_name; // Assume single attribute
    array_key.dcoords = chunk->tile_coords;
    array_key.dim_len = chunk->dim_len;
    array_key.emptytile_template =
        (chunk->array_type == TILESTORE_SPARSE_CSR) ? BF_EMPTYTILE_SPARSE_CSR : BF_EMPTYTILE_DENSE;

    BF_GetBuf(array_key, &chunk->curpage);

    chunk->dirty = 0; // Initialize the dirty flag

    /* (Future Work) Get new tile domains
     * - Dense Array  : Brute-force calculation
     * - Sparse Array : Use util function (future work) */
    for (int d = 0; d < chunk->dim_len; d++)
    {
        chunk->tile_domains[d][0] = chunk->array_domains[d][0] + chunk->tile_coords[d] * chunk->tile_extents[d];
        chunk->tile_domains[d][1] = chunk->tile_domains[d][0] + chunk->tile_extents[d] - 1;
        if (chunk->tile_domains[d][1] > chunk->array_domains[d][1]) // Marginal tile handling
            chunk->tile_domains[d][1] = chunk->array_domains[d][1];
    }
}

/* Check whether given coordinates is valid & contained in current tile.
 * Return value
 *  -1 : Invalid coordinate
 *  0  : Valid, but not in current tile
 *  1  : Valid, and exist in current tile */
int _chunk_coordinate_check(Chunk *chunk, int64_t *cell_coords_array)
{

    /* Validity check (Is within chunk domains?) */
    for (int d = 0; d < chunk->dim_len; d++)
    {
        if (cell_coords_array[d] < (chunk->chunk_domains)[d][0] || cell_coords_array[d] > (chunk->chunk_domains)[d][1])
            return -1;
    }

    /* Existence check (Is within current tile?) */
    if (chunk->curpage == NULL) // No current tile
        return 0;

    for (int d = 0; d < chunk->dim_len; d++)
    {
        if (cell_coords_array[d] < (chunk->tile_domains)[d][0] || cell_coords_array[d] > (chunk->tile_domains)[d][1])
            return 0;
    }

    /* If valid and exist in current tile */
    return 1;
}

/* Process the chunk access request by doing
 *  - Coordinate validity / existence check
 *  - Replace current tile if neccesary
 *  - Calculate and return the offset value of the given cell */
int _chunk_process(Chunk *chunk, uint64_t *cell_coords_chunk)
{
    uint64_t cell_index;                       // Index of the cell in the tile
    uint64_t cell_offset;                      // Offset of the cell in the tile
    int multiplier;                            // For cell index calculation purpose
    int coordinate_result;                     // Coordinate checking result
    uint64_t tile_coords_new[chunk->dim_len];  // Tile coordinates for new tile
    int64_t cell_coords_array[chunk->dim_len]; // Array-level input coordinates

    /* Check coordinates for validity and existence */
    for (int d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }
    coordinate_result = _chunk_coordinate_check(chunk, cell_coords_array);

    if (coordinate_result < 0) // Invalid coordinate
        return CHUNK_INVALID_CELL_COORDS;

    else if (coordinate_result == 0) // Valid, but not in current tile
    {
        /* Get new tile coordinates */
        for (uint32_t d = 0; d < chunk->dim_len; d++)
        {
            tile_coords_new[d] =
                (uint64_t)floor(((uint64_t)cell_coords_array[d] -
                                 chunk->array_domains[d][0]) /
                                chunk->tile_extents[d]);
        }

        // replace tile
        _chunk_replace_tile(chunk, tile_coords_new);
    }

    /* (Future Work) Calculate cell offset
     * - Fixed-sized attribute array : cell_index == cell_offset
     * - Variable-sized attribute array : Use util function to get cell_offset (future work) */
    cell_index = 0;
    multiplier = 1;

    if (chunk->cell_order == TILESTORE_ROW_MAJOR) // row-major
    {
        for (int d = chunk->dim_len - 1; d >= 0; d--)
        {
            cell_index += (cell_coords_array[d] - (chunk->tile_domains)[d][0]) * multiplier;
            multiplier *= (chunk->tile_domains[d][1] - chunk->tile_domains[d][0] + 1);
        }
    }
    else if (chunk->cell_order == TILESTORE_COL_MAJOR) // col-major
    {
        for (int d = 0; d < chunk->dim_len; d++)
        {
            cell_index += (cell_coords_array[d] - (chunk->tile_domains)[d][0]) * multiplier;
            multiplier *= (chunk->tile_domains[d][1] - chunk->tile_domains[d][0] + 1);
        }
    }

    cell_offset = cell_index;

    /* On success */
    return cell_offset;
}

/*******************************************************
/*            Chunk interface definitions              *
********************************************************/

/* Allocate chunk specified by the parameter */
int chunk_get_chunk(const char *array_name,
                    uint64_t *chunk_size,
                    uint64_t *chunk_coords,
                    Chunk **chunk)
{
    uint64_t **array_domains;
    char **attr_names;
    uint64_t *tile_extents;
    uint32_t dim_len;
    uint32_t attr_len;
    uint64_t end_coord;

    /* Get neccesary informations of the given array */
    storage_util_get_dim_domains(array_name, &array_domains, &dim_len);
    storage_util_get_tile_extents(array_name, &tile_extents, &dim_len);
    storage_util_get_attr_names(array_name, &attr_names, &attr_len);

    /* Newly allocate chunk structure */
    *chunk = (Chunk *)malloc(sizeof(Chunk));

    /* Fixed attribute initialization */
    (*chunk)->array_name = malloc(sizeof(char) * MAX_ARRAY_NAME_LENGTH); // 255
    strcpy((*chunk)->array_name, array_name);

    (*chunk)->attr_name = malloc(sizeof(char) * MAX_ATTR_NAME_LENGTH); // 255
    strcpy((*chunk)->attr_name, attr_names[0]);                        // Assume single attribute

    storage_util_get_cell_order(array_name, &((*chunk)->cell_order));

    storage_util_get_array_type(array_name, &((*chunk)->array_type));

    (*chunk)->dim_len = dim_len;

    (*chunk)->array_domains = array_domains;

    (*chunk)->chunk_domains = malloc(sizeof(uint64_t *) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++) // Assign chunk_domains
    {
        ((*chunk)->chunk_domains)[d] = malloc(sizeof(uint64_t) * 2);
        ((*chunk)->chunk_domains)[d][0] = array_domains[d][0] + chunk_size[d] * chunk_coords[d];
        end_coord = ((*chunk)->chunk_domains)[d][0] + chunk_size[d] - 1;
        ((*chunk)->chunk_domains)[d][1] = end_coord <= array_domains[d][1] ? end_coord : array_domains[d][1];
    }

    (*chunk)->tile_extents = tile_extents;

    (*chunk)->chunk_coords = malloc(sizeof(uint64_t) * dim_len);
    memcpy((*chunk)->chunk_coords, chunk_coords, sizeof(uint64_t) * dim_len);

    /* Variable attribute generation */
    (*chunk)->tile_coords = malloc(sizeof(uint64_t) * dim_len);

    (*chunk)->tile_domains = malloc(sizeof(uint64_t *) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++)
    {
        ((*chunk)->tile_domains)[d] = malloc(sizeof(uint64_t) * 2);
    }

    (*chunk)->curpage = NULL; // Initially NULL.

    (*chunk)->dirty = 0; // Initialize the dirty flag

    /* Free */
    storage_util_free_attr_names(&attr_names, attr_len);

    /* On success */
    return CHUNK_OK;
}

/* Allocate chunk at the specified 'custom' position. */
int chunk_get_chunk_custom_pos(const char *array_name, uint64_t *starting_cell_coord, uint64_t *chunk_size, Chunk **chunk)
{
    uint64_t **array_domains;
    char **attr_names;
    uint64_t *tile_extents;
    uint32_t dim_len;
    uint32_t attr_len;
    uint64_t end_coord;

    /* Get neccesary informations of the given array */
    storage_util_get_dim_domains(array_name, &array_domains, &dim_len);
    storage_util_get_tile_extents(array_name, &tile_extents, &dim_len);
    storage_util_get_attr_names(array_name, &attr_names, &attr_len);

    /* Newly allocate chunk structure */
    *chunk = (Chunk *)malloc(sizeof(Chunk));

    /* Fixed attribute initialization */
    (*chunk)->array_name = malloc(sizeof(char) * MAX_ARRAY_NAME_LENGTH); // 255
    strcpy((*chunk)->array_name, array_name);

    (*chunk)->attr_name = malloc(sizeof(char) * MAX_ATTR_NAME_LENGTH); // 255
    strcpy((*chunk)->attr_name, attr_names[0]);                        // Assume single attribute

    storage_util_get_cell_order(array_name, &((*chunk)->cell_order));

    storage_util_get_array_type(array_name, &((*chunk)->array_type));

    (*chunk)->dim_len = dim_len;

    (*chunk)->array_domains = array_domains;

    (*chunk)->chunk_domains = malloc(sizeof(uint64_t *) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++) // Assign chunk_domains
    {
        ((*chunk)->chunk_domains)[d] = malloc(sizeof(uint64_t) * 2);
        ((*chunk)->chunk_domains)[d][0] = starting_cell_coord[d];
        end_coord = ((*chunk)->chunk_domains)[d][0] + chunk_size[d] - 1;
        ((*chunk)->chunk_domains)[d][1] = end_coord <= array_domains[d][1] ? end_coord : array_domains[d][1];
    }

    (*chunk)->tile_extents = tile_extents;

    (*chunk)->chunk_coords = NULL; // Unused paramter

    /* Variable attribute generation */
    (*chunk)->tile_coords = malloc(sizeof(uint64_t) * dim_len);

    (*chunk)->tile_domains = malloc(sizeof(uint64_t *) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++)
    {
        ((*chunk)->tile_domains)[d] = malloc(sizeof(uint64_t) * 2);
    }

    (*chunk)->curpage = NULL; // Initially NULL.

    (*chunk)->dirty = 0; // Initialize the dirty flag

    /* Free */
    storage_util_free_attr_names(&attr_names, attr_len);

    /* On success */
    return CHUNK_OK;
}

/* Allocate window chunk based on the given chunk iterator. */
int chunk_get_chunk_window(ChunkIterator *chunk_iter, int64_t *window_size, Chunk **chunk)
{
    uint32_t dim_len = chunk_iter->chunk->dim_len;
    uint64_t *center_coord_chunk = malloc(sizeof(uint64_t) * dim_len); // Chunk-level Coordinate of the window center
    int64_t *center_coord_array = malloc(sizeof(int64_t) * dim_len);   // Array-level Coordinate of the window center
    int64_t start_coord;
    uint64_t end_coord;

    /* Newly allocate chunk structure */
    *chunk = (Chunk *)malloc(sizeof(Chunk));

    /* Calculate center coordinate. (Assuming normal iterator, not custom iterator) */
    uint64_t curr = chunk_iter->chunk_idx;
    if (chunk_iter->chunk->cell_order == TILESTORE_ROW_MAJOR)
    {
        for (int d = dim_len - 1; d >= 0; d--)
        {
            center_coord_chunk[d] = (curr % chunk_iter->chunk_size[d]);
            curr /= chunk_iter->chunk_size[d];
        }
    }
    else
    {
        for (int d = 0; d < dim_len; d++)
        {
            center_coord_chunk[d] = (curr % chunk_iter->chunk_size[d]);
            curr /= chunk_iter->chunk_size[d];
        }
    }

    /* Fixed attribute initialization */
    (*chunk)->array_name = malloc(sizeof(char) * MAX_ARRAY_NAME_LENGTH); // 255
    strcpy((*chunk)->array_name, chunk_iter->chunk->array_name);

    (*chunk)->attr_name = malloc(sizeof(char) * MAX_ATTR_NAME_LENGTH); // 255
    strcpy((*chunk)->attr_name, chunk_iter->chunk->attr_name);         // Assume single attribute

    (*chunk)->cell_order = chunk_iter->chunk->cell_order;

    (*chunk)->dim_len = dim_len;

    (*chunk)->array_domains = malloc(sizeof(uint64_t *) * dim_len);

    (*chunk)->chunk_domains = malloc(sizeof(uint64_t *) * dim_len);

    for (uint32_t d = 0; d < dim_len; d++) // Assign chunk_domains
    {
        ((*chunk)->array_domains)[d] = malloc(sizeof(uint64_t) * 2);
        ((*chunk)->array_domains)[d][0] = chunk_iter->chunk->array_domains[d][0];
        ((*chunk)->array_domains)[d][1] = chunk_iter->chunk->array_domains[d][1];

        ((*chunk)->chunk_domains)[d] = malloc(sizeof(uint64_t) * 2);
        center_coord_array[d] = chunk_iter->chunk->chunk_domains[d][0] + center_coord_chunk[d];
        start_coord = center_coord_array[d] - (window_size[d] / 2);
        end_coord = start_coord + window_size[d] - 1;

        /* Adjust chunk_domains so that the chunk can contains valid region only */
        // if (start_coord >= (int64_t)((*chunk)->array_domains)[d][0])
        //     ((*chunk)->chunk_domains)[d][0] = (uint64_t)start_coord;
        // else
        //     ((*chunk)->chunk_domains)[d][0] = ((*chunk)->array_domains)[d][0];
        ((*chunk)->chunk_domains)[d][0] = start_coord >= (int64_t)((*chunk)->array_domains)[d][0] ? start_coord : ((*chunk)->array_domains)[d][0];
        ((*chunk)->chunk_domains)[d][1] = end_coord <= ((*chunk)->array_domains)[d][1] ? end_coord : ((*chunk)->array_domains)[d][1];
    }

    (*chunk)->tile_extents = malloc(sizeof(uint64_t) * dim_len);
    memcpy((*chunk)->tile_extents, chunk_iter->chunk->tile_extents, sizeof(uint64_t) * dim_len);

    (*chunk)->chunk_coords = NULL; // Unused paramter

    /* Variable attribute generation */
    (*chunk)->tile_coords = malloc(sizeof(uint64_t) * dim_len);

    (*chunk)->tile_domains = malloc(sizeof(uint64_t *) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++)
    {
        ((*chunk)->tile_domains)[d] = malloc(sizeof(uint64_t) * 2);
    }

    (*chunk)->curpage = NULL; // Initially NULL.

    (*chunk)->dirty = 0; // Initialize the dirty flag

    /* Free the resources */
    free(center_coord_chunk);
    free(center_coord_array);

    /* On success */
    return CHUNK_OK;
}

/* Allocate window for sparse chunk based on the sparse random order iterator */
int chunk_get_sparse_chunk_window(ChunkSparseRandomOrderIterator *chunk_iter, int64_t *window_size, Chunk **chunk)
{
    uint32_t dim_len = chunk_iter->chunk->dim_len;
    uint64_t *tile_extents = chunk_iter->chunk->tile_extents;
    uint64_t *center_coord_array; // Array-level Coordinate of the window center
    int64_t start_coord;
    uint64_t end_coord;

    /* Newly allocate chunk structure */
    *chunk = (Chunk *)malloc(sizeof(Chunk));

    center_coord_array = bf_util_get_coords_of_a_cell_of_idx_uint64(
        chunk_iter->chunk->curpage,
        dim_len,
        chunk_iter->idx_in_tile);

    /* Fixed attribute initialization */
    (*chunk)->array_name = malloc(sizeof(char) * MAX_ARRAY_NAME_LENGTH); // 255
    strcpy((*chunk)->array_name, chunk_iter->chunk->array_name);

    (*chunk)->attr_name = malloc(sizeof(char) * MAX_ATTR_NAME_LENGTH); // 255
    strcpy((*chunk)->attr_name, chunk_iter->chunk->attr_name);         // Assume single attribute

    (*chunk)->cell_order = chunk_iter->chunk->cell_order;

    (*chunk)->dim_len = dim_len;

    (*chunk)->array_domains = malloc(sizeof(uint64_t *) * dim_len);

    (*chunk)->chunk_domains = malloc(sizeof(uint64_t *) * dim_len);

    for (uint32_t d = 0; d < dim_len; d++) // Assign chunk_domains
    {
        ((*chunk)->array_domains)[d] = malloc(sizeof(uint64_t) * 2);
        ((*chunk)->array_domains)[d][0] = chunk_iter->chunk->array_domains[d][0];
        ((*chunk)->array_domains)[d][1] = chunk_iter->chunk->array_domains[d][1];

        ((*chunk)->chunk_domains)[d] = malloc(sizeof(uint64_t) * 2);
        start_coord = center_coord_array[d] - (window_size[d] / 2);
        end_coord = start_coord + window_size[d] - 1;

        ((*chunk)->chunk_domains)[d][0] = start_coord >= (int64_t)((*chunk)->array_domains)[d][0]
                                              ? start_coord
                                              : ((*chunk)->array_domains)[d][0];
        ((*chunk)->chunk_domains)[d][1] = end_coord <= ((*chunk)->array_domains)[d][1]
                                              ? end_coord
                                              : ((*chunk)->array_domains)[d][1];
    }

    (*chunk)->tile_extents = malloc(sizeof(uint64_t) * dim_len);
    memcpy((*chunk)->tile_extents, tile_extents, sizeof(uint64_t) * dim_len);

    (*chunk)->chunk_coords = NULL; // Unused paramter

    /* Variable attribute generation */
    (*chunk)->tile_coords = malloc(sizeof(uint64_t) * dim_len);

    (*chunk)->tile_domains = malloc(sizeof(uint64_t *) * dim_len);
    for (uint32_t d = 0; d < dim_len; d++)
    {
        ((*chunk)->tile_domains)[d] = malloc(sizeof(uint64_t) * 2);
    }

    (*chunk)->curpage = NULL; // Initially NULL.

    (*chunk)->dirty = 0; // Initialize the dirty flag

    /* Free the resources */
    free(center_coord_array);

    /* On success */
    return CHUNK_OK;
}

int chunk_get_cell_int(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    *((int *)cell_value) = bf_util_pagebuf_get_int(chunk->curpage, (uint64_t)cell_offset);

    /* On success */
    return CHUNK_OK;
}

int chunk_get_cell_float(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    *((float *)cell_value) = bf_util_pagebuf_get_float(chunk->curpage, (uint64_t)cell_offset);

    /* On success */
    return CHUNK_OK;
}

int chunk_get_cell_double(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    (*(double *)cell_value) = bf_util_pagebuf_get_double(chunk->curpage, (uint64_t)cell_offset);

    /* On success */
    return CHUNK_OK;
}

int chunk_write_cell_int(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Mark this chunk as dirty */
    chunk->dirty = 1;

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Write the cell value */
    bf_util_pagebuf_set_int(chunk->curpage, (uint64_t)cell_offset, *((int *)cell_value));

    /* On success */
    return CHUNK_OK;
}

int chunk_write_cell_float(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Mark this chunk as dirty */
    chunk->dirty = 1;

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Write the cell value */
    bf_util_pagebuf_set_float(chunk->curpage, (uint64_t)cell_offset, *((float *)cell_value));

    /* On success */
    return CHUNK_OK;
}

int chunk_write_cell_double(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Mark this chunk as dirty */
    chunk->dirty = 1;

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Write the cell value */
    bf_util_pagebuf_set_double(chunk->curpage, (uint64_t)cell_offset, *((double *)cell_value));

    /* On success */
    return CHUNK_OK;
}

int chunk_get_cell_validity(Chunk *chunk, uint64_t *cell_coords_chunk)
{
    assert(false);
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    // if (bf_util_pagebuf_get_validity(chunk->curpage, (uint64_t)cell_offset) == FALSE)
    //     return CHUNK_CELL_NULL_VALUE;
    // else
    //     return cell_offset;
}

/* Unpin the holding tile and free all resources allocated to the chunk */
void chunk_destroy(Chunk *chunk)
{
    array_key array_key;


    /* Unpin the tile */
    if (chunk->curpage != NULL)
    {
        array_key.arrayname = chunk->array_name;
        array_key.attrname = chunk->attr_name; // Assume single attribute
        array_key.dcoords = chunk->tile_coords;
        array_key.dim_len = chunk->dim_len;
        if (chunk->dirty == 1) // If dirty
            BF_TouchBuf(array_key);
        BF_UnpinBuf(array_key);
    }

    /* Free all resources allocated to the chunk */
    free(chunk->array_name);
    free(chunk->attr_name);
    storage_util_free_dim_domains(&chunk->array_domains, chunk->dim_len);

    for (int d = 0; d < chunk->dim_len; d++)
    {
        free(chunk->chunk_domains[d]);
        free(chunk->tile_domains[d]);
    }

    free(chunk->chunk_domains);
    free(chunk->chunk_coords);
    storage_util_free_tile_extents(&chunk->tile_extents, chunk->dim_len);
    free(chunk->tile_coords);
    free(chunk->tile_domains);
    free(chunk);
}

// sparse
uint64_t _chunk_sparse_get_idx_for_coordinates(Chunk *chunk, uint64_t *cell_coords_array, uint8_t return_mode)
{
    // the return mode argument

    uint64_t target[chunk->dim_len];
    int64_t low = 0;
    int64_t high = (int64_t)bf_util_pagebuf_get_unfilled_idx(chunk->curpage) - 1;
    int64_t mid;

    while (low <= high)
    {
        mid = (high + low) / 2;

        // calculate target
        for (uint32_t d = 0; d < chunk->dim_len; d++)
        {
            target[d] = (uint64_t)((int32_t *)bf_util_pagebuf_get_coords(chunk->curpage, d))[mid];
        }

        int comp_result = bf_util_compare_coords(target, cell_coords_array, chunk->dim_len, chunk->cell_order);
        if (comp_result == COMP_RHS_IS_BIGGER)
            low = mid + 1;
        else if (comp_result == COMP_LHS_IS_BIGGER)
            high = mid - 1;
        else if (comp_result == COMP_EQUAL)
            return (uint64_t)mid;
    }

    if (return_mode == RETURN_LUB_IF_NOT_FOUND)
    {
        return (uint64_t)low; // the low might be lub all the cases.
    }
    return NOT_FOUND;
}

int _chunk_sparse_get_cell_value_for_coordinates_int(Chunk *chunk, uint64_t *cell_coords_chunk, int *value)
{
    // calculate array-level coordinates
    // TODO: duplicated?
    uint64_t cell_coords_array[chunk->dim_len];
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }

    // binary search
    uint64_t idx = _chunk_sparse_get_idx_for_coordinates(chunk, cell_coords_array, RETURN_NOT_FOUND_IF_NOT_FOUND);
    if (idx == NOT_FOUND)
        return NOT_FOUND;

    // return
    *(value) = bf_util_pagebuf_get_int(chunk->curpage, idx);
    return CHUNK_OK;
}

int chunk_sparse_get_cell_int(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    //
    if (chunk->dirty)
    {
        fprintf(stderr, "chunk_sparse_get_cell_int(): CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE!!\n");
        return CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE;
    }

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    error_code = _chunk_sparse_get_cell_value_for_coordinates_int(chunk, cell_coords_chunk, (int *)cell_value);
    if (error_code == NOT_FOUND)
        return CHUNK_INVALID_CELL_COORDS;

    /* On success */
    return CHUNK_OK;
}

int chunk_sparse_write_cell_int(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    // please be aware that this function get coordinates *in chunk*,
    //      rather than *in array*.
    assert(false);      // FIXME:

    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    // calculate array-level coordinates
    // TODO: duplicated?
    int64_t cell_coords_array[chunk->dim_len];
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }

    /* Mark this chunk as dirty */
    chunk->dirty = 1;

    PFpage *page = chunk->curpage;
    uint64_t current_unfilled_pagebuf_offset = bf_util_pagebuf_get_unfilled_pagebuf_offset(page);
    uint64_t pagebuf_len = bf_util_pagebuf_get_len(page);

    // double pagesize if it is needed
    bool_t is_size_enough = pagebuf_len >= (current_unfilled_pagebuf_offset + sizeof(int));
    // if (!is_size_enough)
    // {
    //     bf_util_double_pagesize(page);
    // }

    // write (assume that length=1, no null)
    // write pagebuf
    uint64_t pagebuf_idx = current_unfilled_pagebuf_offset / sizeof(int);
    bf_util_pagebuf_set_int(page, pagebuf_idx, *((int *)cell_value));
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, (pagebuf_idx + 1) * sizeof(int));

    // write coords
    uint64_t current_unfilled_idx = bf_util_pagebuf_get_unfilled_idx(page);
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        int *coord = bf_util_pagebuf_get_coords(page, d);
        coord[current_unfilled_idx] = cell_coords_array[d];
    }
    bf_util_pagebuf_set_unfilled_idx(page, current_unfilled_idx + 1);

    /* On success */
    return CHUNK_OK;
}

int _chunk_sparse_get_cell_value_for_coordinates_float(Chunk *chunk, uint64_t *cell_coords_chunk, float *value)
{
    // calculate array-level coordinates
    // TODO: duplicated?
    uint64_t cell_coords_array[chunk->dim_len];
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }

    // binary search
    uint64_t idx = _chunk_sparse_get_idx_for_coordinates(chunk, cell_coords_array, RETURN_NOT_FOUND_IF_NOT_FOUND);
    if (idx == NOT_FOUND)
        return NOT_FOUND;

    // return
    *(value) = bf_util_pagebuf_get_float(chunk->curpage, idx);
    return CHUNK_OK;
}

int chunk_sparse_get_cell_float(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    //
    if (chunk->dirty)
    {
        fprintf(stderr, "chunk_sparse_get_cell_int(): CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE!!\n");
        return CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE;
    }

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    error_code = _chunk_sparse_get_cell_value_for_coordinates_float(chunk, cell_coords_chunk, (float *)cell_value);
    if (error_code == NOT_FOUND)
        return CHUNK_INVALID_CELL_COORDS;

    /* On success */
    return CHUNK_OK;
}

int chunk_sparse_write_cell_float(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    // please be aware that this function get coordinates *in chunk*,
    //      rather than *in array*.
    assert(false);          // FIXME:

    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    // calculate array-level coordinates
    // TODO: duplicated?
    int64_t cell_coords_array[chunk->dim_len];
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }

    /* Mark this chunk as dirty */
    chunk->dirty = 1;

    PFpage *page = chunk->curpage;
    uint64_t current_unfilled_pagebuf_offset = bf_util_pagebuf_get_unfilled_pagebuf_offset(page);
    uint64_t pagebuf_len = bf_util_pagebuf_get_len(page);

    // double pagesize if it is needed
    bool_t is_size_enough = pagebuf_len >= (current_unfilled_pagebuf_offset + sizeof(float));
    // if (!is_size_enough)
    // {
    //     bf_util_double_pagesize(page);
    // }

    // write (assume that length=1, no null)
    // write pagebuf
    uint64_t pagebuf_idx = current_unfilled_pagebuf_offset / sizeof(float);
    bf_util_pagebuf_set_float(page, pagebuf_idx, *((float *)cell_value));
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, (pagebuf_idx + 1) * sizeof(float));

    // write coords
    uint64_t current_unfilled_idx = bf_util_pagebuf_get_unfilled_idx(page);
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        int *coord = bf_util_pagebuf_get_coords(page, d);
        coord[current_unfilled_idx] = cell_coords_array[d];
    }
    bf_util_pagebuf_set_unfilled_idx(page, current_unfilled_idx + 1);

    /* On success */
    return CHUNK_OK;
}

int _chunk_sparse_get_cell_value_for_coordinates_double(Chunk *chunk, uint64_t *cell_coords_chunk, double *value)
{
    // calculate array-level coordinates
    // TODO: duplicated?
    uint64_t cell_coords_array[chunk->dim_len];
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }

    // binary search
    uint64_t idx = _chunk_sparse_get_idx_for_coordinates(chunk, cell_coords_array, RETURN_NOT_FOUND_IF_NOT_FOUND);
    if (idx == NOT_FOUND)
        return NOT_FOUND;

    // return
    *(value) = bf_util_pagebuf_get_double(chunk->curpage, idx);
    return CHUNK_OK;
}

int chunk_sparse_get_cell_double(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    //
    if (chunk->dirty)
    {
        fprintf(stderr, "chunk_sparse_get_cell_int(): CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE!!\n");
        return CHUNK_ATTEMP_TO_READ_DIRTY_CELL_IN_SPARSE_TILE;
    }

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    /* Get the cell value and return it */
    error_code = _chunk_sparse_get_cell_value_for_coordinates_double(chunk, cell_coords_chunk, (double *)cell_value);
    if (error_code == NOT_FOUND)
        return CHUNK_INVALID_CELL_COORDS;

    /* On success */
    return CHUNK_OK;
}

int chunk_sparse_write_cell_double(Chunk *chunk, uint64_t *cell_coords_chunk, void *cell_value)
{
    // please be aware that this function get coordinates *in chunk*,
    //      rather than *in array*.
    assert(false);          // FIXME:

    int cell_offset; // Offset of the cell in the tile
    int error_code;  // Code of the error produced while processing the request

    /* Process the request and get cell offset */
    cell_offset = _chunk_process(chunk, cell_coords_chunk);
    if (cell_offset < 0) // coordinate error
        return error_code = cell_offset;

    // calculate array-level coordinates
    // TODO: duplicated?
    int64_t cell_coords_array[chunk->dim_len];
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        cell_coords_array[d] = (chunk->chunk_domains)[d][0] + cell_coords_chunk[d];
    }

    /* Mark this chunk as dirty */
    chunk->dirty = 1;

    PFpage *page = chunk->curpage;
    uint64_t current_unfilled_pagebuf_offset = bf_util_pagebuf_get_unfilled_pagebuf_offset(page);
    uint64_t pagebuf_len = bf_util_pagebuf_get_len(page);

    // double pagesize if it is needed
    bool_t is_size_enough = pagebuf_len >= (current_unfilled_pagebuf_offset + sizeof(double));
    // if (!is_size_enough)
    // {
    //     bf_util_double_pagesize(page);
    // }

    // write (assume that length=1, no null)
    // write pagebuf
    uint64_t pagebuf_idx = current_unfilled_pagebuf_offset / sizeof(double);
    bf_util_pagebuf_set_double(page, pagebuf_idx, *((double *)cell_value));
    bf_util_pagebuf_set_unfilled_pagebuf_offset(page, (pagebuf_idx + 1) * sizeof(double));

    // write coords
    uint64_t current_unfilled_idx = bf_util_pagebuf_get_unfilled_idx(page);
    for (uint32_t d = 0; d < chunk->dim_len; d++)
    {
        int *coord = bf_util_pagebuf_get_coords(page, d);
        coord[current_unfilled_idx] = cell_coords_array[d];
    }
    bf_util_pagebuf_set_unfilled_idx(page, current_unfilled_idx + 1);

    /* On success */
    return CHUNK_OK;
}

// TODO: 
extern unsigned long long bf_getbuf_cnt_total;
extern unsigned long long bf_getbuf_cnt_hit;
extern unsigned long long bf_getbuf_io_total;
extern unsigned long long bf_getbuf_io_hit;

// TODO: Can't we merge this function with _replace_tile() ?
int chunk_getbuf(Chunk* chunk, emptytile_template_type_t empty_template) {
    array_key key;
    key.arrayname = chunk->array_name;
    key.attrname = chunk->attr_name;
    key.dcoords = chunk->chunk_coords;
    key.dim_len = chunk->dim_len;
    key.emptytile_template = empty_template;
    
    // explicit getting buffer
    if (chunk->curpage != NULL) {
        BF_UpdateNextTs(key);       // notify bf to update skip list

        fprintf(stderr, "[BufferTile] future - getbuf %s,%lu,%lu,%lu\n", 
            key.arrayname, key.dcoords[0], key.dcoords[1], BF_curr_ts);
        BF_curr_ts++;
        
        // We should increment these since the chunk abstraction prevents buffer hit. 
        // TODO: remove chunk abstraction

        // Statistics        
        uint64_t size = bf_util_pagebuf_get_len(chunk->curpage);
        if (chunk->curpage->type == SPARSE_FIXED) {
            uint64_t *lens = bf_util_pagebuf_get_coords_lens(chunk->curpage);
            size += (lens[0] + lens[1]);
        }

        bf_getbuf_cnt_total++;
        bf_getbuf_cnt_hit++;

        bf_getbuf_io_total += size;
        bf_getbuf_io_hit += size;

        return 1;
    }

    BF_GetBuf(key, &chunk->curpage);

    memcpy(chunk->tile_coords, chunk->chunk_coords, chunk->dim_len * sizeof(uint64_t));
    
    for (int d = 0; d < chunk->dim_len; d++)
    {
        chunk->tile_domains[d][0] = chunk->array_domains[d][0] + chunk->tile_coords[d] * chunk->tile_extents[d];
        chunk->tile_domains[d][1] = chunk->tile_domains[d][0] + chunk->tile_extents[d] - 1;
        if (chunk->tile_domains[d][1] > chunk->array_domains[d][1]) // Marginal tile handling
            chunk->tile_domains[d][1] = chunk->array_domains[d][1];
    }

    fprintf(stderr, "[BufferTile] future - getbuf %s,%lu,%lu,%lu\n", 
            key.arrayname, key.dcoords[0], key.dcoords[1], BF_curr_ts);
    BF_curr_ts++;

    return 0;
}

int chunk_unpinbuf(Chunk* chunk) {
    // explicit getting buffer
    if (chunk->curpage == NULL) return 1;

    array_key key;
    key.arrayname = chunk->array_name;
    key.attrname = chunk->attr_name;
    key.dcoords = chunk->chunk_coords;
    key.dim_len = chunk->dim_len;
    BF_UnpinBuf(key);
  
    return 0;
}
