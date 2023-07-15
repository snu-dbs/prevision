#include <stdio.h>
#include <string.h>
#include <math.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"

/*
 * Utilities for lam operations.
 * Consists of :
 *  - Coordinate calculation (chunk, cell)
 *  - Optimizability check
 */

void calculate_chunk_coord(uint32_t dim_len, int chunk_number, uint64_t *array_size, uint64_t *coord)
{
    int k;

    for (k = 0; k < dim_len; k++)
    {
        uint32_t n = dim_len - 1, divisor = 1, loop = dim_len - 1 - k;
        while (k < dim_len - 1 && loop > 0)
        {
            divisor = divisor * array_size[n];
            n--;
            loop--;
        }
        if (k == dim_len - 1)
        {
            coord[k] = chunk_number;
            break;
        }
        coord[k] = chunk_number / divisor;
        chunk_number = chunk_number % divisor;
    }
}

void calculate_cell_coord(uint32_t dim_len, int cell_number, uint64_t *chunk_size, uint64_t *coord)
{
    int k;
    /* Calculate coordinate of current cell. */
    for (k = 0; k < dim_len; k++)
    {
        uint32_t n = dim_len - 1, divisor = 1, loop = dim_len - 1 - k;
        while (k < dim_len - 1 && loop > 0)
        {
            divisor = divisor * chunk_size[n];
            n--;
            loop--;
        }
        if (k == dim_len - 1)
        {
            coord[k] = cell_number;
            break;
        }
        coord[k] = cell_number / divisor;
        cell_number = cell_number % divisor;
    }
}

bool check_tile_equality_unary(Chunk *A, Chunk *B)
{
    uint32_t ndim = A->dim_len;
    if (A->dim_len != B->dim_len)
        return false;

    for (int d = 0; d < ndim; d++)
    {
        if (A->tile_extents[d] != B->tile_extents[d])
            return false;
        if (A->chunk_domains[d][1] - A->chunk_domains[d][0] + 1 != A->tile_extents[d])
            return false;
        if (B->chunk_domains[d][1] - B->chunk_domains[d][0] + 1 != B->tile_extents[d])
            return false;
    }

    return true;
}

bool check_tile_equality_binary(Chunk *A, Chunk *B, Chunk *C)
{
    uint32_t ndim = A->dim_len;
    if (A->dim_len != B->dim_len)
        return false;
    if (B->dim_len != C->dim_len)
        return false;
    for (int d = 0; d < ndim; d++)
    {
        if (A->tile_extents[d] != B->tile_extents[d])
            return false;
        if (B->tile_extents[d] != C->tile_extents[d])
            return false;
        if (A->chunk_domains[d][1] - A->chunk_domains[d][0] + 1 != A->tile_extents[d])
            return false;
        if (B->chunk_domains[d][1] - B->chunk_domains[d][0] + 1 != B->tile_extents[d])
            return false;
        if (C->chunk_domains[d][1] - C->chunk_domains[d][0] + 1 != C->tile_extents[d])
            return false;
    }

    return true;
}