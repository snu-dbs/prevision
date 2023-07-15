#include <stdio.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>

#include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "bf_struct.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"

/* Sparse count aggregate returns the number of non-empty cells
along the specified axis. (i.e. Empty cells are not counted)*/
void group_by_dim_sparse_count_aggregate_opt(Chunk *opnd,
                                             Chunk *result,
                                             void **state,
                                             int opnd_attr_type,
                                             uint8_t *dim_selection_map,
                                             Descriptor array_desc,
                                             uint64_t *chunk_size,
                                             uint64_t iteration)
{
    /* Prepare variables needed for the aggregation */
    uint64_t num_cell_in_opnd_chunk = 1;
    uint64_t num_cell_in_result_chunk = 1;
    for (uint32_t d = 0; d < opnd->dim_len; d++)
    {
        num_cell_in_opnd_chunk *= opnd->tile_extents[d]; // Optimizability checked
        if (dim_selection_map[d] == 1)
        {
            num_cell_in_result_chunk *= opnd->tile_extents[d];
        }
    }

    /* Allocate state if this is the first call */
    if (iteration == 0)
    {
        state[0] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Count

        /* Initialize the hash function if this is the first call */
        if (state[3] = NULL)
        {
            state[3] = (uint64_t *)calloc(opnd->dim_len * 2, sizeof(uint64_t));

            // Initialize Multiplier
            int prev = -1;
            int res_idx = result->dim_len - 1;

            for (uint32_t d = opnd->dim_len - 1; d >= 0; d--)
            {
                if (dim_selection_map[d] == 1)
                {
                    if (prev < 0)
                    {
                        ((uint64_t *)state[3])[d] = 1;
                    }
                    else
                    {
                        ((uint64_t *)state[3])[d] =
                            ((uint64_t *)state[3])[prev] * result->tile_extents[res_idx];
                        res_idx--;
                    }
                    prev = d;
                }
            }
        }

        // Initialize Coordinate offset
        uint32_t selected = 0;
        for (uint32_t d = 0; d < opnd->dim_len; d++)
        {
            if (dim_selection_map[d] == 1)
            {
                ((uint64_t *)state[3])[opnd->dim_len + d] = result->chunk_domains[selected][0];
                selected++;
            }
        }
    }
}