#include <stdio.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>
#include <limits.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "bf_struct.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"

void group_by_dim_dense_stdev_aggregate_opt(Chunk *opnd,
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
        if (opnd_attr_type == TILESTORE_INT32)
        {
            state[0] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Sum
            state[1] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Square sum
        }
        else if (opnd_attr_type == TILESTORE_FLOAT32)
        {
            state[0] = (float *)calloc(num_cell_in_result_chunk, sizeof(float)); // Sum
            state[1] = (float *)calloc(num_cell_in_result_chunk, sizeof(float)); // Square sum
        }
        else if (opnd_attr_type == TILESTORE_FLOAT64)
        {
            state[0] = (double *)calloc(num_cell_in_result_chunk, sizeof(double)); // Sum
            state[1] = (double *)calloc(num_cell_in_result_chunk, sizeof(double)); // Square sum
        }

        state[2] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Count

        // Get result page from BF if this is the first call
        array_key res_key;
        if (result->curpage == NULL) {
            res_key.arrayname = result->array_name;
            res_key.attrname = result->attr_name;
            res_key.dcoords = result->chunk_coords;
            res_key.dim_len = result->dim_len;
            BF_GetBuf(res_key, &result->curpage);
        }

        memcpy(result->tile_coords, result->chunk_coords, sizeof(uint64_t) * result->dim_len);


        /* Initialize the mapping list if this is the first call */
        if (state[3] == NULL)
        {
            state[3] = (uint64_t *)calloc(num_cell_in_opnd_chunk, sizeof(uint64_t)); // Mapping List

            uint32_t selected = result->dim_len - 1;
            uint64_t *divisors = malloc(sizeof(uint64_t) * opnd->dim_len);
            uint64_t *res_divisors = malloc(sizeof(uint64_t) * result->dim_len);
            uint64_t *res_cell_coord = malloc(sizeof(uint64_t) * result->dim_len);

            // Pre-calculation of the divisors
            for (int d = opnd->dim_len - 1; d >= 0; d--)
            {
                if (dim_selection_map[d] == 1)
                    res_divisors[selected--] =
                        selected == result->dim_len - 1
                            ? 1
                            : result->tile_extents[selected + 1] * res_divisors[selected + 1];
                divisors[d] =
                    d == opnd->dim_len - 1
                        ? 1
                        : opnd->tile_extents[d + 1] * divisors[d + 1];
            }

            for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
            {
                // Cell coordinate calculation
                uint64_t cell_idx = i;
                selected = 0;
                for (uint32_t d = 0; d < opnd->dim_len; d++)
                {
                    if (dim_selection_map[d] == 1)
                        res_cell_coord[selected++] = cell_idx / divisors[d];
                    cell_idx %= divisors[d];
                }

                // Result index calculation
                for (uint32_t d = 0; d < result->dim_len; d++)
                {
                    ((uint64_t *)state[3])[i] += res_cell_coord[d] * res_divisors[d];
                }
            }

            // Free
            free(divisors);
            free(res_divisors);
            free(res_cell_coord);
        }
    }

    /* Here, we handle the buffer-page directly.
     * GetBuf() the required tile and prepare for the aggregation */
    array_key opnd_key;
    if (opnd->curpage == NULL) {
        opnd_key.arrayname = opnd->array_name;
        opnd_key.attrname = opnd->attr_name;
        opnd_key.dcoords = opnd->chunk_coords;
        opnd_key.dim_len = opnd->dim_len;
        BF_GetBuf(opnd_key, &opnd->curpage);
    }

    memcpy(opnd->tile_coords, opnd->chunk_coords, sizeof(uint64_t) * opnd->dim_len);

    /* Iterate each cell and aggregate it at the right position */
    if (opnd_attr_type == TILESTORE_INT32)
    {
        int *opnd_buf = (int *)bf_util_get_pagebuf(opnd->curpage);
        for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
        {
            ((int *)state[0])[((uint64_t *)state[3])[i]] += opnd_buf[i];
            ((int *)state[1])[((uint64_t *)state[3])[i]] += (int)pow(opnd_buf[i], 2);
            ((int *)state[2])[((uint64_t *)state[3])[i]]++;
        }
    }
    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        float *opnd_buf = (float *)bf_util_get_pagebuf(opnd->curpage);
        for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
        {
            ((float *)state[0])[((uint64_t *)state[3])[i]] += opnd_buf[i];
            ((float *)state[1])[((uint64_t *)state[3])[i]] += (float)pow(opnd_buf[i], 2);
            ((int *)state[2])[((uint64_t *)state[3])[i]]++;
        }
    }
    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        double *opnd_buf = (double *)bf_util_get_pagebuf(opnd->curpage);
        for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
        {
            ((double *)state[0])[((uint64_t *)state[3])[i]] += opnd_buf[i];
            ((double *)state[1])[((uint64_t *)state[3])[i]] += pow(opnd_buf[i], 2);
            ((int *)state[2])[((uint64_t *)state[3])[i]]++;
        }
    }
}

void group_by_dim_dense_stdev_write_opt(Chunk *result,
                                        void **state,
                                        int opnd_attr_type)
{
    /* Prepare variables needed for the aggregation */
    uint64_t num_cell_in_result_chunk = 1;
    for (uint32_t d = 0; d < result->dim_len; d++)
    {
        num_cell_in_result_chunk *= (result->chunk_domains[d][1] - result->chunk_domains[d][0] + 1);
    }

    double *res_buf = (double *)bf_util_get_pagebuf(result->curpage);

    if (opnd_attr_type == TILESTORE_INT32)
    {
        /* Lower the aggregation and write it to the result chunk */
        for (uint64_t i = 0; i < num_cell_in_result_chunk; i++)
        {
            double mean_square = pow(((int *)state[0])[i], 2) / (double)((int *)state[2])[i]; // sum^2 / cnt
            double var = (((int *)state[1])[i] - mean_square) / (((int *)state[2])[i] - 1);   // (sq_sum - mean_sq) / (cnt - 1)
            res_buf[i] = var <= 0 ? 0 : sqrt(var);
        }
    }
    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        /* Lower the aggregation and write it to the result chunk */
        for (uint64_t i = 0; i < num_cell_in_result_chunk; i++)
        {
            double mean_square = pow(((float *)state[0])[i], 2) / (double)((int *)state[2])[i]; // sum^2 / cnt
            double var = (((float *)state[1])[i] - mean_square) / (((int *)state[2])[i] - 1);   // (sq_sum - mean_sq) / (cnt - 1)
            res_buf[i] = var <= 0 ? 0 : sqrt(var);
        }
    }
    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        /* Lower the aggregation and write it to the result chunk */
        for (uint64_t i = 0; i < num_cell_in_result_chunk; i++)
        {
            double mean_square = pow(((double *)state[0])[i], 2) / (double)((int *)state[2])[i]; // sum^2 / cnt
            double var = (((double *)state[1])[i] - mean_square) / (((int *)state[2])[i] - 1);   // (sq_sum - mean_sq) / (cnt - 1)
            res_buf[i] = var <= 0 ? 0 : sqrt(var);
        }
    }

    array_key res_key;
    res_key.arrayname = result->array_name;
    res_key.attrname = result->attr_name;
    res_key.dcoords = result->chunk_coords;
    res_key.dim_len = result->dim_len;
    BF_TouchBuf(res_key);

    /* Free the resources */
    free(state[0]);
    free(state[1]);
    free(state[2]);
}

void group_by_dim_dense_stdev_aggregate_iter(Chunk *opnd,
                                             Chunk *result,
                                             void **state,
                                             int opnd_attr_type,
                                             uint8_t *dim_selection_map,
                                             Descriptor array_desc,
                                             uint64_t *chunk_size,
                                             uint64_t iteration)
{
    /* Prepare variables needed for the aggregation */
    uint64_t num_cell_in_result_chunk = 1;
    uint64_t num_cell_in_opnd_partition = 1;
    uint32_t *opnd_dim_order = malloc(sizeof(uint32_t) * opnd->dim_len);
    uint32_t selected_start = 0;
    uint32_t unselected_start = array_desc.dim_len;

    for (uint32_t d = 0; d < opnd->dim_len; d++)
    {
        if (dim_selection_map[d] == 0) // Not selected
        {
            num_cell_in_opnd_partition *= (opnd->chunk_domains[d][1] - opnd->chunk_domains[d][0] + 1);
            opnd_dim_order[d] = unselected_start++;
        }
        else // Selected
        {
            num_cell_in_result_chunk *= (opnd->chunk_domains[d][1] - opnd->chunk_domains[d][0] + 1);
            opnd_dim_order[d] = selected_start++;
        }
    }

    /* Allocate state if this is the first call */
    if (iteration == 0)
    {
        if (opnd_attr_type == TILESTORE_INT32)
        {
            state[0] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Sum
            state[1] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Square sum
        }
        else if (opnd_attr_type == TILESTORE_FLOAT32)
        {
            state[0] = (float *)calloc(num_cell_in_result_chunk, sizeof(float)); // Sum
            state[1] = (float *)calloc(num_cell_in_result_chunk, sizeof(float)); // Square sum
        }
        else if (opnd_attr_type == TILESTORE_FLOAT64)
        {
            state[0] = (double *)calloc(num_cell_in_result_chunk, sizeof(double)); // Sum
            state[1] = (double *)calloc(num_cell_in_result_chunk, sizeof(double)); // Square sum
        }

        state[2] = (int *)calloc(num_cell_in_result_chunk, sizeof(int)); // Count
    }

    /* Initialize the chunk iterator for the opnd chunk */
    ChunkIterator *opnd_iter = chunk_custom_order_iterator_init(opnd, opnd_dim_order);

    /* Do the aggregation */
    // Int
    if (opnd_attr_type == TILESTORE_INT32)
    {
        // Loop seperation to avoid the call of 'chunk_iterator_has_next()' which is a branch function.
        int opnd_cell_value;
        int partition_count, partition_sum, partition_square_sum;
        for (uint64_t i = 0; i < num_cell_in_result_chunk - 1; i++)
        {
            partition_count = 0;
            partition_sum = 0;
            partition_square_sum = 0;
            for (uint64_t j = 0; j < num_cell_in_opnd_partition; j++)
            {
                opnd_cell_value = chunk_iterator_get_cell_int(opnd_iter);
                partition_count++;
                partition_sum += opnd_cell_value;
                partition_square_sum += (int)pow(opnd_cell_value, 2);
                chunk_iterator_get_next(opnd_iter);
            }
            ((int *)state[0])[i] += partition_sum;
            ((int *)state[1])[i] += partition_square_sum;
            ((int *)state[2])[i] += partition_count;
        }

        partition_count = 0;
        partition_sum = 0;
        partition_square_sum = 0;
        for (uint64_t j = 0; j < num_cell_in_opnd_partition - 1; j++)
        {
            opnd_cell_value = chunk_iterator_get_cell_int(opnd_iter);
            partition_count++;
            partition_sum += opnd_cell_value;
            partition_square_sum += (int)pow(opnd_cell_value, 2);
            chunk_iterator_get_next(opnd_iter);
        }
        // The last cell. Should not call 'chunk_iterator_get_next()'
        opnd_cell_value = chunk_iterator_get_cell_int(opnd_iter);
        partition_count++;
        partition_sum += opnd_cell_value;
        partition_square_sum += (int)pow(opnd_cell_value, 2);
        ((int *)state[0])[num_cell_in_result_chunk - 1] += partition_sum;
        ((int *)state[1])[num_cell_in_result_chunk - 1] += partition_square_sum;
        ((int *)state[2])[num_cell_in_result_chunk - 1] += partition_count;
    }

    // Float
    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        // Loop seperation to avoid the call of 'chunk_iterator_has_next()' which is a branch function.
        float opnd_cell_value;
        int partition_count;
        float partition_sum, partition_square_sum;
        for (uint64_t i = 0; i < num_cell_in_result_chunk - 1; i++)
        {
            partition_count = 0;
            partition_sum = 0;
            partition_square_sum = 0;
            for (uint64_t j = 0; j < num_cell_in_opnd_partition; j++)
            {
                opnd_cell_value = chunk_iterator_get_cell_float(opnd_iter);
                partition_count++;
                partition_sum += opnd_cell_value;
                partition_square_sum += (float)pow(opnd_cell_value, 2);
                chunk_iterator_get_next(opnd_iter);
            }
            ((float *)state[0])[i] += partition_sum;
            ((float *)state[1])[i] += partition_square_sum;
            ((int *)state[2])[i] += partition_count;
        }

        partition_count = 0;
        partition_sum = 0;
        partition_square_sum = 0;
        for (uint64_t j = 0; j < num_cell_in_opnd_partition - 1; j++)
        {
            opnd_cell_value = chunk_iterator_get_cell_float(opnd_iter);
            partition_count++;
            partition_sum += opnd_cell_value;
            partition_square_sum += (float)pow(opnd_cell_value, 2);
            chunk_iterator_get_next(opnd_iter);
        }
        // The last cell. Should not call 'chunk_iterator_get_next()'
        opnd_cell_value = chunk_iterator_get_cell_float(opnd_iter);
        partition_count++;
        partition_sum += opnd_cell_value;
        partition_square_sum += (float)pow(opnd_cell_value, 2);
        ((float *)state[0])[num_cell_in_result_chunk - 1] += partition_sum;
        ((float *)state[1])[num_cell_in_result_chunk - 1] += partition_square_sum;
        ((int *)state[2])[num_cell_in_result_chunk - 1] += partition_count;
    }

    // Double
    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        // Loop seperation to avoid the call of 'chunk_iterator_has_next()' which is a branch function.
        double opnd_cell_value;
        int partition_count;
        double partition_sum, partition_square_sum;
        for (uint64_t i = 0; i < num_cell_in_result_chunk - 1; i++)
        {
            partition_count = 0;
            partition_sum = 0;
            partition_square_sum = 0;
            for (uint64_t j = 0; j < num_cell_in_opnd_partition; j++)
            {
                opnd_cell_value = chunk_iterator_get_cell_double(opnd_iter);
                partition_count++;
                partition_sum += opnd_cell_value;
                partition_square_sum += pow(opnd_cell_value, 2);
                chunk_iterator_get_next(opnd_iter);
            }
            ((double *)state[0])[i] += partition_sum;
            ((double *)state[1])[i] += partition_square_sum;
            ((int *)state[2])[i] += partition_count;
        }

        partition_count = 0;
        partition_sum = 0;
        partition_square_sum = 0;
        for (uint64_t j = 0; j < num_cell_in_opnd_partition - 1; j++)
        {
            opnd_cell_value = chunk_iterator_get_cell_double(opnd_iter);
            partition_count++;
            partition_sum += opnd_cell_value;
            partition_square_sum += pow(opnd_cell_value, 2);
            chunk_iterator_get_next(opnd_iter);
        }
        // The last cell. Should not call 'chunk_iterator_get_next()'
        opnd_cell_value = chunk_iterator_get_cell_double(opnd_iter);
        partition_count++;
        partition_sum += opnd_cell_value;
        partition_square_sum += pow(opnd_cell_value, 2);
        ((double *)state[0])[num_cell_in_result_chunk - 1] += partition_sum;
        ((double *)state[1])[num_cell_in_result_chunk - 1] += partition_square_sum;
        ((int *)state[2])[num_cell_in_result_chunk - 1] += partition_count;
    }
    /* Free the resources */
    free(opnd_dim_order);
    chunk_iterator_free(opnd_iter);
}

void group_by_dim_dense_stdev_write_iter(Chunk *result,
                                         void **state,
                                         int opnd_attr_type)
{
    /* Prepare variables needed for the aggregation */
    uint64_t num_cell_in_result_chunk = 1;
    for (uint32_t d = 0; d < result->dim_len; d++)
    {
        num_cell_in_result_chunk *= (result->chunk_domains[d][1] - result->chunk_domains[d][0] + 1);
    }

    /* Initialize chunk iterator for the result chunk */
    ChunkIterator *res_iter = chunk_iterator_init(result);

    /* Lower the aggregation and write it to the result chunk */
    // Int
    if (opnd_attr_type == TILESTORE_INT32)
    {
        // Loop seperation to avoid the call of 'chunk_iterator_has_next()' which is a branch function.
        for (uint64_t i = 0; i < num_cell_in_result_chunk - 1; i++)
        {
            double mean_square = pow(((int *)state[0])[i], 2) / (double)((int *)state[2])[i]; // sum^2 / cnt
            double var = (((int *)state[1])[i] - mean_square) / (((int *)state[2])[i] - 1);   // (sq_sum - mean_sq) / (cnt - 1)
            chunk_iterator_write_cell_double(res_iter, (var <= 0 ? 0 : sqrt(var)));
            chunk_iterator_get_next(res_iter);
        }
        // The last cell. Should not call 'chunk_iterator_get_next()'
        double mean_square = pow(((int *)state[0])[num_cell_in_result_chunk - 1], 2) /
                             (double)((int *)state[2])[num_cell_in_result_chunk - 1]; // sum^2 / cnt
        double var = (((int *)state[1])[num_cell_in_result_chunk - 1] - mean_square) /
                     (((int *)state[2])[num_cell_in_result_chunk - 1] - 1); // (sq_sum - mean_sq) / (cnt - 1)
        chunk_iterator_write_cell_double(res_iter, (var <= 0 ? 0 : sqrt(var)));
    }

    // Float
    if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        // Loop seperation to avoid the call of 'chunk_iterator_has_next()' which is a branch function.
        for (uint64_t i = 0; i < num_cell_in_result_chunk - 1; i++)
        {
            double mean_square = pow(((float *)state[0])[i], 2) / (double)((int *)state[2])[i]; // sum^2 / cnt
            double var = (((float *)state[1])[i] - mean_square) / (((int *)state[2])[i] - 1);   // (sq_sum - mean_sq) / (cnt - 1)
            chunk_iterator_write_cell_double(res_iter, (var <= 0 ? 0 : sqrt(var)));
            chunk_iterator_get_next(res_iter);
        }
        // The last cell. Should not call 'chunk_iterator_get_next()'
        double mean_square = pow(((float *)state[0])[num_cell_in_result_chunk - 1], 2) /
                             (double)((int *)state[2])[num_cell_in_result_chunk - 1]; // sum^2 / cnt
        double var = (((float *)state[1])[num_cell_in_result_chunk - 1] - mean_square) /
                     (((int *)state[2])[num_cell_in_result_chunk - 1] - 1); // (sq_sum - mean_sq) / (cnt - 1)
        chunk_iterator_write_cell_double(res_iter, (var <= 0 ? 0 : sqrt(var)));
    }

    if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        // Loop seperation to avoid the call of 'chunk_iterator_has_next()' which is a branch function.
        for (uint64_t i = 0; i < num_cell_in_result_chunk - 1; i++)
        {
            double mean_square = pow(((double *)state[0])[i], 2) / (double)((int *)state[2])[i]; // sum^2 / cnt
            double var = (((double *)state[1])[i] - mean_square) / (((int *)state[2])[i] - 1);   // (sq_sum - mean_sq) / (cnt - 1)
            chunk_iterator_write_cell_double(res_iter, (var <= 0 ? 0 : sqrt(var)));
            chunk_iterator_get_next(res_iter);
        }
        // The last cell. Should not call 'chunk_iterator_get_next()'
        double mean_square = pow(((double *)state[0])[num_cell_in_result_chunk - 1], 2) /
                             (double)((int *)state[2])[num_cell_in_result_chunk - 1]; // sum^2 / cnt
        double var = (((double *)state[1])[num_cell_in_result_chunk - 1] - mean_square) /
                     (((int *)state[2])[num_cell_in_result_chunk - 1] - 1); // (sq_sum - mean_sq) / (cnt - 1)
        chunk_iterator_write_cell_double(res_iter, (var <= 0 ? 0 : sqrt(var)));
    }

    /* Free the resources */
    chunk_iterator_free(res_iter);
    free(state[0]);
    free(state[1]);
    free(state[2]);
}