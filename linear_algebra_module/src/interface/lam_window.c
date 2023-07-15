#include <stdio.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"

Chunk *lam_chunk_window(Chunk *opnd,
                        int opnd_attr_type,
                        int64_t *window_size,
                        AGGREGATE_FUNC_TYPE func,
                        Array *output_array,
                        uint64_t pos,
                        uint64_t *chunk_size)
{
    uint32_t dim_len = output_array->desc.dim_len;

    /* Compute number of chunks per each dimension. */
    uint64_t *num_chunk_per_dim = malloc(sizeof(uint64_t) * dim_len);
    for (int i = 0; i < dim_len; i++)
    {
        num_chunk_per_dim[i] = ((output_array->desc.dim_domains[i][1] - output_array->desc.dim_domains[i][0] + chunk_size[i]) / chunk_size[i]);
    }

    /* Compute result chunk coordinate. */
    uint64_t *result_chunk_coords;
    result_chunk_coords = malloc(sizeof(uint64_t) * dim_len);

    Chunk *result;
    calculate_chunk_coord(dim_len, pos, num_chunk_per_dim, result_chunk_coords);
    chunk_get_chunk(output_array->desc.object_name, chunk_size, result_chunk_coords, &result);

    /* Actual window operations for each aggr_func */
    switch (func)
    {
    case AGGREGATE_COUNT:
        window_dense_count(opnd, window_size, result);
        break;
    case AGGREGATE_SUM:
        window_dense_sum(opnd, window_size, result, opnd_attr_type);
        break;
    case AGGREGATE_AVG:
        window_dense_avg(opnd, window_size, result, opnd_attr_type);
        break;
    case AGGREGATE_MAX:
        window_dense_max(opnd, window_size, result, opnd_attr_type);
        break;
    case AGGREGATE_MIN:
        window_dense_min(opnd, window_size, result, opnd_attr_type);
        break;
    case AGGREGATE_VAR:
        window_dense_var(opnd, window_size, result, opnd_attr_type);
        break;
    case AGGREGATE_STDEV:
        window_dense_stdev(opnd, window_size, result, opnd_attr_type);
        break;
    default:
        break;
    }

    /* Free the resources */
    free(num_chunk_per_dim);
    free(result_chunk_coords);

    /* On success */
    return result;
}
