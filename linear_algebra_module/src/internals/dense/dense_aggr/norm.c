#include <stdio.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>

// #include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "bf_struct.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"

/***************************************************
 *  APIs for Scala Aggregation
 ***************************************************/
void reduce_as_scala_dense_norm_aggregate_opt(PFpage *opnd,
                                              void **state,
                                              int opnd_attr_type,
                                              uint64_t iteration)
{
    /* Allocate state if this is the first call */
    if (iteration == 0)
    {
        // Container for Square Sum. Currently supports only result type of 'double'.
        state[0] = (double *)calloc(1, sizeof(double));
    }

    /* Iterate each cell and aggregate it at the right position */
    if (opnd_attr_type == TILESTORE_INT32)
    {
        uint64_t num_cell_in_opnd_chunk = opnd->pagebuf_len / sizeof(int32_t);
        int *opnd_buf = (int *)bf_util_get_pagebuf(opnd);

        for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
        {
            ((double *)state[0])[0] += pow(opnd_buf[i], 2);
        }
    }
    else if (opnd_attr_type == TILESTORE_FLOAT32)
    {
        uint64_t num_cell_in_opnd_chunk = opnd->pagebuf_len / sizeof(float);
        float *opnd_buf = (float *)bf_util_get_pagebuf(opnd);

        for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
        {
            ((double *)state[0])[0] += pow(opnd_buf[i], 2);
        }
    }
    else if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        uint64_t num_cell_in_opnd_chunk = opnd->pagebuf_len / sizeof(double);
        double *opnd_buf = (double *)bf_util_get_pagebuf(opnd);

        for (uint64_t i = 0; i < num_cell_in_opnd_chunk; i++)
        {
            ((double *)state[0])[0] += pow(opnd_buf[i], 2);
        }
    }
}

double reduce_as_scala_norm_finalize(void **state)
{
    double square_sum = ((double *)state[0])[0];
    double result = square_sum <= 0 ? 0 : sqrt(square_sum);

    /* Free the resources */
    free(state[0]);

    /* Return result */
    return result;
}