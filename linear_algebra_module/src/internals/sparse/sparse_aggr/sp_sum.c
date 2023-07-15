#include <stdio.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>

#include "bf.h"
#include "utils.h"
#include "bf_struct.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"

void reduce_as_scala_sparse_sum_aggregate_opt(
        PFpage *opnd,
        void **state,
        int opnd_attr_type,
        uint64_t iteration) {
    /* Allocate state if this is the first call */
    if (iteration == 0)
    {
        // Container for Square Sum. Currently supports only result type of 'double'.
        state[0] = (double *)calloc(1, sizeof(double));
    }

    /* Iterate each cell and aggregate it at the right position */
    if (opnd_attr_type == TILESTORE_FLOAT64)
    {
        uint64_t nnz = opnd->unfilled_pagebuf_offset / sizeof(double);
        double *opnd_buf = (double *)bf_util_get_pagebuf(opnd);

        for (uint64_t i = 0; i < nnz; i++)
        {
            ((double *)state[0])[0] += opnd_buf[i];
        }
        fprintf(stderr, "work\n");
    } else {
        fprintf(stderr, "Not implemented yet!\n");
        assert(0);
    }
}