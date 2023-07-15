#include <stdio.h>
#include <string.h>
#include <math.h>
#include <assert.h>

#include "bf.h"
#include "utils.h"
#include "chunk_struct.h"
#include "chunk_interface.h"
#include "lam_interface.h"
#include "lam_internals.h"
#include "array_struct.h"

#include <sys/time.h>

unsigned long long __lam_svd_cnt = 0;
unsigned long long __lam_svd_time = 0;

#define __LAM_STAT_START() \
    struct timeval start;  \
    gettimeofday(&start, NULL);

#define __LAM_STAT_END()                                                                               \
    struct timeval end;                                                                                \
    gettimeofday(&end, NULL);                                                                          \
    unsigned long long diff = ((end.tv_sec - start.tv_sec) * 1000000) + (end.tv_usec - start.tv_usec); \
    __lam_svd_time += diff;                                                                            \
    __lam_svd_cnt++;

void lam_qr_opnd(Chunk *opnd,
                 Chunk *q_chunk,
                 int opnd_attr_type,
                 uint64_t opnd_pos,
                 double *stacked_r_factors)
{
    uint64_t r_factor_pos = opnd->tile_extents[1] * opnd->tile_extents[1] * opnd_pos;

    chunk_getbuf(opnd, BF_EMPTYTILE_NONE);

    /* Branch according to the array type */
    if (opnd->curpage->type == DENSE_FIXED)
    {
        chunk_getbuf(q_chunk, BF_EMPTYTILE_DENSE);

        q_chunk->dirty = 1;
    } else if (opnd->curpage->type == SPARSE_FIXED) {
        // TODO: Not Implemented Yet.
        assert(false);
    }
}