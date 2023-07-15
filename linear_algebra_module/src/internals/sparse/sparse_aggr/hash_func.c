#include <tiledb/tiledb.h>

#include "bf.h"
#include "utils.h"
#include "bf_struct.h"
#include "chunk_struct.h"
#include "chunk_interface.h"

/* Mapping operand coordinate -> result index */
inline uint64_t _sparse_coord_hash(uint64_t *coord,
                                   uint64_t *state,
                                   uint32_t dim_len)
{
    uint64_t idx = (uint64_t)0;

    for (uint32_t d = 0; d < dim_len; d++)
    {
        idx += (coord[d] - state[dim_len + d]) * state[d];
    }

    return idx;
}